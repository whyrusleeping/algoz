package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	api "github.com/bluesky-social/indigo/api"
	bsky "github.com/bluesky-social/indigo/api/bsky"
	cliutil "github.com/bluesky-social/indigo/cmd/gosky/util"
	"github.com/bluesky-social/indigo/util"
	"github.com/bluesky-social/indigo/xrpc"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/whyrusleeping/go-did"
	"golang.org/x/crypto/acme/autocert"

	cli "github.com/urfave/cli/v2"

	gorm "gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var log = logging.Logger("algoz")

type PostRef struct {
	ID        uint      `gorm:"primarykey"`
	CreatedAt time.Time `gorm:"index"`

	Cid        string
	Rkey       string `gorm:"uniqueIndex:idx_post_rkeyuid"`
	Uid        uint   `gorm:"uniqueIndex:idx_post_rkeyuid"`
	NotFound   bool
	Likes      int
	Reposts    int
	Replies    int
	ThreadSize int
}

type Feed struct {
	gorm.Model
	Name        string `gorm:"unique"`
	Description string
}

type FeedIncl struct {
	gorm.Model
	Feed uint `gorm:"uniqueIndex:idx_feed_post"`
	Post uint `gorm:"uniqueIndex:idx_feed_post"`
}

type FeedLike struct {
	ID   uint   `gorm:"primarykey"`
	Uid  uint   `gorm:"index"`
	Rkey string `gorm:"index"`
	Ref  uint
}

type FeedRepost struct {
	ID   uint   `gorm:"primarykey"`
	Uid  uint   `gorm:"index"`
	Rkey string `gorm:"index"`
	Ref  uint
}

type User struct {
	gorm.Model
	Did       string `gorm:"index"`
	Handle    string
	LastCrawl string

	Blessed bool
}

type LastSeq struct {
	ID  uint `gorm:"primarykey"`
	Seq int64
}

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{}
	app.Commands = []*cli.Command{
		runCmd,
	}

	app.RunAndExitOnError()
}

type Server struct {
	db      *gorm.DB
	bgshost string
	xrpcc   *xrpc.Client
	bgsxrpc *xrpc.Client
	plc     *api.PLCServer

	feeds  []feedSpec
	didDoc *did.Document

	userCache *lru.Cache
}

type feedSpec struct {
	Name        string
	Uri         string
	Description string
}

var runCmd = &cli.Command{
	Name: "run",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "database-url",
			Value:   "sqlite://data/algoz.db",
			EnvVars: []string{"DATABASE_URL"},
		},
		&cli.StringFlag{
			Name:     "atp-bgs-host",
			Required: true,
			EnvVars:  []string{"ATP_BGS_HOST"},
		},
		&cli.BoolFlag{
			Name:    "readonly",
			EnvVars: []string{"READONLY"},
		},
		&cli.StringFlag{
			Name:  "plc-host",
			Value: "https://plc.directory",
		},
		&cli.StringFlag{
			Name:  "pds-host",
			Value: "https://bsky.social",
		},
		&cli.StringFlag{
			Name: "did-doc",
		},
		&cli.StringFlag{
			Name: "auto-tls-domain",
		},
		&cli.BoolFlag{
			Name: "no-index",
		},
	},
	Action: func(cctx *cli.Context) error {

		log.Info("Connecting to database")
		db, err := cliutil.SetupDatabase(cctx.String("database-url"))
		if err != nil {
			return err
		}

		log.Info("Migrating database")
		db.AutoMigrate(&User{})
		db.AutoMigrate(&LastSeq{})
		db.AutoMigrate(&PostRef{})
		db.AutoMigrate(&FeedIncl{})
		db.AutoMigrate(&Feed{})
		db.AutoMigrate(&FeedLike{})
		db.AutoMigrate(&FeedRepost{})

		log.Infof("Configuring HTTP server")
		e := echo.New()
		e.Use(middleware.Logger())
		e.HTTPErrorHandler = func(err error, c echo.Context) {
			log.Error(err)
		}

		xc := &xrpc.Client{
			Host: cctx.String("pds-host"),
		}

		plc := &api.PLCServer{
			Host: cctx.String("plc-host"),
		}

		bgsws := cctx.String("atp-bgs-host")
		if !strings.HasPrefix(bgsws, "ws") {
			return fmt.Errorf("specified bgs host must include 'ws://' or 'wss://'")
		}

		bgshttp := strings.Replace(bgsws, "ws", "http", 1)
		bgsxrpc := &xrpc.Client{
			Host: bgshttp,
		}

		ucache, _ := lru.New(100000)
		s := &Server{
			db:        db,
			bgshost:   cctx.String("atp-bgs-host"),
			xrpcc:     xc,
			bgsxrpc:   bgsxrpc,
			plc:       plc,
			userCache: ucache,
		}

		if d := cctx.String("did-doc"); d != "" {
			doc, err := loadDidDocument(d)
			if err != nil {
				return err
			}

			s.didDoc = doc
		}

		// Create some initial feed definitions
		feeds := []feedSpec{
			{
				Name:        "coolstuff",
				Description: "posts by specific cool people, maybe",
				Uri:         "at://catsanddogs",
			},
			{
				Name:        "mostpop",
				Description: "most popular posts for every ten minutes",
				Uri:         "at://bearcatfriend",
			},
		}

		for _, f := range feeds {
			if err := s.db.FirstOrCreate(&Feed{
				Name:        f.Name,
				Description: f.Description,
			}).Error; err != nil {
				return err
			}
		}

		e.Use(middleware.CORS())
		e.GET("/xrpc/app.bsky.feed.getFeedSkeleton", s.handleGetFeedSkeleton)
		e.GET("/xrpc/app.bsky.feed.describeFeedGenerator", s.handleDescribeFeedGenerator)
		e.GET("/.well-known/did.json", s.handleServeDidDoc)

		atd := cctx.String("auto-tls-domain")
		if atd != "" {
			cachedir, err := os.UserCacheDir()
			if err != nil {
				return err
			}

			e.AutoTLSManager.HostPolicy = autocert.HostWhitelist(atd)
			// Cache certificates to avoid issues with rate limits (https://letsencrypt.org/docs/rate-limits)
			e.AutoTLSManager.Cache = autocert.DirCache(filepath.Join(cachedir, "certs"))
		}
		go func() {
			if atd != "" {
				panic(e.StartAutoTLS(":3339"))
			} else {
				panic(e.Start(":3339"))
			}
		}()

		if cctx.Bool("no-index") {
			select {}
		}

		ctx := context.TODO()
		if err := s.Run(ctx); err != nil {
			return fmt.Errorf("failed to run: %w", err)
		}

		return nil
	},
}

func loadDidDocument(fn string) (*did.Document, error) {
	fi, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	var doc did.Document
	if err := json.NewDecoder(fi).Decode(&doc); err != nil {
		return nil, err
	}

	return &doc, nil
}

type FeedItem struct {
	Post string `json:"post"`
}

func (s *Server) handleGetFeedSkeleton(e echo.Context) error {

	ctx := e.Request().Context()
	feed := e.QueryParam("feed")

	puri, err := util.ParseAtUri(feed)
	if err != nil {
		return err
	}

	switch puri.Rkey {
	case "coolstuff":
		feed, err := s.getCoolstuff(ctx)
		if err != nil {
			return err
		}

		return e.JSON(200, &bsky.FeedGetFeedSkeleton_Output{
			Feed: feed,
		})
	case "mostpop":
		feed, err := s.getMostPop(ctx)
		if err != nil {
			return err
		}

		return e.JSON(200, &bsky.FeedGetFeedSkeleton_Output{
			Feed: feed,
		})
	default:
		return &echo.HTTPError{
			Code:    400,
			Message: "no such feed",
		}
	}

}

func (s *Server) getMostPop(ctx context.Context) ([]*bsky.FeedDefs_SkeletonFeedPost, error) {
	// Get current time
	now := time.Now().Truncate(time.Minute * 10)

	// Get the time 6 hours ago
	sixHoursAgo := now.Add(-24 * time.Hour)

	// Convert to PostgreSQL-compatible datetime strings
	nowStr := now.UTC().Format("2006-01-02 15:04:05")
	sixHoursAgoStr := sixHoursAgo.UTC().Format("2006-01-02 15:04:05")

	// Raw SQL query
	query := `
		SELECT id, likes, interval
		FROM (
			SELECT id, likes, 
				date_trunc('10 minutes', created_at) AS interval,
				RANK() OVER (
					PARTITION BY date_trunc('10 minutes', created_at) 
					ORDER BY likes DESC
				) as rank
			FROM post_refs
			WHERE created_at BETWEEN $1 AND $2
		) t
		WHERE rank = 1;
	`

	query = `
		SELECT *
		FROM (
			SELECT *
				TIMESTAMP WITH TIME ZONE 'epoch' +
				FLOOR((EXTRACT('epoch' FROM created_at) / 600)) * 600
				* INTERVAL '1 second' AS interval,
				RANK() OVER (
					PARTITION BY TIMESTAMP WITH TIME ZONE 'epoch' +
					FLOOR((EXTRACT('epoch' FROM created_at) / 600)) * 600
					* INTERVAL '1 second'
					ORDER BY likes DESC
				) as rank
			FROM post_refs
			WHERE created_at BETWEEN $1 AND $2
		) t
		WHERE rank = 1;
	`

	query = `
	SELECT *
	FROM (
		SELECT *,
			RANK() OVER (
				PARTITION BY TIMESTAMP WITH TIME ZONE 'epoch' +
				FLOOR((EXTRACT('epoch' FROM created_at) / 600)) * 600
				* INTERVAL '1 second'
				ORDER BY likes DESC
			) as rank
		FROM post_refs
		WHERE created_at BETWEEN $1 AND $2
	) t
	WHERE rank = 1;
`

	// Execute query and scan the results into a PostRef slice
	var posts []PostRef
	if err := s.db.Debug().Raw(query, sixHoursAgoStr, nowStr).Scan(&posts).Error; err != nil {
		return nil, err
	}

	return s.postsToFeed(ctx, posts)
}

func (s *Server) postsToFeed(ctx context.Context, posts []PostRef) ([]*bsky.FeedDefs_SkeletonFeedPost, error) {
	out := []*bsky.FeedDefs_SkeletonFeedPost{}
	for _, p := range posts {
		uri, err := s.uriForPost(ctx, &p)
		if err != nil {
			return nil, err
		}

		out = append(out, &bsky.FeedDefs_SkeletonFeedPost{Post: uri})
	}

	return out, nil
}

func (s *Server) getCoolstuff(ctx context.Context) ([]*bsky.FeedDefs_SkeletonFeedPost, error) {
	log.Info("serving coolstuff")
	f, err := s.getFeedRef(ctx, "coolstuff")
	if err != nil {
		return nil, err
	}

	var posts []PostRef
	if err := s.db.Table("feed_incls").Where("feed_incls.feed = ?", f.ID).Joins("INNER JOIN post_refs on post_refs.id = feed_incls.post").Select("post_refs.*").Order("post_refs.created_at desc").Limit(50).Find(&posts).Error; err != nil {
		return nil, err
	}

	return s.postsToFeed(ctx, posts)
}

func (s *Server) uriForPost(ctx context.Context, pr *PostRef) (string, error) {
	var u User
	if err := s.db.First(&u, "id = ?", pr.Uid).Error; err != nil {
		return "", err
	}

	return "at://" + u.Did + "/app.bsky.feed.post/" + pr.Rkey, nil
}

func (s *Server) handleDescribeFeedGenerator(e echo.Context) error {
	var out bsky.FeedDescribeFeedGenerator_Output
	out.Did = s.didDoc.ID.String()
	for _, s := range s.feeds {
		out.Feeds = append(out.Feeds, &bsky.FeedDescribeFeedGenerator_Feed{s.Uri})
	}

	return e.JSON(200, out)
}

func (s *Server) handleServeDidDoc(e echo.Context) error {
	return e.JSON(200, s.didDoc)
}

func (s *Server) deletePost(ctx context.Context, u *User, path string) error {
	log.Infof("deleting post: %s", path)

	// TODO:
	return nil
}

func (s *Server) deleteLike(ctx context.Context, u *User, path string) error {
	parts := strings.Split(path, "/")

	rkey := parts[len(parts)-1]

	var lk FeedLike
	if err := s.db.First(&lk, "uid = ? AND rkey = ?", u.ID, rkey).Error; err != nil {
		return err
	}

	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&PostRef{}).Where("id = ?", lk.Ref).Update("likes", gorm.Expr("likes - 1")).Error; err != nil {
			return err
		}

		return tx.Delete(&lk).Error
	})
}

func (s *Server) deleteRepost(ctx context.Context, u *User, path string) error {
	parts := strings.Split(path, "/")

	rkey := parts[len(parts)-1]

	var rp FeedRepost
	if err := s.db.First(&rp, "uid = ? AND rkey = ?", u.ID, rkey).Error; err != nil {
		return err
	}

	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&PostRef{}).Where("id = ?", rp.Ref).Update("reposts", gorm.Expr("reposts - 1")).Error; err != nil {
			return err
		}

		return tx.Delete(&rp).Error
	})
}

func (s *Server) indexPost(ctx context.Context, u *User, rec *bsky.FeedPost, rkey string, pcid cid.Cid) error {
	log.Infof("indexing post: %s", rkey)

	pref := &PostRef{
		Cid:      pcid.String(),
		Rkey:     rkey,
		Uid:      u.ID,
		NotFound: false,
	}

	// Update columns to default value on `id` conflict
	if err := s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "uid"}, {Name: "rkey"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"cid": "not_found"}),
	}).Create(pref).Error; err != nil {
		return err
	}

	if rec.Reply != nil {
		if err := s.incrementReplyTo(ctx, rec.Reply.Parent.Uri); err != nil {
			return err
		}

		if err := s.incrementReplyRoot(ctx, rec.Reply.Root.Uri); err != nil {
			return err
		}
	}

	if u.Blessed {
		if err := s.addPostToFeed(ctx, "coolstuff", pref); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) incrementReplyTo(ctx context.Context, uri string) error {
	pref, err := s.getPostByUri(ctx, uri)
	if err != nil {
		return err
	}

	if err := s.db.Model(&PostRef{}).Where("id = ?", pref.ID).Update("replies", gorm.Expr("replies + 1")).Error; err != nil {
		return err
	}

	return err
}

func (s *Server) getPostByUri(ctx context.Context, uri string) (*PostRef, error) {
	puri, err := util.ParseAtUri(uri)
	if err != nil {
		return nil, err
	}

	u, err := s.getOrCreateUser(ctx, puri.Did)
	if err != nil {
		return nil, err
	}

	var ref PostRef
	if err := s.db.Find(&ref, "uid = ? AND rkey = ?", u.ID, puri.Rkey).Error; err != nil {
		return nil, err
	}

	if ref.ID == 0 {
		ref.Rkey = puri.Rkey
		ref.Uid = u.ID
		ref.NotFound = true

		if err := s.db.Create(&ref).Error; err != nil {
			return nil, err
		}
	}

	return &ref, nil
}

func (s *Server) incrementReplyRoot(ctx context.Context, uri string) error {
	pref, err := s.getPostByUri(ctx, uri)
	if err != nil {
		return err
	}

	if err := s.db.Model(&PostRef{}).Where("id = ?", pref.ID).Update("thread_size", gorm.Expr("thread_size + 1")).Error; err != nil {
		return err
	}

	return err
}

func (s *Server) indexProfile(ctx context.Context, u *User, rec *bsky.ActorProfile) error {
	n := ""
	if rec.DisplayName != nil {
		n = *rec.DisplayName
	}

	log.Infof("Indexing profile: %s", n)

	return nil
}

func (s *Server) updateUserHandle(ctx context.Context, did string, handle string) error {
	u, err := s.getOrCreateUser(ctx, did)
	if err != nil {
		return err
	}

	return s.db.Model(&User{}).Where("id = ?", u.ID).Update("handle", handle).Error
}

func (s *Server) handleLike(ctx context.Context, u *User, rec *bsky.FeedLike, path string) error {
	parts := strings.Split(path, "/")

	p, err := s.getPostByUri(ctx, rec.Subject.Uri)
	if err != nil {
		return err
	}

	// TODO: this isnt a scalable way to handle likes, but its the only way
	// right now to ensure that deletes can be correctly processed
	if err := s.db.Create(&FeedLike{
		Uid:  u.ID,
		Rkey: parts[len(parts)-1],
		Ref:  p.ID,
	}).Error; err != nil {
		return err
	}

	if err := s.db.Model(&PostRef{}).Where("id = ?", p.ID).Update("likes", gorm.Expr("likes + 1")).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) getFeedRef(ctx context.Context, feed string) (*Feed, error) {
	var f Feed
	if err := s.db.First(&f, "name = ?", feed).Error; err != nil {
		return nil, err
	}

	return &f, nil
}

func (s *Server) addPostToFeed(ctx context.Context, feed string, p *PostRef) error {
	f, err := s.getFeedRef(ctx, feed)
	if err != nil {
		return err
	}

	if err := s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "feed"}, {Name: "post"}},
		DoNothing: true,
	}).Create(&FeedIncl{
		Post: p.ID,
		Feed: f.ID,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleRepost(ctx context.Context, u *User, rec *bsky.FeedRepost, path string) error {
	parts := strings.Split(path, "/")

	p, err := s.getPostByUri(ctx, rec.Subject.Uri)
	if err != nil {
		return err
	}

	if err := s.db.Create(&FeedRepost{
		Uid:  u.ID,
		Rkey: parts[len(parts)-1],
		Ref:  p.ID,
	}).Error; err != nil {
		return err
	}

	if err := s.db.Model(&PostRef{}).Where("id = ?", p.ID).Update("reposts", gorm.Expr("reposts + 1")).Error; err != nil {
		return err
	}

	return nil
}

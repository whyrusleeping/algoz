package main

import (
	"context"
	"fmt"
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

	cli "github.com/urfave/cli/v2"

	gorm "gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var log = logging.Logger("algoz")

type PostRef struct {
	gorm.Model
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
	User uint   `gorm:"index"`
	Rkey string `gorm:"index"`
	Ref  uint
}

type FeedRepost struct {
	ID   uint   `gorm:"primarykey"`
	User uint   `gorm:"index"`
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

	userCache *lru.Cache
}

var runCmd = &cli.Command{
	Name: "run",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "database-url",
			Value:   "sqlite://data/thecloud.db",
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

		log.Infof("Configuring HTTP server")
		e := echo.New()
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

		// Create some initial feed definitions
		if err := s.db.FirstOrCreate(&Feed{
			Name:        "coolstuff",
			Description: "posts by specific cool people, maybe",
		}).Error; err != nil {
			return err
		}

		if err := s.db.FirstOrCreate(&Feed{
			Name:        "mostpop",
			Description: "most popular posts for every ten minutes",
		}).Error; err != nil {
			return err
		}

		e.Use(middleware.CORS())
		e.GET("/xrpc/app.bsky.feed.getFeedSkeleton", s.handleGetFeedSkeleton)
		e.GET("/xrpc/app.bsky.feed.describeFeedGenerator", s.handleDescribeFeedGenerator)
		//e.GET(".well-known/did.json", s.handleServeDidDoc)

		go func() {
			panic(e.Start(":3999"))
		}()

		ctx := context.TODO()
		if err := s.Run(ctx); err != nil {
			return fmt.Errorf("failed to run: %w", err)
		}

		return nil
	},
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

		return e.JSON(200, feed)
	case "mostpop":
		feed, err := s.getMostPop(ctx)
		if err != nil {
			return err
		}

		return e.JSON(200, feed)
	default:
		return &echo.HTTPError{
			Code:    400,
			Message: "no such feed",
		}
	}

}

func (s *Server) getMostPop(ctx context.Context) ([]FeedItem, error) {
	// Get current time
	now := time.Now()

	// Get the time 6 hours ago
	sixHoursAgo := now.Add(-6 * time.Hour)

	// Convert to SQLite-compatible datetime strings
	nowStr := now.Format("2006-01-02 15:04:05")
	sixHoursAgoStr := sixHoursAgo.Format("2006-01-02 15:04:05")

	// Raw SQL query
	query := `
		SELECT *
		FROM post_refs
		WHERE id IN (
			SELECT id
			FROM (
				SELECT id, MAX(likes), strftime('%H:%M', created_at) AS interval
				FROM post_refs
				WHERE created_at BETWEEN ? AND ?
				GROUP BY interval
			)
		)
	`

	// Execute query and scan the results into a PostRef slice
	var posts []PostRef
	if err := s.db.Raw(query, sixHoursAgoStr, nowStr).Scan(&posts).Error; err != nil {
		return nil, err
	}

	return s.postsToFeed(ctx, posts)
}

func (s *Server) postsToFeed(ctx context.Context, posts []PostRef) ([]FeedItem, error) {
	var out []FeedItem
	for _, p := range posts {
		uri, err := s.uriForPost(ctx, &p)
		if err != nil {
			return nil, err
		}

		out = append(out, FeedItem{Post: uri})
	}

	return out, nil
}

func (s *Server) getCoolstuff(ctx context.Context) ([]FeedItem, error) {
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
	panic("NYI")
}

/* // TODO: simplify a lot of things (maybe) by also hosting our own did:web document
func (s *Server) handleServeDidDoc(e echo.Context) error {
	// TODO:
	return nil
}
*/

func (s *Server) deletePost(ctx context.Context, u *User, path string) error {
	log.Infof("deleting post: %s", path)

	// TODO:
	return nil
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

	if u.Blessed || true {
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

func (s *Server) handleLike(ctx context.Context, u *User, rec *bsky.FeedLike) error {
	p, err := s.getPostByUri(ctx, rec.Subject.Uri)
	if err != nil {
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

func (s *Server) handleRepost(ctx context.Context, u *User, rec *bsky.FeedRepost) error {
	p, err := s.getPostByUri(ctx, rec.Subject.Uri)
	if err != nil {
		return err
	}

	if err := s.db.Model(&PostRef{}).Where("id = ?", p.ID).Update("reposts", gorm.Expr("reposts + 1")).Error; err != nil {
		return err
	}

	return nil
}

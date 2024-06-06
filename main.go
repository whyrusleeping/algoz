package main

import (
	"context"
	"crypto"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/did"
	"github.com/bluesky-social/indigo/util"
	cliutil "github.com/bluesky-social/indigo/util/cliutil"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/golang-jwt/jwt/v5"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/whyrusleeping/algoz/models"
	. "github.com/whyrusleeping/algoz/models"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/crypto/acme/autocert"

	cli "github.com/urfave/cli/v2"

	gorm "gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var feedRequestsHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "feed_request_durations",
	Buckets: prometheus.ExponentialBuckets(1, 2, 15),
}, []string{"feed"})

var handleOpHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "handle_op_duration",
	Help:    "A histogram of op handling durations",
	Buckets: prometheus.ExponentialBuckets(1, 2, 15),
}, []string{"op", "collection"})

var firehoseSeqGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "algoz_firehose_seq",
})

//type User = models.User
//type PostRef = models.PostRef
//type FeedLike = models.FeedLike
//type FeedRepost = models.FeedRepost
//type Feed = models.Feed

var EpochOne time.Time = time.Unix(1, 1)

var log = logging.Logger("algoz")

type FeedBuilder interface {
	Name() string
	Description() string
	GetFeed(context.Context, *User, int, *string) (*bsky.FeedGetFeedSkeleton_Output, error)
	Processor
}

type Labeler interface {
	Processor
}

type Processor interface {
	HandlePost(context.Context, *User, *PostRef, *bsky.FeedPost) error
	HandleLike(context.Context, *User, *bsky.FeedPost) error
	HandleRepost(context.Context, *User, *PostRef, string) error
}

type LastSeq struct {
	ID  uint `gorm:"primarykey"`
	Seq int64
}

type UserAssoc struct {
	Uid   uint   `gorm:"index"`
	Assoc string `gorm:"index"`
}

type PostText struct {
	gorm.Model
	Post uint `gorm:"uniqueIndex"`
	Text string
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
	db        *gorm.DB
	bgshost   string
	xrpcc     *xrpc.Client
	bgsxrpc   *xrpc.Client
	directory identity.Directory

	processors []Processor
	fbm        map[string]FeedBuilder

	feeds  []feedSpec
	didDoc *did.Document

	userLk    sync.Mutex
	userCache *lru.Cache
	keyCache  *lru.Cache

	Maintenance        bool
	MaintenancePostUri string

	cursor   int64
	cursorLk sync.Mutex
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
		&cli.BoolFlag{
			Name: "maintenance",
		},
		&cli.StringFlag{
			Name: "img-class-host",
		},
		&cli.StringFlag{
			Name: "qualitea-class-host",
		},
	},
	Action: func(cctx *cli.Context) error {

		//logging.SetLogLevel("*", "INFO")
		maintenance := cctx.Bool("maintenance")

		log.Info("Connecting to database")
		var db *gorm.DB

		if !maintenance {
			adb, err := cliutil.SetupDatabase(cctx.String("database-url"), 40)
			if err != nil {
				return err
			}
			db = adb

			log.Info("Migrating database")
			db.AutoMigrate(&models.User{})
			db.AutoMigrate(&models.Follow{})
			db.AutoMigrate(&LastSeq{})
			db.AutoMigrate(&models.PostRef{})
			db.AutoMigrate(&models.FeedIncl{})
			db.AutoMigrate(&models.Feed{})
			db.AutoMigrate(&models.FeedLike{})
			db.AutoMigrate(&models.FeedRepost{})
			db.AutoMigrate(&models.Block{})
			db.AutoMigrate(&UserAssoc{})
			db.AutoMigrate(&PostText{})
		}

		log.Infof("Configuring HTTP server")
		e := echo.New()
		e.Use(middleware.Logger())
		e.HTTPErrorHandler = func(err error, c echo.Context) {
			log.Error(err)
			c.JSON(500, map[string]any{
				"error": err.Error(),
			})
		}

		xc := &xrpc.Client{
			Host:    cctx.String("pds-host"),
			Headers: map[string]string{},
		}
		if rbt := os.Getenv("RATELIMIT_BYPASS_TOKEN"); rbt != "" {
			xc.Headers["X-Ratelimit-Bypass"] = rbt
		}

		dir := identity.DefaultDirectory()

		bgsws := cctx.String("atp-bgs-host")
		if !strings.HasPrefix(bgsws, "ws") {
			return fmt.Errorf("specified bgs host must include 'ws://' or 'wss://'")
		}

		bgshttp := strings.Replace(bgsws, "ws", "http", 1)
		bgsxrpc := &xrpc.Client{
			Host:    bgshttp,
			Headers: map[string]string{},
		}
		if rbt := os.Getenv("RATELIMIT_BYPASS_TOKEN"); rbt != "" {
			bgsxrpc.Headers["X-Ratelimit-Bypass"] = rbt
		}

		ucache, _ := lru.New(100000)
		kcache, _ := lru.New(100000)
		s := &Server{
			db:                 db,
			bgshost:            cctx.String("atp-bgs-host"),
			xrpcc:              xc,
			bgsxrpc:            bgsxrpc,
			directory:          dir,
			userCache:          ucache,
			keyCache:           kcache,
			fbm:                make(map[string]FeedBuilder),
			Maintenance:        cctx.Bool("maintenance"),
			MaintenancePostUri: "at://did:plc:vpkhqolt662uhesyj6nxm7ys/app.bsky.feed.post/3kkcyoihzlk2b",
		}

		if d := cctx.String("did-doc"); d != "" {
			doc, err := loadDidDocument(d)
			if err != nil {
				return err
			}

			s.didDoc = doc
		}

		noindex := cctx.Bool("no-index")
		if s.Maintenance {
			noindex = true
		}

		mydid := "did:plc:vpkhqolt662uhesyj6nxm7ys"
		middlebit := mydid + "/app.bsky.feed.generator/"

		// Create some initial feed definitions
		s.feeds = []feedSpec{
			{
				Name:        "upandup",
				Description: "posts that recently hit 12 likes",
				Uri:         "at://" + middlebit + "upandup",
			},
			{
				Name:        "coolstuff",
				Description: "posts by specific cool people, maybe",
				Uri:         "at://" + middlebit + "coolstuff",
			},
			{
				Name:        "mostpop",
				Description: "most popular posts for every ten minutes",
				Uri:         "at://" + middlebit + "mostpop",
			},
			{
				Name:        "cats",
				Description: "cat pictures",
				Uri:         "at://" + middlebit + "cats",
			},
			{
				Name:        "dogs",
				Description: "dog pictures",
				Uri:         "at://" + middlebit + "dogs",
			},
			{
				Name:        "nsfw",
				Description: "nsfw pics",
				Uri:         "at://" + middlebit + "nsfw",
			},
			{
				Name:        "seacreatures",
				Description: "All your favorite sea creatures",
				Uri:         "at://" + middlebit + "seacreatures",
			},
			{
				Name:        "flowers",
				Description: "pictures of the flowers (potentially nsfw)",
				Uri:         "at://" + middlebit + "flowers",
			},
			{
				Name:        "qualitea",
				Description: "some weird ML classifier",
				Uri:         "at://" + middlebit + "qualitea",
			},
		}

		allpicsuri := "at://" + middlebit + "allpics"
		s.AddFeedBuilder(allpicsuri, &AllPicsFeed{
			name: "allpics",
			desc: "Every picture posted on the app",
			s:    s,
		})

		allqpsuri := "at://" + middlebit + "allqps"
		s.AddFeedBuilder(allqpsuri, &QuotePostsFeed{
			s: s,
		})

		followpicsuri := "at://" + middlebit + "followpics"
		s.AddFeedBuilder(followpicsuri, &FollowPics{
			s: s,
		})

		cozyuri := "at://" + middlebit + "cozy"
		s.AddFeedBuilder(cozyuri, &GoodFollows{
			s: s,
		})

		enjoyuri := "at://" + middlebit + "enjoy"
		s.AddFeedBuilder(enjoyuri, &EnjoyFeed{
			s: s,
		})

		devfeeduri := "at://" + middlebit + "devfeed"
		s.AddFeedBuilder(devfeeduri, &DevFeed{
			s: s,
		})

		infeed := &InfrequentPosters{
			s: s,
		}
		infpostsuri := "at://" + middlebit + "infreq"
		s.AddFeedBuilder(infpostsuri, infeed)

		go infeed.upkeep()

		folikeuri := "at://" + middlebit + "followlikes"
		s.AddFeedBuilder(folikeuri, NewFollowLikes(s))

		fetcher := NewImageFetcher(s.xrpcc)

		s.AddProcessor(NewImageLabeler(cctx.String("img-class-host"), s.db, s.xrpcc, fetcher, s.addPostToFeed))
		//s.AddProcessor(NewGoodPostFinder(cctx.String("qualitea-class-host"), fetcher, s.addPostToFeed))

		mixtopicsuri := "at://" + middlebit + "topicmix"
		s.AddFeedBuilder(mixtopicsuri, NewTopicMixer(s))

		if !maintenance {
			for _, f := range s.feeds {
				if err := s.db.Create(&models.Feed{
					Name:        f.Name,
					Description: f.Description,
				}).Error; err != nil {
					fmt.Println(err)
				}
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

		go func() {
			http.Handle("/metrics", promhttp.Handler())
			http.ListenAndServe(":5252", nil)
		}()

		if noindex {
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

func (s *Server) AddProcessor(p Processor) {
	s.processors = append(s.processors, p)
}

func (s *Server) AddFeedBuilder(uri string, fb FeedBuilder) {
	s.AddProcessor(fb)
	s.fbm[uri] = fb

	s.feeds = append(s.feeds, feedSpec{
		Name:        fb.Name(),
		Description: fb.Description(),
		Uri:         uri,
	})
}

type cachedKey struct {
	EOL time.Time
	Key any
}

func (s *Server) getKeyForDid(ctx context.Context, did syntax.DID) (crypto.PublicKey, error) {
	ident, err := s.directory.LookupDID(ctx, did)
	if err != nil {
		return nil, err
	}

	return ident.PublicKey()
}

func (s *Server) fetchKey(tok *jwt.Token) (any, error) {
	ctx := context.TODO()

	issuer, ok := tok.Claims.(jwt.MapClaims)["iss"].(string)
	if !ok {
		return nil, fmt.Errorf("missing 'iss' field from auth header JWT")
	}
	did, err := syntax.ParseDID(issuer)
	if err != nil {
		return nil, fmt.Errorf("invalid DID in 'iss' field from auth header JWT")
	}

	val, ok := s.keyCache.Get(did)
	if ok {
		return val, nil
	}

	k, err := s.getKeyForDid(ctx, did)
	if err != nil {
		return nil, fmt.Errorf("failed to look up public key for DID: %w", err)
	}
	s.keyCache.Add(did, k)
	return k, nil
}

func (s *Server) checkJwt(ctx context.Context, tokv string) (string, error) {
	return s.checkJwtConfig(ctx, tokv)
}

func (s *Server) checkJwtConfig(ctx context.Context, tokv string, config ...jwt.ParserOption) (string, error) {
	validMethods := []string{SigningMethodES256K.Alg(), SigningMethodES256.Alg()}
	config = append(config, jwt.WithValidMethods(validMethods))
	p := jwt.NewParser(config...)
	tok, err := p.Parse(tokv, s.fetchKey)
	if err != nil {
		return "", fmt.Errorf("failed to parse auth header JWT: %w", err)
	}
	did := tok.Claims.(jwt.MapClaims)["iss"].(string)
	return did, nil
}

type FeedItem struct {
	Post string `json:"post"`
}

func (s *Server) handleGetFeedSkeleton(e echo.Context) error {
	ctx, span := otel.Tracer("algoz").Start(e.Request().Context(), "handleGetFeedSkeleton")
	defer span.End()

	if s.Maintenance {
		return e.JSON(200, &bsky.FeedGetFeedSkeleton_Output{
			Feed: []*bsky.FeedDefs_SkeletonFeedPost{
				{Post: s.MaintenancePostUri},
			},
		})

	}

	feed := e.QueryParam("feed")

	start := time.Now()
	defer func() {
		feedRequestsHist.WithLabelValues(feed).Observe(float64(time.Since(start).Milliseconds()))
	}()

	span.SetAttributes(attribute.String("feed", feed))

	var authedUser *User
	if auth := e.Request().Header.Get("Authorization"); auth != "" {
		parts := strings.Split(auth, " ")
		if parts[0] != "Bearer" || len(parts) != 2 {
			return fmt.Errorf("invalid auth header")
		}

		did, err := s.checkJwt(ctx, parts[1])
		if err != nil {
			return fmt.Errorf("check jwt: %w", err)
		}

		//did := "did:plc:vpkhqolt662uhesyj6nxm7ys"

		u, err := s.getOrCreateUser(ctx, did)
		if err != nil {
			return fmt.Errorf("failed to create user: %w", err)
		}

		authedUser = u
		span.SetAttributes(attribute.String("did", u.Did))
	}

	var limit int = 100
	if lims := e.QueryParam("limit"); lims != "" {
		v, err := strconv.Atoi(lims)
		if err != nil {
			return err
		}
		limit = v
	}

	// TODO: technically should be checking that the full URI matches
	puri, err := util.ParseAtUri(feed)
	if err != nil {
		return err
	}

	var cursor *string
	if c := e.QueryParam("cursor"); c != "" {
		cursor = &c
	}

	span.SetAttributes(attribute.Bool("cursor", cursor != nil))

	fb, ok := s.fbm[feed]
	if ok {
		out, err := fb.GetFeed(ctx, authedUser, limit, cursor)
		if err != nil {
			return &echo.HTTPError{
				Code:    500,
				Message: fmt.Sprintf("getFeed failed: %s", err),
			}
		}

		return e.JSON(200, out)
	}

	switch puri.Rkey {
	case "coolstuff", "cats", "dogs", "nsfw", "seacreatures", "flowers", "sports":
		// all of these feeds are fed by the same 'feed_incls' table
		feed, outcurs, err := s.getFeed(ctx, puri.Rkey, limit, cursor, nil)
		if err != nil {
			return err
		}

		return e.JSON(200, &bsky.FeedGetFeedSkeleton_Output{
			Feed:   feed,
			Cursor: outcurs,
		})
	case "topic-art", "topic-gaming", "topic-animals", "topic-dev", "topic-bluesky", "topic-science", "topic-tv", "topic-nature", "topic-writing", "topic-sports", "topic-books", "topic-comics", "topic-music":
		// all of these feeds are fed by the same 'feed_incls' table
		feed, outcurs, err := s.getTopicFeed(ctx, puri.Rkey, limit, cursor)
		if err != nil {
			return err
		}

		return e.JSON(200, &bsky.FeedGetFeedSkeleton_Output{
			Feed:   feed,
			Cursor: outcurs,
		})
	case "qualitea":
		//lt := 1
		feed, outcurs, err := s.getFeed(ctx, puri.Rkey, limit, cursor, nil)
		if err != nil {
			return err
		}

		return e.JSON(200, &bsky.FeedGetFeedSkeleton_Output{
			Feed:   feed,
			Cursor: outcurs,
		})
	case "upandup":
		feed, outcurs, err := s.getFeedAddOrder(ctx, puri.Rkey, limit, cursor)
		if err != nil {
			return err
		}

		return e.JSON(200, &bsky.FeedGetFeedSkeleton_Output{
			Feed:   feed,
			Cursor: outcurs,
		})
	case "mostpop":
		// mostpop is fed by a sql query over all the posts
		feed, curs, err := s.getMostPop(ctx, limit, cursor)
		if err != nil {
			return err
		}

		return e.JSON(200, &bsky.FeedGetFeedSkeleton_Output{
			Feed:   feed,
			Cursor: curs,
		})
	case "bestoffollows":
		if authedUser == nil {
			return &echo.HTTPError{
				Code:    403,
				Message: "auth required for feed",
			}
		}
		feed, outcurs, err := s.getMostRecentFromFollows(ctx, authedUser, limit, cursor)
		if err != nil {
			return err
		}

		return e.JSON(200, &bsky.FeedGetFeedSkeleton_Output{
			Feed:   feed,
			Cursor: outcurs,
		})
	case "latestmutuals":
		if authedUser == nil {
			return &echo.HTTPError{
				Code:    403,
				Message: "auth required for feed",
			}
		}
		feed, outcurs, err := s.getMostRecentFromMutuals(ctx, authedUser, limit, cursor)
		if err != nil {
			return err
		}

		return e.JSON(200, &bsky.FeedGetFeedSkeleton_Output{
			Feed:   feed,
			Cursor: outcurs,
		})
	default:
		return &echo.HTTPError{
			Code:    400,
			Message: "no such feed",
		}
	}
}

func (s *Server) getWhatsLit(ctx context.Context, limit int, cursor *string) ([]*bsky.FeedDefs_SkeletonFeedPost, *string, error) {
	panic("no")
}

func (s *Server) getMostPop(ctx context.Context, limit int, cursor *string) ([]*bsky.FeedDefs_SkeletonFeedPost, *string, error) {
	// Get current time
	now := time.Now().UTC().Truncate(time.Minute * 10)

	if cursor != nil {
		tc, err := parseTimeCursor(*cursor)
		if err != nil {
			return nil, nil, err
		}

		now = tc.UTC().Truncate(time.Minute * 10)
	}

	// Get the time 6 hours ago
	sixHoursAgo := now.Add(-6 * time.Hour)

	// Raw SQL query
	query := `
		SELECT *
		FROM post_refs
		LEFT JOIN users on post_refs.uid = users.id
		WHERE (users.blocked IS NULL OR users.blocked = false) AND post_refs.created_at BETWEEN $1 AND $2
		ORDER BY likes DESC
		LIMIT 1
	`

	// Initialize an empty slice to store the posts
	var posts []PostRef

	// Iterate over each 10-minute window in the past 6 hours
	for t := now; t.After(sixHoursAgo); t = t.Add(-10 * time.Minute) {
		// Define the start and end of the window
		windowEnd := t
		windowStart := t.Add(-10 * time.Minute)

		// Convert to PostgreSQL-compatible datetime strings
		windowStartStr := windowStart.Format("2006-01-02 15:04:05")
		windowEndStr := windowEnd.Format("2006-01-02 15:04:05")

		// Execute query and scan the results into a PostRef
		var post PostRef
		if err := s.db.Raw(query, windowStartStr, windowEndStr).Scan(&post).Error; err != nil {
			// If an error occurred, return the error
			return nil, nil, err
		}

		if post.ID > 0 {
			// Add the post to the slice of posts
			posts = append(posts, post)
		}

		if len(posts) >= limit {
			break
		}
	}

	skelposts, err := s.postsToFeed(ctx, posts)
	if err != nil {
		return nil, nil, err
	}

	outcurs := posts[len(posts)-1].CreatedAt.UTC().Format(time.RFC3339)

	return skelposts, &outcurs, nil
}

func (s *Server) scrapeFollowsForUser(ctx context.Context, u *User) error {

	ident, err := s.directory.LookupDID(ctx, syntax.DID(u.Did))
	if err != nil {
		return fmt.Errorf("resolving authority for user: %w", err)
	}

	pdsUrl := ident.PDSEndpoint()

	client := &xrpc.Client{Host: pdsUrl}
	if strings.Contains(pdsUrl, "host.bsky.network") && !strings.Contains(pdsUrl, "hedgehog") {
		client = s.xrpcc
	}

	var cursor string
	for {
		resp, err := atproto.RepoListRecords(ctx, client, "app.bsky.graph.follow", cursor, 100, u.Did, false, "", "")
		if err != nil {
			return fmt.Errorf("fetching follows: %w", err)
		}

		for _, rec := range resp.Records {
			fol := rec.Value.Val.(*bsky.GraphFollow)

			puri, err := util.ParseAtUri(rec.Uri)
			if err != nil {
				return err
			}

			// TODO: technically need to pass collection/rkey here, but this works
			if err := s.handleFollow(ctx, u, fol, puri.Rkey); err != nil {
				return err
			}
		}

		if len(resp.Records) == 0 {
			break
		}

		if resp.Cursor == nil {
			log.Warnf("no cursor set in response from list records")
			break
		}
		cursor = *resp.Cursor
	}

	if err := s.db.Table("users").Where("id = ?", u.ID).Update("scraped_follows", true).Error; err != nil {
		return err
	}
	u.SetFollowsScraped(true)

	return nil
}

func (s *Server) latestPostForUser(ctx context.Context, uid uint) (*PostRef, error) {
	var u User
	if err := s.db.First(&u, "id = ?", uid).Error; err != nil {
		return nil, err
	}

	lp := u.LatestPost

	if lp == 0 {
		var p PostRef
		if err := s.db.Order("created_at DESC").Limit(1).Find(&p, "uid = ? AND NOT is_reply", uid).Error; err != nil {
			return nil, err
		}

		if p.ID == 0 {
			return nil, nil
		}

		if err := s.setUserLastPost(&u, &p); err != nil {
			return nil, err
		}

		return &p, nil
	} else {
		var p PostRef
		if err := s.db.Find(&p, "id = ?", lp).Error; err != nil {
			return nil, err
		}

		if p.ID == 0 {
			return nil, nil
		}

		return &p, nil
	}
}

func (s *Server) getMostRecentFromFollows(ctx context.Context, u *User, limit int, cursor *string) ([]*bsky.FeedDefs_SkeletonFeedPost, *string, error) {
	if !u.HasFollowsScraped() {
		if err := s.scrapeFollowsForUser(ctx, u); err != nil {
			return nil, nil, err
		}
	}

	/*
		query := `
			SELECT post_refs.*
			FROM post_refs
			INNER JOIN (
				SELECT following, MAX(created_at) as MaxCreated
				FROM post_refs
				INNER JOIN follows ON post_refs.uid = follows.following
				WHERE follows.uid = ?
				GROUP BY following
			) post_refs_grouped ON post_refs.uid = post_refs_grouped.following AND post_refs.created_at = post_refs_grouped.MaxCreated
		`
	*/

	start := 0
	if cursor != nil {
		num, err := strconv.Atoi(*cursor)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid cursor: %w", err)
		}

		start = num
	}

	var fusers []User
	if err := s.db.Table("follows").Joins("LEFT JOIN users on follows.following = users.id").Where("follows.uid = ?", u.ID).Limit(limit).Offset(start).Order("follows.id ASC").Scan(&fusers).Error; err != nil {
		return nil, nil, err
	}

	for _, f := range fusers {
		if f.LatestPost == 0 {
			_, err := s.latestPostForUser(ctx, f.ID)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	var out []PostRef
	if err := s.db.Table("follows").
		Joins("LEFT JOIN users on follows.following = users.id").
		Joins("INNER JOIN post_refs on users.latest_post = post_refs.id").
		Where("follows.uid = ?", u.ID).
		Limit(limit).
		Offset(start).
		Order("post_refs.created_at DESC").
		Scan(&out).Error; err != nil {
		return nil, nil, err
	}

	fp, err := s.postsToFeed(ctx, out)
	if err != nil {
		return nil, nil, err
	}

	curs := fmt.Sprint(start + limit)

	return fp, &curs, nil
}

func (s *Server) getMostRecentFromMutuals(ctx context.Context, u *User, limit int, cursor *string) ([]*bsky.FeedDefs_SkeletonFeedPost, *string, error) {
	if !u.HasFollowsScraped() {
		if err := s.scrapeFollowsForUser(ctx, u); err != nil {
			return nil, nil, fmt.Errorf("caching follows: %w", err)
		}
	}

	/*
		query := `
			SELECT post_refs.*
			FROM post_refs
			INNER JOIN (
				SELECT following, MAX(created_at) as MaxCreated
				FROM post_refs
				INNER JOIN follows ON post_refs.uid = follows.following
				WHERE follows.uid = ?
				GROUP BY following
			) post_refs_grouped ON post_refs.uid = post_refs_grouped.following AND post_refs.created_at = post_refs_grouped.MaxCreated
		`
	*/

	start := 0
	if cursor != nil {
		num, err := strconv.Atoi(*cursor)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid cursor: %w", err)
		}

		start = num
	}

	var fusers []User
	if err := s.db.Raw(`
	WITH mutuals AS (
		SELECT f1.following AS fid FROM follows f1 INNER JOIN follows f2 on f1.following = f2.uid AND f2.following = f1.uid WHERE f1.uid = ?
	)
	SELECT id, latest_post FROM mutuals LEFT JOIN users on fid = users.id`, u.ID).Scan(&fusers).Error; err != nil {
		return nil, nil, fmt.Errorf("mutuals query failed: %w", err)
	}

	var out []PostRef
	for _, f := range fusers {
		p, err := s.latestPostForUser(ctx, f.ID)
		if err != nil {
			return nil, nil, err
		}
		if p != nil {
			out = append(out, *p)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)

	})

	if start < len(out) {
		out = out[start:]
	}

	if len(out) > limit {
		out = out[:limit]
	}

	fp, err := s.postsToFeed(ctx, out)
	if err != nil {
		return nil, nil, err
	}

	curs := fmt.Sprint(start + limit)

	return fp, &curs, nil
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

func parseTimeCursor(c string) (time.Time, error) {
	return time.Parse(time.RFC3339, c)
}

// same as normal getFeed, except it sorts by when the post was added to the feed, not by post creation date
func (s *Server) getFeedAddOrder(ctx context.Context, feed string, limit int, cursor *string) ([]*bsky.FeedDefs_SkeletonFeedPost, *string, error) {
	log.Infof("serving %s", feed)
	f, err := s.getFeedRef(ctx, feed)
	if err != nil {
		return nil, nil, err
	}

	var posts []PostRef
	q := s.db.Table("feed_incls").Where("feed_incls.feed = ?", f.ID).Joins("INNER JOIN post_refs on post_refs.id = feed_incls.post").Select("post_refs.*").Order("feed_incls.id desc").Limit(limit)
	if cursor != nil {
		curs, err := strconv.Atoi(*cursor)
		if err != nil {
			return nil, nil, err
		}

		q = q.Where("feed_incls.id < ?", curs)
	}
	if err := q.Find(&posts).Error; err != nil {
		return nil, nil, err
	}

	skelposts, err := s.postsToFeed(ctx, posts)
	if err != nil {
		return nil, nil, err
	}

	if len(skelposts) == 0 {
		return []*bsky.FeedDefs_SkeletonFeedPost{}, nil, nil
	}

	var outcursv int
	if err := s.db.Table("feed_incls").Where("feed = ? AND post = ?", f.ID, posts[len(posts)-1].ID).Select("id").Scan(&outcursv).Error; err != nil {
		return nil, nil, err
	}
	outcurs := fmt.Sprint(outcursv)

	return skelposts, &outcurs, nil
}

func (s *Server) getFeed(ctx context.Context, feed string, limit int, cursor *string, likethresh *int) ([]*bsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := otel.Tracer("algoz").Start(ctx, "getFeed")
	defer span.End()

	f, err := s.getFeedRef(ctx, feed)
	if err != nil {
		return nil, nil, err
	}

	var posts []PostRef
	q := s.db.Table("feed_incls").Where("feed_incls.feed = ?", f.ID).Joins("INNER JOIN post_refs on post_refs.id = feed_incls.post").Select("post_refs.*").Order("post_refs.created_at desc").Limit(limit)
	if cursor != nil {
		t, err := time.Parse(time.RFC3339, *cursor)
		if err != nil {
			return nil, nil, err
		}

		q = q.Where("post_refs.created_at < ?", t)
	}
	if likethresh != nil {
		q = q.Where("post_refs.likes > ?", *likethresh)
	}
	if err := q.Find(&posts).Error; err != nil {
		return nil, nil, err
	}

	if len(posts) == 0 {
		return []*bsky.FeedDefs_SkeletonFeedPost{}, nil, nil
	}

	skelposts, err := s.postsToFeed(ctx, posts)
	if err != nil {
		return nil, nil, err
	}

	outcurs := posts[len(posts)-1].CreatedAt.Format(time.RFC3339)

	return skelposts, &outcurs, nil
}

type topicFeedCursor struct {
	Offset int
}

func (tfc *topicFeedCursor) ToString() string {
	b, err := json.Marshal(tfc)
	if err != nil {
		panic(err)
	}

	return string(b)
}

func parseCursor(s string) (*topicFeedCursor, error) {
	var out topicFeedCursor
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, err
	}

	return &out, nil
}

func hotnessForPost(p PostRef) float64 {
	num := float64(p.Likes + p.ThreadSize)

	age := time.Since(p.CreatedAt)

	denom := math.Pow(age.Hours()+2, 2.5)

	return num / denom
}

func hotnessSort(posts []PostRef) {
	scores := make(map[uint]float64, len(posts))

	for _, p := range posts {
		scores[p.ID] = hotnessForPost(p)
	}

	sort.Slice(posts, func(i, j int) bool {
		return scores[posts[i].ID] > scores[posts[j].ID]
	})
}

type topicPostsCache struct {
	cachedAt time.Time
	posts    []PostRef
}

func (s *Server) getTopicFeed(ctx context.Context, feed string, limit int, cursor *string) ([]*bsky.FeedDefs_SkeletonFeedPost, *string, error) {
	var curs *topicFeedCursor
	if cursor != nil {
		c, err := parseCursor(*cursor)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid cursor: %w", err)
		}

		curs = c
	} else {
		curs = &topicFeedCursor{
			Offset: 0,
		}
	}

	log.Infof("serving topic %s", feed)
	f, err := s.getFeedRef(ctx, feed)
	if err != nil {
		return nil, nil, err
	}

	newest := time.Now()
	oldest := newest.Add(time.Hour * -24)

	// get all posts in the last 24 hours
	var posts []PostRef
	q := s.db.Raw(`WITH mytopic AS (SELECT * FROM feed_incls WHERE feed = ? AND created_at > ? AND created_at < ?)
	SELECT post_refs.* from mytopic LEFT JOIN post_refs on post_refs.id = mytopic.post`, f.ID, oldest, newest)
	if err := q.Scan(&posts).Error; err != nil {
		return nil, nil, err
	}

	hotnessSort(posts)

	if curs.Offset > len(posts) {
		return []*bsky.FeedDefs_SkeletonFeedPost{}, cursor, nil
	}
	posts = posts[curs.Offset:]

	if limit > len(posts) {
		limit = len(posts)
	}

	posts = posts[:limit]

	curs.Offset += len(posts)

	skelposts, err := s.postsToFeed(ctx, posts)
	if err != nil {
		return nil, nil, err
	}

	if len(skelposts) == 0 {
		return []*bsky.FeedDefs_SkeletonFeedPost{}, nil, nil
	}

	outcurs := curs.ToString()

	return skelposts, &outcurs, nil
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
	log.Debugf("deleting post: %s", path)

	// TODO:
	return nil
}

func (s *Server) deleteLike(ctx context.Context, u *User, path string) error {
	parts := strings.Split(path, "/")

	rkey := parts[len(parts)-1]

	var lk FeedLike
	if err := s.db.Find(&lk, "uid = ? AND rkey = ?", u.ID, rkey).Error; err != nil {
		return err
	}

	if lk.ID == 0 {
		log.Warnf("handling delete with no record reference: %s %s", u.Did, path)
		return nil
	}

	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&PostRef{}).Where("id = ?", lk.Ref).Update("likes", gorm.Expr("likes - 1")).Error; err != nil {
			return err
		}

		return tx.Delete(&FeedLike{}, "id = ?", lk.ID).Error
	})
}

func (s *Server) deleteRepost(ctx context.Context, u *User, path string) error {
	parts := strings.Split(path, "/")

	rkey := parts[len(parts)-1]

	var rp FeedRepost
	if err := s.db.Find(&rp, "uid = ? AND rkey = ?", u.ID, rkey).Error; err != nil {
		return err
	}

	if rp.ID == 0 {
		return fmt.Errorf("could not find repost: uid=%d, %s", u.ID, path)
	}

	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&PostRef{}).Where("id = ?", rp.Ref).Update("reposts", gorm.Expr("reposts - 1")).Error; err != nil {
			return err
		}

		return tx.Delete(&rp).Error
	})
}

func (s *Server) deleteFollow(ctx context.Context, u *User, path string) error {
	parts := strings.Split(path, "/")

	rkey := parts[len(parts)-1]

	if err := s.db.Delete(&Follow{}, "uid = ? AND rkey = ?", u.ID, rkey).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) indexPost(ctx context.Context, u *User, rec *bsky.FeedPost, path string, pcid cid.Cid) error {
	log.Debugf("indexing post: %s", path)

	t, err := time.Parse(util.ISO8601, rec.CreatedAt)
	if err != nil {
		log.Infof("post had invalid creation time: %s", err)
	}

	parts := strings.Split(path, "/")
	rkey := parts[len(parts)-1]

	pref := &PostRef{
		CreatedAt: t,
		Cid:       pcid.String(),
		Rkey:      rkey,
		Uid:       u.ID,
		NotFound:  false,
	}

	if rec.Reply != nil {
		repto, err := s.getPostByUri(ctx, rec.Reply.Parent.Uri)
		if err != nil {
			return err
		}
		pref.ReplyTo = repto.ID
		pref.IsReply = true

		reproot, err := s.getPostByUri(ctx, rec.Reply.Root.Uri)
		if err != nil {
			return err
		}

		pref.ThreadRoot = reproot.ID
	}

	if rec.Embed != nil {
		if rec.Embed.EmbedImages != nil && len(rec.Embed.EmbedImages.Images) > 0 {
			pref.HasImage = true
		}

		var rpref string
		if rec.Embed.EmbedRecord != nil {
			rpref = rec.Embed.EmbedRecord.Record.Uri
		}
		if rec.Embed.EmbedRecordWithMedia != nil {
			rpref = rec.Embed.EmbedRecordWithMedia.Record.Record.Uri
		}
		if rpref != "" && strings.Contains(rpref, "app.bsky.feed.post") {
			rp, err := s.getPostByUri(ctx, rpref)
			if err != nil {
				return err
			}

			pref.Reposting = rp.ID
		}
	}

	// Update columns to default value on `id` conflict
	if err := s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "uid"}, {Name: "rkey"}},
		DoUpdates: clause.AssignmentColumns([]string{"cid", "not_found"}),
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
	} else {
		// track latest post from each user
		if err := s.setUserLastPost(u, pref); err != nil {
			return err
		}
	}

	for _, fb := range s.processors {
		if err := fb.HandlePost(ctx, u, pref, rec); err != nil {
			log.Errorf("handle post failed: %s", err)
		}
	}

	if u.Blessed {
		if err := s.addPostToFeed(ctx, "coolstuff", pref); err != nil {
			return err
		}
	}

	/*
		if err := s.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&PostText{
			Post: pref.ID,
			Text: rec.Text,
		}).Error; err != nil {
			return err
		}
	*/

	return nil
}

func (s *Server) setUserLastPost(u *User, p *PostRef) error {
	return u.DoLocked(func() error {
		if err := s.db.Table("users").Where("id = ?", u.ID).Update("latest_post", p.ID).Error; err != nil {
			return err
		}
		u.LatestPost = p.ID
		return nil
	})
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
		ref.CreatedAt = EpochOne

		rec, rcid, err := s.getRecord(ctx, uri)
		if err != nil {
			log.Errorf("failed to fetch missing record: %s", err)
		} else {
			crt, err := time.Parse(util.ISO8601, rec.CreatedAt)
			if err != nil {
				return nil, err
			}

			ref.CreatedAt = crt
			ref.Cid = rcid.String()
		}

		if err := s.db.Create(&ref).Error; err != nil {
			return nil, err
		}
	}

	return &ref, nil
}

func (s *Server) getRecord(ctx context.Context, uri string) (*bsky.FeedPost, cid.Cid, error) {
	puri, err := util.ParseAtUri(uri)
	if err != nil {
		return nil, cid.Undef, err
	}

	out, err := atproto.RepoGetRecord(ctx, s.xrpcc, "", puri.Collection, puri.Did, puri.Rkey)
	if err != nil {
		return nil, cid.Undef, err
	}

	var c cid.Cid
	if out.Cid != nil {
		cc, err := cid.Decode(*out.Cid)
		if err != nil {
			return nil, cid.Undef, err
		}
		c = cc
	}

	fp, ok := out.Value.Val.(*bsky.FeedPost)
	if !ok {
		return nil, cid.Undef, fmt.Errorf("record was not a feed post")
	}

	return fp, c, nil
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

	_ = n

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

	if rec.Subject == nil {
		return fmt.Errorf("like had nil subject")
	}

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

	if p.Likes == 11 {
		if err := s.addPostToFeed(ctx, "upandup", p); err != nil {
			return err
		}
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

	if rec.Subject == nil {
		return fmt.Errorf("repost had nil subject")
	}

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

	var ptext PostText
	if err := s.db.Find(&ptext, "post = ?", p.ID).Error; err != nil {
		return err
	}

	for _, proc := range s.processors {
		if err := proc.HandleRepost(ctx, u, p, ptext.Text); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) handleFollow(ctx context.Context, u *User, rec *bsky.GraphFollow, path string) error {
	parts := strings.Split(path, "/")

	target, err := s.getOrCreateUser(ctx, rec.Subject)
	if err != nil {
		return err
	}

	f := &Follow{
		Uid:       u.ID,
		Rkey:      parts[len(parts)-1],
		Following: target.ID,
	}
	if err := s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "uid"}, {Name: "following"}},
		DoNothing: true,
	}).Create(f).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleBlock(ctx context.Context, u *User, rec *bsky.GraphBlock, path string) error {
	parts := strings.Split(path, "/")

	target, err := s.getOrCreateUser(ctx, rec.Subject)
	if err != nil {
		return err
	}

	if err := s.db.Create(&Block{
		Uid:     u.ID,
		Rkey:    parts[len(parts)-1],
		Blocked: target.ID,
	}).Error; err != nil {
		return err
	}

	return nil
}

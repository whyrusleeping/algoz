package main

import (
	"context"
	"fmt"
	"strings"

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
)

var log = logging.Logger("algoz")

type PostRef struct {
	gorm.Model
	Cid string
	Tid string `gorm:"index"`
	Uid uint   `gorm:"index"`
}

type User struct {
	gorm.Model
	Did       string `gorm:"index"`
	Handle    string
	LastCrawl string
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

		e.Use(middleware.CORS())

		e.GET("/xrpc/app.bsky.feed.getFeedSkeleton", s.handleGetFeedSkeleton)
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
	feed := e.QueryParam("feed")

	puri, err := util.ParseAtUri(feed)
	if err != nil {
		return err
	}

	_ = puri

	out := []FeedItem{
		FeedItem{
			Post: "at://did:plc:vpkhqolt662uhesyj6nxm7ys/app.bsky.feed.post/3jw2emriitk2u",
		},
	}

	return e.JSON(200, out)
}

/* // TODO: simplify a lot of things by also hosting our own did:web document
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

func (s *Server) indexPost(ctx context.Context, u *User, rec *bsky.FeedPost, tid string, pcid cid.Cid) error {
	log.Infof("indexing post: %s", tid)

	if err := s.db.Create(&PostRef{
		Cid: pcid.String(),
		Tid: tid,
		Uid: u.ID,
	}).Error; err != nil {
		return err
	}

	return nil
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
	return nil
}

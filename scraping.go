package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/bluesky-social/jetstream/pkg/client"
	jpar "github.com/bluesky-social/jetstream/pkg/client/schedulers/parallel"
	"github.com/bluesky-social/jetstream/pkg/models"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
	. "github.com/whyrusleeping/algoz/models"
)

func (s *Server) loadCursor() (int64, error) {
	var lastSeq LastSeq
	if err := s.db.Find(&lastSeq).Error; err != nil {
		return 0, err
	}

	if lastSeq.ID == 0 {
		return 0, s.db.Create(&lastSeq).Error
	}

	return lastSeq.Seq, nil
}

func (s *Server) getCursor() int64 {
	s.cursorLk.Lock()
	defer s.cursorLk.Unlock()
	return s.cursor
}

func (s *Server) updateLastCursor(curs int64) error {
	s.cursorLk.Lock()
	if curs < s.cursor {
		s.cursorLk.Unlock()
		return nil
	}
	s.cursor = curs
	s.cursorLk.Unlock()

	if curs%200 == 0 {
		if err := s.db.Model(LastSeq{}).Where("id = 1").Update("seq", curs).Error; err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) RunJetstream(ctx context.Context) error {
	loadedCursor, err := s.loadCursor()
	if err != nil {
		return fmt.Errorf("get last cursor: %w", err)
	}

	s.cursor = loadedCursor

	handleFunc := func(ctx context.Context, evt *models.Event) error {
		did := evt.Did

		switch {
		case evt.Commit != nil:
			c := evt.Commit

			if !interestedInRecordType(c.Collection) {
				return nil
			}

			ek := repomgr.EventKind(c.Operation)
			switch ek {
			case repomgr.EvtKindCreateRecord, repomgr.EvtKindUpdateRecord:
				var rec any
				switch c.Collection {
				case "app.bsky.feed.post":
					var r bsky.FeedPost
					if err := json.Unmarshal([]byte(c.Record), &r); err != nil {
						return err
					}
					rec = &r
				case "app.bsky.actor.profile":
					var r bsky.ActorProfile
					if err := json.Unmarshal([]byte(c.Record), &r); err != nil {
						return err
					}
					rec = &r
				case "app.bsky.feed.like":
					var r bsky.FeedLike
					if err := json.Unmarshal([]byte(c.Record), &r); err != nil {
						return err
					}
					rec = &r
				case "app.bsky.feed.repost":
					var r bsky.FeedRepost
					if err := json.Unmarshal([]byte(c.Record), &r); err != nil {
						return err
					}
					rec = &r
				case "app.bsky.graph.follow":
					var r bsky.GraphFollow
					if err := json.Unmarshal([]byte(c.Record), &r); err != nil {
						return err
					}
					rec = &r
				case "app.bsky.graph.block":
					var r bsky.GraphBlock
					if err := json.Unmarshal([]byte(c.Record), &r); err != nil {
						return err
					}
					rec = &r
				default:
					return nil
				}
				if err := s.handleOp(ctx, ek, evt.TimeUS, c.Collection+"/"+c.RKey, did, nil, rec); err != nil {
					log.Errorf("failed to handle op: %s", err)
					return nil
				}

			case repomgr.EvtKindDeleteRecord:
				if err := s.handleOp(ctx, ek, evt.TimeUS, c.Collection+"/"+c.RKey, did, nil, nil); err != nil {
					if !errors.Is(err, ErrMissingDeleteTarget) {
						log.Errorf("failed to handle delete: %s", err)
					}
					return nil
				}
			}

			return nil
			/*
				case evt.Account != nil:
					evt := evt.Account
					if err := s.updateUserHandle(ctx, evt.Did, evt.Handle); err != nil {
						log.Errorf("failed to update user handle: %s", err)
					}
					return nil
			*/
		case evt.Identity != nil:
			evt := evt.Identity
			if err := s.directory.Purge(ctx, syntax.AtIdentifier{
				Inner: syntax.DID(evt.Did),
			}); err != nil {
				log.Errorf("failed to purge identity: %s", err)
			}
			s.keyCache.Remove(evt.Did)
			return nil
		default:
			return nil
		}
	}

	for {

		l := slog.Default()
		sched := jpar.NewScheduler(100, "algoz", l, handleFunc)
		cfg := client.DefaultClientConfig()
		cfg.WebsocketURL = "wss://jetstream1.us-west.bsky.network/subscribe"

		jcli, err := client.NewClient(cfg, l, sched)
		if err != nil {
			log.Error("failed to setup jetstream client: ", err)
			continue
		}

		if err := jcli.ConnectAndRead(ctx, &s.cursor); err != nil {
			log.Errorf("stream processing error: %s", err)
		}
	}
}

func (s *Server) Run(ctx context.Context) error {
	loadedCursor, err := s.loadCursor()
	if err != nil {
		return fmt.Errorf("get last cursor: %w", err)
	}

	s.cursor = loadedCursor

	var lelk sync.Mutex
	lastTime := time.Now()

	handleFunc := func(ctx context.Context, xe *events.XRPCStreamEvent) error {
		lelk.Lock()
		lastTime = time.Now()
		lelk.Unlock()

		switch {
		case xe.RepoCommit != nil:
			evt := xe.RepoCommit
			if evt.TooBig && evt.Prev != nil {
				log.Errorf("skipping non-genesis too big events for now: %d", evt.Seq)
				return nil
			}

			if evt.TooBig {
				return nil
				if err := s.processTooBigCommit(ctx, evt); err != nil {
					log.Errorf("failed to process tooBig event: %s", err)
					return nil
				}

				return nil
			}

			r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
			if err != nil {
				log.Errorf("reading repo from car (seq: %d, len: %d): %w", evt.Seq, len(evt.Blocks), err)
				return nil
			}

			for _, op := range evt.Ops {
				if !interestedInRecordType(op.Path) {
					continue
				}

				ek := repomgr.EventKind(op.Action)
				switch ek {
				case repomgr.EvtKindCreateRecord, repomgr.EvtKindUpdateRecord:
					if op.Cid == nil {
						log.Errorf("invalid op, nil cid")
						return nil
					}

					rc, rec, err := r.GetRecord(ctx, op.Path)
					if err != nil {
						e := fmt.Errorf("getting record %s (%s) within seq %d for %s: %w", op.Path, *op.Cid, evt.Seq, evt.Repo, err)
						log.Error(e)
						return nil
					}

					if lexutil.LexLink(rc) != *op.Cid {
						log.Errorf("mismatch in record and op cid: %s != %s", rc, *op.Cid)
						return nil
					}

					if err := s.handleOp(ctx, ek, evt.Seq, op.Path, evt.Repo, &rc, rec); err != nil {
						log.Errorf("failed to handle op: %s", err)
						return nil
					}

				case repomgr.EvtKindDeleteRecord:
					if err := s.handleOp(ctx, ek, evt.Seq, op.Path, evt.Repo, nil, nil); err != nil {
						if !errors.Is(err, ErrMissingDeleteTarget) {
							log.Errorf("failed to handle delete: %s", err)
						}
						return nil
					}
				}
			}

			return nil
		case xe.RepoHandle != nil:
			evt := xe.RepoHandle
			if err := s.updateUserHandle(ctx, evt.Did, evt.Handle); err != nil {
				log.Errorf("failed to update user handle: %s", err)
			}
			return nil
		case xe.RepoIdentity != nil:
			evt := xe.RepoIdentity
			if err := s.directory.Purge(ctx, syntax.AtIdentifier{
				Inner: syntax.DID(evt.Did),
			}); err != nil {
				log.Errorf("failed to purge identity: %s", err)
			}
			s.keyCache.Remove(evt.Did)
			return nil
		default:
			return nil
		}
	}

	var backoff time.Duration
	for {
		d := websocket.DefaultDialer
		con, _, err := d.Dial(fmt.Sprintf("%s/xrpc/com.atproto.sync.subscribeRepos?cursor=%d", s.bgshost, s.getCursor()), http.Header{})
		if err != nil {
			log.Errorf("failed to dial: %s", err)
			time.Sleep(backoff)

			if backoff < time.Minute {
				backoff = (backoff * 2) + time.Second
			}
			continue
		}

		backoff = 0

		go func() {
			for range time.Tick(time.Second) {
				lelk.Lock()
				t := lastTime
				lelk.Unlock()

				if time.Since(t) > time.Second*30 {
					con.Close()
					return
				}
			}
		}()

		sched := parallel.NewScheduler(150, 1000, con.RemoteAddr().String(), handleFunc)
		if err := events.HandleRepoStream(ctx, con, sched); err != nil {
			log.Errorf("stream processing error: %s", err)
		}
	}
}

func interestedInRecordType(path string) bool {
	cols := map[string]bool{
		"app.bsky.feed.post":     true,
		"app.bsky.actor.profile": true,
		"app.bsky.feed.like":     true,
		"app.bsky.feed.repost":   true,
		"app.bsky.graph.follow":  true,
		"app.bsky.graph.block":   true,
	}
	parts := strings.Split(path, "/")
	return cols[parts[0]]
}

// handleOp receives every incoming repo event and is where indexing logic lives
func (s *Server) handleOp(ctx context.Context, op repomgr.EventKind, seq int64, path string, did string, rcid *cid.Cid, rec any) error {
	col := strings.Split(path, "/")[0]
	start := time.Now()
	defer func() {
		handleOpHist.WithLabelValues(string(op), col).Observe(float64(time.Since(start).Milliseconds()))
	}()
	if op == repomgr.EvtKindCreateRecord || op == repomgr.EvtKindUpdateRecord {
		log.Infof("handling event(%d): %s - %s", seq, did, path)
		u, err := s.getOrCreateUser(ctx, did)
		if err != nil {
			return fmt.Errorf("checking user: %w", err)
		}
		switch rec := rec.(type) {
		case *bsky.FeedPost:
			if err := s.indexPost(ctx, u, rec, path); err != nil {
				return fmt.Errorf("indexing post: %w", err)
			}
		case *bsky.ActorProfile:
			if err := s.indexProfile(ctx, u, rec); err != nil {
				return fmt.Errorf("indexing profile: %w", err)
			}
		case *bsky.FeedLike:
			if err := s.handleLike(ctx, u, rec, path); err != nil {
				return fmt.Errorf("handling like: %w", err)
			}
		case *bsky.FeedRepost:
			if err := s.handleRepost(ctx, u, rec, path); err != nil {
				return fmt.Errorf("handling repost: %w", err)
			}
		case *bsky.GraphFollow:
			if err := s.handleFollow(ctx, u, rec, path); err != nil {
				return fmt.Errorf("handling repost: %w", err)
			}
		case *bsky.GraphBlock:
			if err := s.handleBlock(ctx, u, rec, path); err != nil {
				return fmt.Errorf("handling repost: %w", err)
			}
		default:
		}

	} else if op == repomgr.EvtKindDeleteRecord {
		u, err := s.getOrCreateUser(ctx, did)
		if err != nil {
			return err
		}

		parts := strings.Split(path, "/")
		// Not handling like/repost deletes because it requires individually tracking *every* single like
		switch parts[0] {
		// TODO: handle profile deletes, its an edge case, but worth doing still
		case "app.bsky.feed.post":
			if err := s.deletePost(ctx, u, path); err != nil {
				return err
			}
		case "app.bsky.feed.like":
			if err := s.deleteLike(ctx, u, path); err != nil {
				return err
			}
		case "app.bsky.feed.repost":
			if err := s.deleteRepost(ctx, u, path); err != nil {
				return err
			}
		case "app.bsky.graph.follow":
			if err := s.deleteFollow(ctx, u, path); err != nil {
				return err
			}
		}
	}

	if err := s.updateLastCursor(seq); err != nil {
		log.Error("Failed to update cursor: ", err)
	}

	return nil
}

func (s *Server) processTooBigCommit(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
	/*
		// TODO: use the since value, cant do it right now because we dont have an easy method to walk partial MST trees
		since := ""
		if evt.Since != nil {
			since = *evt.Since
		}
	*/

	repodata, err := comatproto.SyncGetRepo(ctx, s.bgsxrpc, evt.Repo, "")
	if err != nil {
		return err
	}

	r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(repodata))
	if err != nil {
		return err
	}

	u, err := s.getOrCreateUser(ctx, evt.Repo)
	if err != nil {
		return err
	}

	return r.ForEach(ctx, "", func(k string, v cid.Cid) error {
		rcid, rec, err := r.GetRecord(ctx, k)
		if err != nil {
			log.Errorf("failed to get record from repo checkout: %s", err)
			return nil
		}

		return s.handleOp(ctx, repomgr.EvtKindCreateRecord, evt.Seq, k, u.Did, &rcid, rec)
	})
}

func (s *Server) getOrCreateUser(ctx context.Context, did string) (*User, error) {
	s.userLk.Lock()
	cu, ok := s.userCache.Get(did)
	if ok {
		s.userLk.Unlock()
		u := cu.(*User)
		u.Lk.Lock()
		u.Lk.Unlock()
		if u.ID == 0 {
			return nil, fmt.Errorf("user creation failed")
		}

		return cu.(*User), nil
	}

	var u User
	s.userCache.Add(did, &u)

	u.Lk.Lock()
	defer u.Lk.Unlock()
	s.userLk.Unlock()

	if err := s.db.Find(&u, "did = ?", did).Error; err != nil {
		return nil, err
	}
	if u.ID == 0 {
		// TODO: figure out peoples handles
		/*
			h, err := s.handleFromDid(ctx, did)
			if err != nil {
				log.Errorw("failed to resolve did to handle", "did", did, "err", err)
			} else {
				u.Handle = h
			}
		*/

		u.Did = did
		if err := s.db.Create(&u).Error; err != nil {
			s.userCache.Remove(did)

			return nil, err
		}
	}

	return &u, nil
}

func (s *Server) handleFromDid(ctx context.Context, did string) (string, error) {
	resp, err := s.directory.LookupDID(ctx, syntax.DID(did))
	if err != nil {
		return "", err
	}

	return resp.Handle.String(), nil
}

func (s *Server) Cleanup(epoch time.Time) error {
	if err := s.db.Exec("delete from post_texts where created_at < ?", epoch).Error; err != nil {
		return err
	}

	if err := s.db.Exec("delete from feed_likes where rkey < ?", TID(epoch)).Error; err != nil {
		return err
	}

	if err := s.db.Exec("delete from post_refs where created_at < ?", epoch).Error; err != nil {
		return err
	}

	if err := s.db.Exec("delete from feed_reposts where rkey < ?", TID(epoch)).Error; err != nil {
		return err
	}

	if err := s.db.Exec("delete from feed_incls where created_at < ?", epoch).Error; err != nil {
		return err
	}

	if err := s.db.Exec("delete from quiet_posts where created_at < ?", epoch).Error; err != nil {
		return err
	}

	if err := s.db.Exec("vacuum", epoch).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) CleanupRoutine() {
	oneMonth := time.Hour * 24 * 30
	for range time.Tick(time.Hour) {
		if err := s.Cleanup(time.Now().Add(-1 * oneMonth)); err != nil {
			log.Errorf("cleanup failed: %s", err)
		}
	}
}

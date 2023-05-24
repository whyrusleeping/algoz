package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"

	api "github.com/bluesky-social/indigo/api"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
)

func (s *Server) getLastCursor() (int64, error) {
	var lastSeq LastSeq
	if err := s.db.Find(&lastSeq).Error; err != nil {
		return 0, err
	}

	if lastSeq.ID == 0 {
		return 0, s.db.Create(&lastSeq).Error
	}

	return lastSeq.Seq, nil
}

func (s *Server) updateLastCursor(curs int64) error {
	return s.db.Model(LastSeq{}).Where("id = 1").Update("seq", curs).Error
}

func (s *Server) Run(ctx context.Context) error {
	cur, err := s.getLastCursor()
	if err != nil {
		return fmt.Errorf("get last cursor: %w", err)
	}

	d := websocket.DefaultDialer
	con, _, err := d.Dial(fmt.Sprintf("%s/xrpc/com.atproto.sync.subscribeRepos?cursor=%d", s.bgshost, cur), http.Header{})
	if err != nil {
		return fmt.Errorf("events dial failed: %w", err)
	}

	pool := events.NewConsumerPool(8, 32, func(ctx context.Context, xe *events.XRPCStreamEvent) error {
		switch {
		case xe.RepoCommit != nil:
			evt := xe.RepoCommit
			if evt.TooBig && evt.Prev != nil {
				log.Errorf("skipping non-genesis too big events for now: %d", evt.Seq)
				return nil
			}

			if evt.TooBig {
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
				ek := repomgr.EventKind(op.Action)
				switch ek {
				case repomgr.EvtKindCreateRecord, repomgr.EvtKindUpdateRecord:
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
						log.Errorf("failed to handle delete: %s", err)
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
		default:
			return nil
		}
	})
	if err != nil {
		return err
	}

	return events.HandleRepoStream(ctx, con, pool)
}

// handleOp receives every incoming repo event and is where indexing logic lives
func (s *Server) handleOp(ctx context.Context, op repomgr.EventKind, seq int64, path string, did string, rcid *cid.Cid, rec any) error {
	if op == repomgr.EvtKindCreateRecord || op == repomgr.EvtKindUpdateRecord {
		log.Infof("handling event(%d): %s - %s", seq, did, path)
		u, err := s.getOrCreateUser(ctx, did)
		if err != nil {
			return fmt.Errorf("checking user: %w", err)
		}
		switch rec := rec.(type) {
		case *bsky.FeedPost:
			if err := s.indexPost(ctx, u, rec, path, *rcid); err != nil {
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
		}
	}

	if seq%50 == 0 {
		if err := s.updateLastCursor(seq); err != nil {
			log.Error("Failed to update cursor: ", err)
		}
	}

	return nil
}

func (s *Server) processTooBigCommit(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
	repodata, err := comatproto.SyncGetRepo(ctx, s.bgsxrpc, evt.Repo, "", evt.Commit.String())
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
		if strings.HasPrefix(k, "app.bsky.feed.post") || strings.HasPrefix(k, "app.bsky.actor.profile") {
			rcid, rec, err := r.GetRecord(ctx, k)
			if err != nil {
				log.Errorf("failed to get record from repo checkout: %s", err)
				return nil
			}

			switch rec := rec.(type) {
			case *bsky.FeedPost:
				if err := s.indexPost(ctx, u, rec, k, rcid); err != nil {
					return fmt.Errorf("indexing post: %w", err)
				}
			case *bsky.ActorProfile:
				if err := s.indexProfile(ctx, u, rec); err != nil {
					return fmt.Errorf("indexing profile: %w", err)
				}
			default:
			}

		}
		return nil
	})
}

func (s *Server) getOrCreateUser(ctx context.Context, did string) (*User, error) {
	cu, ok := s.userCache.Get(did)
	if ok {
		return cu.(*User), nil
	}

	var u User
	if err := s.db.Find(&u, "did = ?", did).Error; err != nil {
		return nil, err
	}
	if u.ID == 0 {
		// TODO: figure out peoples handles
		h, err := s.handleFromDid(ctx, did)
		if err != nil {
			log.Errorw("failed to resolve did to handle", "did", did, "err", err)
		} else {
			u.Handle = h
		}

		u.Did = did
		if err := s.db.Create(&u).Error; err != nil {
			return nil, err
		}
	}

	s.userCache.Add(did, &u)

	return &u, nil
}

func (s *Server) handleFromDid(ctx context.Context, did string) (string, error) {
	handle, _, err := api.ResolveDidToHandle(ctx, s.xrpcc, s.plc, did)
	if err != nil {
		return "", err
	}

	return handle, nil
}

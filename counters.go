package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/util"
	. "github.com/whyrusleeping/algoz/models"
	gorm "gorm.io/gorm"
)

type PostCountTask struct {
	Op   string
	Post uint
	Val  int
}

type PostCounts struct {
	Likes      int
	Replies    int
	Reposts    int
	Quotes     int
	ThreadSize int
}

func (s *Server) runCountAggregator() {
	for {
		np, err := s.aggregateCounts()
		if err != nil {
			slog.Error("failed to aggregate counts", "err", err)
		}

		if np < 500 {
			time.Sleep(time.Second * 2)
		}
	}
}

func (s *Server) aggregateCounts() (int, error) {
	if s.db == nil {
		return 0, nil
	}
	start := time.Now()
	var taskCount int
	if err := s.db.Transaction(func(tx *gorm.DB) error {
		var tasks []PostCountTask
		if err := tx.Raw("DELETE FROM post_count_tasks RETURNING *").Scan(&tasks).Error; err != nil {
			return err
		}

		taskCount = len(tasks)
		slog.Info("processing post count tasks", "count", len(tasks))
		if len(tasks) == 0 {
			return nil
		}

		batch := make(map[uint]*PostCounts)
		for _, t := range tasks {
			pc, ok := batch[t.Post]
			if !ok {
				pc = &PostCounts{}
				batch[t.Post] = pc
			}

			switch t.Op {
			case "like":
				pc.Likes += t.Val
			case "reply":
				pc.Replies += t.Val
			case "repost":
				pc.Reposts += t.Val
			case "quote":
				pc.Quotes += t.Val
			case "thread":
				pc.ThreadSize += t.Val
			default:
				return fmt.Errorf("unrecognized counts task type: %q", t.Op)
			}
		}

		for post, counts := range batch {
			upd := make(map[string]any)
			if counts.Likes != 0 {
				upd["likes"] = gorm.Expr("likes + ?", counts.Likes)
			}
			if counts.Replies != 0 {
				upd["replies"] = gorm.Expr("replies + ?", counts.Replies)
			}
			if counts.Reposts != 0 {
				upd["reposts"] = gorm.Expr("reposts + ?", counts.Reposts)
			}
			if counts.Quotes != 0 {
				upd["quotes"] = gorm.Expr("quotes + ?", counts.Quotes)
			}
			if counts.ThreadSize != 0 {
				upd["thread_size"] = gorm.Expr("thread_size + ?", counts.ThreadSize)
			}
			if err := tx.Table("post_refs").Where("id = ?", post).Updates(upd).Error; err != nil {
				return err
			}
		}
		return nil
	}, nil); err != nil {
		return 0, fmt.Errorf("update counts transaction failed (%d): %w", taskCount, err)
	}

	took := time.Since(start)
	slog.Info("processed count tasks", "count", taskCount, "time", took, "rate", float64(taskCount)/took.Seconds())
	return taskCount, nil
}

func (s *Server) incrementReplyTo(ctx context.Context, uri string) error {
	pref, err := s.getPostInfo(ctx, uri)
	if err != nil {
		return err
	}

	if err := s.db.Create(&PostCountTask{
		Op:   "reply",
		Post: pref.ID,
		Val:  1,
	}).Error; err != nil {
		return err
	}

	return err
}

func (s *Server) incrementReplyRoot(ctx context.Context, uri string) error {
	pref, err := s.getPostInfo(ctx, uri)
	if err != nil {
		return err
	}

	if err := s.db.Create(&PostCountTask{
		Op:   "thread",
		Post: pref.ID,
		Val:  1,
	}).Error; err != nil {
		return err
	}

	return err
}

func (s *Server) incrementRepostCount(ctx context.Context, fpid uint) error {
	if err := s.db.Create(&PostCountTask{
		Op:   "repost",
		Post: fpid,
		Val:  1,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) decrementRepostCount(ctx context.Context, fpid uint) error {
	if err := s.db.Create(&PostCountTask{
		Op:   "repost",
		Post: fpid,
		Val:  -1,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) incrementQuotedBy(ctx context.Context, fpid uint) error {
	if err := s.db.Create(&PostCountTask{
		Op:   "quote",
		Post: fpid,
		Val:  1,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) decrementQuotedBy(ctx context.Context, fpid uint) error {
	if err := s.db.Create(&PostCountTask{
		Op:   "quote",
		Post: fpid,
		Val:  -1,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) incrementLikeCount(ctx context.Context, p uint) error {
	if err := s.db.Create(&PostCountTask{
		Op:   "like",
		Post: p,
		Val:  1,
	}).Error; err != nil {
		return err
	}
	return nil
}

func (s *Server) decrementLikeCount(ctx context.Context, p uint) error {
	if err := s.db.Create(&PostCountTask{
		Op:   "like",
		Post: p,
		Val:  -1,
	}).Error; err != nil {
		return err
	}
	return nil
}

type postInfo struct {
	Uid       uint
	ID        uint
	CreatedAt time.Time
}

func (s *Server) getPostInfo(ctx context.Context, uri string) (*postInfo, error) {
	aturi := syntax.ATURI(uri)
	v, ok := s.postCache.Get(aturi)
	if ok {
		return v, nil
	}

	p, err := s.getPostFieldsByUri(ctx, aturi.String(), "id, uid, created_at")
	if err != nil {
		return nil, err
	}

	pi := &postInfo{
		Uid:       p.Uid,
		ID:        p.ID,
		CreatedAt: p.CreatedAt,
	}
	s.postCache.Add(aturi, pi)
	return pi, nil
}

func (s *Server) getPostFieldsByUri(ctx context.Context, uri, fields string) (*PostRef, error) {
	puri, err := util.ParseAtUri(uri)
	if err != nil {
		return nil, err
	}

	u, err := s.getOrCreateUser(ctx, puri.Did)
	if err != nil {
		return nil, fmt.Errorf("get user: %w", err)
	}

	var ref PostRef
	if err := s.db.Table("post_refs").Select(fields).Where("uid = ? AND rkey = ?", u.ID, puri.Rkey).Scan(&ref).Error; err != nil {
		return nil, err
	}

	if ref.ID == 0 {
		ref.Rkey = puri.Rkey
		ref.Uid = u.ID
		ref.NotFound = true
		ref.CreatedAt = EpochOne

		/*
			rec, rcid, err := s.getRecord(ctx, uri)
			if err != nil {
				log.Error("failed to fetch missing record: %s", err)
			} else {
				crt, err := time.Parse(util.ISO8601, rec.CreatedAt)
				if err != nil {
					return nil, err
				}

				ref.CreatedAt = crt
				ref.Cid = rcid.String()
			}
		*/

		if err := s.db.Create(&ref).Error; err != nil {
			// most likely we lost a race to create this post
			var oref PostRef
			if err := s.db.Find(&oref, "uid = ? AND rkey = ?", u.ID, puri.Rkey).Error; err != nil {
				return nil, err
			}
			if oref.ID != 0 {
				return &oref, nil
			}

			return nil, fmt.Errorf("failed to create missing post ref: %w", err)
		}
	}

	return &ref, nil
}

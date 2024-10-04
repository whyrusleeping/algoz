package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	. "github.com/whyrusleeping/algoz/models"
)

type InfrequentPosters struct {
	s *Server
}

func (f *InfrequentPosters) Name() string {
	return "infrequentposters"
}

func (f *InfrequentPosters) Description() string {
	return "posts from people you follow who don't post often"
}

func (f *InfrequentPosters) upkeep() {
	if f.s.db == nil {
		log.Errorf("maintenance mode")
		return
	}
	for {
		if err := f.s.db.Exec("REFRESH MATERIALIZED VIEW CONCURRENTLY usr_post_counts").Error; err != nil {
			log.Errorf("failed to refresh view: %s", err)
		}

		time.Sleep(time.Minute * 5)
	}
}

type QuietPost struct {
	ID        uint `gorm:"primarykey"`
	CreatedAt time.Time
	Author    uint
	Post      uint
}

type infreqCursor struct {
	C time.Time
	T int
}

func parseInfreqCursor(cursor string) (*infreqCursor, error) {
	if len(cursor) < 2 {
		return nil, fmt.Errorf("invalid cursor")
	}
	if cursor[0] == '{' {
		var c infreqCursor
		if err := json.Unmarshal([]byte(cursor), &c); err != nil {
			return nil, err
		}

		return &c, nil
	}

	t, err := time.Parse(time.RFC3339, cursor)
	if err != nil {
		return nil, err
	}

	return &infreqCursor{C: t, T: 10}, nil
}

func (ifc *infreqCursor) ToString() string {
	b, err := json.Marshal(ifc)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (f *InfrequentPosters) GetFeed(ctx context.Context, u *User, limit int, cursor *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	if !u.HasFollowsScraped() {
		if err := f.s.scrapeFollowsForUser(ctx, u); err != nil {
			return nil, fmt.Errorf("failed to scrape follows: %w", err)
		}
	}

	oldest := time.Now().Add(time.Hour * -24)

	params := []any{u.ID, oldest, limit}
	var extra string
	var curs *infreqCursor
	if cursor != nil {
		c, err := parseInfreqCursor(*cursor)
		if err != nil {
			return nil, err
		}

		curs = c

		oldest = c.C.Add(time.Hour * -24)

		extra = "AND post_refs.created_at < ?"
		params = []any{u.ID, oldest, c.C, limit}
	} else {
		extra = "AND post_refs.created_at < NOW()"
		curs = &infreqCursor{T: 10}
	}

	nqs := fmt.Sprintf(`SELECT post_refs.* FROM quiet_posts
		LEFT JOIN post_refs ON quiet_posts.post = post_refs.id 
		WHERE 
			quiet_posts.author in (SELECT following FROM follows WHERE uid = ?) AND 
			quiet_posts.created_at > ?
			%s
		ORDER BY quiet_posts.created_at DESC
		LIMIT ?`, extra)

	qs := fmt.Sprintf(`SELECT * FROM post_refs
		WHERE post_refs.uid IN (SELECT following
		    FROM follows
		    WHERE uid = ?
		    AND following IN (
		        SELECT uid FROM usr_post_counts WHERE count < ? AND count > 0
		    ))
			AND post_refs.created_at > ?
			%s
		ORDER BY post_refs.created_at DESC
		LIMIT ?;`, extra)

	_ = nqs
	qs = nqs

	var out []PostRef
	q := f.s.db.Raw(qs, params...)
	if err := q.Find(&out).Error; err != nil {
		return nil, err
	}

	// first request, not many results back
	/*
		if cursor == nil && len(out) < 20 {
			curs.T = 20

			params[1] = 20

			var out []PostRef
			q := f.s.db.Raw(qs, params...)
			if err := q.Find(&out).Error; err != nil {
				return nil, err
			}
		}
	*/

	skelposts, err := f.s.postsToFeed(ctx, out)
	if err != nil {
		return nil, err
	}

	if len(out) > 0 {
		curs.C = out[len(out)-1].CreatedAt
	}

	outcurs := curs.ToString()

	return &bsky.FeedGetFeedSkeleton_Output{
		Cursor: &outcurs,
		Feed:   skelposts,
	}, nil
}

func (f *InfrequentPosters) isPosterQuiet(u *User) (bool, error) {
	var cnt int
	if err := f.s.db.Raw("SELECT count FROM usr_post_counts WHERE uid = ?", u.ID).Scan(&cnt).Error; err != nil {
		return false, fmt.Errorf("checking user post counts: %w", err)
	}

	if cnt < 10 {
		return true, nil
	}

	return false, nil
}

func (f *InfrequentPosters) HandlePost(ctx context.Context, u *User, pref *PostRef, fp *bsky.FeedPost) error {
	q, err := f.isPosterQuiet(u)
	if err != nil {
		return err
	}

	if !q {
		return nil
	}

	if err := f.s.db.Create(&QuietPost{
		Author: pref.Uid,
		Post:   pref.ID,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (f *InfrequentPosters) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (f *InfrequentPosters) HandleRepost(context.Context, *User, *postInfo, string) error {
	return nil
}

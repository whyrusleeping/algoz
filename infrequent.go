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
	for {
		if err := f.s.db.Exec("REFRESH MATERIALIZED VIEW CONCURRENTLY usr_post_counts").Error; err != nil {
			log.Errorf("failed to refresh view: %s", err)
		}

		time.Sleep(time.Minute)
	}
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
			return nil, err
		}
	}

	oldest := time.Now().Add(time.Hour * -24)

	params := []any{u.ID, 10, oldest, limit}
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
		params = []any{u.ID, c.T, oldest, c.C, limit}
	} else {
		curs = &infreqCursor{T: 10}
	}

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

	var out []PostRef
	q := f.s.db.Raw(qs, params...)
	if err := q.Find(&out).Error; err != nil {
		return nil, err
	}

	// first request, not many results back
	if cursor == nil && len(out) < 20 {
		curs.T = 20

		params[1] = 20

		var out []PostRef
		q := f.s.db.Raw(qs, params...)
		if err := q.Find(&out).Error; err != nil {
			return nil, err
		}
	}

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

func (f *InfrequentPosters) HandlePost(context.Context, *User, *PostRef, *bsky.FeedPost) error {
	return nil
}

func (f *InfrequentPosters) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (f *InfrequentPosters) HandleRepost(context.Context, *User, *PostRef, string) error {
	return nil
}

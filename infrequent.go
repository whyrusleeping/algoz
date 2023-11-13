package main

import (
	"context"
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

func (f *InfrequentPosters) GetFeed(ctx context.Context, u *User, limit int, cursor *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	if !u.HasFollowsScraped() {
		if err := f.s.scrapeFollowsForUser(ctx, u); err != nil {
			return nil, err
		}
	}

	oldest := time.Now().Add(time.Hour * -24)

	params := []any{u.ID, oldest, limit}
	var extra string
	if cursor != nil {
		t, err := time.Parse(time.RFC3339, *cursor)
		if err != nil {
			return nil, err
		}

		oldest = t.Add(time.Hour * -24)

		extra = "AND post_refs.created_at < ?"
		params = []any{u.ID, oldest, t, limit}
	}

	qs := fmt.Sprintf(`WITH inf_follows AS (
		SELECT follows.* FROM follows
		LEFT JOIN usr_post_counts ON 
			follows.uid = ? AND
			follows.following = usr_post_counts.uid
		WHERE usr_post_counts.count < 10
	)
	SELECT post_refs.* 
	FROM "post_refs"
	INNER JOIN inf_follows ON inf_follows.following = post_refs.uid
	WHERE post_refs.created_at > ?
	%s
	ORDER BY post_refs.created_at DESC 
	LIMIT ?;`, extra)

	var out []PostRef
	q := f.s.db.Debug().Raw(qs, params...)
	if err := q.Find(&out).Error; err != nil {
		return nil, err
	}

	skelposts, err := f.s.postsToFeed(ctx, out)
	if err != nil {
		return nil, err
	}

	var outcurs *string
	if len(out) > 0 {
		oc := out[len(out)-1].CreatedAt.Format(time.RFC3339)
		outcurs = &oc
	}

	return &bsky.FeedGetFeedSkeleton_Output{
		Cursor: outcurs,
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

package main

import (
	"context"
	"fmt"
	"time"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	. "github.com/whyrusleeping/algoz/models"
)

type FollowPics struct {
	s *Server
}

func (f *FollowPics) Name() string {
	return "followpics"
}

func (f *FollowPics) Description() string {
	return "just pictures from people you follow"
}

func (f *FollowPics) GetFeed(ctx context.Context, u *User, limit int, cursor *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	if !u.HasFollowsScraped() {
		if err := f.s.scrapeFollowsForUser(ctx, u); err != nil {
			return nil, err
		}
	}

	oldest := time.Now().Add(time.Hour * -6)

	params := []any{u.ID, oldest, limit}
	var extra string
	if cursor != nil {
		t, err := time.Parse(time.RFC3339, *cursor)
		if err != nil {
			return nil, err
		}

		oldest = t.Add(time.Hour * -6)

		extra = "AND post_refs.created_at < ?"
		params = []any{u.ID, oldest, t, limit}
	}

	qs := fmt.Sprintf(`SELECT post_refs.* 
FROM "post_refs"
INNER JOIN follows ON follows.uid = ? AND follows.following = post_refs.uid
WHERE has_image 
AND post_refs.created_at > ?
%s
ORDER BY post_refs.created_at DESC 
LIMIT ?;`, extra)

	var out []PostRef
	q := f.s.db.Debug().Raw(qs, params...)
	/*
		q := f.s.db.Table("feed_incls").
			Joins("INNER JOIN post_refs on post_refs.id = feed_incls.post").
			Joins("INNER JOIN follows on post_refs.uid = follows.following").
			Where("feed_incls.feed = ?", fr.ID).
			Where("follows.uid = ?", u.ID).
			Select("post_refs.*").Order("post_refs.created_at desc").Limit(limit)
	*/
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

func (f *FollowPics) HandlePost(context.Context, *User, *PostRef, *bsky.FeedPost) error {
	return nil
}

func (f *FollowPics) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (f *FollowPics) HandleRepost(context.Context, *User, *postInfo, string) error {
	return nil
}

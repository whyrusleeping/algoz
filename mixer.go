package main

import (
	"context"
	"time"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	. "github.com/whyrusleeping/algoz/models"
)

type TopicMixer struct {
	s *Server
}

func (f *TopicMixer) Name() string {
	return "topicmix"
}

func (f *TopicMixer) Description() string {
	return "show you what you might like"
}

func (f *TopicMixer) GetFeed(ctx context.Context, u *User, lim int, curs *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	tpcs, err := f.topicsForUser(ctx, u)
	if err != nil {
		return nil, err
	}

	recent := time.Now()
	oldest := time.Now().Add(time.Hour * -24)

	if curs != nil {
		pt, err := time.Parse(time.RFC3339, *curs)
		if err != nil {
			return nil, err
		}

		recent = pt
		oldest = pt.Add(time.Hour * -24)
	}

	q := `SELECT post_refs.* FROM feed_incls LEFT JOIN post_refs on post_refs.id = feed_incls.post WHERE feed in (?) AND feed_incls.created_at > ? AND feed_incls.created_at < ? ORDER BY feed_incls.created_at DESC limit ?`
	var posts []PostRef
	if err := f.s.db.Raw(q, tpcs, oldest, recent, lim).Scan(&posts).Error; err != nil {
		return nil, err
	}

	skelposts, err := f.s.postsToFeed(ctx, posts)
	if err != nil {
		return nil, err
	}

	var ocurs *string
	if len(posts) > 0 {
		ts := posts[len(posts)-1].CreatedAt.Format(time.RFC3339)
		ocurs = &ts
	}

	return &bsky.FeedGetFeedSkeleton_Output{
		Cursor: ocurs,
		Feed:   skelposts,
	}, nil
}

func (f *TopicMixer) topicsForUser(ctx context.Context, u *User) ([]uint, error) {
	q := `SELECT feed FROM feed_incls INNER JOIN feed_likes ON feed_likes.ref = feed_incls.post WHERE uid = ? AND feed > 1000000 ORDER BY feed_likes.id DESC limit 50`
	var out []uint
	if err := f.s.db.Raw(q, u.ID).Scan(&out).Error; err != nil {
		return nil, err
	}

	sums := make(map[uint]int)
	for _, v := range out {
		sums[v]++
	}

	var final []uint
	for k, v := range sums {
		if v > 0 {
			final = append(final, k)
		}
	}

	return final, nil
}

func (f *TopicMixer) HandlePost(context.Context, *User, *PostRef, *bsky.FeedPost) error {
	return nil
}

func (f *TopicMixer) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (f *TopicMixer) HandleRepost(context.Context, *User, *PostRef, string) error {
	return nil
}

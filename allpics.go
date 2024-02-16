package main

import (
	"context"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	. "github.com/whyrusleeping/algoz/models"
)

type AllPicsFeed struct {
	name string
	desc string

	// TODO: bad bad bad, need to decouple this once we solidify the interfaces
	s *Server
}

var _ (FeedBuilder) = (*AllPicsFeed)(nil)

func (f *AllPicsFeed) Name() string {
	return f.name
}

func (f *AllPicsFeed) Description() string {
	return f.desc
}

func (f *AllPicsFeed) GetFeed(ctx context.Context, u *User, lim int, cursor *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	skel, curs, err := f.s.getFeed(ctx, "allpics", lim, cursor, nil)
	if err != nil {
		return nil, err
	}

	return &bsky.FeedGetFeedSkeleton_Output{
		Cursor: curs,
		Feed:   skel,
	}, nil
}

func (f *AllPicsFeed) HandlePost(ctx context.Context, u *User, pref *PostRef, rec *bsky.FeedPost) error {
	if rec.Embed != nil && rec.Embed.EmbedImages != nil {
		if err := f.s.addPostToFeed(ctx, "allpics", pref); err != nil {
			return err
		}
	}

	return nil
}

func (f *AllPicsFeed) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (f *AllPicsFeed) HandleRepost(context.Context, *User, *PostRef, string) error {
	return nil
}

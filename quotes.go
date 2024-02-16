package main

import (
	"context"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	. "github.com/whyrusleeping/algoz/models"
)

type QuotePostsFeed struct {
	// TODO: bad bad bad, need to decouple this once we solidify the interfaces
	s *Server
}

var _ (FeedBuilder) = (*QuotePostsFeed)(nil)

func (f *QuotePostsFeed) Name() string {
	return "allqps"
}

func (f *QuotePostsFeed) Description() string {
	return "All the quote posts"
}

func (f *QuotePostsFeed) GetFeed(ctx context.Context, u *User, lim int, cursor *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	skel, curs, err := f.s.getFeed(ctx, "allqps", lim, cursor, nil)
	if err != nil {
		return nil, err
	}

	return &bsky.FeedGetFeedSkeleton_Output{
		Cursor: curs,
		Feed:   skel,
	}, nil
}

func (f *QuotePostsFeed) HandlePost(ctx context.Context, u *User, pref *PostRef, rec *bsky.FeedPost) error {
	if rec.Embed != nil && rec.Embed.EmbedRecord != nil {
		if err := f.s.addPostToFeed(ctx, "allqps", pref); err != nil {
			return err
		}
	}

	return nil
}

func (f *QuotePostsFeed) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (f *QuotePostsFeed) HandleRepost(context.Context, *User, *PostRef, string) error {
	return nil
}

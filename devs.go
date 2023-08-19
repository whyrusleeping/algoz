package main

import (
	"context"
	"strings"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	. "github.com/whyrusleeping/algoz/models"
)

type DevFeed struct {
	s *Server
}

func (f *DevFeed) Name() string {
	return "devfeed"
}

func (f *DevFeed) Description() string {
	return "posts from developers in the ATProto ecosystem"
}

func (f *DevFeed) GetFeed(ctx context.Context, u *User, limit int, cursor *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	fposts, ocurs, err := f.s.getFeedAddOrder(ctx, "devfeed", limit, cursor)
	if err != nil {
		return nil, err
	}

	return &bsky.FeedGetFeedSkeleton_Output{
		Feed:   fposts,
		Cursor: ocurs,
	}, nil
}

var devwords = []string{
	"github",
	"postgres",
	"postgresql",
	"golang",
	"typescript",
	"javascript",
	"vim",
	"emacs",
	"atproto",
	"#atdev",
	"pull-request",
	"trie",
	"serialize",
	"unmarshal",
	"goroutine",
	"concurrency",
	"docker",
	"kubernetes",
	"linux",
	"bash",
}

var devset map[string]bool

func init() {
	for _, w := range devwords {
		devset[w] = true
	}
}

func containsDevKeywords(txt string) bool {
	parts := strings.Split(strings.ToLower(txt), " ")

	for _, p := range parts {
		if devset[p] {
			return true
		}
	}

	return false
}

func (f *DevFeed) userIsDev(ctx context.Context, u *User) (bool, error) {
	var id uint
	if err := f.s.db.Model(&UserAssoc{}).Where("uid = ? AND assoc = ?", u.ID, "dev").Select("id").Scan(&id).Error; err != nil {
		return false, err
	}

	return id > 0, nil
}

func (f *DevFeed) HandlePost(ctx context.Context, u *User, pr *PostRef, fp *bsky.FeedPost) error {
	if strings.Contains(fp.Text, "#atdev") {
		if err := f.s.addPostToFeed(ctx, "devfeed", pr); err != nil {
			return err
		}
		return nil
	}

	isDev, err := f.userIsDev(ctx, u)
	if err != nil {
		return err
	}
	if isDev {
		if containsDevKeywords(fp.Text) {
			if err := f.s.addPostToFeed(ctx, "devfeed", pr); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *DevFeed) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (f *DevFeed) HandleRepost(ctx context.Context, u *User, fp *bsky.FeedPost) error {
	return nil
}

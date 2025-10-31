package main

import (
	"context"
	"slices"
	"time"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/whyrusleeping/algoz/models"
	. "github.com/whyrusleeping/algoz/models"
)

type SimclustersFeed struct {
	s *Server
}

func (f *SimclustersFeed) Name() string {
	return "followpics"
}

func (f *SimclustersFeed) Description() string {
	return "just pictures from people you follow"
}

func (f *SimclustersFeed) postsForCluster(ctx context.Context, c uint) ([]*models.PostRef, error) {
	earliest := time.Now().Add(time.Hour * -48)

	var posts []*models.PostRef
	if err := f.s.db.Raw("SELECT * FROM post_refs WHERE created_at > ? AND uid IN (SELECT uid FROM cluster_records WHERE cluster = ? AND influencer) ORDER BY created_at DESC", earliest, c).Scan(&posts).Error; err != nil {
		return nil, err
	}

	return posts, nil
}

func (f *SimclustersFeed) clusterInterestsForUser(ctx context.Context, u *User) ([]uint, error) {
	var clusters []uint
	if err := f.s.db.Raw("SELECT cluster FROM cluster_records WHERE uid = ? AND NOT influencer", u.ID).Scan(&clusters).Error; err != nil {
		return nil, err
	}

	return clusters, nil
}

func (f *SimclustersFeed) GetFeed(ctx context.Context, u *User, limit int, cursor *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	clusters, err := f.clusterInterestsForUser(ctx, u)
	if err != nil {
		return nil, err
	}

	tcursor := time.Now()
	if cursor != nil {
		t, err := time.Parse(time.RFC3339, *cursor)
		if err != nil {
			return nil, err
		}

		tcursor = t
	}

	var allposts []models.PostRef
	for _, c := range clusters {
		posts, err := f.postsForCluster(ctx, c)
		if err != nil {
			return nil, err
		}

		for _, p := range posts {
			if p.CreatedAt.Before(tcursor) {
				allposts = append(allposts, *p)
			}
		}
	}

	slices.SortFunc(allposts, func(a, b models.PostRef) int {
		if a.CreatedAt.After(b.CreatedAt) {
			return -1
		}

		return 1
	})

	if len(allposts) > limit {
		allposts = allposts[:limit]
	}

	skelposts, err := f.s.postsToFeed(ctx, allposts)
	if err != nil {
		return nil, err
	}

	var outcurs *string
	if len(allposts) > 0 {
		oc := allposts[len(allposts)-1].CreatedAt.Format(time.RFC3339)
		outcurs = &oc
	}

	return &bsky.FeedGetFeedSkeleton_Output{
		Cursor: outcurs,
		Feed:   skelposts,
	}, nil
}

func (f *SimclustersFeed) HandlePost(context.Context, *User, *PostRef, *bsky.FeedPost) error {
	return nil
}

func (f *SimclustersFeed) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (f *SimclustersFeed) HandleRepost(context.Context, *User, *postInfo, string) error {
	return nil
}

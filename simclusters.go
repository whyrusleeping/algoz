package main

import (
	"context"
	"time"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/whyrusleeping/algoz/models"
	. "github.com/whyrusleeping/algoz/models"
)

type SimclustersFeed struct {
	s *Server

	postsCache    *lru.TwoQueueCache[uint, *clusterPostsCache]
	clustersCache *lru.TwoQueueCache[uint, *userClustersCache]
}

func NewSimclustersFeed(s *Server) *SimclustersFeed {
	pc, _ := lru.New2Q[uint, *clusterPostsCache](15000)
	uc, _ := lru.New2Q[uint, *userClustersCache](50000)

	return &SimclustersFeed{
		s:             s,
		postsCache:    pc,
		clustersCache: uc,
	}
}

type clusterPostsCache struct {
	CachedAt time.Time
	Posts    []*models.PostRef
}

type userClustersCache struct {
	CachedAt time.Time
	Clusters []uint
}

func (f *SimclustersFeed) Name() string {
	return "followpics"
}

func (f *SimclustersFeed) Description() string {
	return "just pictures from people you follow"
}

func (f *SimclustersFeed) postsForCluster(ctx context.Context, c uint) ([]*models.PostRef, error) {
	val, ok := f.postsCache.Get(c)
	if ok && time.Since(val.CachedAt) < time.Minute {
		return val.Posts, nil
	}

	earliest := time.Now().Add(time.Hour * -48)

	var posts []*models.PostRef
	if err := f.s.db.Raw("SELECT * FROM post_refs WHERE created_at > ? AND reply_to = 0 AND uid IN (SELECT uid FROM cluster_records WHERE cluster = ? AND influencer) ORDER BY created_at DESC", earliest, c).Scan(&posts).Error; err != nil {
		return nil, err
	}

	f.postsCache.Add(c, &clusterPostsCache{
		Posts:    posts,
		CachedAt: time.Now(),
	})
	return posts, nil
}

func (f *SimclustersFeed) clusterInterestsForUser(ctx context.Context, u *User) ([]uint, error) {
	val, ok := f.clustersCache.Get(u.ID)
	if ok && time.Since(val.CachedAt) < time.Hour {
		return val.Clusters, nil
	}

	var clusters []uint
	if err := f.s.db.Raw("SELECT cluster FROM cluster_records WHERE uid = ? AND NOT influencer", u.ID).Scan(&clusters).Error; err != nil {
		return nil, err
	}

	f.clustersCache.Add(u.ID, &userClustersCache{
		Clusters: clusters,
		CachedAt: time.Now(),
	})

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

	hotnessSort(allposts)
	/*
		slices.SortFunc(allposts, func(a, b models.PostRef) int {
			if a.CreatedAt.After(b.CreatedAt) {
				return -1
			}

			return 1
		})
	*/

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

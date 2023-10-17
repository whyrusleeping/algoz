package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	lru "github.com/hashicorp/golang-lru"
	"github.com/whyrusleeping/algoz/models"
	. "github.com/whyrusleeping/algoz/models"
)

type FollowLikes struct {
	s     *Server
	cache *lru.Cache
}

func (f *FollowLikes) Name() string {
	return "followlikes"
}

func (f *FollowLikes) Description() string {
	return "posts that people you follow like"
}

type flPosts struct {
	posts []likeCount
	EOL   time.Time
}

type likeCount struct {
	Ref uint
	Cnt int
}

func (f *FollowLikes) getLikeList(ctx context.Context, u *User) ([]likeCount, error) {
	v, ok := f.cache.Get(u.ID)
	if ok {
		if time.Now().Before(v.(*flPosts).EOL) {
			return v.(*flPosts).posts, nil
		}
	}

	tid := TID(time.Now().Add(time.Hour * -24))

	fq := f.s.db.Model(models.Follow{}).Where("uid = ?", u.ID).Select("following")
	var res []likeCount
	if err := f.s.db.Raw(`select ref, count(DISTINCT uid) as cnt from feed_likes where rkey > ? and uid in (?) group by ref`, tid, fq).Scan(&res).Error; err != nil {
		return nil, err
	}

	n := 0
	for _, x := range res {
		if x.Cnt >= 3 {
			res[n] = x
			n++
		}
	}
	res = res[:n]

	sort.Slice(res, func(i, j int) bool {
		return res[i].Ref > res[j].Ref
	})

	f.cache.Add(u.ID, &flPosts{
		posts: res,
		EOL:   time.Now().Add(time.Minute),
	})

	return res, nil
}

func (f *FollowLikes) GetFeed(ctx context.Context, u *User, lim int, curs *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	if !u.HasFollowsScraped() {
		if err := f.s.scrapeFollowsForUser(ctx, u); err != nil {
			return nil, err
		}
	}

	res, err := f.getLikeList(ctx, u)
	if err != nil {
		return nil, err
	}

	var start int
	if curs != nil {
		n, err := strconv.Atoi(*curs)
		if err != nil {
			return nil, fmt.Errorf("failed to parse cursor: %w", err)
		}
		start = n
	}

	if len(res) <= start {
		res = res[:0]
	} else {
		res = res[start:]
	}

	if len(res) > lim {
		res = res[:lim]
	}

	var ids []uint
	for _, r := range res {
		ids = append(ids, r.Ref)
	}

	var fposts []PostRef
	if err := f.s.db.Find(&fposts, "id in (?)", ids).Error; err != nil {
		return nil, err
	}

	sort.Slice(fposts, func(i, j int) bool {
		return fposts[i].CreatedAt.After(fposts[j].CreatedAt)
	})

	skelposts, err := f.s.postsToFeed(ctx, fposts)
	if err != nil {
		return nil, err
	}

	c := fmt.Sprint(start + len(res))
	return &bsky.FeedGetFeedSkeleton_Output{
		Cursor: &c,
		Feed:   skelposts,
	}, nil
}

func (f *FollowLikes) HandlePost(context.Context, *User, *PostRef, *bsky.FeedPost) error {
	return nil
}

func (f *FollowLikes) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (f *FollowLikes) HandleRepost(context.Context, *User, *PostRef, string) error {
	return nil
}

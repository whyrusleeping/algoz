package main

import (
	"context"
	"sort"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	. "github.com/whyrusleeping/algoz/models"
)

type EnjoyFeed struct {
	s *Server
}

func (f *EnjoyFeed) Name() string {
	return "enjoyfeed"
}

func (f *EnjoyFeed) Description() string {
	return "a hopefully enjoyable feed"
}

func (f *EnjoyFeed) GetFeed(ctx context.Context, u *User, lim int, curs *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	if !u.HasFollowsScraped() {
		if err := f.s.scrapeFollowsForUser(ctx, u); err != nil {
			return nil, err
		}
	}

	fposts, err := f.postsFromFriends(ctx, u)
	if err != nil {
		return nil, err
	}

	if len(fposts) > lim {
		fposts = fposts[:lim]
	}

	skelposts, err := f.s.postsToFeed(ctx, fposts)
	if err != nil {
		return nil, err
	}

	return &bsky.FeedGetFeedSkeleton_Output{
		Cursor: nil,
		Feed:   skelposts,
	}, nil
}

func (f *EnjoyFeed) postsFromFriends(ctx context.Context, u *User) ([]PostRef, error) {
	friends, err := f.getFriendsForUser(ctx, u)
	if err != nil {
		return nil, err
	}

	var prefs []PostRef
	if err := f.s.db.Table("post_refs").Where("uid in (?)", friends).Order("created_at DESC").Limit(200).Scan(&prefs).Error; err != nil {
		return nil, err
	}

	return prefs, nil
}

type uidResult struct {
	Uid uint
	Val int
}

func (f *EnjoyFeed) getFriendsForUser(ctx context.Context, u *User) ([]uint, error) {
	var repliesToUser []uidResult
	if err := f.s.db.Raw(`SELECT uid, COUNT(*) as val 
FROM post_refs 
WHERE reply_to IN (
    SELECT id 
    FROM post_refs 
    WHERE uid = ?
)
GROUP BY uid 
ORDER BY val DESC 
LIMIT 50
`, u.ID).Scan(&repliesToUser).Error; err != nil {
		return nil, err
	}

	var userRepliesTo []uidResult
	if err := f.s.db.Raw(`select p2.uid, COUNT(*) as val
	FROM post_refs p1
	INNER JOIN post_refs p2 ON p1.reply_to = p2.id
	WHERE p1.uid = ? AND p2.uid != ?
	GROUP BY p2.uid
	ORDER BY val DESC
	LIMIT 50
	`, u.ID, u.ID).Scan(&userRepliesTo).Error; err != nil {
		return nil, err
	}

	var userLikesPosts []uidResult
	if err := f.s.db.Raw(`SELECT post_refs.id, count(*) AS val
	FROM feed_likes
	LEFT JOIN post_refs ON feed_likes.ref = post_refs.id
	WHERE feed_likes.uid = ?
	GROUP BY post_refs.id
	ORDER BY val DESC
	LIMIT 50
	`, u.ID).Scan(&userLikesPosts).Error; err != nil {
		return nil, err
	}

	vals := make(map[uint]int)
	for _, ur := range repliesToUser {
		vals[ur.Uid] = (ur.Val * 3)
	}

	for _, ur := range userRepliesTo {
		vals[ur.Uid] += (ur.Val * 5)
	}

	for _, ur := range userLikesPosts {
		vals[ur.Uid] += (ur.Val * 2)
	}

	var list []uidResult
	for u, v := range vals {
		list = append(list, uidResult{
			Uid: u,
			Val: v,
		})
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Val > list[j].Val
	})

	var out []uint
	for _, ur := range list {
		out = append(out, ur.Uid)
		if len(out) >= 20 {
			break
		}
	}

	return out, nil
}

func (f *EnjoyFeed) HandlePost(context.Context, *User, *PostRef, *bsky.FeedPost) error {
	return nil
}

func (f *EnjoyFeed) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (f *EnjoyFeed) HandleRepost(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

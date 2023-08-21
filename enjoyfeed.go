package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/whyrusleeping/algoz/models"
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

func (f *EnjoyFeed) networkTopPosts(ctx context.Context) ([]PostRef, error) {
	yesterday := time.Now().Add(time.Hour * -24)

	var prefs []PostRef
	if err := f.s.db.Order("likes DESC").Limit(20).Find(&prefs, "reply_to = 0 AND created_at > ?", yesterday).Error; err != nil {
		return nil, err
	}

	return prefs, nil
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

type friendStats struct {
	ID            uint
	Replies       int
	RepliesTo     int
	LikedPosts    int
	RenderedScore int
	Handle        string
}

func (fs *friendStats) Score() int {
	if fs.RepliesTo == 0 {
		return 0
	}

	base := (fs.Replies + 1) * (fs.RepliesTo * 4)

	base += (fs.LikedPosts * 3)

	return base
}

func (f *EnjoyFeed) getFriendsForUser(ctx context.Context, u *User) ([]uint, error) {
	cutoff := time.Now().Add(time.Hour * 24 * -30)

	// how many times other users have replied to this user
	var repliesToUser []uidResult
	if err := f.s.db.Raw(`SELECT uid, COUNT(*) as val 
FROM post_refs 
WHERE reply_to IN (
    SELECT id 
    FROM post_refs 
    WHERE uid = ? AND post_refs.created_at > ?
)
GROUP BY uid 
ORDER BY val DESC 
LIMIT 50
`, u.ID, cutoff).Scan(&repliesToUser).Error; err != nil {
		return nil, err
	}

	// how many times this user has replied to other users
	var userRepliesTo []uidResult
	if err := f.s.db.Raw(`select p2.uid, COUNT(*) as val
	FROM post_refs p1
	INNER JOIN post_refs p2 ON p1.reply_to = p2.id
	WHERE p1.uid = ? AND p2.uid != ? AND p1.created_at > ?
	GROUP BY p2.uid
	ORDER BY val DESC
	LIMIT 50
	`, u.ID, u.ID, cutoff).Scan(&userRepliesTo).Error; err != nil {
		return nil, err
	}

	// how many times this user likes posts by other users
	var userLikesPosts []uidResult
	if err := f.s.db.Raw(`SELECT post_refs.uid, count(*) AS val
	FROM feed_likes
	LEFT JOIN post_refs ON feed_likes.ref = post_refs.id
	WHERE feed_likes.uid = ? AND post_refs.created_at > ?
	GROUP BY post_refs.uid
	ORDER BY val DESC
	LIMIT 50
	`, u.ID, cutoff).Scan(&userLikesPosts).Error; err != nil {
		return nil, err
	}

	stats := make(map[uint]*friendStats)
	getf := func(id uint) *friendStats {
		f, ok := stats[id]
		if !ok {
			f = new(friendStats)
			f.ID = id
			stats[id] = f
		}

		return f
	}

	for _, ur := range repliesToUser {
		f := getf(ur.Uid)
		f.Replies = ur.Val
	}

	for _, ur := range userRepliesTo {
		f := getf(ur.Uid)
		f.RepliesTo = ur.Val
	}

	for _, ur := range userLikesPosts {
		f := getf(ur.Uid)
		f.LikedPosts = ur.Val
	}

	var list []*friendStats
	for _, v := range stats {
		v.RenderedScore = v.Score()
		list = append(list, v)
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Score() > list[j].Score()
	})

	var uids []uint
	for _, l := range list {
		if l.RenderedScore > 0 {
			uids = append(uids, l.ID)
		}
	}

	var omap []models.User
	if err := f.s.db.Find(&omap, "id in (?)", uids).Error; err != nil {
		return nil, err
	}

	for _, m := range omap {
		stats[m.ID].Handle = m.Handle
	}

	b, _ := json.Marshal(list)
	fmt.Println(string(b))

	var out []uint
	for _, ur := range list {
		out = append(out, ur.ID)
		if len(out) >= 30 {
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

func (f *EnjoyFeed) HandleRepost(context.Context, *User, *PostRef, string) error {
	return nil
}

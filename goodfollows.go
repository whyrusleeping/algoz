package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	. "github.com/whyrusleeping/algoz/models"
)

type GoodFollows struct {
	s *Server
}

func (f *GoodFollows) Name() string {
	return "cozyfollows"
}

func (f *GoodFollows) Description() string {
	return "some posts from people you follow"
}

const cozyBucketWidth = time.Hour * 3

func (f *GoodFollows) loadPostsInRange(ctx context.Context, u *User, curs *goodFollowCursor) ([]PostRef, error) {
	q := `WITH my_follows AS ( 
  SELECT * FROM follows WHERE uid = ? 
)
SELECT p1.* FROM post_refs AS p1
INNER JOIN my_follows ON p1.uid = my_follows.following
LEFT JOIN post_refs AS p2 ON p1.reply_to = p2.id
WHERE
p1.reposting = 0 AND
(
  p1.reply_to = 0 OR
  p2.uid IN (SELECT following FROM my_follows)
) AND
p1.created_at < ? AND
p1.created_at > ?
ORDER BY p1.created_at DESC;`

	var all []PostRef
	if err := f.s.db.Debug().Raw(q, u.ID, curs.Late, curs.Early).Scan(&all).Error; err != nil {
		return nil, err
	}

	return all, nil
}

type goodFollowCursor struct {
	User   uint
	Offset int
	Late   time.Time
	Early  time.Time
}

func (c *goodFollowCursor) advance() *goodFollowCursor {
	return &goodFollowCursor{
		Late:   c.Early,
		Early:  c.Early.Add(-1 * cozyBucketWidth),
		Offset: 0,
		User:   c.User,
	}
}

func (c *goodFollowCursor) toString() string {
	return fmt.Sprintf("%d:%d:%x:%x", c.User, c.Offset, c.Late.Format(time.RFC3339), c.Early.Format(time.RFC3339))
}

func parseGfc(curs string) (*goodFollowCursor, error) {
	parts := strings.Split(curs, ":")
	if len(parts) != 4 {
		return nil, fmt.Errorf("cursor must have four parts")
	}

	u, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, err
	}

	off, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}

	hs, err := hex.DecodeString(parts[2])
	if err != nil {
		return nil, err
	}

	ft, err := time.Parse(time.RFC3339, string(hs))
	if err != nil {
		return nil, err
	}

	hs2, err := hex.DecodeString(parts[3])
	if err != nil {
		return nil, err
	}

	tt, err := time.Parse(time.RFC3339, string(hs2))
	if err != nil {
		return nil, err
	}

	return &goodFollowCursor{
		User:   uint(u),
		Offset: off,
		Late:   ft,
		Early:  tt,
	}, nil
}

func (f *GoodFollows) GetFeed(ctx context.Context, u *User, lim int, curs *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	if u == nil {
		u = &User{}
		u.ID = 2853
		u.ScrapedFollows = true
	}
	if !u.HasFollowsScraped() {
		if err := f.s.scrapeFollowsForUser(ctx, u); err != nil {
			return nil, err
		}
	}

	late := time.Now()
	early := late.Add(-1 * cozyBucketWidth)

	var cursor *goodFollowCursor
	if curs != nil {
		gfc, err := parseGfc(*curs)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}

		cursor = gfc
	} else {
		cursor = &goodFollowCursor{
			User:  u.ID,
			Late:  late,
			Early: early,
		}
	}

	out, err := f.loadPage(ctx, u, cursor)
	if err != nil {
		return nil, err
	}

	if len(out) <= cursor.Offset {
		// reached the end of cursor in a strange way
		cursor = cursor.advance()
		out, err = f.loadPage(ctx, u, cursor)
		if err != nil {
			return nil, err
		}
	}

	out = out[cursor.Offset:]

	cursor.Offset += lim
	if len(out) > lim {
		out = out[:lim]
	} else {
		// end of this page, advance the cursor
		cursor = cursor.advance()
	}

	skelposts, err := f.s.postsToFeed(ctx, out)
	if err != nil {
		return nil, err
	}

	outcurs := cursor.toString()
	return &bsky.FeedGetFeedSkeleton_Output{
		Cursor: &outcurs,
		Feed:   skelposts,
	}, nil
}

func (f *GoodFollows) loadPage(ctx context.Context, u *User, cursor *goodFollowCursor) ([]PostRef, error) {
	all, err := f.loadPostsInRange(ctx, u, cursor)
	if err != nil {
		return nil, err
	}

	byposter := make(map[uint][]PostRef)
	for _, p := range all {
		byposter[p.Uid] = append(byposter[p.Uid], p)
	}

	var out []PostRef
	for _, posts := range byposter {
		filtered := selectBest(posts)
		out = append(out, filtered...)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})

	return out, nil
}

func selectBest(posts []PostRef) []PostRef {
	if len(posts) <= 2 {
		return posts
	}

	var top, unique PostRef
	var ts, us float64
	for _, p := range posts {
		p := p
		tscore := topScore(p)
		if top.ID == 0 || tscore > ts {
			top = p
			ts = tscore
		}

		uscore := uniqScore(p)
		if unique.ID == 0 || uscore > us {
			unique = p
			us = uscore
		}
	}

	if top.ID != unique.ID {
		return []PostRef{top, unique}
	}

	return []PostRef{top}
}

// prioritize most engagement
func topScore(p PostRef) float64 {
	base := float64(p.Likes + (2 * p.Replies) + p.ThreadSize)
	if p.ReplyTo != 0 {
		base = base / 2
	}

	return base

}

// prioritize new and low engagement
func uniqScore(p PostRef) float64 {
	base := float64(3*p.Likes + (2 * p.Replies) + p.ThreadSize)

	age := time.Since(p.CreatedAt)

	if age > time.Minute*5 {
		base = base * 0.8
	}
	if age > time.Minute*10 {
		base = base * 0.7
	}
	if age > time.Minute*20 {
		base = base * 0.5
	}
	if age > time.Minute*30 {
		base = base * 0.1
	}

	if p.ReplyTo != 0 {
		base = base / 2
	}

	return base
}

func (f *GoodFollows) HandlePost(context.Context, *User, *PostRef, *bsky.FeedPost) error {
	return nil
}

func (f *GoodFollows) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (f *GoodFollows) HandleRepost(context.Context, *User, *PostRef, string) error {
	return nil
}

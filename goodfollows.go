package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	bsky "github.com/bluesky-social/indigo/api/bsky"
)

type GoodFollows struct {
	name string
	desc string
	s    *Server
}

func (f *GoodFollows) Name() string {
	return f.name
}

func (f *GoodFollows) Description() string {
	return f.desc
}

func (f *GoodFollows) GetFeed(ctx context.Context, u *User, lim int, curs *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	q := `WITH my_follows as ( SELECT * from follows where uid = ? )
SELECT post_refs.* from post_refs
INNER JOIN my_follows on post_refs.uid = my_follows.following
WHERE post_refs.reply_to = 0 AND post_refs.reposting = 0
ORDER BY post_refs.created_at DESC;`

	var all []PostRef
	if err := f.s.db.Raw(q, u.ID).Scan(&all).Error; err != nil {
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

	if curs != nil {
		t, err := time.Parse(time.RFC3339, *curs)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}

		for i := 0; i < len(out); i++ {
			if out[i].CreatedAt.After(t) {
				out = out[i:]
				break
			}
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})

	if len(out) > lim {
		out = out[:lim]
	}

	skelposts, err := f.s.postsToFeed(ctx, out)
	if err != nil {
		return nil, err
	}

	outcurs := out[len(out)-1].CreatedAt.Format(time.RFC3339)

	return &bsky.FeedGetFeedSkeleton_Output{
		Cursor: &outcurs,
		Feed:   skelposts,
	}, nil
}

func selectBest(posts []PostRef) []PostRef {
	if len(posts) <= 2 {
		return posts
	}

	var top, unique *PostRef
	var ts, us float64
	for _, p := range posts {
		p := p
		tscore := topScore(p)
		if top == nil || tscore > ts {
			top = &p
			ts = tscore
		}

		uscore := uniqScore(p)
		if unique == nil || uscore > us {
			unique = &p
			us = uscore
		}
	}

	if top != unique {
		return []PostRef{*top, *unique}
	}

	return []PostRef{*top}
}

// prioritize most engagement
func topScore(p PostRef) float64 {
	return float64(p.Likes + (2 * p.Replies) + p.ThreadSize)
}

// prioritize new and low engagement
func uniqScore(p PostRef) float64 {
	out := 50 / float64(p.Likes)

	age := time.Since(p.CreatedAt)
	if age < time.Minute*5 {
		out *= 2
	} else if age < time.Minute*10 {
		out *= 1.5
	}

	return out
}

func (f *GoodFollows) HandlePost(context.Context, *User, *PostRef, *bsky.FeedPost) error {
	return nil
}

func (f *GoodFollows) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (f *GoodFollows) HandleRepost(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

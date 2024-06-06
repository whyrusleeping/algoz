package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	. "github.com/whyrusleeping/algoz/models"
)

type TopicMixer struct {
	s *Server

	cached map[uint]*cachedSet
	clk    sync.Mutex
}

func NewTopicMixer(s *Server) *TopicMixer {
	return &TopicMixer{
		s:      s,
		cached: make(map[uint]*cachedSet),
	}
}

type cachedSet struct {
	posts []*PostRef
	eol   time.Time
	lk    sync.Mutex
}

type postStack struct {
	posts []*PostRef
	i     int
}

func (s *postStack) Take() *PostRef {
	if len(s.posts) > s.i {
		p := s.posts[s.i]
		s.i++
		return p
	}
	return nil
}

func (f *TopicMixer) Name() string {
	return "topicmix"
}

func (f *TopicMixer) Description() string {
	return "show you what you might like"
}

func (f *TopicMixer) PostsForTopic(t uint) ([]*PostRef, error) {
	f.clk.Lock()
	tc, ok := f.cached[t]
	if !ok {
		tc = &cachedSet{}
		f.cached[t] = tc
	}
	f.clk.Unlock()

	tc.lk.Lock()
	defer tc.lk.Unlock()
	if time.Now().Before(tc.eol) {
		return tc.posts, nil
	}

	var posts []*PostRef
	if err := f.s.db.Raw("SELECT post_refs.* FROM feed_incls LEFT JOIN post_refs on post_refs.id = feed_incls.post WHERE feed = ? AND post_refs.created_at > NOW() - interval '3 days' AND likes > 8", t).Scan(&posts).Error; err != nil {
		return nil, err
	}

	sort.Slice(posts, func(i, j int) bool {
		return posts[i].Likes > posts[j].Likes
	})

	if len(posts) > 100 {
		posts = posts[:100]
	}

	rankPosts(posts)
	tc.posts = posts
	tc.eol = time.Now().Add(time.Minute)

	return posts, nil
}

func rankPosts(posts []*PostRef) {
	scores := make(map[uint]float64, len(posts))
	for _, p := range posts {
		scores[p.ID] = float64(p.Likes+p.Reposts) / math.Pow(float64(time.Since(p.CreatedAt).Hours()+4), 2)
	}

	sort.Slice(posts, func(i, j int) bool {
		return scores[posts[i].ID] > scores[posts[j].ID]
	})
}

func (f *TopicMixer) GetFeed(ctx context.Context, u *User, lim int, curs *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	tpcs, err := f.topicsForUser(ctx, u)
	if err != nil {
		return nil, err
	}

	var allposts [][]*PostRef
	for _, t := range tpcs {
		p, err := f.PostsForTopic(t.Feed)
		if err != nil {
			return nil, err
		}

		n := t.Cnt
		if time.Since(t.Latest) < time.Hour*24 {
			n = n * 2
		}
		// super janky frequency adjustment by topic
		for i := 0; i < n; i++ {
			allposts = append(allposts, p)
		}
	}

	var offs int
	if curs != nil {
		v, err := strconv.Atoi(*curs)
		if err != nil {
			return nil, err
		}
		offs = v
	}

	// janky blending code, discover has better stuff for this
	var sources []*postStack
	for _, pl := range allposts {
		sources = append(sources, &postStack{posts: pl})
	}

	rand.Shuffle(len(sources), func(i, j int) {
		sources[i], sources[j] = sources[j], sources[i]
	})

	var out []*PostRef
	for i := 0; i < (offs+lim)*4; i++ {
		p := sources[i%len(sources)].Take()
		if p != nil {
			out = append(out, p)
		}
	}

	//rankPosts(out)

	if offs < len(out) {
		out = out[offs:]
	}
	if len(out) > lim {
		out = out[:lim]
	}

	var final []PostRef
	for _, p := range out {
		final = append(final, *p)
	}

	skelposts, err := f.s.postsToFeed(ctx, final)
	if err != nil {
		return nil, err
	}

	v := fmt.Sprint(offs + len(out))

	return &bsky.FeedGetFeedSkeleton_Output{
		Cursor: &v,
		Feed:   skelposts,
	}, nil
}

type topicStat struct {
	Feed   uint
	Cnt    int
	Latest time.Time
}

func (f *TopicMixer) topicsForUser(ctx context.Context, u *User) ([]topicStat, error) {
	q := `select feed, count(*) as cnt, max(created_at) as latest from (select feed, feed_incls.created_at from feed_incls inner join feed_likes on feed_likes.ref = feed_incls.post where uid = ? AND feed > 1000000 order by feed_likes.id DESC limit 100) group by feed`

	//q := `SELECT feed FROM feed_incls INNER JOIN feed_likes ON feed_likes.ref = feed_incls.post WHERE uid = ? AND feed > 1000000 ORDER BY feed_likes.id DESC limit 50`
	var out []topicStat
	if err := f.s.db.Raw(q, u.ID).Scan(&out).Error; err != nil {
		return nil, err
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Cnt > out[j].Cnt {
			return true
		}

		return out[i].Latest.Before(out[j].Latest)
	})

	return out, nil
}

func (f *TopicMixer) HandlePost(context.Context, *User, *PostRef, *bsky.FeedPost) error {
	return nil
}

func (f *TopicMixer) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (f *TopicMixer) HandleRepost(context.Context, *User, *PostRef, string) error {
	return nil
}

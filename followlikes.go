package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	lru "github.com/hashicorp/golang-lru"
	. "github.com/whyrusleeping/algoz/models"
)

type FollowLikes struct {
	s           *Server
	postsCache  *lru.Cache
	followCache *lru.Cache

	lk     sync.Mutex
	chunks []*likeChunk

	lastLoad    string
	lastRefresh time.Time
}

func NewFollowLikes(s *Server) *FollowLikes {
	c, _ := lru.New(20000)
	fc, _ := lru.New(200_000)

	return &FollowLikes{
		s:           s,
		postsCache:  c,
		followCache: fc,
		lastLoad:    TID(time.Now().Add(time.Hour * -24)),
	}
}

func (fl *FollowLikes) refresher() {
	for {
		if err := fl.refreshLikes(); err != nil {
			log.Errorf("failed to refresh likes: %s", err)
		}

		time.Sleep(time.Second * 30)
	}
}

const chunkSize = 2000

func (fl *FollowLikes) refreshLikes() error {
	fl.lk.Lock()
	defer fl.lk.Unlock()
	if time.Since(fl.lastRefresh) < time.Minute {
		return nil
	}

	start := time.Now()
	defer func() {
		fmt.Println("followLikes setup took: ", time.Since(start))
	}()

	var likes []*FeedLike
	if err := fl.s.db.Find(&likes, "rkey > ?", fl.lastLoad).Error; err != nil {
		return err
	}

	for i := 0; i < len(likes); i++ {
		fl.addLikeUnlock(likes[i])
	}

	if len(likes) > 0 {
		fl.lastLoad = likes[len(likes)-1].Rkey
	}
	fl.lastRefresh = start

	return nil
}

type likeChunk struct {
	lk    sync.Mutex
	likes []*FeedLike
}

func (f *FollowLikes) addLikeUnlock(like *FeedLike) {
	if len(f.chunks) == 0 {
		f.chunks = append(f.chunks, &likeChunk{})
	}

	last := f.chunks[len(f.chunks)-1]
	if last.isFull() {
		next := &likeChunk{}
		f.chunks = append(f.chunks, next)
		last = next

		// maybe rotate the buffer forward
		earliestLike := f.chunks[0].likes[0]
		thresh := TID(time.Now().Add(time.Hour * -24))
		if earliestLike.Rkey < thresh {
			f.chunks = f.chunks[1:]
		}
	}

	last.append(like)
}

func (f *FollowLikes) addLike(like *FeedLike) {
	f.lk.Lock()
	last := f.chunks[len(f.chunks)-1]
	if last.isFull() {
		next := &likeChunk{}
		f.chunks = append(f.chunks, next)
		last = next
	}
	f.lk.Unlock()

	last.append(like)
}

func (lc *likeChunk) forEach(ctx context.Context, cb func(*FeedLike) error) error {
	lc.lk.Lock()
	likes := lc.likes
	lc.lk.Unlock()

	for _, l := range likes {
		if err := cb(l); err != nil {
			return err
		}
	}

	return nil
}

func (lc *likeChunk) append(l *FeedLike) {
	lc.lk.Lock()
	defer lc.lk.Unlock()
	lc.likes = append(lc.likes, l)
}

func (lc *likeChunk) isFull() bool {
	lc.lk.Lock()
	defer lc.lk.Unlock()
	return len(lc.likes) > chunkSize
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

func (f *FollowLikes) walkRecentLikes(ctx context.Context, cb func(*FeedLike) error) error {
	f.lk.Lock()
	chks := f.chunks
	f.lk.Unlock()

	for _, c := range chks {
		if err := c.forEach(ctx, cb); err != nil {
			return err
		}
	}

	return nil
}

type followCache struct {
	fmap map[uint]struct{}
	eol  time.Time
}

func (f *FollowLikes) getFollows(ctx context.Context, u *User) (map[uint]struct{}, error) {
	fol, ok := f.followCache.Get(u.ID)
	if ok {
		fc := fol.(*followCache)
		if time.Now().Before(fc.eol) {
			return fc.fmap, nil
		}
	}

	var follows []Follow
	if err := f.s.db.Find(&follows, "uid = ?", u.ID).Error; err != nil {
		return nil, err
	}

	fm := make(map[uint]struct{})
	for _, f := range follows {
		fm[f.Following] = struct{}{}
	}

	f.followCache.Add(u.ID, &followCache{
		fmap: fm,
		eol:  time.Now().Add(time.Minute * 15),
	})

	return fm, nil
}

func (f *FollowLikes) GetFeed(ctx context.Context, u *User, lim int, curs *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	if !u.HasFollowsScraped() {
		if err := f.s.scrapeFollowsForUser(ctx, u); err != nil {
			return nil, err
		}
	}

	fmap, err := f.getFollows(ctx, u)
	if err != nil {
		return nil, err
	}

	posts := make(map[uint]int)
	if err := f.walkRecentLikes(ctx, func(l *FeedLike) error {
		_, ok := fmap[l.Uid]
		if ok {
			posts[l.Ref]++
		}
		return nil
	}); err != nil {
		return nil, err
	}

	var postids []uint
	for pid, cnt := range posts {
		if cnt >= 3 {
			postids = append(postids, pid)
		}
	}

	sort.Slice(postids, func(i, j int) bool {
		return postids[i] > postids[j]
	})

	var start int
	if curs != nil {
		n, err := strconv.Atoi(*curs)
		if err != nil {
			return nil, fmt.Errorf("failed to parse cursor: %w", err)
		}
		start = n
	}

	if len(postids) <= start {
		postids = postids[:0]
	} else {
		postids = postids[start:]
	}

	if len(postids) > lim {
		postids = postids[:lim]
	}

	var fposts []PostRef
	if err := f.s.db.Find(&fposts, "id in (?)", postids).Error; err != nil {
		return nil, err
	}

	sort.Slice(fposts, func(i, j int) bool {
		return fposts[i].CreatedAt.After(fposts[j].CreatedAt)
	})

	skelposts, err := f.s.postsToFeed(ctx, fposts)
	if err != nil {
		return nil, err
	}

	c := fmt.Sprint(start + len(postids))
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

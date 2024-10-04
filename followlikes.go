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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	. "github.com/whyrusleeping/algoz/models"
)

var fanoutCachedUsers = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "algo_followlikes_live_user_count",
	Help: "Number of live users we are fanning out posts for",
})

type FollowLikes struct {
	s           *Server
	postsCache  *lru.Cache
	followCache *lru.Cache

	lk     sync.Mutex
	chunks []*likeChunk

	lulk      sync.Mutex
	liveUsers map[uint]*liveUserCache

	lastLoad    string
	lastRefresh time.Time
}

func NewFollowLikes(s *Server) *FollowLikes {
	c, _ := lru.New(20000)
	fc, _ := lru.New(200_000)

	fl := &FollowLikes{
		s:           s,
		postsCache:  c,
		followCache: fc,
		lastLoad:    TID(time.Now().Add(time.Hour * -24)),
		liveUsers:   make(map[uint]*liveUserCache),
	}

	if s.Maintenance {
		log.Errorf("follow likes, maintenance mode: limited functionality")
	} else {
		go fl.refresher()
		go fl.janitor()
	}

	return fl
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
	if time.Since(fl.lastRefresh) < time.Minute {
		return nil
	}

	start := time.Now()
	defer func() {
		log.Infof("followLikes setup took: %s", time.Since(start))
	}()

	var likes []*FeedLike
	if err := fl.s.db.Order("id ASC").Find(&likes, "rkey > ?", fl.lastLoad).Error; err != nil {
		return err
	}

	for i := 0; i < len(likes); i++ {
		fl.addLikeToBuffer(likes[i])
	}

	fl.fanoutLikes(context.TODO(), likes)

	if len(likes) > 0 {
		fl.lastLoad = likes[len(likes)-1].Rkey
	}
	fl.lastRefresh = start

	return nil
}

func (f *FollowLikes) janitor() {
	ticker := time.Tick(time.Minute * 5)
	for range ticker {
		f.cleanupOfflineUsers()
	}
}

func (f *FollowLikes) cleanupOfflineUsers() {
	f.lulk.Lock()
	defer f.lulk.Unlock()

	now := time.Now()
	for u, cache := range f.liveUsers {
		if now.Sub(cache.lastAccess) > time.Hour*6 {
			delete(f.liveUsers, u)
		}
	}
	fanoutCachedUsers.Set(float64(len(f.liveUsers)))
}

func (f *FollowLikes) fanoutLikes(ctx context.Context, likes []*FeedLike) {
	f.lulk.Lock()
	defer f.lulk.Unlock()

	for u, cache := range f.liveUsers {
		follows, err := f.getFollows(ctx, u)
		if err != nil {
			log.Errorw("failed to get follows during fanout", "user", u, "err", err)
			continue
		}

		cache.updateLikes(likes, follows)
	}
}

type likeChunk struct {
	lk    sync.Mutex
	likes []*FeedLike
}

func (f *FollowLikes) addLikeToBuffer(like *FeedLike) {
	f.lk.Lock()
	defer f.lk.Unlock()

	if len(f.chunks) == 0 {
		f.chunks = append(f.chunks, &likeChunk{})
	}

	last := f.chunks[len(f.chunks)-1]
	if last.isFull() {
		next := &likeChunk{}
		f.chunks = append(f.chunks, next)
		last = next

		// maybe rotate the buffer forward
		thresh := TID(time.Now().Add(time.Hour * -25))
		for len(f.chunks[0].likes) > 0 {
			earliestLike := f.chunks[0].likes[0]
			if earliestLike.Rkey > thresh {
				break
			}
			f.chunks = f.chunks[1:]
		}
	}
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

func (f *FollowLikes) getFollows(ctx context.Context, uid uint) (map[uint]struct{}, error) {
	fol, ok := f.followCache.Get(uid)
	if ok {
		fc := fol.(*followCache)
		if time.Now().Before(fc.eol) {
			return fc.fmap, nil
		}
	}

	var follows []Follow
	if err := f.s.db.Find(&follows, "uid = ?", uid).Error; err != nil {
		return nil, err
	}

	fm := make(map[uint]struct{})
	for _, f := range follows {
		fm[f.Following] = struct{}{}
	}

	f.followCache.Add(uid, &followCache{
		fmap: fm,
		eol:  time.Now().Add(time.Minute * 15),
	})

	return fm, nil
}

type liveUserCache struct {
	lk         sync.Mutex
	postCounts map[uint]int
	lastAccess time.Time
}

func (luc *liveUserCache) updateLikes(likes []*FeedLike, follows map[uint]struct{}) {
	luc.lk.Lock()
	defer luc.lk.Unlock()

	for _, lk := range likes {
		if _, ok := follows[lk.Uid]; ok {
			luc.postCounts[lk.Ref]++
		}
	}
}

func (f *FollowLikes) getLikedPostsAboveThreshold(ctx context.Context, u *User, thresh int) ([]uint, error) {
	f.lulk.Lock()
	v, ok := f.liveUsers[u.ID]
	if !ok {
		v = &liveUserCache{}
		f.liveUsers[u.ID] = v
		fanoutCachedUsers.Set(float64(len(f.liveUsers)))
	}
	f.lulk.Unlock()

	v.lk.Lock()
	defer v.lk.Unlock()

	if v.postCounts == nil {
		follows, err := f.getFollows(ctx, u.ID)
		if err != nil {
			return nil, err
		}

		posts := make(map[uint]int)
		if err := f.walkRecentLikes(ctx, func(l *FeedLike) error {
			_, ok := follows[l.Uid]
			if ok {
				posts[l.Ref]++
			}
			return nil
		}); err != nil {
			return nil, err
		}

		v.postCounts = posts
	}

	var out []uint
	for k, c := range v.postCounts {
		if c >= thresh {
			out = append(out, k)
		}
	}

	v.lastAccess = time.Now()

	sort.Slice(out, func(i, j int) bool {
		return out[i] > out[j]
	})

	return out, nil
}

func (f *FollowLikes) getLikedPostsFallback(ctx context.Context, u *User) ([]uint, error) {
	fmap, err := f.getFollows(ctx, u.ID)
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

	return postids, nil
}
func (f *FollowLikes) GetFeed(ctx context.Context, u *User, lim int, curs *string) (*bsky.FeedGetFeedSkeleton_Output, error) {
	if !u.HasFollowsScraped() {
		if err := f.s.scrapeFollowsForUser(ctx, u); err != nil {
			return nil, err
		}
	}

	postids, err := f.getLikedPostsAboveThreshold(ctx, u, 3)
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

	if len(postids) <= start {
		postids = postids[:0]
	} else {
		postids = postids[start:]
	}

	if len(postids) > lim {
		postids = postids[:lim]
	}

	var fposts []PostRef
	if err := f.s.db.Debug().Find(&fposts, "id in (?)", postids).Error; err != nil {
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

func (f *FollowLikes) HandleRepost(context.Context, *User, *postInfo, string) error {
	return nil
}

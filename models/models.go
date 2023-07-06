package models

import (
	"sync"
	"time"

	"gorm.io/gorm"
)

type PostRef struct {
	ID        uint      `gorm:"primarykey"`
	CreatedAt time.Time `gorm:"index:idx_post_uid_created"`

	Cid        string
	Rkey       string `gorm:"uniqueIndex:idx_post_rkeyuid"`
	Uid        uint   `gorm:"uniqueIndex:idx_post_rkeyuid,index:idx_post_uid_created"`
	NotFound   bool
	Likes      int
	Reposts    int
	Replies    int
	ThreadSize int
	ThreadRoot uint
	ReplyTo    uint `gorm:"index"`
	IsReply    bool `gorm:"index"`
	HasImage   bool
	Reposting  uint
}

type Feed struct {
	gorm.Model
	Name        string `gorm:"unique"`
	Description string
}

type FeedIncl struct {
	gorm.Model
	Feed uint `gorm:"uniqueIndex:idx_feed_post"`
	Post uint `gorm:"uniqueIndex:idx_feed_post"`
}

type FeedLike struct {
	ID   uint   `gorm:"primarykey"`
	Uid  uint   `gorm:"index"`
	Rkey string `gorm:"index"`
	Ref  uint
}

type FeedRepost struct {
	ID   uint   `gorm:"primarykey"`
	Uid  uint   `gorm:"index"`
	Rkey string `gorm:"index"`
	Ref  uint
}

type Follow struct {
	ID        uint   `gorm:"primarykey"`
	Uid       uint   `gorm:"uniqueIndex:idx_uid_following"`
	Following uint   `gorm:"uniqueIndex:idx_uid_following"`
	Rkey      string `gorm:"index"`
}

type Block struct {
	ID      uint   `gorm:"primarykey"`
	Uid     uint   `gorm:"index"`
	Blocked uint   `gorm:"index"`
	Rkey    string `gorm:"index"`
}

type User struct {
	gorm.Model
	Did    string `gorm:"uniqueIndex"`
	Handle string

	LatestPost uint

	Blessed        bool
	Blocked        bool
	ScrapedFollows bool

	Lk sync.Mutex `gorm:"-"`
}

func (u *User) DoLocked(f func() error) error {
	u.Lk.Lock()
	defer u.Lk.Unlock()
	return f()
}

func (u *User) HasFollowsScraped() bool {
	u.Lk.Lock()
	defer u.Lk.Unlock()
	return u.ScrapedFollows
}

func (u *User) SetFollowsScraped(v bool) {
	u.Lk.Lock()
	defer u.Lk.Unlock()
	u.ScrapedFollows = v
}

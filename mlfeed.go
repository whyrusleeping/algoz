package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	. "github.com/whyrusleeping/algoz/models"
)

type GoodPostFinder struct {
	host     string
	addLabel AddLabelFunc
	fetcher  *ImageFetcher
}

func NewGoodPostFinder(deciderHost string, fetcher *ImageFetcher, addlabel AddLabelFunc) *GoodPostFinder {
	return &GoodPostFinder{
		host:     deciderHost,
		addLabel: addlabel,
		fetcher:  fetcher,
	}
}

func (gpf *GoodPostFinder) HandlePost(ctx context.Context, u *User, pref *PostRef, rec *bsky.FeedPost) error {
	if len(rec.Text) < 10 {
		return nil
	}

	isreply := rec.Reply != nil

	var img []byte
	if rec.Embed != nil && rec.Embed.EmbedImages != nil && len(rec.Embed.EmbedImages.Images) > 0 {
		rimg := rec.Embed.EmbedImages.Images[0]

		blob, err := gpf.fetcher.Fetch(ctx, u.Did, rimg)
		if err != nil {
			return err
		}

		img = blob
	}

	good, err := gpf.checkPost(ctx, u.Did, rec.Text, img, isreply)
	if err != nil {
		return err
	}

	if good {
		if err := gpf.addLabel(ctx, "qualitea", pref); err != nil {
			return err
		}
	}

	return nil
}

func (gpf *GoodPostFinder) checkPost(ctx context.Context, did string, text string, img []byte, isreply bool) (bool, error) {
	val := map[string]any{
		"text":     text,
		"is_reply": isreply,
	}
	if img != nil {
		val["image"] = base64.StdEncoding.EncodeToString(img)
	}
	bb, err := json.Marshal(val)
	if err != nil {
		return false, err
	}

	req, err := http.NewRequest("POST", gpf.host+"/process_post", bytes.NewReader(bb))
	if err != nil {
		return false, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("ERROR BODY: ", string(b))
		return false, fmt.Errorf("check post non-200 response: %d", resp.StatusCode)
	}

	var out struct {
		Result bool
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, err
	}

	return out.Result, nil
}

func (gpf *GoodPostFinder) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (gpf *GoodPostFinder) HandleRepost(context.Context, *User, *PostRef, string) error {
	return nil
}

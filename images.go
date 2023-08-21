package main

import (
	"context"
	"fmt"

	"github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"
	. "github.com/whyrusleeping/algoz/models"
	"gorm.io/gorm"
)

type ImageLabeler struct {
	ic *ImageClassifier
	db *gorm.DB

	xrpcc *xrpc.Client

	addLabel AddLabelFunc
}

type AddLabelFunc func(context.Context, string, *PostRef) error

func NewImageLabeler(classifierHost string, db *gorm.DB, xrpcc *xrpc.Client, addlabel AddLabelFunc) *ImageLabeler {
	ic := &ImageClassifier{
		Host:       classifierHost,
		Categories: []string{"cat", "dog", "mammal", "bird", "clothed person", "cloud", "sky", "flower", "sea creature", "bird", "text post"},
	}

	return &ImageLabeler{
		ic:       ic,
		db:       db,
		xrpcc:    xrpcc,
		addLabel: addlabel,
	}
}

func (il *ImageLabeler) HandlePost(ctx context.Context, u *User, pref *PostRef, rec *bsky.FeedPost) error {
	if rec.Embed != nil && rec.Embed.EmbedImages != nil {
		for _, img := range rec.Embed.EmbedImages.Images {
			class, err := il.fetchAndClassifyImage(ctx, u.Did, img)
			if err != nil {
				return fmt.Errorf("classification failed: %w", err)
			}

			switch class {
			case "cat":
				if err := il.addLabel(ctx, "cats", pref); err != nil {
					return err
				}
			case "dog":
				if err := il.addLabel(ctx, "dogs", pref); err != nil {
					return err
				}
			case "sea creature":
				if err := il.addLabel(ctx, "seacreatures", pref); err != nil {
					return err
				}
			case "flower":
				if err := il.addLabel(ctx, "flowers", pref); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (il *ImageLabeler) fetchAndClassifyImage(ctx context.Context, did string, img *bsky.EmbedImages_Image) (string, error) {
	blob, err := atproto.SyncGetBlob(ctx, il.xrpcc, img.Image.Ref.String(), did)
	if err != nil {
		return "", err
	}

	return il.ic.Classify(ctx, blob)
}

func (il *ImageLabeler) HandleLike(context.Context, *User, *bsky.FeedPost) error {
	return nil
}

func (il *ImageLabeler) HandleRepost(context.Context, *User, *PostRef, string) error {
	return nil
}

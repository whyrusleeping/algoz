package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/labstack/echo/v4"
)

const testPinnedPostURI = "at://did:plc:example/app.bsky.feed.post/pinned"

func TestValidatePinnedPostURI(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		wantErr bool
	}{
		{name: "disabled", uri: ""},
		{name: "post", uri: testPinnedPostURI},
		{name: "not an AT URI", uri: "https://example.com/post", wantErr: true},
		{name: "wrong collection", uri: "at://did:plc:example/app.bsky.feed.like/record", wantErr: true},
		{name: "missing record key", uri: "at://did:plc:example/app.bsky.feed.post", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePinnedPostURI(tt.uri)
			if tt.wantErr && err == nil {
				t.Fatal("validatePinnedPostURI succeeded, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validatePinnedPostURI returned error: %v", err)
			}
		})
	}
}

func TestCursorFromRequest(t *testing.T) {
	tests := []struct {
		name        string
		target      string
		wantCursor  *string
		wantPresent bool
	}{
		{name: "absent", target: "/feed"},
		{name: "empty", target: "/feed?cursor=", wantPresent: true},
		{name: "populated", target: "/feed?cursor=next", wantCursor: stringPointer("next"), wantPresent: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest("GET", tt.target, nil)
			cursor, present := cursorFromRequest(e.NewContext(req, httptest.NewRecorder()))

			if present != tt.wantPresent {
				t.Fatalf("cursor present = %t, want %t", present, tt.wantPresent)
			}
			if tt.wantCursor == nil {
				if cursor != nil {
					t.Fatalf("cursor = %q, want nil", *cursor)
				}
			} else if cursor == nil || *cursor != *tt.wantCursor {
				t.Fatalf("cursor = %v, want %q", cursor, *tt.wantCursor)
			}
		})
	}
}

func stringPointer(value string) *string {
	return &value
}

func TestPinPostOnInitialPage(t *testing.T) {
	s := &Server{PinnedPostURI: testPinnedPostURI}
	out := &bsky.FeedGetFeedSkeleton_Output{
		Feed: []*bsky.FeedDefs_SkeletonFeedPost{
			{Post: "at://did:plc:example/app.bsky.feed.post/one"},
			{Post: "at://did:plc:example/app.bsky.feed.post/two"},
		},
	}

	s.pinPost(out, false)

	if got, want := len(out.Feed), 3; got != want {
		t.Fatalf("feed length = %d, want %d", got, want)
	}
	if got := out.Feed[0].Post; got != testPinnedPostURI {
		t.Fatalf("first post = %q, want pinned post %q", got, testPinnedPostURI)
	}
	if out.Feed[0].Reason == nil || out.Feed[0].Reason.FeedDefs_SkeletonReasonPin == nil {
		t.Fatal("pinned post does not have a skeletonReasonPin")
	}

	encoded, err := json.Marshal(out.Feed[0])
	if err != nil {
		t.Fatalf("marshal pinned post: %v", err)
	}
	if !strings.Contains(string(encoded), `"$type":"app.bsky.feed.defs#skeletonReasonPin"`) {
		t.Fatalf("pinned post JSON does not contain pin reason: %s", encoded)
	}
}

func TestPinPostNotAddedToCursorPage(t *testing.T) {
	s := &Server{PinnedPostURI: testPinnedPostURI}
	original := &bsky.FeedDefs_SkeletonFeedPost{
		Post: "at://did:plc:example/app.bsky.feed.post/one",
	}
	out := &bsky.FeedGetFeedSkeleton_Output{
		Feed: []*bsky.FeedDefs_SkeletonFeedPost{original},
	}
	s.pinPost(out, true)

	if got, want := len(out.Feed), 1; got != want {
		t.Fatalf("feed length = %d, want %d", got, want)
	}
	if out.Feed[0] != original {
		t.Fatal("cursor page was modified")
	}
}

func TestPinPostMovesExistingPostInsteadOfDuplicatingIt(t *testing.T) {
	s := &Server{PinnedPostURI: testPinnedPostURI}
	pinned := &bsky.FeedDefs_SkeletonFeedPost{Post: testPinnedPostURI}
	out := &bsky.FeedGetFeedSkeleton_Output{
		Feed: []*bsky.FeedDefs_SkeletonFeedPost{
			{Post: "at://did:plc:example/app.bsky.feed.post/one"},
			pinned,
			{Post: testPinnedPostURI},
		},
	}

	s.pinPost(out, false)

	if got, want := len(out.Feed), 2; got != want {
		t.Fatalf("feed length = %d, want %d", got, want)
	}
	if out.Feed[0] != pinned {
		t.Fatal("existing pinned post was not moved to the top")
	}
	if pinned.Reason == nil || pinned.Reason.FeedDefs_SkeletonReasonPin == nil {
		t.Fatal("existing pinned post does not have a skeletonReasonPin")
	}
}

func TestMaintenanceFeedPinsOnlyWhenCursorParameterIsAbsent(t *testing.T) {
	s := &Server{
		Maintenance:        true,
		MaintenancePostUri: "at://did:plc:example/app.bsky.feed.post/maintenance",
		PinnedPostURI:      testPinnedPostURI,
	}

	tests := []struct {
		name      string
		target    string
		wantPosts []string
	}{
		{
			name:   "cursor-less initial page",
			target: "/xrpc/app.bsky.feed.getFeedSkeleton",
			wantPosts: []string{
				testPinnedPostURI,
				s.MaintenancePostUri,
			},
		},
		{
			name:   "empty cursor parameter is still a cursor page",
			target: "/xrpc/app.bsky.feed.getFeedSkeleton?cursor=",
			wantPosts: []string{
				s.MaintenancePostUri,
			},
		},
		{
			name:   "non-empty cursor page",
			target: "/xrpc/app.bsky.feed.getFeedSkeleton?cursor=next",
			wantPosts: []string{
				s.MaintenancePostUri,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest("GET", tt.target, nil)
			recorder := httptest.NewRecorder()

			if err := s.handleGetFeedSkeleton(e.NewContext(req, recorder)); err != nil {
				t.Fatalf("handleGetFeedSkeleton returned error: %v", err)
			}

			var out bsky.FeedGetFeedSkeleton_Output
			if err := json.Unmarshal(recorder.Body.Bytes(), &out); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if got, want := len(out.Feed), len(tt.wantPosts); got != want {
				t.Fatalf("feed length = %d, want %d; body: %s", got, want, recorder.Body.String())
			}
			for i, want := range tt.wantPosts {
				if got := out.Feed[i].Post; got != want {
					t.Errorf("feed[%d] = %q, want %q", i, got, want)
				}
			}
		})
	}
}

func TestPinPostDisabled(t *testing.T) {
	s := &Server{}
	original := &bsky.FeedDefs_SkeletonFeedPost{
		Post: "at://did:plc:example/app.bsky.feed.post/one",
	}
	out := &bsky.FeedGetFeedSkeleton_Output{
		Feed: []*bsky.FeedDefs_SkeletonFeedPost{original},
	}

	s.pinPost(out, false)

	if got, want := len(out.Feed), 1; got != want {
		t.Fatalf("feed length = %d, want %d", got, want)
	}
	if out.Feed[0] != original {
		t.Fatal("feed was modified when pinning was disabled")
	}
}

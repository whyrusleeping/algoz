# ATP Custom Algos

This repo is a WIP, working more on a nice framework for algorithms than any particular algorithm.

## Pinning a post

Set `PINNED_POST_URI` (or pass `--pinned-post-uri`) to an `app.bsky.feed.post` AT-URI when starting the server:

```sh
PINNED_POST_URI=at://did:plc:example/app.bsky.feed.post/record-key ./algoz run
```

The post is placed at the top of every feed's initial, cursor-less response and marked with `app.bsky.feed.defs#skeletonReasonPin`. It is not added to subsequent cursor pages. If the post is already in the first page, it is moved to the top rather than duplicated.



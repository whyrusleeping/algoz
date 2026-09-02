# Project memory

> Agent-maintained operational notes. Advisory, not normative — verify before relying.
> Design truth belongs in spec.md; procedures in plans/; work items in backlog/.

## Environment & tooling
- 2026-09-02: Production deploys run on SSH host `sirius` from `/home/why/code/algoz` in tmux session `0`, where `always_runalgoz.sh` loops `runalgoz.sh` and the latter launches `./algoz run ... | tee -a algoz.log`.
- 2026-09-02: Production `runalgoz.sh` currently sets neither `PINNED_POST_URI` nor `--pinned-post-uri`; verify deploys via `https://feeds.bluesky.day/xrpc/app.bsky.feed.describeFeedGenerator`, `/.well-known/did.json`, process uptime, and fresh 200 feed logs.
- 2026-09-02: The sirius checkout's `origin` is now HTTPS and local `master` tracks `origin/master`, so future deploys can use `git pull --ff-only`; local development uses GitHub SSH because HTTPS push hangs.
- 2026-09-02: Production is pinned to `at://did:plc:eon2iu7v3x2ukgxkqaf7e5np/app.bsky.feed.post/3mukm26duos2c` via `--pinned-post-uri` in sirius `runalgoz.sh`; backup is `runalgoz.sh.before-pin-20260902`.

## Lessons learned
- 2026-09-02: Deploy algoz on sirius by fetching public GitHub HTTPS (sirius lacks a GitHub SSH key), fast-forwarding, running `go test ./...`, building `algoz.new`, backing up and atomically replacing `algoz`, then TERMing only the current `./algoz run` child so the tmux loop restarts it.
- 2026-09-02: For a bsky.app post URL, resolve its handle with `com.atproto.identity.resolveHandle`, construct `at://<did>/app.bsky.feed.post/<rkey>`, add `--pinned-post-uri` to sirius `runalgoz.sh`, then TERM the algoz child for looped restart.

## Codebase gotchas
- 2026-09-02: Algoz startup on sirius logs a missing `uni_users_did` constraint and duplicate feed-key errors but continues serving; judge deployment health by process survival, public endpoints, and fresh successful feed requests.

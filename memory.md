# Project memory

> Agent-maintained operational notes. Advisory, not normative — verify before relying.
> Design truth belongs in spec.md; procedures in plans/; work items in backlog/.

## Environment & tooling
- 2026-09-02: Production deploys run on SSH host `sirius` from `/home/why/code/algoz` in tmux session `0`, where `always_runalgoz.sh` loops `runalgoz.sh` and the latter launches `./algoz run ... | tee -a algoz.log`.
- 2026-09-02: Production `runalgoz.sh` currently sets neither `PINNED_POST_URI` nor `--pinned-post-uri`; verify deploys via `https://feeds.bluesky.day/xrpc/app.bsky.feed.describeFeedGenerator`, `/.well-known/did.json`, process uptime, and fresh 200 feed logs.
- 2026-09-02: The sirius checkout's `origin` is now HTTPS and local `master` tracks `origin/master`, so future deploys can use `git pull --ff-only`; local development uses GitHub SSH because HTTPS push hangs.

## Lessons learned
- 2026-09-02: Deploy algoz on sirius by fetching public GitHub HTTPS (sirius lacks a GitHub SSH key), fast-forwarding, running `go test ./...`, building `algoz.new`, backing up and atomically replacing `algoz`, then TERMing only the current `./algoz run` child so the tmux loop restarts it.

## Codebase gotchas
- 2026-09-02: Algoz startup on sirius logs a missing `uni_users_did` constraint and duplicate feed-key errors but continues serving; judge deployment health by process survival, public endpoints, and fresh successful feed requests.

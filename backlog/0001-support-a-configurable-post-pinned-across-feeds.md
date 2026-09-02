---
id: "0001"
title: Support a configurable post pinned across feeds
status: done
priority: 2
created: "2026-09-02"
updated: "2026-09-02"
depends_on: []
spec_refs: []
---

## Description
Add an optional server setting for a post AT-URI. When configured, prepend that post (with pin metadata) to every successful feed response only on the initial request where no cursor is present. Do not inject it into scrolling pages. Avoid duplicate entries if the post is already in a feed, validate configuration, and add tests.

## Acceptance criteria

## Work log

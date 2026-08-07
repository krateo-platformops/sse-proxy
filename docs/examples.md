---
type: ExampleIndex
title: sse-proxy — examples
description: Index of the runnable examples — in-cluster consumers of the /events snapshot and the /notifications SSE stream.
resource: ghcr.io/krateo-platformops/sse-proxy
tags: [examples]
timestamp: 2026-08-07T00:00:00Z
---

# Examples

Both examples are one-shot Jobs that run **in-cluster** against a stock Krateo
installer deploy with the observability (clickstack) blueprint enabled. They discover
the proxy's URL from the fixed-name `sse-proxy-internal-endpoint` Secret the deployed
chart renders — no port-forwarding, no hardcoded Service names.

- [events-snapshot](../examples/events-snapshot/README.md) — fetch the recent-events
  JSON snapshot from `GET /events?limit=5` and print it.
- [notifications-stream](../examples/notifications-stream/README.md) — subscribe to
  `GET /notifications` for 30 s and print the raw SSE frames (connect comment,
  keepalives, event frames).

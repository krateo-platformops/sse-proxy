---
type: Example
title: Subscribe to the /notifications SSE stream
description: A one-shot Job that opens the SSE stream for 30 seconds and prints the raw frames — the connect comment, keepalives and any event frames.
resource: ghcr.io/krateo-platformops/sse-proxy
tags: [sse, notifications, job, curl]
timestamp: 2026-08-07T00:00:00Z
---

# Subscribe to the `/notifications` SSE stream

Opens `GET /notifications` with `curl -N` for 30 seconds and prints whatever the hub
pushes: you will always see the `: connected` comment and (past 25 s) a `: keepalive`;
any Kubernetes event landing in ClickHouse meanwhile arrives as an
`event: krateo` + `data: {...}` frame. Add `?composition_id=<uuid>` to the URL in
`job.yaml` to subscribe to a single composition's topic instead of the firehose.

## Preconditions

Same as [events-snapshot](../events-snapshot/README.md): a stock installer deploy with
the observability blueprint (the `sse-proxy-internal-endpoint` Secret in
`krateo-system`), auth at the stock default (off). To see event frames during the
window, cause any event — e.g. scale a Deployment.

## Apply

```sh
kubectl apply -f ./job.yaml
kubectl -n krateo-system wait --for=condition=complete --timeout=60s job/sse-proxy-notifications-stream
kubectl -n krateo-system logs job/sse-proxy-notifications-stream
```

Clean up with `kubectl delete -f ./job.yaml` (it also self-deletes after 5 minutes).

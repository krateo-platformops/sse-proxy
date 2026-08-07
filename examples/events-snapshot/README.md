---
type: Example
title: Fetch the /events snapshot in-cluster
description: A one-shot Job that reads the sse-proxy URL from the sse-proxy-internal-endpoint Secret and prints the recent-events JSON snapshot.
resource: ghcr.io/krateo-platformops/sse-proxy
tags: [events, job, curl]
timestamp: 2026-08-07T00:00:00Z
---

# Fetch the `/events` snapshot in-cluster

The smallest useful consumer: a Job that curls `GET /events?limit=5` and prints the
`SSEK8sEvent[]` JSON. It resolves the proxy's base URL from the
`sse-proxy-internal-endpoint` Secret (the same fixed-name Secret snowplow's
RESTActions use), so it works whatever the release/Service name is.

## Preconditions

- A stock Krateo installer deploy with the observability (clickstack) blueprint
  enabled — the `krateo-sse-proxy` chart and its `sse-proxy-internal-endpoint` Secret
  live in `krateo-system`.
- Auth left at the stock default (the deployed chart does not set `JWT_SIGN_KEY`, so
  the endpoint is open in-cluster). If you enabled auth, add an
  `Authorization: Bearer <jwt>` header to the curl.

## Apply

```sh
kubectl apply -f ./job.yaml
kubectl -n krateo-system wait --for=condition=complete job/sse-proxy-events-snapshot
kubectl -n krateo-system logs job/sse-proxy-events-snapshot
```

An empty array `[]` is a valid result on a quiet cluster (no Kubernetes events in the
lookback window). Clean up with `kubectl delete -f ./job.yaml` (it also self-deletes
after 5 minutes).

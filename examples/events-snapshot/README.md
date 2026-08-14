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
- Auth: a stock deploy **enforces** JWT validation (RS256 against authn's JWKS — the
  chart sets no `URL_AUTHN`, so the binary uses its in-cluster authn default). This Job's
  curl sends no token, so against a stock deploy `/events` answers **401**; add an
  `Authorization: Bearer <jwt>` header (a signed Krateo JWT) to `job.yaml` to get data.
  The plain curl succeeds only when the proxy is running open (both `URL_AUTHN` and
  `JWT_JWKS_URL` set empty).

## Apply

```sh
kubectl apply -f ./job.yaml
kubectl -n krateo-system wait --for=condition=complete job/sse-proxy-events-snapshot
kubectl -n krateo-system logs job/sse-proxy-events-snapshot
```

An empty array `[]` is a valid result on a quiet cluster (no Kubernetes events in the
lookback window). Clean up with `kubectl delete -f ./job.yaml` (it also self-deletes
after 5 minutes).

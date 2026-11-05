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
- Auth: JWT validation is mandatory (RS256 against authn's JWKS), and there is no open
  mode. Without a valid token `/events` answers **401** and the Job fails. The Job reads
  a signed Krateo JWT from a Secret:

```sh
kubectl -n krateo-system create secret generic sse-proxy-example-token \
  --from-literal=token="$(cat path/to/jwt)"
```

## Apply

```sh
kubectl apply -f ./job.yaml
kubectl -n krateo-system wait --for=condition=complete job/sse-proxy-events-snapshot
kubectl -n krateo-system logs job/sse-proxy-events-snapshot
```

An empty array `[]` is a valid result on a quiet cluster (no Kubernetes events in the
lookback window). Clean up with `kubectl delete -f ./job.yaml` (it also self-deletes
after 5 minutes).

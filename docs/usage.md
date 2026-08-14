---
type: Usage
title: sse-proxy — usage
description: How sse-proxy reaches a cluster — the Krateo installer via clickstack-chart, standalone helm install, the deploy/ reference manifest, local runs.
resource: oci://ghcr.io/krateo-platformops/charts/krateo-sse-proxy
tags: [install, helm, installer, clickstack]
timestamp: 2026-08-07T00:00:00Z
---

# Usage

This is a **source repo**: it publishes one artifact, the multi-arch image
`ghcr.io/krateo-platformops/sse-proxy` (currently `1.1.2`). **Its deployed chart lives
in [`clickstack-chart`](https://github.com/krateo-platformops/clickstack-chart)
(`charts/krateo-sse-proxy`)** — the observability blueprint repo — published as
`oci://ghcr.io/krateo-platformops/charts/krateo-sse-proxy`. There is no chart in this repo,
and no `sse-proxy-chart` repo (the CI's `<repo>-chart` convention does not apply here;
see [release](./release.md)).

## Path 1 — via the Krateo installer (the normal way)

sse-proxy ships inside the observability (clickstack) blueprint: the installer
umbrella emits a `CompositionDefinition` per clickstack-chart chart, and
`core-provider` reconciles `krateo-sse-proxy` as its own Composition alongside
`krateo-observability` and the two OTel collectors. A platform install with the
observability feature enabled gets sse-proxy with no extra steps; the portal's events
bell is wired to it out of the box, and snowplow reaches `/events` through the
`sse-proxy-internal-endpoint` Secret the chart renders.

A version bump is a change **in clickstack-chart** (the chart's `image.tag` /
`appVersion`), then to the installer's chart pin — never a `kubectl set image` on the
running Deployment.

## Path 2 — standalone `helm install`

The chart works without the installer, provided ClickHouse (clickstack) is reachable:

```sh
helm install sse-proxy oci://ghcr.io/krateo-platformops/charts/krateo-sse-proxy \
  --version 0.1.6 --namespace krateo-system \
  --set clickhouse.url=http://krateo-clickstack-clickhouse-clickhouse-headless.krateo-system.svc:8123
```

Chart values (image, `clickhouse.*`, `service.*`, resources) are documented in
[configuration](./configuration.md). Note the chart's deliberate `replicaCount: 1`
(stateful in-memory hub — see [overview](./overview.md)). The chart sets no `URL_AUTHN`,
so the binary falls back to its cluster-internal default and a stock deploy **enforces
auth** (RS256 against the in-cluster authn's JWKS); RBAC scoping stays off since the
chart wires no env for it.

## Path 3 — the `deploy/` reference manifest

[`deploy/deployment.yaml`](../deploy/deployment.yaml) is a standalone (non-Helm)
reference: Deployment + Service + the ServiceAccount / ClusterRole /
ClusterRoleBinding that RBAC scoping requires (`create subjectaccessreviews`,
`list namespaces`). It pins the published image `1.1.2` and shows the `URL_AUTHN` +
`RBAC_SCOPING_ENABLED` wiring (auth is RS256/JWKS against authn, always enforced) —
with the caveat that scoping only takes effect on an image built after `1.1.2` (the
feature is on `main`, unreleased; see [release](./release.md)).

```sh
kubectl apply -f deploy/deployment.yaml
```

## Running locally

The binary needs nothing but env vars:

```sh
CLICKHOUSE_URL=http://localhost:8123 LISTEN_ADDR=:8080 go run .
curl -s 'http://localhost:8080/events?limit=5'
```

`KUBERNETES_API_URL` overrides the in-cluster API endpoint so the RBAC scoper works
outside a pod (this is how the tests fake the API — `rbac_test.go`).

## Consuming it

- Browser: `EventSource('/notifications')`, optionally `?composition_id=<uuid>`.
- Anything else: `GET /events` — see [api](./api.md) and the
  [examples](./examples.md), which run in-cluster against a stock installer deploy.

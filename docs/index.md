---
type: Component
title: sse-proxy — index
description: The map of the sse-proxy doc bundle — the SSE hub that feeds the Krateo portal events/notifications bell from ClickHouse.
resource: ghcr.io/krateo-platformops/sse-proxy
tags: [portal, sse, events, observability]
timestamp: 2026-08-07T00:00:00Z
---

# sse-proxy

sse-proxy is the **event push channel of the Krateo portal**: it polls the
observability stack's ClickHouse (`otel_logs`, rows written by the clickstack OTel
collector's `k8sobjects` receiver) for new Kubernetes events and fans them out to
connected browsers over Server-Sent Events. The portal's events bell consumes both of
its surfaces: `GET /notifications` (the live SSE stream) and `GET /events` (the JSON
history snapshot, fetched through snowplow RESTActions).

This repo is the **source repo** — it ships one artifact, the multi-arch image
`ghcr.io/krateo-platformops/sse-proxy`. The deployed Helm chart lives in
[`clickstack-chart`](https://github.com/krateo-platformops/clickstack-chart)
(`charts/krateo-sse-proxy`, published as
`oci://ghcr.io/krateo-platformops/charts/krateo-sse-proxy`).

> **Release-state note (2026-08-07):** the latest published image is `1.1.2`. Two
> `main` features are **not in any published image yet**: the RBAC-derived multi-tenant
> scoping (#6) and the Go module identity migration — the `v1.2.0` tag that followed
> them shipped nothing (a `v`-prefixed tag matches no release trigger). The docs below
> describe `main` and flag what is release-gated. See [release](./release.md).

## The bundle (start here)

- [overview](./overview.md) — what it does and how it works: the poller, the hub,
  topics, auth, RBAC scoping, OTel gating, its place between clickstack / snowplow /
  frontend.
- [usage](./usage.md) — how it reaches a cluster: the installer → clickstack-chart
  path, standalone helm, the `deploy/` reference manifest, local runs.
- [configuration](./configuration.md) — the whole config surface (env vars) and how
  the deployed chart wires them.
- [api](./api.md) — the HTTP/SSE contract: `/events`, `/notifications`, `/health`,
  the `SSEK8sEvent` payload, auth acceptance.
- [examples](./examples.md) — the runnable examples under `examples/`.
- [release](./release.md) — how a release ships, and the CI conventions that do (and
  deliberately do not) apply to this repo.
- [log](./log.md) — curated history.
- [llms.txt](./llms.txt) — the version-pinned agent index of this bundle.

## Code-adjacent internals

The implementation is small (nine Go files, heavily commented — the package and
per-file doc comments in [`main.go`](../main.go), [`auth.go`](../auth.go),
[`rbac.go`](../rbac.go), [`telemetry.go`](../telemetry.go), [`metrics.go`](../metrics.go)
and [`logging.go`](../logging.go) are the authoritative internals documentation).
[`deploy/deployment.yaml`](../deploy/deployment.yaml) is the standalone reference
manifest, including the ServiceAccount RBAC that scoping needs.

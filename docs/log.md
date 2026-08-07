---
type: Log
title: sse-proxy — log
description: Curated chronological history — notable changes, decisions and incidents; release notes stay in GitHub Releases.
resource: ghcr.io/krateo-platformops/sse-proxy
tags: [history]
timestamp: 2026-08-07T00:00:00Z
---

# Log

Curated history, newest first.

## 2026-08-07 — adopted the Krateo Documentation Standard

This bundle: root `docs/` + `examples/` + thin README, CI-linted. Fixed en route: the
README's dead-org plumbing import path (pre-migration org); `deploy/deployment.yaml`
re-grounded in reality (image `0.1.0` → the published `1.1.2`, a comment pointing at a
never-existing `sse-proxy.yaml` workflow, `replicas: 2` vs the deployed chart's
deliberate single-replica hub, ClickHouse URL/Service type aligned with the deployed
chart). Documented the release gap: `v1.2.0` was a `v`-prefixed tag and shipped no
image, so RBAC scoping and the module-identity migration are on `main` only.

## 2026-08-03/05 — Go module identity migration + shared CI (unreleased)

Module renamed `github.com/krateo-platformops/sse-proxy` (plumbing v1.13.0), the
release workflow re-pointed at the org's shared `component-image-build` (#7), and the
repo tagged `v1.2.0` — which matched no release trigger (the `v` prefix), so no
artifact shipped. Next bare-semver tag releases both this and RBAC scoping.

## 2026-07-20 — server-side RBAC-derived multi-tenant scoping (#6, unreleased)

`RBAC_SCOPING_ENABLED` derives each caller's authorized-namespace set from Kubernetes
RBAC (SubjectAccessReview, fail-closed, 60 s cache) and enforces it in the ClickHouse
queries and at SSE fan-out. Requires JWT auth; ships with the SA RBAC in `deploy/`.

## 2026-06-25 — 1.1.2: gated OTel + structured logging (#4, #5)

Default-off OTel (master gate `OTEL_ENABLED`, per-signal overrides; OTel-Go v1.44):
otelhttp spans, the three `sse_proxy_*` metrics, always-on JSON slog with trace
correlation. `1.1.2` is the image the platform runs today.

## 2026-06-22 — 1.1.1: composition filter keys on `involvedObject.uid` (#3)

Composition event panels were empty because `/events?composition_id=` filtered on
`LogAttributes['krateo.io/composition-id']` — which the collector resolves to the
*owning* composition, and only for pod-associated objects, so it never matches a
top-level composition (e.g. a user blueprint). The filter now keys on the event's own
`involvedObject.uid`. Shipped live with chart 0.1.5.

## 2026-06-22 — 1.1.0: composition topics + snowplow-style JWT auth (#1)

Per-composition SSE topics and server-side `composition_id`/`limit` filtering (bound
ClickHouse parameters), plus opt-in HS256 JWT validation identical to snowplow's
(`JWT_SIGN_KEY`; header/cookie/query acceptance for EventSource).

## 2026-06-15 — 1.0.0: split out of `krateo-clickstack`

Extracted from the former multi-image clickstack code repo into a single-image repo on
the canonical Krateo CI; the chart stayed with the observability blueprint in
clickstack-chart. Along the way the deployed chart moved to a **single replica**: each
pod is its own stateful hub+poller, and an L4 LB pins each client to one backend, so a
degraded replica broke a fixed subset of portal bells.

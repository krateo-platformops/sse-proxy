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

## 2026-11-10 — /events is bounded, prunable, and answers before its caller gives up
A load test on krateo-057 timed out nearly every `/events` read from about 70 req/s of portal
traffic, and surfaced them as snowplow 5xx from about 310 req/s. Two causes:

- A scoped read of a quiet namespace never filled `limit`, so it widened 1h → 24h → unbounded.
  The namespace filter was a JSON extraction over `Body`, which cannot prune, so the last step
  parsed every k8s-events row in the table. Measured per request: 3 queries, 246 granules,
  ~1.47M rows, 1.66 GiB, ~3.7 CPU-s on an idle ClickHouse.
- Nothing bounded the query on the server. 2,438 reads ran past the 10 s at which snowplow had
  given up, up to 64 s, and all of them ran to completion.

Now every read is time-bounded (`?window=`, default 24h, max 168h, on the primary-key column).
A scoped read also filters on `ResourceAttributes['k8s.namespace.name']`, which a bloom index
can prune, and keeps the exact JSON predicate. The same request costs 2 queries, 2 granules,
~30k rows, 14 MiB and ~76 CPU-ms, with byte-identical rows. A 4 s deadline
(`EVENTS_QUERY_TIMEOUT`) answers `200` with `X-Events-Status: partial|timeout` instead of
letting the caller time out. ClickHouse stops at the same deadline (`max_execution_time`, and
cancel-on-close with `readonly=2`). Behaviour change: an `/events` call without `window` no
longer returns events older than 24h.

## 2026-11-05 — query-string tokens refused, and CORS preflights answered
`?access_token=` and `?token=` are gone (#40, installer#395). They existed because
`EventSource` cannot set headers, and a token in a URL reaches access logs and spans. A request
carrying either now gets 401 even with valid credentials, and a warning names the parameter.
Removed only after the portal moved to `fetch` with a Bearer header (frontend#501) and every
cluster running this proxy had that frontend.

That move also exposed a gap. A cross-origin `fetch` with `Authorization` needs a preflight,
which this proxy answered with 401, so the bell could not connect on a cluster with no gateway
answering preflights. a0-openstack-d3 was in that state. `withCORS` now answers them first.

## 2026-08-14 — auth hardened to mandatory-by-design (no pass-through, unreleased)

Removed the escape hatch: the proxy now has **no open/pass-through mode**. If no JWKS
source can be resolved — i.e. `URL_AUTHN` **and** `JWT_JWKS_URL` are both empty — it is
a **fatal error at startup** and the process refuses to start rather than serving
unauthenticated. The startup log line is now `auth ENFORCED (RS256/JWKS, mandatory)`.
This supersedes the earlier "enforced by default (fatal only if both empty)" note below:
enforcement is no longer a default you could opt out of by blanking both vars — that
combination is now fail-closed, not open. For a local dev harness, point `URL_AUTHN` /
`JWT_JWKS_URL` at a fake JWKS server instead of trying to disable auth.
`RBAC_SCOPING_ENABLED` stays genuinely opt-in; since auth is always on, it now just
requires the JWKS source to resolve.

## 2026-08-14 — JWT auth migrated to RS256/JWKS against authn (unreleased)

Replaced the original shared-secret HMAC verification with stateless **RS256**
signature verification against authn's **public** key, resolved by the token's `kid`
from authn's **JWKS** endpoint (`<URL_AUTHN>/.well-known/jwks.json`) via
`plumbing/jwtutil.ValidateWithKeySource` — mirroring how snowplow verifies. The proxy
now holds no shared secret and key rotation needs no redeploy; only RS256 is accepted
(HMAC tokens are rejected, closing off algorithm confusion). Config mirrors snowplow:
`URL_AUTHN` (with a cluster-internal default) plus the optional `JWT_JWKS_URL` override
and the `JWT_JWKS_CACHE_TTL` / `JWT_JWKS_MIN_REFRESH_INTERVAL` /
`JWT_JWKS_REQUEST_TIMEOUT` tuning knobs. Auth now **enforces by default** (the
`URL_AUTHN` default always builds a key source); to run open, set both `URL_AUTHN` and
`JWT_JWKS_URL` empty. Errors split cleanly: a token fault is `401`, an unreachable JWKS
endpoint or unknown `kid` is a transient `503`. `RBAC_SCOPING_ENABLED` still requires
auth enabled (now: fatal only if `URL_AUTHN` and `JWT_JWKS_URL` are both empty).

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
ClickHouse parameters), plus opt-in JWT validation identical to snowplow's at the time
(header/cookie/query acceptance for EventSource). The verification mechanism was later
migrated to RS256/JWKS (see the 2026-08-14 entry).

## 2026-06-15 — 1.0.0: split out of `krateo-clickstack`

Extracted from the former multi-image clickstack code repo into a single-image repo on
the canonical Krateo CI; the chart stayed with the observability blueprint in
clickstack-chart. Along the way the deployed chart moved to a **single replica**: each
pod is its own stateful hub+poller, and an L4 LB pins each client to one backend, so a
degraded replica broke a fixed subset of portal bells.

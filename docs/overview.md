---
type: Architecture
title: sse-proxy — overview
description: How the SSE proxy works — the ClickHouse poller, the in-memory fan-out hub, topics, RS256/JWKS JWT auth, RBAC-derived tenant scoping, and gated OTel.
resource: ghcr.io/krateo-platformops/sse-proxy
tags: [architecture, sse, clickhouse, events]
timestamp: 2026-08-07T00:00:00Z
---

# Overview

One process, three moving parts: a **poller**, a **hub**, and the **HTTP surface**.
Everything is standard library except the Krateo plumbing JWT helper
(`github.com/krateo-platformops/plumbing/jwtutil`) and the gated OTel SDK — no
client-go, no ClickHouse driver (plain HTTP with `FORMAT JSONEachRow`).

## Where the events come from

The clickstack OTel collector's `k8sobjects` receiver writes every Kubernetes event as
raw JSON into ClickHouse `otel_logs.Body` (tagged
`ResourceAttributes['telemetry.source'] = 'k8s-events'`); the collector's
`compositionresolver` processor enriches `LogAttributes` with
`krateo.io/composition-id` for objects it can associate to an owning composition. The
proxy is a pure reader of that table — the observability (clickstack) blueprint must be
installed for it to have anything to serve.

## The poller

Every **3 seconds** (`main.go`, `poller.run`) — and only while at least one SSE client
is connected — the poller queries `otel_logs` for event rows newer than its
`lastSeenUnix` watermark (capped at 500 rows, `pollSQL`). The watermark initialises to
one hour ago so a restart surfaces recent history. Each row is mapped to an
`SSEK8sEvent` and broadcast twice:

- under the global topic **`krateo`** (the firehose the bell listens to), and
- under the topic **`<composition-id>`** when the row carries one, so
  per-composition subscribers receive only their own events.

## The hub

An in-memory registry of connected clients (`hub` in `main.go`). Delivery is
**non-blocking**: each client has a 64-message buffer and slow clients drop messages
rather than stall the fan-out. A client subscribed with `?composition_id=` receives
only that topic; otherwise it gets every message (the browser filters by SSE event
name). A keepalive comment goes out every 25 s to hold intermediaries open.

**The hub is stateful and per-pod** — each replica has its own poller, watermark and
client set. That is why the deployed chart deliberately runs **one replica**
(`replicaCount: 1` in `clickstack-chart/charts/krateo-sse-proxy/values.yaml`): behind
an L4 LoadBalancer a client pins to one backend, so with multiple replicas a degraded
one breaks a fixed subset of users. The poller resumes from its watermark on restart.

## The HTTP surface

Three endpoints (contract details in [api](./api.md)):

| Endpoint | What |
|---|---|
| `GET /events` | on-demand JSON snapshot straight from ClickHouse (limit ≤ 200, optional `composition_id` filter) |
| `GET /notifications` | the SSE stream (also `/notifications/`) |
| `GET /health` | unauthenticated probe |

The `/events` `composition_id` filter deliberately keys on the event's own
`involvedObject.uid` — **not** on `LogAttributes['krateo.io/composition-id']`, which
the collector resolves to the *owning* composition and only for pod-associated
objects, so it is absent or wrong for top-level compositions like user blueprints
(fixed in 1.1.1; see [log](./log.md)). Both `composition_id` and `limit` are bound as
ClickHouse query parameters (`{name:Type}`), never string-concatenated; UUID / integer
validation happens first.

## Authentication (enforced by default)

`/events` and `/notifications` validate the caller's Krateo JWT exactly as snowplow
does: stateless **RS256** signature verification against authn's **public** key,
resolved by the token's `kid` from authn's **JWKS** endpoint
(`<URL_AUTHN>/.well-known/jwks.json`) via `plumbing/jwtutil.ValidateWithKeySource`.
authn signs with its RSA private key and publishes the matching public key set, so the
proxy holds **no shared secret** and key rotation needs no redeploy. Only RS256 is
accepted — a token offering HMAC is rejected before the key is consulted, so algorithm
confusion is impossible. Because the browser `EventSource` API cannot set headers, the
SSE path also accepts the token via the session cookie (`krateo-session` by default) or
`?access_token=` / `?token=` (accepted as the documented EventSource fallback; the proxy
never logs request URLs).

Auth **enforces by default**: `URL_AUTHN` carries a cluster-internal default
(`http://authn.krateo-system.svc.cluster.local:8082`), so the key source is always
built. To run the proxy open (e.g. a local dev harness), set **both** `URL_AUTHN` and
`JWT_JWKS_URL` empty ⇒ all endpoints open, logged loudly at startup (`auth.go`). A token
fault (expired/invalid) is a **401**; an unreachable JWKS endpoint or unknown `kid` is a
transient **503** that says nothing about the token.

## Multi-tenant scoping (opt-in, `main`-only for now)

With `RBAC_SCOPING_ENABLED=true` (`rbac.go`, **not yet in a published image** — see
[release](./release.md)) the proxy enforces per-tenant namespace scoping at the query
boundary; the frontend is never trusted:

1. Identity (username + groups) comes from the **verified** JWT — scoping refuses to
   start without auth enabled (fail closed).
2. A `SubjectAccessReview` asks whether the subject can `list events` (verb / resource /
   apiGroup configurable) **cluster-wide** — if yes, the caller is unscoped.
3. Otherwise one SAR per namespace (8-way bounded concurrency) derives the allowed
   set, which is injected as a bound `Array(String)` predicate on `/events` and
   enforced per-message at SSE fan-out. Empty set or any resolution error ⇒ no data.

Scopes are cached per (user, groups) for `RBAC_SCOPING_CACHE_TTL` (default 60 s); an
SSE connection keeps the scope resolved at connect time. Scoped callers never see
cluster-scoped (namespace-less) events. The Kubernetes API is called with the pod's own
ServiceAccount via a minimal stdlib client; it needs `create subjectaccessreviews` +
`list namespaces` ([`deploy/deployment.yaml`](../deploy/deployment.yaml)).

## Observability of the proxy itself

All OTel is **gated, default-off** (`telemetry.go`): `OTEL_ENABLED` is the master
gate, `OTEL_TRACING_ENABLED` / `OTEL_METRICS_ENABLED` default to it and override it
when set. When on: OTLP/HTTP export, `service.name=sse-proxy`, W3C trace propagation,
`otelhttp` server spans on the mux (health filtered out) and client spans on the
ClickHouse calls. Three metrics (`metrics.go`): `sse_proxy_connected_clients`,
`sse_proxy_events_delivered_total`, `sse_proxy_poll_errors_total`. Logging is always-on
structured JSON slog with trace correlation when a span is active (`logging.go`).

## Platform peers

- **frontend** — the portal events bell opens `EventSource` on `/notifications`.
- **snowplow** — the `composition-events` / `events` RESTActions fetch `/events`
  in-cluster through the fixed-name `sse-proxy-internal-endpoint` Secret rendered by
  the deployed chart.
- **clickstack** (ClickHouse + OTel collectors) — the data source; sse-proxy ships in
  the same blueprint ([usage](./usage.md)).

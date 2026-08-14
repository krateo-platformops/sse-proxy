---
type: Configuration
title: sse-proxy — configuration
description: Every env var sse-proxy reads, with defaults and release-state caveats, and how the deployed chart in clickstack-chart wires them.
resource: ghcr.io/krateo-platformops/sse-proxy
tags: [configuration, env]
timestamp: 2026-08-07T00:00:00Z
---

# Configuration

The whole config surface is **environment variables** — no flags, no config files.
Grouped by the file that reads them.

## Core (`main.go`)

| Var | Default | Purpose |
|---|---|---|
| `CLICKHOUSE_URL` | `http://krateo-clickstack-clickhouse.clickhouse-system.svc:8123` | ClickHouse HTTP endpoint the poller and `/events` query. The code default is a bare-local convenience; the **deployed** chart sets `http://krateo-clickstack-clickhouse-clickhouse-headless.krateo-system.svc:8123` — always set this explicitly. |
| `CLICKHOUSE_USER` | `default` | ClickHouse basic-auth user |
| `CLICKHOUSE_PASSWORD` | empty | ClickHouse basic-auth password (chart: from the `clickhouse-credentials` Secret, optional) |
| `LISTEN_ADDR` | `:8080` | listen address |

## Logging (`logging.go`)

| Var | Default | Purpose |
|---|---|---|
| `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error` — JSON slog to stderr, always on |

## Authentication (`auth.go`) — mandatory, enforced by design

Validation is stateless **RS256** against authn's **public** key, resolved by the
token's `kid` from authn's **JWKS** endpoint — the proxy holds no shared secret, and
key rotation needs no redeploy (mirrors snowplow). Only RS256 is accepted (HMAC tokens
are rejected, so algorithm confusion is impossible). Auth is **mandatory** — there is
no open/pass-through mode. `URL_AUTHN` has a cluster-internal default, so a JWKS source
is normally always resolved; if **both** `URL_AUTHN` and `JWT_JWKS_URL` are empty the
proxy **refuses to start** (fatal error, logged as `auth ENFORCED (RS256/JWKS,
mandatory)`) rather than serving unauthenticated. For a local dev harness, point
`URL_AUTHN` / `JWT_JWKS_URL` at a fake JWKS server instead.

| Var | Default | Purpose |
|---|---|---|
| `URL_AUTHN` | `http://authn.krateo-system.svc.cluster.local:8082` | authn's base URL (the identical env snowplow reads); the JWKS document URL is derived as `<URL_AUTHN>/.well-known/jwks.json`. Setting this **and** `JWT_JWKS_URL` empty does **not** disable auth — with no JWKS source the proxy refuses to start (fail-closed) |
| `JWT_JWKS_URL` | empty (derived from `URL_AUTHN`) | full JWKS document URL, overriding the `URL_AUTHN`-derived one |
| `JWT_JWKS_CACHE_TTL` | `5m` | how long a fetched JWKS key set is cached (Go duration) |
| `JWT_JWKS_MIN_REFRESH_INTERVAL` | `30s` | minimum gap between JWKS refetches on an unknown `kid` (Go duration) |
| `JWT_JWKS_REQUEST_TIMEOUT` | `5s` | timeout for a single JWKS fetch (Go duration) |
| `REFRESH_SESSION_COOKIE` | `krateo-session` | cookie name the SSE path reads the token from (snowplow's convention) |

## RBAC scoping (`rbac.go`) — opt-in, **unreleased** (on `main`, not in image `1.1.2`; see [release](./release.md))

| Var | Default | Purpose |
|---|---|---|
| `RBAC_SCOPING_ENABLED` | `false` | enforce SubjectAccessReview-derived per-tenant namespace scoping. **Requires the JWKS source to resolve** (auth is always on); since the proxy already refuses to start with `URL_AUTHN` and `JWT_JWKS_URL` both empty, scoping identity is always backed by a verified token |
| `RBAC_SCOPING_VERB` | `list` | the RBAC verb the scoping check keys on |
| `RBAC_SCOPING_RESOURCE` | `events` | the RBAC resource |
| `RBAC_SCOPING_APIGROUP` | empty (core) | the RBAC apiGroup |
| `RBAC_SCOPING_CACHE_TTL` | `60s` | per-(user, groups) scope cache TTL (Go duration) |
| `KUBERNETES_API_URL` | in-cluster | Kubernetes API endpoint override (tests / local runs; in-cluster uses the mounted ServiceAccount token + CA) |

The ServiceAccount needs `create subjectaccessreviews.authorization.k8s.io` and
`list namespaces` ([`deploy/deployment.yaml`](../deploy/deployment.yaml)).

## OpenTelemetry (`telemetry.go`) — gated, default-off

| Var | Default | Purpose |
|---|---|---|
| `OTEL_ENABLED` | `false` | master gate; the default for both per-signal flags |
| `OTEL_TRACING_ENABLED` | = `OTEL_ENABLED` | traces: otelhttp server spans + ClickHouse client spans, OTLP/HTTP export |
| `OTEL_METRICS_ENABLED` | = `OTEL_ENABLED` | metrics: the three `sse_proxy_*` instruments, OTLP/HTTP export |
| `OTEL_EXPORTER_OTLP_ENDPOINT` (+ standard `OTEL_EXPORTER_OTLP_*`) | SDK defaults | where signals go — read by the OTLP exporters themselves |

With everything off (the default) no provider, exporter or propagator is registered —
behaviour is identical to the pre-OTel binary.

## How the deployed chart wires this

The chart (`clickstack-chart/charts/krateo-sse-proxy`) sets **only**
`CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` (Secret-sourced, optional)
and `LISTEN_ADDR`, from these values:

| Value | Default |
|---|---|
| `image.registry` / `image.repository` / `image.tag` | `ghcr.io` / `krateo-platformops/sse-proxy` / `1.1.2` (`global.imageRegistry` overrides the host for mirrors/air-gap) |
| `replicaCount` | `1` — deliberate: the hub is a stateful in-memory singleton ([overview](./overview.md)) |
| `service.type` / `service.port` | `ClusterIP` / `8080` |
| `clickhouse.url` / `clickhouse.user` / `clickhouse.passwordSecret` | the in-cluster clickstack ClickHouse; `clickhouse-credentials`/`password`, optional |
| `listenAddr` | `:8080` |
| `resources`, `podAnnotations`, `nodeSelector`, `tolerations`, `affinity`, `serviceAccount.*`, `imagePullSecrets` | standard knobs |

Because the chart sets **no** `URL_AUTHN`, the binary falls back to its cluster-internal
default (`http://authn.krateo-system.svc.cluster.local:8082`), so a stock deploy runs
with **auth enforced** against the in-cluster authn's JWKS — no chart change needed.
There is no open/pass-through mode to opt into: clearing `URL_AUTHN` (and `JWT_JWKS_URL`)
would leave no JWKS source, and the proxy would refuse to start rather than serve
unauthenticated. **RBAC scoping and OTel** stay **off** in a stock deploy — the chart exposes
no values for those env vars today, so enabling them means adding the env to the chart
(or using the [`deploy/`](../deploy/deployment.yaml) reference manifest). The chart also
renders the fixed-name `sse-proxy-internal-endpoint` Secret (`server-url` pointing at
its own Service) that snowplow RESTActions consume.

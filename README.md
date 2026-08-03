# krateo-sse-proxy

Stateful in-memory **Server-Sent-Events hub** for the Krateo portal notifications/events bell.
Each pod runs its own poller (querying ClickHouse) and an SSE hub. Deployed by the
`krateo-sse-proxy` chart in
[`krateo-clickstack-chart`](https://github.com/krateo-platformops/krateo-clickstack-chart).

> Split out of the former multi-image `krateo-clickstack` code repo so each component is a
> single-image repo on the canonical Krateo CI (one multi-platform `release-tag.yaml`).

## Endpoints

| Endpoint | Kind | Query params |
| --- | --- | --- |
| `GET /events` | JSON snapshot — `SSEK8sEvent[]` of recent events | `composition_id` (UUID, server-side filter; absent ⇒ all activity), `limit` (1–200, default & cap 200) |
| `GET /notifications` (and `/notifications/`) | SSE stream (`text/event-stream`) | `composition_id` (subscribe to one composition's events; absent ⇒ global firehose, client filters by SSE event name) |
| `GET /health` | liveness/readiness probe (unauthenticated) | — |

Each event object now includes `compositionId` (the `krateo.io/composition-id`, empty for
cluster-wide events).

`composition_id` and `limit` are bound as ClickHouse query parameters (`{name:Type}`), never
string-concatenated into SQL; `composition_id` is additionally validated as a UUID.

## Authentication

`/events` and `/notifications` validate the user's krateo JWT **the same way snowplow does** —
stateless HMAC (`HS256`) verification against the shared `JWT_SIGN_KEY` secret via
`github.com/krateoplatformops/plumbing/jwtutil` (no JWKS, no call-out to `authn`). The portal
RESTAction forwards it as `Authorization: Bearer <jwt>`. Because the browser `EventSource` API
cannot set headers, `/notifications` also accepts the token via the `krateo-session` cookie or a
`?access_token=` / `?token=` query parameter.

Auth is **opt-in**: when `JWT_SIGN_KEY` is unset the endpoints stay open (previous behaviour);
set it to enforce validation. `/health` is always unauthenticated.

## Multi-tenant scoping (server-side, RBAC-derived)

With `RBAC_SCOPING_ENABLED=true` the proxy enforces **per-tenant namespace scoping at the query
boundary** — the frontend is never trusted; a direct API call with a valid token still only sees
what its Kubernetes RBAC allows:

1. The caller's identity (username + groups) comes from the **verified** JWT.
2. The proxy asks the local API server, via `SubjectAccessReview`, whether that subject can
   `list events` (verb/resource configurable) **cluster-wide** — if yes, the caller is unscoped
   (org/fleet admin).
3. Otherwise it lists all namespaces and issues one SAR per namespace; the allowed set becomes a
   mandatory filter, injected as a **bound** ClickHouse `Array(String)` parameter on `/events`
   (`involvedObject.namespace IN {namespaces}`) and enforced per-message at SSE fan-out on
   `/notifications`. An empty set ⇒ no data (**fail closed** — resolution errors also deny).

Results are cached per (user, groups) for `RBAC_SCOPING_CACHE_TTL` (default 60s); SSE streams
apply the scope resolved at connect time. Scoped callers never see cluster-scoped
(namespace-less) events. Scoping **requires** auth: enabling it without `JWT_SIGN_KEY` is fatal
at startup. The proxy's ServiceAccount needs `create subjectaccessreviews` + `list namespaces`
(see `deploy/deployment.yaml`).

### Configuration (env)

| Var | Default | Purpose |
| --- | --- | --- |
| `CLICKHOUSE_URL` | in-cluster ClickHouse | events source |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `default` / empty | ClickHouse basic auth |
| `LISTEN_ADDR` | `:8080` | listen address |
| `JWT_SIGN_KEY` | empty (auth disabled) | shared HMAC secret; must match snowplow/authn |
| `REFRESH_SESSION_COOKIE` | `krateo-session` | cookie name the SSE path reads the token from |
| `RBAC_SCOPING_ENABLED` | `false` | enforce RBAC-derived namespace scoping (requires `JWT_SIGN_KEY`) |
| `RBAC_SCOPING_VERB` / `RBAC_SCOPING_RESOURCE` / `RBAC_SCOPING_APIGROUP` | `list` / `events` / core | the RBAC rule that gates visibility |
| `RBAC_SCOPING_CACHE_TTL` | `60s` | per-(user,groups) scope cache TTL |
| `KUBERNETES_API_URL` | in-cluster | API endpoint override (tests/local runs) |

## Build & release
Image: `ghcr.io/krateo-platformops/sse-proxy`. Pushing a semver tag (`X.Y.Z`) builds and pushes
a multi-platform (`linux/amd64,linux/arm64`) image via the canonical `release-tag.yaml`.

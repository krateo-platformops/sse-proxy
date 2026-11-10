---
type: API
title: sse-proxy — API
description: The HTTP/SSE contract — /events, /notifications, /health, the SSEK8sEvent payload, token acceptance and injection-safety guarantees.
resource: ghcr.io/krateo-platformops/sse-proxy
tags: [api, http, sse]
timestamp: 2026-08-07T00:00:00Z
---

# API

sse-proxy owns **no CRDs** and exposes three HTTP endpoints (`main.go`). All payload
shapes mirror the frontend's `SSEK8sEvent` TypeScript interface.

## `GET /events` — JSON snapshot

Returns recent Kubernetes events as a JSON array of `SSEK8sEvent`, newest first.

| Query param | Constraint | Effect |
|---|---|---|
| `composition_id` | must be an RFC 4122 UUID, else `400` | server-side filter on the event's `krateo.io/composition-id` attribute, which covers the composition's own events and those of the resources it manages (see [overview](./overview.md)) |
| `limit` | clamped to `[1, 200]`, default `200` | max rows |
| `window` | a Go duration (`6h`, `90m`); default `EVENTS_DEFAULT_WINDOW` (`24h`), clamped to `EVENTS_MAX_WINDOW` (`168h`); unparseable or non-positive ⇒ `400` | how far back to look. Every query is time-bounded; there is no unbounded form |

All params are bound as ClickHouse query parameters, never concatenated into SQL.
Auth (mandatory): `Authorization: Bearer <jwt>` or the session cookie. With RBAC
scoping on, results are additionally filtered to the caller's authorized namespaces
(empty scope ⇒ `[]`, no ClickHouse round-trip).

### Completeness headers and the query deadline

The body is always a JSON array of `SSEK8sEvent` (possibly empty). Two response headers
say how complete it is; both are listed in `Access-Control-Expose-Headers`.

| Header | Values |
|---|---|
| `X-Events-Status` | `complete`: the full answer for the window. `partial`: the deadline passed after a narrower window finished; the body is the full answer for that narrower window and nothing older. `timeout`: the deadline passed before any window finished; the body is `[]` |
| `X-Events-Window` | seconds the body is a complete answer for: the requested window when `complete`, the narrower window when `partial`, `0` when `timeout` |

The deadline is `EVENTS_QUERY_TIMEOUT` (default `4s`) for all of one request's ClickHouse
work. It sits below snowplow's 10 s upstream timeout, so a slow ClickHouse produces a `200`
with `X-Events-Status: timeout` or `partial` instead of a cut-off request and a `5xx`.
ClickHouse stops at the same time: every query carries `max_execution_time` (the time left)
and `cancel_http_readonly_queries_on_client_close=1` with `readonly=2`. A deadline is logged
at most once a minute, with the number of suppressed occurrences. Any other ClickHouse
failure is still a `500`.

### How the read stays cheap

- The window is compared on `TimestampTime`, the `otel_logs` primary-key and partition
  column, so it prunes parts and granules.
- The request tries `1h`, then `24h`, then the requested window, stopping at the first that
  fills `limit`. A narrower window that already holds `limit` rows is provably the same
  answer as the wider one, because the query is `ORDER BY Timestamp DESC LIMIT n`.
- A scoped read also filters on `ResourceAttributes['k8s.namespace.name']`, which the
  `idx_res_attr_value` bloom index can prune. The exact
  `involvedObject.namespace` predicate stays as the authority on scope.

## `GET /notifications` — the SSE stream

Also served at `/notifications/`. Response is `text/event-stream` with
`X-Accel-Buffering: no`; the server writes:

- `: connected` — an initial comment confirming the subscription;
- `: keepalive` — a comment every 25 s;
- event frames:

  ```
  event: <topic>
  data: <SSEK8sEvent JSON>
  ```

  where `<topic>` is `krateo` (the global firehose) or a composition id.

| Query param | Effect |
|---|---|
| `composition_id` | subscribe to that composition's topic only; absent ⇒ the global firehose (the client filters by SSE event name) |

Credentials, in order: `Authorization: Bearer` header, then the session cookie
(`krateo-session`). A request with `access_token` or `token` in the query string gets 401
even with valid credentials, because a URL reaches access logs and spans. CORS preflights
are answered before authentication. Slow consumers drop messages (a buffer of one full poll, 500 messages) rather than stall the
hub; with RBAC scoping on, the scope is fixed at connect time and enforced per message.

## `GET /health`

Always unauthenticated; `200 ok`. Used by the liveness/readiness probes.

## The `SSEK8sEvent` payload

```json
{
  "metadata":       { "name": "...", "namespace": "...", "uid": "...", "creationTimestamp": "..." },
  "involvedObject": { "apiVersion": "...", "kind": "...", "name": "...", "namespace": "...", "uid": "..." },
  "reason": "...",
  "message": "...",
  "type": "Normal | Warning",
  "firstTimestamp": "...", "lastTimestamp": "...", "eventTime": "...",
  "source": { "component": "..." },
  "compositionId": "krateo.io/composition-id or empty"
}
```

`metadata` mirrors the involved object (name/namespace/uid) and `firstTimestamp` /
`lastTimestamp` / `creationTimestamp` all carry the resolved event time — the proxy
reconstructs the shape from the flattened ClickHouse row.

## Failure modes

| Condition | Response |
|---|---|
| missing/invalid/expired JWT (auth on) | `401` (plumbing error shape; no token material echoed) |
| malformed `composition_id` | `400` |
| ClickHouse or RBAC-scope resolution error | `500` (fail closed — a scoping error never widens access) |

## Metrics (when `OTEL_METRICS_ENABLED`)

`sse_proxy_connected_clients` (gauge), `sse_proxy_events_delivered_total` (counter),
`sse_proxy_poll_errors_total` (counter) — exported over OTLP, meter
`github.com/krateo-platformops/sse-proxy`.

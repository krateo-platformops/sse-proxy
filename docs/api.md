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
| `composition_id` | must be an RFC 4122 UUID, else `400` | server-side filter on the event's `involvedObject.uid` (the composition CR's uid — deliberately **not** the collector-resolved owning-composition attribute; see [overview](./overview.md)) |
| `limit` | clamped to `[1, 200]`, default `200` | max rows |

Both params are bound as ClickHouse query parameters, never concatenated into SQL.
Auth (when enabled): `Authorization: Bearer <jwt>` or the session cookie. With RBAC
scoping on, results are additionally filtered to the caller's authorized namespaces
(empty scope ⇒ `[]`, no ClickHouse round-trip).

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

Token acceptance (when auth is enabled), in order: `Authorization: Bearer` header →
session cookie (`krateo-session`) → `?access_token=` → `?token=`. The query-param
fallback exists only because `EventSource` cannot set headers; the proxy never logs
request URLs. Slow consumers drop messages (64-message buffer) rather than stall the
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

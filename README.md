# sse-proxy

Stateful in-memory **Server-Sent-Events hub** for the Krateo portal notifications/events
bell: it polls ClickHouse for new Kubernetes events and pushes them to connected browsers.

## What is this

A single-binary Go service (stdlib HTTP + one Krateo dependency): each pod runs a
ClickHouse poller and an SSE fan-out hub. The portal's events bell opens
`GET /notifications` (SSE) and fetches history from `GET /events` (JSON). This is the
**source repo** — it ships only the image `ghcr.io/krateo-platformops/sse-proxy`; the
deployed Helm chart lives in
[`clickstack-chart`](https://github.com/krateo-platformops/clickstack-chart)
(`charts/krateo-sse-proxy`). Full picture: [docs/index.md](docs/index.md).

## Install

Normally installed by the **Krateo installer** as part of the observability
(clickstack) blueprint — the `krateo-sse-proxy` chart is reconciled as a Composition.
Standalone of the installer, the same chart installs directly:

```sh
helm install sse-proxy oci://ghcr.io/krateo-platformops/charts/krateo-sse-proxy \
  --version 0.1.6 --namespace krateo-system
```

Details (installer path, standalone manifest, local run): [docs/usage.md](docs/usage.md).

## Configure

Everything is env vars — see [docs/configuration.md](docs/configuration.md). Most used:

| Var | Default | Effect |
|---|---|---|
| `CLICKHOUSE_URL` | in-cluster ClickHouse | where events are polled from |
| `JWT_SIGN_KEY` | empty (auth **disabled**) | opt-in HS256 JWT validation — shared secret with authn/snowplow |
| `RBAC_SCOPING_ENABLED` | `false` | opt-in server-side per-tenant namespace scoping (requires `JWT_SIGN_KEY`; on `main`, not yet in a published image) |

## Examples

- [examples/events-snapshot](examples/events-snapshot) — fetch the recent-events JSON
  snapshot from inside the cluster (a one-shot Job).
- [examples/notifications-stream](examples/notifications-stream) — subscribe to the live
  SSE stream and print the raw frames (a one-shot Job).

## Docs

- [docs/index.md](docs/index.md) — the map
- [docs/overview.md](docs/overview.md) — what it does and how it works
- [docs/usage.md](docs/usage.md) — how it is installed / consumed
- [docs/configuration.md](docs/configuration.md) — the whole config surface
- [docs/api.md](docs/api.md) — the HTTP/SSE contract
- [docs/examples.md](docs/examples.md) — examples index
- [docs/release.md](docs/release.md) — how a release ships
- [docs/log.md](docs/log.md) — curated history

## Develop & release

`go test ./...` (pure stdlib tests; the RBAC tests fake the Kubernetes API via
`KUBERNETES_API_URL`). Tag bare semver `X.Y.Z` — **no `v` prefix**, a `v`-tag ships
nothing — to build + push the multi-arch image. Runbook: [docs/release.md](docs/release.md).

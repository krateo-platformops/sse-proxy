---
type: Runbook
title: sse-proxy — release
description: How a release ships — a bare-semver tag builds the multi-arch image; rollout happens via clickstack-chart; the CI conventions that do and do not apply here.
resource: ghcr.io/krateo-platformops/sse-proxy
tags: [release, ci, oci]
timestamp: 2026-08-07T00:00:00Z
---

# Release

This repo releases **one artifact**: the image `ghcr.io/krateo-platformops/sse-proxy`.
The chart releases separately from
[`clickstack-chart`](https://github.com/krateo-platformops/clickstack-chart)
(`charts/krateo-sse-proxy` → `oci://ghcr.io/krateo-platformops/charts/krateo-sse-proxy`).

## The runbook

1. **Merge to `main`** with PR CI green
   ([`security.yml`](../.github/workflows/security.yml): the org's shared security
   scan + the docs-standard lint).
2. **Tag with plain semver — `X.Y.Z`, no `v` prefix.**
   [`release-tag.yaml`](../.github/workflows/release-tag.yaml) triggers on
   `[0-9]+.[0-9]+.[0-9]+` only; a `v`-prefixed tag ships **nothing**, silently. It has
   happened here: the `v1.2.0` tag (Go module identity migration + RBAC scoping)
   produced no image — as of 2026-08-07 GHCR holds only `1.1.2` (+ `latest`), so the
   scoping feature is still release-gated. The next release must be a bare tag
   `> 1.1.2`.

   ```sh
   git tag 1.2.1 && git push origin 1.2.1
   ```

3. **CI builds and publishes**, no manual steps: the `build` job calls the shared
   `component-image-build` workflow (`krateo-platformops/.github`) → multi-arch
   (amd64+arm64) image at `ghcr.io/krateo-platformops/sse-proxy:X.Y.Z`.
4. **Roll it out via clickstack-chart**: bump `image.tag` (default) + `appVersion` in
   `charts/krateo-sse-proxy`, release the chart, then bump the Krateo installer's pin
   (or `helm upgrade` a standalone install). A tag here rolls **nothing** by itself.

## The `crds` job — a convention that is a no-op here

`release-tag.yaml` also carries the org-canonical `crds` job, which (for CRD-owning
components) runs `make generate` and PRs the result into the `<repo>-chart` repo — for
this repo that would be `krateo-platformops/sse-proxy-chart`, **which does not exist**
(verified 2026-08-07). That is fine, by design of the job itself: this repo has no
`Makefile`/`make generate` target, so the generate step detects "not a CRD owner" and
skips before the chart-repo push is ever attempted. Reality to remember: sse-proxy's
chart home is clickstack-chart, not a `<repo>-chart` twin; if this repo ever grows
CRDs, the convention must be re-pointed first.

## Verify

```sh
TOKEN=$(curl -s "https://ghcr.io/token?scope=repository:krateo-platformops/sse-proxy:pull" | jq -r .token)
curl -s -H "Authorization: Bearer $TOKEN" \
  https://ghcr.io/v2/krateo-platformops/sse-proxy/tags/list
```

The new `X.Y.Z` must be listed before the clickstack-chart bump.

## Docs

`docs/llms.txt` pins this bundle; update its pin (and [log](./log.md), when notable)
in the release PR.

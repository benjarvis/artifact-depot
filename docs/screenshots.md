---
title: Screenshots
nav_order: 5
---

# UI Screenshots
{: .no_toc }

## Repositories

The Repositories page is the dashboard. The header summarises totals
across the cluster (artifact count, on-disk size, repository count) and
breaks the storage footprint down by repository in a pie chart. Every
artifact format -- raw, Docker, APT, Yum, PyPI, npm, Cargo, Helm, Go --
appears in the same flat list with a colour-coded format badge.

<div class="shot-pair">
  <a href="screenshots/repositories-light.png" target="_blank"><img src="screenshots/repositories-light.png" alt="Repositories list with stats header (light mode)"></a>
  <a href="screenshots/repositories-dark.png" target="_blank"><img src="screenshots/repositories-dark.png" alt="Repositories list with stats header (dark mode)"></a>
</div>

## Repository detail (Browse)

Clicking into a repository opens its artifact browser. The tree is
lazy-loaded; the Size column shows aggregated bytes per directory, and
the Updated column reflects last-modified times. Hosted, cache, and
proxy repos all share this layout -- only the top header changes.

<div class="shot-pair">
  <a href="screenshots/repository-detail-light.png" target="_blank"><img src="screenshots/repository-detail-light.png" alt="Repository detail browse (light mode)"></a>
  <a href="screenshots/repository-detail-dark.png" target="_blank"><img src="screenshots/repository-detail-dark.png" alt="Repository detail browse (dark mode)"></a>
</div>

## Stores

Blob stores are managed independently from repositories. The list shows
which storage backend each store uses (file or S3), the live blob count
maintained by GC, and the on-disk total.

<div class="shot-pair">
  <a href="screenshots/stores-light.png" target="_blank"><img src="screenshots/stores-light.png" alt="Stores list (light mode)"></a>
  <a href="screenshots/stores-dark.png" target="_blank"><img src="screenshots/stores-dark.png" alt="Stores list (dark mode)"></a>
</div>

## Store detail (Browse)

The store detail page exposes the physical content-addressable layout
underneath -- directories are the first two prefix bytes of each
BLAKE3 blob ID, with the actual blob files at the leaves.

<div class="shot-pair">
  <a href="screenshots/store-detail-light.png" target="_blank"><img src="screenshots/store-detail-light.png" alt="Store detail browse (light mode)"></a>
  <a href="screenshots/store-detail-dark.png" target="_blank"><img src="screenshots/store-detail-dark.png" alt="Store detail browse (dark mode)"></a>
</div>

## Tasks

The Tasks page lets an operator kick off the singleton background jobs
(Garbage Collection, Integrity Check, Rebuild Directory Entries) and
watch them run. Progress bars stream live byte and blob counts; the
last completed result for each job stays on the page until the next
run.

<div class="shot-pair">
  <a href="screenshots/tasks-light.png" target="_blank"><img src="screenshots/tasks-light.png" alt="Tasks page with a running integrity check (light mode)"></a>
  <a href="screenshots/tasks-dark.png" target="_blank"><img src="screenshots/tasks-dark.png" alt="Tasks page with a running integrity check (dark mode)"></a>
</div>

## Settings

Cluster-wide operational settings -- access logging, upload size limits,
GC interval, default Docker repo, CORS, rate limiting, tracing endpoint,
and JWT lifetimes. Changes propagate to every cluster instance within
the 30-second refresh window.

<div class="shot-pair">
  <a href="screenshots/settings-light.png" target="_blank"><img src="screenshots/settings-light.png" alt="Settings page (light mode)"></a>
  <a href="screenshots/settings-dark.png" target="_blank"><img src="screenshots/settings-dark.png" alt="Settings page (dark mode)"></a>
</div>

## API documentation

The full OpenAPI spec is rendered in-browser via Swagger UI. Every
endpoint can be expanded to inspect parameters, request bodies,
response schemas, and try-it-now interactively against the live
server.

<div class="shot-pair">
  <a href="screenshots/api-docs-light.png" target="_blank"><img src="screenshots/api-docs-light.png" alt="Swagger UI (light mode)"></a>
  <a href="screenshots/api-docs-dark.png" target="_blank"><img src="screenshots/api-docs-dark.png" alt="Swagger UI (dark mode)"></a>
</div>

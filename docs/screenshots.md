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
  <img src="screenshots/repositories-light.png" alt="Repositories list with stats header (light mode)">
  <img src="screenshots/repositories-dark.png" alt="Repositories list with stats header (dark mode)">
</div>

## Repository detail (Browse)

Clicking into a repository opens its artifact browser. The tree is
lazy-loaded; the Size column shows aggregated bytes per directory, and
the Updated column reflects last-modified times. Hosted, cache, and
proxy repos all share this layout -- only the top header changes.

<div class="shot-pair">
  <img src="screenshots/repository-detail-light.png" alt="Repository detail browse (light mode)">
  <img src="screenshots/repository-detail-dark.png" alt="Repository detail browse (dark mode)">
</div>

## Stores

Blob stores are managed independently from repositories. The list shows
which storage backend each store uses (file or S3), the live blob count
maintained by GC, and the on-disk total.

<div class="shot-pair">
  <img src="screenshots/stores-light.png" alt="Stores list (light mode)">
  <img src="screenshots/stores-dark.png" alt="Stores list (dark mode)">
</div>

## Store detail (Browse)

The store detail page exposes the physical content-addressable layout
underneath -- directories are the first two prefix bytes of each
BLAKE3 blob ID, with the actual blob files at the leaves.

<div class="shot-pair">
  <img src="screenshots/store-detail-light.png" alt="Store detail browse (light mode)">
  <img src="screenshots/store-detail-dark.png" alt="Store detail browse (dark mode)">
</div>

## Tasks

The Tasks page lets an operator kick off the singleton background jobs
(Garbage Collection, Integrity Check, Rebuild Directory Entries) and
watch them run. Progress bars stream live byte and blob counts; the
last completed result for each job stays on the page until the next
run.

<div class="shot-pair">
  <img src="screenshots/tasks-light.png" alt="Tasks page with a running integrity check (light mode)">
  <img src="screenshots/tasks-dark.png" alt="Tasks page with a running integrity check (dark mode)">
</div>

## Settings

Cluster-wide operational settings live here -- access logging, upload
size limits, GC interval / minimum gap, default Docker repo, CORS,
rate limiting, the logging endpoints (file / OTLP / Splunk HEC), the
tracing endpoint, and JWT lifetimes. Changes are pushed to the
settings handle and propagated to every cluster instance within the
30-second refresh window.

<div class="shot-pair">
  <img src="screenshots/settings-light.png" alt="Settings page (light mode)">
  <img src="screenshots/settings-dark.png" alt="Settings page (dark mode)">
</div>

## API documentation

The full OpenAPI spec is rendered in-browser via Swagger UI. Every
endpoint can be expanded to inspect parameters, request bodies,
response schemas, and try-it-now interactively against the live
server.

<div class="shot-pair">
  <img src="screenshots/api-docs-light.png" alt="Swagger UI (light mode)">
  <img src="screenshots/api-docs-dark.png" alt="Swagger UI (dark mode)">
</div>

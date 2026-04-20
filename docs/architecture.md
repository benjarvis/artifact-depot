---
title: Architecture
nav_order: 2
---

# Architecture

## Goals

- **Replace Nexus/Artifactory** with a purpose-built system focused on efficiency and performance
- **Single binary** -- one `depot` executable with embedded web UI; no JVM, no external dependencies for basic operation
- **Scale-out** -- pluggable storage backends scale from a laptop (redb + filesystem) to a distributed cluster (DynamoDB + S3)
- **Format-extensible** -- new artifact formats plug into the same repo/store abstraction

## High-Level Architecture

```
                     +---------------------------------------------+
                     |              HTTP (axum)                     |
                     |                                              |
                     |  /api/v1/...        REST API                 |
                     |  /repository/{r}/.. Unified artifact surface |
                     |                     (all formats dispatch on |
                     |                      the repo's format)      |
                     |  /v2/...            Docker Registry V2 / OCI |
                     |  /service/rest/v1/. Nexus-compat REST subset |
                     |  /swagger-ui/       OpenAPI docs             |
                     +--------------------+------------------------+
                                          |
                     +--------------------v------------------------+
                     |              API Layer                       |
                     |                                              |
                     |  repositories  artifacts  docker  auth       |
                     |  users  roles  stores  settings  logging     |
                     |  tasks  backup  system  nexus-compat         |
                     |  pypi  apt  yum  npm  cargo  helm  golang    |
                     +--------------------+------------------------+
                                          |
                     +--------------------v------------------------+
                     |            Repository Layer                  |
                     |                                              |
                     |  Hosted    Cache         Proxy (Group)       |
                     |  -----    -----         --------------       |
                     |  Local    Pull-through   Mux ordered         |
                     |  CRUD     + TTL          member list         |
                     +--------------------+------------------------+
                                          |
                    +---------------------+---------------------+
                    |                                           |
         +----------v-----------+              +---------------v-----------+
         |    KV Store trait     |              |    Blob Store trait        |
         |                      |              |                            |
         |  redb   |  DynamoDB  |              |  Filesystem  |  S3         |
         | (embed) | (distrib.) |              | (content-    |             |
         +---------+------------+              |  addressable)|             |
                                               +--------------+------------+
```

## Repository Types

### Hosted

Accept uploads directly. Artifacts are hashed (BLAKE3), deduplicated, and stored in the blob store. Metadata (path, size, content type, timestamps) is persisted in the KV store. Supports upload, download, delete, list, and search.

### Cache

Write-through cache of a remote upstream repository. On a cache miss, the artifact is fetched from the upstream URL, stored locally, and served. Subsequent requests within the TTL are served from the local cache. If the upstream is unreachable, stale cached content is served rather than returning an error.

Configuration requires `upstream_url` and optionally `cache_ttl_secs` (default: 0 = check on every access) and `upstream_auth` for authenticated upstreams.

### Proxy (Group)

Virtual repository that searches an ordered list of member repositories. A read request walks the member list and returns the first hit. Writes are routed to the first hosted member in the list. This allows combining hosted and cache repos behind a single URL. Recursive resolution up to depth 8.

## Artifact Formats

| Format | Protocol |
|--------|----------|
| Raw | REST PUT/GET/DELETE |
| Docker | OCI Distribution Spec (V2) |
| PyPI | PEP 503 Simple Index |
| APT | Debian repository with GPG signing |
| Go | Go module proxy protocol (GOPROXY) |
| Helm | Helm chart repository (index.yaml) |
| Cargo | Cargo sparse registry protocol |
| Yum | YUM/DNF repository with repomd |
| npm | npm registry protocol |

All formats support hosted, cache, and proxy repository types. Every format is served from the unified `/repository/{repo}/...` URL surface; the dispatcher in `server/src/server/api/artifacts.rs` inspects the repo's configured format and routes the request to the matching format crate. Docker / OCI is the lone exception: its clients hard-code the `/v2/` prefix, so it is routed directly from there.

### Docker / OCI

Full OCI Distribution Spec support including:

- `application/vnd.docker.distribution.manifest.v2+json` (Docker V2)
- `application/vnd.docker.distribution.manifest.list.v2+json` (Docker multi-arch)
- `application/vnd.oci.image.manifest.v1+json` (OCI)
- `application/vnd.oci.image.index.v1+json` (OCI multi-arch)

Docker routes come in two flavors:
- **Single-segment** `/v2/{name}/...` -- flat repos, used with `default_docker_repo`
- **Two-segment** `/v2/{repo}/{image}/...` -- namespaced, for proxy/cache/multi-image setups

Repositories can have a dedicated `listen` address that spawns an additional port where image names route directly to that repo without a path prefix.

## Storage Layer

### KV Store

The `KvStore` trait abstracts metadata storage. All records are serialized with string keys. Two backends are available:

#### redb (default)

Embedded, zero-dependency ACID key-value store. Depot runs redb in a **sharded** configuration (`scaling_factor`, default 8, must be a power of two): the `kv_store.path` is a directory containing one redb file per shard, with keys partitioned by hash. Each shard serializes its own write transactions, so effective write throughput scales with the number of shards. A shared page cache (`cache_size`, default 256 MiB) is split evenly across shards.

redb is single-node: it runs in the same process as `depot`, and clustering requires the DynamoDB backend.

#### DynamoDB (feature-gated)

AWS DynamoDB or any DynamoDB-compatible store (e.g. ScyllaDB Alternator). Scales horizontally with no schema migrations, no connection pooling, and no vacuum/compaction tuning. The DynamoDB backend is enabled via the `dynamodb` feature flag at compile time.

### Blob Store

The `BlobStore` trait abstracts content storage. Blob stores are created and managed at runtime via the Stores API (`/api/v1/stores`). There is no auto-bootstrap: on a fresh instance the server logs a warning and expects the operator (or a first-run script / compose init container) to create at least one store before repositories can accept writes. Each repository selects its store via the `store` field.

Two backend types are available:

#### Filesystem

Content-addressable layout with two-level prefix sharding:

```
<root>/<id[0:4]>/<id[4:8]>/<id>
```

Each blob is stored once; duplicate uploads are deduplicated by content hash. Options: `sync` (default `true`) fsyncs files and parent directories for durability; `io_size` (default 1024 KiB) sets the read/write buffer size; `direct_io` (default `false`) uses `O_DIRECT` to bypass the page cache.

#### S3

Stores blobs in an S3-compatible bucket (AWS S3, MinIO, Garage, etc.). Supports custom endpoints, region, prefix, and explicit credentials (falls back to the default AWS credential chain if not set). Per-request behaviour is tunable via `max_retries`, `connect_timeout_secs`, `read_timeout_secs`, and `retry_mode` (`"standard"` or `"adaptive"`).

## Content Addressing

All ingested content is hashed with **BLAKE3** (fast, parallelizable). If a blob with the same hash already exists, the new artifact points to the existing blob -- no duplicate data is stored.

Docker images use a **dual-hash** scheme: BLAKE3 is the internal blob store key, while SHA-256 digests (required by the OCI Distribution Spec) are stored as artifact metadata. This avoids re-hashing on every Docker pull.

## Consistency Model

The system is **eventually consistent by default**. Regular KV reads may return stale values in distributed deployments (DynamoDB). Strong consistency is used only for coordination paths:

- **Distributed leases** use `get_versioned()` + `put_if_version()` for atomic compare-and-swap
- **Integrity checks** acquire the GC lease before scanning
- **Settings** are cached in-memory and refreshed from KV every 30 seconds
- **Cross-instance state changes** (repos, stores, settings, tasks) are propagated via the state scanner's polling + event bus (default 1-second poll interval)

The redb backend is inherently strongly consistent (single-node). The DynamoDB backend uses eventually consistent reads by default and strongly consistent reads where required.

## Cluster Coordination

Multiple Depot instances can run against shared DynamoDB + S3 backends.

- **Instance registry**: each instance registers in KV with a hostname and periodic heartbeat (30-second interval, 90-second timeout). Instances that miss their heartbeat are considered dead.
- **Distributed leases**: mutual exclusion via versioned compare-and-swap. The well-known `gc` lease ensures only one instance runs blob garbage collection at a time.
- **Integrity checks** acquire the `gc` lease (with a distinguishing `:check` holder suffix) before scanning, which blocks the GC loop on every instance for the duration of the check.
- **Event bus / state scanner**: a lightweight state scanner polls the KV for cross-instance changes (repos, stores, settings, tasks) and publishes events on a local event bus; workers (e.g. the model materializer and the Docker listener manager) subscribe to reconcile local state without direct instance-to-instance RPC.

## Blob Garbage Collection

The blob reaper runs periodically (default: every 24 hours, configurable via the `gc_interval_secs` runtime setting; `gc_min_interval_secs` enforces a lower bound so manual triggers can't sweep candidates before the grace window has elapsed). GC is gated on holding the distributed `gc` lease, so at most one instance runs a pass at a time.

Each pass has three stages, all driven off a single bloom filter of live `blob_id`s built by scanning artifact records across all shards in parallel:

1. **Per-repo cleanup**: for repos with `cleanup_max_age_days` or `cleanup_max_unaccessed_days` set, expire old artifacts before building the filter. Docker repos with `cleanup_untagged_manifests` get an extra pre-phase that drops untagged manifests.
2. **Docker-format GC** (for each Docker repo): build a bloom filter of layer/config digests referenced by the repo's manifests, and delete `_blobs/sha256:*` artifacts that aren't referenced. A 1-hour grace window protects freshly uploaded blobs, since manifest/blob/tag creation involves multiple non-atomic KV writes.
3. **Unified blob sweep**: scan KV BlobRecords and delete records whose `blob_id` isn't in the live filter (safe -- removing a KV record only prevents future dedup). Then walk the blob store; files not in the filter **that were already candidates from the previous GC pass** are deleted, newly unreferenced files become candidates for the next pass, and previous candidates that are now referenced are cleared.

## Logging

Application logs and per-request events are emitted in two places:

- **stdout** -- standard `tracing` output. The per-request `depot.request` target is filtered out of stdout so only application logs land here.
- **OTLP** -- when `[logging].otlp_endpoint` is set, both application logs and per-request events are exported to an OpenTelemetry collector via the tracing bridge. Each per-request event carries `request_id`, `username`, `ip`, `method`, `path`, `status`, `action`, `elapsed_ns`, `bytes_recv`, `bytes_sent`, `direction`, and a `trace_id` when the current span is sampled.

For file, Splunk, or S3 archival, run an OpenTelemetry collector and point Depot's OTLP exporter at it. The collector's `splunkhec`, `awss3`, and `file` exporters handle batching, retries, and credentials. The bundled Grafana/Tempo/Loki compose stack demonstrates this topology.

## Metrics and Tracing

Prometheus-format metrics are served at `/metrics` on the main listener, or on a dedicated listener if `metrics_listen` is set in the TOML config. Tracked metrics include request counts, durations (by route, method, status), in-flight requests, KV and blob store operation rates and latencies, GC progress, and per-store blob counts and bytes.

OpenTelemetry tracing is enabled by setting `[tracing].otlp_endpoint`. Request spans, KV spans, and blob store spans are emitted via OTLP gRPC to any OTel collector (Tempo, Jaeger, Honeycomb, Datadog, etc.). Sampling defaults to an adaptive rate limiter (100 root traces/sec; configurable via `[tracing].max_traces_per_sec` or a fixed `sampling_ratio`); child spans on sampled traces are always kept.

## Authentication & Authorization

- **Password hashing**: Argon2id (memory-hard, resistant to GPU attacks)
- **Authentication**: JWT bearer tokens (HS256; expiry default 24h, runtime-configurable via the `jwt_expiry_secs` setting) and HTTP Basic auth
- **JWT signing key**: a cluster-wide signing key is generated on first boot and persisted in KV. A background rotator rolls the key on a configurable interval (`jwt_rotation_interval_secs`, default 7d), keeping the previous key around so in-flight tokens remain valid until they expire.
- **LDAP**: optional LDAP authentication with group-to-role mapping (feature-gated)
- **RBAC**: Users are assigned roles; roles grant capabilities (`Read`, `Write`, `Delete`) scoped to repository name patterns (glob matching, e.g. `"*"` for all repos)
- **Bootstrap**: On first start, `admin` and `read-only` roles are created, along with an `admin` user and an unauthenticated `anonymous` user. The admin password is printed to stderr (or set via `default_admin_password` in config)

## Background Workers

The server spawns the following background workers at startup. A supervisor monitors all of them and triggers shutdown if any exits unexpectedly.

| Worker | Interval | Purpose |
|--------|----------|---------|
| Cluster heartbeat | 30s | Register instance, refresh heartbeat, clear stale state |
| Settings refresh | 30s | Reload settings from KV, propagate API changes to this node |
| JWT rotation | `jwt_rotation_interval_secs` (default 7d) | Rotate the cluster-wide JWT signing key, keeping the previous key for validation |
| Update worker | continuous | Batch atime updates, dir-entry recomputation, and store-stats accounting (via channel) |
| Cleanup worker | 30s | Drain repos marked for deletion, reap Docker upload sessions (1h TTL), and reap completed/failed/cancelled tasks |
| Blob GC / reaper | `gc_interval_secs` (default 24h) | Unified blob GC pass with two-pass grace period (lease-gated); also runs per-repo artifact cleanup (max age, max unaccessed days, untagged manifests) and Docker-specific layer/manifest GC |
| State scanner | `state_scan_interval_ms` (default 1s) | Poll KV for cross-instance changes (repos, stores, settings, tasks) and emit events to the local event bus |
| Model materializer | continuous | Subscribe to the event bus and maintain an in-memory snapshot of the cluster model for fast API reads |
| Docker listener manager | 30s | Reconcile dedicated per-repo Docker ports against the current repo configs (add / remove listeners as repos change) |

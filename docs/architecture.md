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
                     |  /api/v1/...     REST API                    |
                     |  /repository/... Raw artifact access         |
                     |  /v2/...         Docker Registry V2 / OCI    |
                     |  /pypi/...       PyPI                        |
                     |  /apt/...        APT                         |
                     |  /yum/...        Yum                         |
                     |  /npm/...        npm                         |
                     |  /cargo/...      Cargo                       |
                     |  /helm/...       Helm                        |
                     |  /golang/...     Go modules                  |
                     |  /swagger-ui/    OpenAPI docs                 |
                     +--------------------+------------------------+
                                          |
                     +--------------------v------------------------+
                     |              API Layer                       |
                     |                                              |
                     |  repositories  artifacts  docker  auth       |
                     |  users  roles  stores  settings  audit       |
                     |  tasks  system                               |
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

| Format | Protocol | URL Prefix |
|--------|----------|------------|
| Raw | REST PUT/GET/DELETE | `/repository/{repo}/` |
| Docker | OCI Distribution Spec (V2) | `/v2/` |
| PyPI | PEP 503 Simple Index | `/pypi/{repo}/` |
| APT | Debian repository with GPG signing | `/apt/{repo}/` |
| Go | Go module proxy protocol (GOPROXY) | `/golang/{repo}/` |
| Helm | Helm chart repository (index.yaml) | `/helm/{repo}/` |
| Cargo | Cargo sparse registry protocol | `/cargo/{repo}/` |
| Yum | YUM/DNF repository with repomd | `/yum/{repo}/` |
| npm | npm registry protocol | `/npm/{repo}/` |

All formats support hosted, cache, and proxy repository types.

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

Embedded, zero-dependency ACID key-value store. Great for development, CI, and small deployments.

**Key limitation: redb serializes write transactions.** Only one write transaction can proceed at a time; concurrent writers queue behind it. This means write throughput is bottlenecked under concurrent load. For single-user or lightly loaded instances this is fine, but **redb is not suitable for production deployments at scale.**

#### DynamoDB (feature-gated)

AWS DynamoDB or any DynamoDB-compatible store (e.g. ScyllaDB Alternator). Scales horizontally with no schema migrations, no connection pooling, and no vacuum/compaction tuning. The DynamoDB backend is enabled via the `dynamodb` feature flag at compile time.

### Blob Store

The `BlobStore` trait abstracts content storage. Blob stores are created and managed at runtime via the Stores API (`/api/v1/stores`). A "default" filesystem blob store is auto-created on first boot. Each repository selects its store via the `store` field.

Two backend types are available:

#### Filesystem

Content-addressable layout with two-level prefix sharding:

```
<root>/<id[0:4]>/<id[4:8]>/<id>
```

Each blob is stored once; duplicate uploads are deduplicated by content hash. The `sync` option (default: true) fsyncs files and parent directories after writes for durability.

#### S3

Stores blobs in an S3-compatible bucket (AWS S3, MinIO, etc.). Supports custom endpoints, region, prefix, and explicit credentials (falls back to the default AWS credential chain if not set).

## Content Addressing

All ingested content is hashed with **BLAKE3** (fast, parallelizable). If a blob with the same hash already exists, the new artifact points to the existing blob -- no duplicate data is stored.

Docker images use a **dual-hash** scheme: BLAKE3 is the internal blob store key, while SHA-256 digests (required by the OCI Distribution Spec) are stored as artifact metadata. This avoids re-hashing on every Docker pull.

## Consistency Model

The system is **eventually consistent by default**. Regular KV reads may return stale values in distributed deployments (DynamoDB). Strong consistency is used only for coordination paths:

- **Distributed leases** use `get_versioned()` + `put_if_version()` for atomic compare-and-swap
- **Integrity checks** acquire a lease before scanning
- **Settings and blob store configuration** are cached in-memory and refreshed from KV every 30 seconds; changes via the API propagate to all instances within one refresh cycle

The redb backend is inherently strongly consistent (single-node). The DynamoDB backend uses eventually consistent reads by default and strongly consistent reads where required.

## Cluster Coordination

Multiple Depot instances can run against shared DynamoDB + S3 backends.

- **Instance registry**: each instance registers in KV with a hostname and periodic heartbeat (30-second interval, 90-second timeout). Instances that miss their heartbeat are considered dead.
- **Distributed leases**: mutual exclusion via versioned compare-and-swap. The well-known `gc` lease ensures only one instance runs blob garbage collection at a time.
- **Integrity checks** acquire the GC lease before scanning, preventing concurrent GC during the check.

## Blob Garbage Collection

The blob reaper runs periodically (default: every 24 hours, configurable via the `gc_interval_secs` runtime setting) and is gated on holding the distributed GC lease.

**Two-phase algorithm:**
1. **Phase A (KV)**: build a bloom filter of all live blob IDs by scanning artifact records, then delete unreferenced dedup refs from KV.
2. **Phase B (storage)**: walk blob store blobs (filesystem or S3). Blobs not in the bloom filter become candidates; candidates from the *previous* pass are deleted.

The two-pass grace period (one full GC interval) prevents races with in-flight ingestions that have written a blob but not yet committed the KV record. Docker artifacts younger than 1 hour are additionally immune to GC, since manifest/blob/tag creation involves multiple non-atomic KV writes.

## Audit Logging

All HTTP requests and mutations are captured as audit events. Three backends are composable:

- **In-memory ring buffer** (default, capacity configurable via `[audit].capacity` in TOML)
- **File** -- append JSON lines to a file (`[audit].file_path` in TOML)
- **Splunk HEC** -- forward events to a Splunk HTTP Event Collector (`[audit.splunk_hec]` in TOML)

Events are queryable via `GET /api/v1/audit/events`.

## Metrics

Prometheus-format metrics are served at `/metrics` on the main listener, or on a dedicated listener if `metrics_listen` is set in the TOML config. Tracked metrics include request counts, durations, in-flight requests, blob store operations, and GC statistics.

## Authentication & Authorization

- **Password hashing**: Argon2id (memory-hard, resistant to GPU attacks)
- **Authentication**: JWT bearer tokens (24-hour expiry, HS256) and HTTP Basic auth
- **LDAP**: optional LDAP authentication with group-to-role mapping (feature-gated)
- **RBAC**: Users are assigned roles; roles grant capabilities (`Read`, `Write`, `Delete`) scoped to repository name patterns (glob matching, e.g. `"*"` for all repos)
- **Bootstrap**: On first start, `admin` and `read-only` roles are created, along with an `admin` user and an unauthenticated `anonymous` user. The admin password is printed to stderr (or set via `default_admin_password` in config)

## Background Workers

The server spawns several background workers at startup:

| Worker | Interval | Purpose |
|--------|----------|---------|
| Cluster heartbeat | 30s | Register instance, refresh heartbeat, clear stale state |
| Settings refresh | 30s | Reload settings from KV, propagate API changes to this node |
| Update worker | continuous | Batch atime updates and dir-entry recomputation (via channel) |
| Cleanup worker | dynamic | Per-repo artifact cleanup (max age, max unaccessed days, untagged manifests) |
| Blob GC / reaper | 24h default | Two-phase blob garbage collection (lease-gated) |

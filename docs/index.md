---
title: Home
nav_order: 1
---

# Artifact Depot Documentation

**Artifact Depot** is a safe-Rust, scale-out artifact repository manager.
It is a modern replacement for Sonatype Nexus and JFrog Artifactory at
datacenter scale, but it is equally well suited as an edge-deployed
artifact cache in CI pipelines and dev containers, or as an in-cluster
Docker cache that keeps a Kubernetes control plane pulling images even
when the cluster is fully air-gapped. It has no SQL dependency -- it
requires only eventually consistent key-value semantics, so it is
web-scale capable out of the box. Depot ships as a single
statically-linked binary or a rootless `FROM scratch` Docker image, with
the web UI embedded in the binary.

## Why a new artifact repository?

The two incumbents, Sonatype Nexus and JFrog Artifactory, are aged JVM
stacks. Running them well means tuning heap sizes, GC parameters, and
JDBC connection pools, and even a well-tuned deployment spends
substantial CPU and RAM per request. A pure-tokio Rust server serving
the same workload uses a fraction of the resources and has no JVM GC
pauses to plan around.

The bundled database in Nexus OSS (historically OrientDB, now H2) is
fine for a small dev install but is not designed for the throughput a
busy CI fleet generates. The supported escape hatch is Sonatype's Pro
tier with PostgreSQL backing -- which is enterprise-priced and still
saddles you with operating a SQL cluster. And even after paying for
SQL, you don't get strong consistency for blob cleanup: the compact
blob store task is a periodic full-store scan that's operationally
painful at scale. Depot leans the other way: the data model is
eventually consistent end to end, which lets the garbage collector
stay small and fast -- a bloom-filter sweep over sharded artifact
partitions, runnable continuously without a maintenance window.
Content-addressable blob storage falls out of the same design, and
with it zero-cost repository **cloning**: a release-specific
dependency tree can be snapshotted off a parent in seconds and kept
for as long as it's relevant without duplicating any underlying
artifacts. The same sharded parallel scans make the brute-force
passes you inevitably need for migrations, audits, and integrity
checks fast enough to run on a live production cluster.

The footprint is small enough -- a single static binary with no
external services for a single-node deployment -- that we can afford
to put a Depot **everywhere**: as the primary in a datacenter, as a
pull-through cache in each office or CI runner, inside airgapped
clusters that need a stable local mirror, and even on individual
engineers' laptops as a transparent dev cache.

## Features

### Artifact formats

Every format supports hosted, cache (pull-through with TTL), and proxy
(group / virtual) repository types. Clients point at the depot with
their usual tools and URLs.

- **Raw** -- arbitrary files by path.
- **Docker / OCI** -- Docker Registry V2 and the OCI Distribution Spec,
  including Docker multi-arch (manifest list) and OCI image indexes.
- **APT** -- Debian-style repositories with GPG-signed
  `Release` / `InRelease` files.
- **Yum / DNF** -- `repomd.xml` + sqlite metadata, optional `.asc`
  signing.
- **PyPI** -- PEP 503 simple index, twine upload.
- **npm** -- full registry protocol, packument + tarballs, scoped
  packages, search.
- **Cargo** -- sparse registry, `cargo publish` / `yank` / `unyank`.
- **Helm** -- chart repository with `index.yaml`.
- **Go modules** -- GOPROXY (`@v/list`, `@v/{version}.info|.mod|.zip`,
  `@latest`).

### Storage

- **Content-addressable blob dedup** using BLAKE3. Identical blobs are
  stored once regardless of which repo or format uploaded them.
- **Dual-hash for Docker / OCI**: BLAKE3 is the internal blob key;
  SHA-256 digests (required by the OCI spec) are stored as metadata
  alongside, so Docker pulls never re-hash.
- **Zero-cost cloning / snapshotting**: cloning a repository copies
  metadata only -- the new artifacts point at the same shared blobs.
  Ideal for release-specific dependency trees.
- **Pluggable KV backends**: embedded [redb](https://www.redb.org/) for
  single-node, or any DynamoDB-compatible store (AWS DynamoDB,
  [ScyllaDB Alternator](https://www.scylladb.com/alternator/)) for
  clusters.
- **Pluggable blob backends**: local filesystem or any S3-compatible
  object store (AWS S3, MinIO, Garage, Ceph, ...).
- **Eventually consistent end to end**. The system is designed for it,
  so the garbage collector is small, fast, and runs continuously --
  a sharded, bloom-filtered sweep rather than a periodic
  stop-the-world scan.

### Scale-out

- Multiple Depot instances run against the same KV + blob backend with
  no coordination overhead on the read / write hot path.
- Distributed leases (versioned compare-and-swap) ensure at-most-one
  execution of background jobs like GC.
- Instance liveness is tracked via heartbeats (30 s interval, 90 s
  timeout); dead nodes' leases are reclaimed automatically.
- Integrity checks run live, without a maintenance window.

### Security and administration

- **Authentication**: JWT bearer tokens (runtime-configurable expiry)
  and HTTP Basic; passwords hashed with Argon2id.
- **JWT signing key rotation** on a configurable interval, keeping the
  previous key valid long enough for in-flight tokens to drain.
- **RBAC**: roles grant capabilities (`Read`, `Write`, `Delete`) scoped
  to repository-name globs.
- **LDAP** authentication with group-to-role mapping (feature-gated).
- **Embedded Vue web UI** for repositories, stores, users, roles,
  settings, logs, and background tasks.
- **Interactive Swagger UI** at `/swagger-ui/` backed by the
  auto-generated OpenAPI spec.
- **Backup & restore**: export / import the whole system
  configuration as a single JSON document via the REST API.
- **Nexus-compatible REST subset** (`/service/rest/v1/...`) for tools
  that still speak the Nexus API.

### Observability

- **OpenTelemetry** first class: request spans, KV spans, and blob
  store spans exported via OTLP gRPC, with adaptive rate-limited
  sampling. Application logs and per-request structured events are
  exported on the same OTLP pipeline, sharing trace / span IDs.
  Stdout always gets application logs; per-request events go only to
  OTLP.
- **Prometheus metrics** at `/metrics` (or on a dedicated listener).
- **Any downstream destination** via the OTel collector: Splunk,
  S3, file archival, Datadog, Honeycomb, etc. -- just configure the
  collector's exporters.
- **Pre-built Grafana dashboards** live alongside the bundled
  Docker Compose and Helm stacks.

### Deployment

- **Single statically-linked musl binary** -- one `depot` executable.
- **Rootless `FROM scratch` Docker image** with a baked-in default
  config.
- **Helm chart** with standalone / distributed / AWS profiles.
- Modest resource footprint: a busy node runs happily in ~128 MiB RAM,
  so you can afford a Depot on every CI runner, dev laptop, and
  airgapped cluster.

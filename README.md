# Artifact Depot

Artifact Depot is a safe-Rust, scale-out artifact repository manager. It is a modern replacement for Sonatype Nexus and JFrog Artifactory at datacenter scale, but it is equally well suited as an edge-deployed artifact cache in CI pipelines and dev containers, or as an in-cluster Docker cache alongside a Kubernetes control plane. It has no SQL dependency -- it requires only eventually consistent key-value semantics, so it is web-scale capable out of the box. Artifact Depot ships as a single statically-linked binary or a rootless `FROM scratch` Docker image, with the web UI embedded in the binary.

Under the hood, all ingested content is hashed with BLAKE3 and stored content-addressable with automatic deduplication; unreferenced blobs are swept by an asynchronous, two-pass garbage collector. Because artifacts reference blobs by content hash, repositories can be cloned (or snapshotted) instantly without duplicating any data -- only the metadata is copied. The metadata backend is pluggable: embedded redb for a single node, or outboard DynamoDB (AWS DynamoDB, or any DynamoDB-compatible store such as [ScyllaDB Alternator](https://www.scylladb.com/alternator/)) for a cluster. Blob storage is equally pluggable: the local filesystem or any S3-compatible object store.

With an outboard DynamoDB-compatible KV and an S3-compatible blob store, any number of Depot instances can run simultaneously against the same backend to scale out load with no coordination overhead on the hot path. Each instance runs happily in a tiny virtual machine.  A distributed lease mechanism ensures at-most-one execution for background work like blob garbage collection, and instance liveness is tracked via heartbeats so failed nodes are detected and their leases reclaimed.

## Features

### Artifact Formats

Hosted, cache (pull-through with TTL), and proxy (group / virtual) repository types are supported for every format below. All formats share a single URL surface -- clients point at `http://depot.example.com/repository/{repo_name}/` and Depot routes the request based on the repository's configured format. Docker / OCI is the only exception: its clients insist on the protocol's own `/v2/` prefix.

| Format | Protocol | Client Tool |
|--------|----------|-------------|
| Raw | REST PUT/GET/DELETE | curl, wget |
| Docker / OCI | OCI Distribution Spec (V2), multi-arch | docker, podman |
| APT | Debian repository with GPG signing | apt |
| Yum | YUM/DNF repository with repomd | dnf, yum |
| PyPI | PEP 503 simple index | pip, twine |
| NPM | npm registry protocol | npm |
| Cargo | Cargo sparse registry | cargo |
| Helm | Helm chart repository | helm |
| Go modules | Go module proxy (GOPROXY) | go |

### Storage

- **Content-addressable blob dedup** -- BLAKE3 hashing with automatic deduplication across every repository and format
- **Dual-hash for Docker / OCI** -- BLAKE3 is the internal blob key; SHA-256 digests (required by the OCI spec) are stored as metadata
- **Pluggable KV backends** -- embedded [redb](https://www.redb.org/) for single-node, or any DynamoDB-compatible store for distributed deployments
- **Pluggable blob backends** -- local filesystem or S3-compatible object storage (AWS S3, MinIO, Garage, etc.)
- **Eventually consistent by default** -- regular reads tolerate stale values; strong consistency is used only for coordination paths (distributed leases, version-checked updates). No SQL, no schema migrations, no connection pool tuning
- **Repository clone / snapshot** -- point-in-time copies of any repository, free of blob duplication because the new artifacts share the same content-addressable blobs

### Scale-Out

Multiple Depot instances run against shared KV + S3 with no coordination overhead for normal reads and writes. A distributed lease ensures at-most-one execution for background work like blob garbage collection. Instance liveness is tracked with a 30-second heartbeat and a 90-second timeout, and a check-pause protocol coordinates integrity checks across the cluster.

### Security and Administration

- **Authentication** -- JWT bearer tokens and HTTP Basic auth, Argon2id password hashing
- **RBAC** -- role-based capabilities (`Read`, `Write`, `Delete`) scoped to repository name globs
- **LDAP** -- optional LDAP authentication with group-to-role mapping (feature-gated)
- **Embedded web UI** -- Vue SPA for repository, user, role, and store management, live settings, and log browsing
- **OpenAPI** -- interactive Swagger UI at `/swagger-ui/`
- **Backup / restore** -- export and import the full system configuration as a single JSON document

### Observability and Auditing

Artifact Depot has first-class OpenTelemetry support -- logs, traces, and metrics all flow through the same OTLP pipeline, so you can point Depot at any OTel-compatible collector (Grafana Tempo / Loki, Jaeger, Honeycomb, Datadog, etc.) and get end-to-end visibility across a cluster.

- **Tracing** -- OTLP gRPC export of request spans with adaptive rate-limited sampling (default: 100 root traces/sec, configurable). Child KV and blob spans are kept on sampled traces, giving a full waterfall from HTTP handler down to the storage backend. Enable with `[tracing] otlp_endpoint = "http://otel-collector:4317"`.
- **Logs** -- structured `tracing` events are exported via OTLP alongside traces, so logs and spans share trace / span IDs and correlate automatically. An in-memory ring buffer is always available at `GET /api/v1/logging/events` (and in the UI's log viewer), with optional append-only JSON lines files and Splunk HTTP Event Collector for long-term retention.
- **Metrics** -- Prometheus metrics at `/metrics` on the main listener, or on a dedicated listener if `metrics_listen` is set. Tracked metrics include request counts and durations (by route, method, and status), in-flight requests, KV and blob store operation rates and latencies, GC progress, and per-store blob counts and bytes. Pre-built Grafana dashboards live in [`docker/standalone/monitoring/dashboards`](docker/standalone/monitoring/dashboards).
- **Audit log** -- every mutation (repository, user, role, store, settings) is captured as a structured audit event with actor, resource, before/after snapshots, and HTTP context; events stream through the same log pipeline and are queryable via the API and UI.

## Getting Started

### Docker

```bash
docker build -t depot .
docker run -p 8080:8080 -v depot-data:/depot depot
```

The image is built `FROM scratch` with a statically-linked musl binary and runs as non-root (uid 65534). It ships a baked-in config at `/etc/depot/depotd.toml` that writes the embedded redb database to `/depot/db`, so a named volume mounted at `/depot` is all that's needed for persistence. Override the config by bind-mounting your own file at `/etc/depot/depotd.toml`. On first start a random admin password is generated and printed to the container log; grep for it with `docker logs <container>`. After logging in, create a filesystem blob store rooted at `/depot/blobs` (via the UI or the API):

```bash
curl -u admin:<generated-password> -X POST http://localhost:8080/api/v1/stores \
  -H 'Content-Type: application/json' \
  -d '{"name":"default","store_type":"file","root":"/depot/blobs"}'
```

### Docker Compose

Pre-built compose stacks live under [`docker/`](docker/):

- [`docker/standalone`](docker/standalone) -- single depot instance with an embedded redb + filesystem store, plus an optional monitoring profile (Grafana, Prometheus, Tempo, Loki)
- [`docker/distributed`](docker/distributed) -- three depot instances sharing ScyllaDB (DynamoDB Alternator) and Garage (S3)
- [`docker/aws`](docker/aws) -- single instance wired to AWS DynamoDB and S3

```bash
cd docker/standalone && docker compose up
```

### Helm

A Helm chart is under [`charts/depot`](charts/depot) with three profiles:

```bash
helm install depot ./charts/depot                                      # standalone (redb + PVC)
helm install depot ./charts/depot -f charts/depot/values-distributed.yaml  # DynamoDB + S3
helm install depot ./charts/depot -f charts/depot/values-aws.yaml          # AWS DynamoDB + S3
```

### Build from Source

```bash
make          # lint, build, and test everything
./target/debug/depot                    # run with defaults (listens on 0.0.0.0:8080)
./target/debug/depot -c etc/depotd.toml # run with a custom config
```

Requires a recent stable Rust toolchain and Node.js; the Vue frontend is built automatically by `ui/build.rs`.

## Usage

Create a repository, upload a file, and download it:

```bash
# Create a raw hosted repository
curl -u admin:admin -X POST http://localhost:8080/api/v1/repositories \
  -H 'Content-Type: application/json' \
  -d '{"name":"my-raw","repo_type":"hosted","format":"raw"}'

# Upload an artifact
curl -u admin:admin -X PUT http://localhost:8080/repository/my-raw/path/to/file.tar.gz \
  -H 'Content-Type: application/gzip' --data-binary @file.tar.gz

# Download it
curl -O http://localhost:8080/repository/my-raw/path/to/file.tar.gz
```

Push and pull Docker images:

```bash
# Create a Docker hosted repository
curl -u admin:admin -X POST http://localhost:8080/api/v1/repositories \
  -H 'Content-Type: application/json' \
  -d '{"name":"docker-local","repo_type":"hosted","format":"docker"}'

# Tag and push
docker tag myimage:latest localhost:8080/docker-local/myimage:latest
docker push localhost:8080/docker-local/myimage:latest

# Pull
docker pull localhost:8080/docker-local/myimage:latest
```

Clone a repository (instant, no blob data copied):

```bash
curl -u admin:admin -X POST http://localhost:8080/api/v1/repositories/my-raw/clone \
  -H 'Content-Type: application/json' \
  -d '{"new_name":"my-raw-snapshot"}'
```

## Binaries

Artifact Depot's workspace ships two binaries:

- **`depot`** (crate: [`server/`](server)) -- the server
- **`depot-bench`** (crate: [`bench/`](bench)) -- demo seeding and benchmarking (`depot-bench demo`, `depot-bench bench`)

The rest of the workspace is split into focused crates: [`core/`](core) (trait-level abstractions), [`ui/`](ui) (embedded Vue frontend + `rust-embed`), [`kv/redb`](kv/redb) and [`kv/dynamo`](kv/dynamo) (KV backends), [`blob/file`](blob/file) and [`blob/s3`](blob/s3) (blob backends), and [`format/*`](format) (one crate per artifact format).

## Documentation

See the [`docs/`](docs/) directory:

- [Architecture](docs/architecture.md) -- storage, consistency model, clustering, GC, logging, and auth
- [Configuration](docs/configuration.md) -- TOML config reference, runtime settings, and blob store management
- [Usage](docs/usage.md) -- CLI reference, supported formats, API docs, and benchmarking

## Code Coverage

`make coverage` runs the full test suite against both the redb and DynamoDB backends in parallel with LLVM source-based coverage instrumentation, then merges the results into a single report. Tests include Rust unit and integration tests, Playwright UI tests, and format-compatibility tests that run real client tools (`docker`, `npm`, `pip`, `apt`, `dnf`, `cargo`, `helm`, `go`) against Depot in Docker containers.

HTML report: `build/coverage/html/index.html`. Requires `cargo-llvm-cov` (installed automatically if missing). DynamoDB coverage requires [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html); if the JAR is not present, only redb backend tests are included.

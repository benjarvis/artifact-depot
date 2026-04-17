# Artifact Depot

A Rust-native, scale-out artifact repository manager. Artifact Depot is a modern replacement for Sonatype Nexus and JFrog Artifactory, focused on efficiency, performance, and operational simplicity. It ships as a single static binary with an embedded web UI and zero runtime dependencies.

## Features

### Artifact Formats

Hosted, cache (pull-through with TTL), and group/proxy (virtual) repository types for all formats:

- **Raw** -- upload, download, search, and browse arbitrary files
- **Docker / OCI** -- Docker Registry V2 and OCI Distribution Spec with full multi-arch manifest support
- **APT** -- Debian/Ubuntu packages with GPG-signed Release files
- **Yum** -- RPM packages with repodata generation
- **PyPI** -- Python packages with PEP 503 simple index
- **NPM** -- Node.js packages with full registry protocol
- **Cargo** -- Rust crates with index generation
- **Helm** -- Kubernetes Helm charts
- **Go modules** -- Go module proxy protocol

### Storage

- **Content-addressable blob dedup** -- BLAKE3 hashing with automatic deduplication across all repositories
- **Pluggable KV backends** -- embedded redb for single-node or DynamoDB-compatible stores for distributed deployments (AWS DynamoDB or on-prem alternatives like [ScyllaDB Alternator](https://www.scylladb.com/alternator/))
- **Pluggable blob backends** -- local filesystem or S3-compatible object storage
- **Eventually consistent by default** -- regular reads tolerate stale values; strong consistency is used only for coordination (leases, version-checked updates). This avoids the operational cost and scaling bottleneck of a centralized SQL database. KV stores scale horizontally with zero schema migrations, no connection pooling, and no vacuum/compaction tuning.

### Scale-Out

Multiple Depot instances can run against shared KV + S3 backends with no coordination overhead for normal reads and writes. A distributed lease mechanism ensures at-most-one execution for background work like blob garbage collection. Instance liveness is tracked via heartbeats, and a check-pause protocol coordinates consistency checks across the cluster.

### Security and Administration

- **Authentication & RBAC** -- JWT and Basic auth, Argon2 password hashing, role-based access control
- **Vue web UI** -- repository, user, role, and store management, plus settings
- **OpenAPI** -- interactive Swagger UI at `/swagger-ui/`

## Getting Started

### Docker

```bash
docker build -t depot .
docker run -p 8080:8080 -v depot-data:/data depot
```

The image is built `FROM scratch` with a statically-linked musl binary. It runs as a non-root user (uid 65534) and exposes port 8080. Mount a volume at `/data` for persistence and bind-mount your config at `/etc/depot/depotd.toml`.

### Build from Source

```bash
make          # lint, build, and test everything

# run with defaults (listens on 0.0.0.0:8080)
./target/debug/depot

# or with a config file
./target/debug/depot -c etc/depotd.toml
```

Requires Rust 1.75+ and Node.js (for the frontend build, which is driven automatically by `build.rs`).

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

## Documentation

See the [`docs/`](docs/) directory:

- [Architecture](docs/architecture.md) -- storage, consistency model, clustering, GC, logging, and auth
- [Configuration](docs/configuration.md) -- TOML config reference, runtime settings, and blob store management
- [Usage](docs/usage.md) -- CLI reference, supported formats, API docs, and benchmarking

## Code Coverage

`make coverage` runs the full test suite against both the redb and DynamoDB backends in parallel with LLVM source-based coverage instrumentation, then merges the results into a single report.

```
Filename                                         Regions    Missed Regions     Cover   Functions  Missed Functions  Executed       Lines      Missed Lines     Cover
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
depot-bench/src/client.rs                           1194               222    81.41%          85                 7    91.76%         843                78    90.75%
depot-bench/src/demo/docker.rs                       518                 9    98.26%          26                 0   100.00%         251                 1    99.60%
depot-bench/src/runner/stats.rs                      348                 0   100.00%          24                 0   100.00%         243                 0   100.00%
depot/src/server/api/artifacts.rs                    707               109    84.58%          28                 2    92.86%         454                51    88.77%
depot/src/server/api/cargo.rs                        727               107    85.28%          43                 4    90.70%         467                47    89.94%
depot/src/server/api/docker.rs                      3194               724    77.33%         164                35    78.66%        1899               412    78.30%
depot/src/server/api/npm.rs                         1279               179    86.00%          78                 6    92.31%         774               100    87.08%
depot/src/server/api/pypi.rs                        1263               207    83.61%          45                 5    88.89%         759               119    84.32%
depot/src/server/api/yum.rs                          918               140    84.75%          47                 4    91.49%         528                77    85.42%
depot/src/server/auth.rs                             341                11    96.77%          41                 2    95.12%         209                 9    95.69%
depot/src/server/blob_reaper.rs                     1507               315    79.10%          54                 6    88.89%         869               212    75.60%
depot/src/server/cluster.rs                          846                96    88.65%          67                 4    94.03%         476                48    89.92%
depot/src/server/service.rs                         1616               355    78.03%         141                23    83.69%         924               175    81.06%
depot/src/server/store/dynamo_kv.rs                  234                44    81.20%          30                10    66.67%         155                17    89.03%
depot/src/server/store/file_blob.rs                  565                43    92.39%          49                 0   100.00%         270                 9    96.67%
depot/src/server/store/s3_blob.rs                    591                79    86.63%          55                11    80.00%         319                58    81.82%
depot/tests/integration.rs                          3841                10    99.74%         158                 1    99.37%        2689                 4    99.85%
...
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
TOTAL                                              67449              7893    88.30%        3624               404    88.85%       39759              4734    88.09%
```

HTML report: `build/coverage/html/index.html`. Requires `cargo-llvm-cov` (installed automatically if missing). DynamoDB coverage requires [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html); if the JAR is not present, only redb backend tests are included.

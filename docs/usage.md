# Usage Guide

## Binaries

Artifact Depot ships two binaries:

- **`depot`** -- the server
- **`depot-bench`** -- demo seeding, background activity, and benchmarking

### `depot`

Start the server.

```bash
depot                           # uses ./depotd.toml (or defaults if missing)
depot -c /etc/depot/depotd.toml # explicit config path
```

### `depot-bench demo`

Seed a running instance with realistic sample data.

```bash
depot-bench demo --url http://localhost:8080 --username admin --password admin
```

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | `http://localhost:8080` | Base URL of the depot instance |
| `--username` | `admin` | Authentication username |
| `--password` | `admin` | Authentication password |
| `--insecure` | `false` | Skip TLS certificate verification |
| `--repos` | `3` | Number of raw hosted repos to create |
| `--docker-repos` | `2` | Number of Docker hosted repos to create |
| `--artifacts` | `50` | Artifacts per raw repo |
| `--images` | `5` | Images per Docker repo |
| `--tags` | `3` | Tags per image |
| `--clean` | `false` | Delete existing repos first |

### `depot-bench trickle`

Run slow, ongoing background activity (uploads, pulls, deletes) against a seeded instance. Useful for populating a live Grafana dashboard or exercising background workers in a demo environment.

```bash
depot-bench trickle --url http://localhost:8080 --username admin --password admin
```

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | `http://localhost:8080` | Base URL of the depot instance |
| `--username` | `admin` | Authentication username |
| `--password` | `admin` | Authentication password |
| `--insecure` | `false` | Skip TLS certificate verification |

### `depot-bench bench`

Run benchmarks against a running instance. The `--store` flag is required and names an existing blob store to back the benchmark's hosted repos.

```bash
depot-bench bench --url http://localhost:8080 --store default --scenario raw-mixed --concurrency 8 --duration 60
```

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | `http://localhost:8080` | Base URL of the depot instance |
| `--username` | `admin` | Authentication username |
| `--password` | `admin` | Authentication password |
| `--insecure` | `false` | Skip TLS certificate verification |
| `--store` | **required** | Blob store name to back the benchmark's hosted repos |
| `--scenario` | `all` | Scenario to run (see below) |
| `--concurrency` | `4` | Number of concurrent workers |
| `--duration` | `30` | Seconds per scenario |
| `--count` | _(none)_ | Total operations (overrides `--duration`) |
| `--artifact-size` | `1048576` (1 MiB) | Raw artifact size in bytes |
| `--layer-size` | `4194304` (4 MiB) | Docker layer size in bytes |
| `--warmup` | `2` | Warmup seconds excluded from stats |
| `--pipeline` | `1` | Number of pipelined requests per worker (in-flight simultaneously on the same connection); `1` is sequential |
| `--random-data` | `false` | Fully random payloads (expensive); default uses a cheap repeating pattern with a unique header per object |
| `--json` | `false` | Output results as JSON |

**Scenarios:**

| Name | Operations |
|------|-----------|
| `raw-upload` | 100% raw upload |
| `raw-download` | 100% raw download |
| `raw-mixed` | 60% download, 30% upload, 5% delete, 5% list |
| `docker-push` | 100% Docker push |
| `docker-pull` | 100% Docker pull |
| `docker-mixed` | 50% push, 50% pull |
| `all` | Run all six scenarios sequentially |

## API Documentation

Interactive API documentation is available at `/swagger-ui/` (requires authentication). The OpenAPI spec is served at `/api-docs/openapi.json`.

The REST API covers repositories, artifacts, users, roles, blob stores, cluster settings, background tasks, backup / restore, and a Nexus-compatible `/service/rest/v1/*` subset for tools that still speak the Nexus API.

## Supported Formats

All formats support hosted, cache (pull-through with TTL), and proxy (group) repository types. Every format is reachable under `http://depot.example.com/repository/{repo_name}/...` — the server dispatches based on the repository's configured format. Docker / OCI is the only exception: the protocol hard-codes the `/v2/` prefix, so Docker clients point at `http://depot.example.com/v2/` directly (optionally with `default_docker_repo` set).

| Format | Client Tool |
|--------|-------------|
| Raw | curl, wget |
| Docker | docker, podman |
| PyPI | pip, twine |
| APT | apt |
| Go | go (GOPROXY) |
| Helm | helm |
| Cargo | cargo |
| Yum | dnf, yum |
| npm | npm |

## Repository Management

Create a repository:

```bash
curl -u admin:admin -X POST http://localhost:8080/api/v1/repositories \
  -H 'Content-Type: application/json' \
  -d '{"name":"my-repo","repo_type":"hosted","format":"raw","store":"default"}'
```

The `format` field accepts any of: `raw`, `docker`, `pypi`, `apt`, `golang`, `helm`, `cargo`, `yum`, `npm`. The `repo_type` field accepts `hosted`, `cache`, or `proxy`. The `store` field is required and names an existing blob store (see [Configuration -- Blob Store Management](configuration.md#blob-store-management)). Cache repos require `upstream_url` and optionally `cache_ttl_secs` and `upstream_auth`. Proxy repos require `members` (an ordered list of repo names) and optionally `write_member` (which member to route writes to).

Clone a repository (instant, no blob data copied -- artifacts in the clone share the same content-addressable blobs as the source):

```bash
curl -u admin:admin -X POST http://localhost:8080/api/v1/repositories/my-repo/clone \
  -H 'Content-Type: application/json' \
  -d '{"new_name":"my-repo-snapshot"}'
```

See the OpenAPI docs at `/swagger-ui/` for the full repository, user, role, store, tasks, backup, and settings APIs.

### Docker

Docker repos can optionally have a dedicated `listen` address (e.g. `"0.0.0.0:5000"`), which spawns an additional listener where image names route directly to that repo without a path prefix.

The `default_docker_repo` runtime setting (via `PUT /api/v1/settings`) routes `/v2/` requests on the main listener to a specific repo, allowing standard Docker paths like `docker push localhost:8080/myimage:latest`.

### Cache Repositories

Cache repositories work for all formats. On a cache miss, the artifact is fetched from the upstream URL, stored locally, and served to the client. Subsequent requests within the TTL are served from the local cache. If the upstream becomes unreachable, stale cached content is served rather than returning an error. Set `upstream_auth` with `username` and `password` for authenticated upstreams.

## Default Users

On first start, two users are bootstrapped:

| User | Roles | Notes |
|------|-------|-------|
| `admin` | `admin` | Password printed to stderr (or set via `default_admin_password`) |
| `anonymous` | _(none)_ | Unauthenticated access; no capabilities by default |

## Repository Cleanup

Repository cleanup runs automatically as part of garbage collection. Per-repository cleanup policies are set on the repository at creation time:

| Field | Description |
|-------|-------------|
| `cleanup_max_unaccessed_days` | Delete artifacts not accessed within this many days |
| `cleanup_max_age_days` | Delete artifacts older than this many days |
| `cleanup_untagged_manifests` | (Docker) Delete manifests with no remaining tag references |

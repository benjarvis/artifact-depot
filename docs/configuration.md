---
title: Configuration
nav_order: 3
---

# Configuration Reference

Depot is configured via a TOML file, passed with `-c` (default: `depotd.toml`). All fields have sensible defaults -- an empty file (or no file at all) produces a working single-node server on port 8080 with an embedded redb database. Blob stores must be created via the REST API on first boot before repositories can accept writes.

Operational settings (CORS, rate limiting, GC intervals, logging overrides, etc.) are managed at runtime via the REST API, not in the TOML file. See [Runtime Settings](#runtime-settings) and [Blob Store Management](#blob-store-management) below.

## Minimal Configuration

```toml
default_admin_password = "admin"

[http]
listen = "0.0.0.0:8080"

[kv_store]
type = "redb"
path = "/var/lib/depot/kv"
```

## Full Annotated Example

```toml
# -- Bootstrap ------------------------------------------------------
default_admin_password = "changeme"  # Admin password on first start (default: random, printed to stderr)

# Secret used to encrypt KV values at rest.  Changing this after data is
# written prevents the server from starting (the canary check will fail).
# When omitted, a built-in fallback key is used and a warning is emitted.
server_secret = "a-long-random-string"

# Optional: cap the tokio worker pool (0 = one per CPU).
worker_threads = 0

# -- Network -------------------------------------------------------
# Plain HTTP listener.  Omit when using [https] exclusively, or pair with
# [https] to serve TLS on one port and plain HTTP on another (e.g. redirect).
[http]
listen = "0.0.0.0:8080"

# TLS listener.  Both tls_cert and tls_key must exist and be PEM.
# [https]
# listen = "0.0.0.0:443"
# tls_cert = "/etc/depot/cert.pem"
# tls_key = "/etc/depot/key.pem"

# Optional dedicated /metrics listener.  Without this, metrics are served on
# the main listener.
metrics_listen = "127.0.0.1:9090"

# -- KV Store -------------------------------------------------------
# Option A: redb (default, embedded, sharded single-node).
[kv_store]
type = "redb"
path = "/var/lib/depot/kv"       # Directory containing sharded redb files
scaling_factor = 8               # Number of shards; power of 2 in [1, 256]
cache_size = 268435456           # Total page cache across all shards (bytes)

# Option B: DynamoDB (requires the "dynamodb" feature at compile time).
# [kv_store]
# type = "dynamodb"
# table_prefix = "depot"                   # Each logical table becomes "{prefix}_{name}"
# region = "us-east-1"
# endpoint_url = "http://localhost:8000"   # Optional, for DynamoDB Local or ScyllaDB Alternator
# max_retries = 3                          # Max retry attempts, 0-20 (default: 3)
# connect_timeout_secs = 3                 # TCP connection timeout (default: 3)
# read_timeout_secs = 10                   # Per-request read timeout (default: 10)
# retry_mode = "standard"                  # "standard" or "adaptive" (default: "standard")

# -- Logging --------------------------------------------------------
# Application logs and per-request events are written to stdout and, when
# configured, exported over OTLP. Run an OpenTelemetry collector in front
# of Depot if you need file, Splunk, or S3 destinations -- the collector
# handles batching, retries, and credentials better than any in-tree sink.
[logging]
otlp_endpoint = "http://otelcol:4318"    # OTLP logs endpoint (optional)

# -- Tracing / OpenTelemetry ---------------------------------------
# [tracing]
# otlp_endpoint = "http://otel-collector:4317"
# service_name = "depot"                  # Default: "depot"
# deployment_environment = "production"   # Optional resource attribute
# sampling_ratio = 0.1                    # Fixed ratio in [0.0, 1.0]; ignored when max_traces_per_sec is set
# max_traces_per_sec = 100                # Adaptive rate-limiting sampler; default: 100, 0 = unlimited

# -- LDAP (requires the "ldap" feature at compile time) -------------
# LDAP can also be configured at runtime via PUT /api/v1/settings/ldap.
# [ldap]
# url = "ldap://ldap.example.com:389"
# bind_dn = "cn=service,dc=example,dc=com"
# bind_password = "password"
# user_base_dn = "ou=users,dc=example,dc=com"
# user_filter = "(uid={username})"           # Default
# group_base_dn = "ou=groups,dc=example,dc=com"
# group_filter = "(member={dn})"             # Default
# group_name_attr = "cn"                     # Default
# starttls = false                           # Default
# tls_skip_verify = false                    # Default
# fallback_to_builtin = true                 # Default: try builtin auth if LDAP user not found
#
# [ldap.group_role_mapping]
# ldap-admins = "admin"
# ldap-readers = "read-only"
```

## Field Reference

### Top-Level Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `default_admin_password` | string | _(random)_ | Set admin password on first start instead of generating one |
| `server_secret` | string | _(built-in fallback)_ | Secret used to encrypt KV values at rest; see the note below |
| `worker_threads` | integer | `0` | Tokio worker-thread count; `0` = one per CPU |
| `metrics_listen` | string | _(none)_ | Dedicated Prometheus metrics listener address (e.g. `"127.0.0.1:9090"`) |

**server_secret:** Once data has been written under a given secret, the server writes an encryption canary to KV. Starting the server with a different `server_secret` will fail the canary check and refuse to start, to protect against silent key-mismatch data loss.

### `[http]`

Plain HTTP listener. At least one of `[http]` or `[https]` must be present.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen` | string | `"0.0.0.0:8080"` | Socket address |

### `[https]`

TLS listener. When present, the server serves TLS here; if `[http]` is also set, it's served as a secondary plain-HTTP listener (useful for redirects).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen` | string | `"0.0.0.0:443"` | Socket address |
| `tls_cert` | path | **required** | PEM certificate file |
| `tls_key` | path | **required** | PEM private key file |

### `[kv_store]` -- redb

Depot runs redb in a sharded layout (one redb file per shard, selected by key hash), so `path` points at a **directory**, not a single file.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | -- | Must be `"redb"` |
| `path` | path | `"depot_data"` | Directory containing the sharded redb files |
| `scaling_factor` | integer | `8` | Number of shards; must be a power of two in `[1, 256]` |
| `cache_size` | integer | `268435456` (256 MiB) | Total page cache across all shards, in bytes |

### `[kv_store]` -- DynamoDB

Requires the `dynamodb` feature flag at compile time.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | -- | Must be `"dynamodb"` |
| `table_prefix` | string | **required** | Prefix for table names. Each logical table becomes `"{prefix}_{name}"` |
| `region` | string | `"us-east-1"` | AWS region |
| `endpoint_url` | string | _(none)_ | Override for DynamoDB Local, ScyllaDB Alternator, etc. |
| `max_retries` | integer | `3` | Retry attempts, `0..=20` |
| `connect_timeout_secs` | integer | `3` | TCP connection timeout, `1..=300` |
| `read_timeout_secs` | integer | `10` | Per-request read timeout, `1..=300` |
| `retry_mode` | string | `"standard"` | `"standard"` or `"adaptive"` |

### `[logging]`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `otlp_endpoint` | string | _(none)_ | OTLP logs endpoint (e.g. `http://otelcol:4318` or `http://loki:3100/otlp`) |

Depot always writes application logs to stdout. The per-request event target (`depot.request`) is filtered out of stdout and, when `otlp_endpoint` is set, exported to an OpenTelemetry collector alongside the application logs. Route the OTLP stream through a collector for file, Splunk, S3, or any other destination.

Legacy `capacity`, `file_path`, `splunk_hec`, and `s3` keys under `[logging]` (or the old `[audit]` section) are silently ignored on startup with a warning; migrate those destinations to your OTel collector config.

### `[tracing]`

OpenTelemetry tracing configuration. Enabling `otlp_endpoint` also raises the global log filter to `debug` for `depot` targets so KV/blob child spans reach the exporter; the stderr log layer stays at `info`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `otlp_endpoint` | string | _(none)_ | OTLP gRPC endpoint for trace export (e.g. `"http://otel-collector:4317"`). Must start with `http://` or `https://` |
| `service_name` | string | `"depot"` | Service name reported to the collector |
| `deployment_environment` | string | _(none)_ | Deployment environment resource attribute (e.g. `"production"`) |
| `sampling_ratio` | float | _(none)_ | Fixed trace sampling ratio `[0.0, 1.0]`; ignored when `max_traces_per_sec` is set |
| `max_traces_per_sec` | integer | `100` | Adaptive rate-limiting sampler for root traces; `0` disables rate limiting |

### `[ldap]`

Requires the `ldap` feature flag at compile time. LDAP can also be configured at runtime via `PUT /api/v1/settings/ldap`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | **required** | LDAP server URL (e.g. `ldap://ldap.example.com:389`) |
| `bind_dn` | string | **required** | Service account DN for user lookups |
| `bind_password` | string | **required** | Service account password |
| `user_base_dn` | string | **required** | Base DN for user searches |
| `user_filter` | string | `"(uid={username})"` | LDAP filter; `{username}` is replaced with login name |
| `group_base_dn` | string | **required** | Base DN for group searches |
| `group_filter` | string | `"(member={dn})"` | Group filter; `{dn}` is replaced with user DN |
| `group_name_attr` | string | `"cn"` | Attribute that holds the group name |
| `group_role_mapping` | map | `{}` | Map LDAP group names to Depot role names |
| `starttls` | bool | `false` | Use STARTTLS |
| `tls_skip_verify` | bool | `false` | Skip TLS certificate verification |
| `fallback_to_builtin` | bool | `true` | Fall back to builtin auth when LDAP user not found |

---

## Runtime Settings

These settings are **not** in the TOML config file. They are stored in the KV store and managed via the REST API:

- `GET /api/v1/settings` -- read current settings
- `PUT /api/v1/settings` -- update settings (admin only)

Changes propagate to all cluster instances within 30 seconds.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `access_log` | bool | `true` | Log all HTTP requests (method, path, status, duration) |
| `max_artifact_bytes` | integer | `34359738368` (32 GiB) | Maximum upload body size |
| `default_docker_repo` | string or null | `null` | Route `/v2/` requests to this repo name |
| `cors` | object or null | `null` | CORS configuration (`allowed_origins`, `allowed_methods`, `allowed_headers`, `max_age_secs`) |
| `rate_limit` | object or null | `null` | Rate limiting (`requests_per_second`, `burst_size`) per client IP |
| `gc_interval_secs` | integer or null | `86400` (24h) | Blob garbage collection interval; `null` disables GC |
| `gc_min_interval_secs` | integer or null | `3600` (1h) | Minimum gap between GC runs; prevents premature candidate deletion |
| `gc_max_orphan_candidates` | integer or null | `1000000` | Cap on the orphan-candidate map carried across GC passes |
| `max_docker_chunk_count` | integer | `10000` | Maximum chunks per Docker chunked upload session |
| `jwt_expiry_secs` | integer | `86400` (24h) | JWT bearer-token lifetime; must be `>= 60` |
| `jwt_rotation_interval_secs` | integer | `604800` (7d) | JWT signing-key rotation interval; must be `>= 3600` and `>= jwt_expiry_secs` |
| `state_scan_interval_ms` | integer | `1000` (1s) | State-scanner poll interval for cross-instance state changes |
| `task_retention_secs` | integer | `86400` (24h) | Retention for completed / failed / cancelled tasks before auto-reap |
| `logging` | object or null | `null` | Override the TOML `[logging]` config at runtime (takes effect after restart). Includes an optional `s3` sub-object to enable S3 log archiving. |
| `tracing_endpoint` | string or null | `null` | Override the TOML `[tracing].otlp_endpoint` (takes effect after restart) |

## Blob Store Management

Blob stores are **not** configured in the TOML file. They are managed at runtime via the REST API:

- `POST /api/v1/stores` -- create a new blob store
- `GET /api/v1/stores` -- list all stores
- `POST /api/v1/stores/{name}/check` -- probe a store for reachability
- `DELETE /api/v1/stores/{name}` -- delete a store

There is no auto-bootstrap: on a fresh instance the server logs a warning and expects at least one store to be created before repositories can accept writes. By convention the first store is named `default`; that is the name the UI's repo-create form and the bundled compose stacks use.

### File store

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | -- | Must be `"file"` |
| `root` | path | **required** | Root directory for blob storage |
| `sync` | bool | `true` | Fsync files and parent directories after writes |
| `io_size` | integer | `1024` | Read/write buffer size in KiB |
| `direct_io` | bool | `false` | Use `O_DIRECT` for reads and writes (bypasses page cache) |

### S3 store

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | -- | Must be `"s3"` |
| `bucket` | string | **required** | S3 bucket name |
| `endpoint` | string | _(none)_ | Custom endpoint for S3-compatible stores (MinIO, Garage, etc.) |
| `region` | string | `"us-east-1"` | AWS region |
| `prefix` | string | _(none)_ | Key prefix within the bucket |
| `access_key` | string | _(none)_ | AWS access key; falls back to the default AWS credential chain |
| `secret_key` | string | _(none)_ | AWS secret key |
| `max_retries` | integer | `3` | Retry attempts |
| `connect_timeout_secs` | integer | `3` | TCP connection timeout |
| `read_timeout_secs` | integer | `0` | Per-request read timeout; `0` disables the timeout |
| `retry_mode` | string | `"standard"` | `"standard"` or `"adaptive"` |

---
title: TOML Reference
parent: Configuration
nav_order: 1
---

# TOML Config File Reference

Depot is configured via a TOML file, passed with `-c` (default:
`depotd.toml`). All fields have sensible defaults -- an empty file (or
no file at all) produces a working single-node server on port 8080 with
an embedded redb database.

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
# table_prefix = "depot"
# region = "us-east-1"
# endpoint_url = "http://localhost:8000"
# max_retries = 3
# connect_timeout_secs = 3
# read_timeout_secs = 10
# retry_mode = "standard"

# -- Logging --------------------------------------------------------
# Application logs and per-request events are written to stdout and, when
# configured, exported over OTLP. Run an OpenTelemetry collector in front
# of Depot if you need Splunk, S3, or file destinations -- the collector
# handles batching, retries, and credentials better than any in-tree sink.
[logging]
otlp_endpoint = "http://otelcol:4318"    # OTLP logs endpoint (optional)

# -- Tracing / OpenTelemetry ---------------------------------------
# [tracing]
# otlp_endpoint = "http://otel-collector:4317"
# service_name = "depot"
# deployment_environment = "production"
# sampling_ratio = 0.1
# max_traces_per_sec = 100

# -- LDAP (requires the "ldap" feature at compile time) -------------
# [ldap]
# url = "ldap://ldap.example.com:389"
# bind_dn = "cn=service,dc=example,dc=com"
# bind_password = "password"
# user_base_dn = "ou=users,dc=example,dc=com"
# user_filter = "(uid={username})"
# group_base_dn = "ou=groups,dc=example,dc=com"
# group_filter = "(member={dn})"
# group_name_attr = "cn"
# starttls = false
# tls_skip_verify = false
# fallback_to_builtin = true
#
# [ldap.group_role_mapping]
# ldap-admins = "admin"
# ldap-readers = "read-only"

# -- Initialization (first-boot provisioning) -----------------------
# Runs only when the KV store is fresh (no users yet). Silently ignored
# on already-initialized clusters. See etc/depotd.init.example.toml for
# a fully populated example.
#
# [initialization]
# [[initialization.stores]]
# name = "default"
# store_type = "file"
# root = "/data/blobs"
```

## Field Reference

### Top-Level Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `default_admin_password` | string | _(random)_ | Set admin password on first start instead of generating one |
| `server_secret` | string | _(built-in fallback)_ | Secret used to encrypt KV values at rest; see the note below |
| `worker_threads` | integer | `0` | Tokio worker-thread count; `0` = one per CPU |
| `metrics_listen` | string | _(none)_ | Dedicated Prometheus metrics listener address (e.g. `"127.0.0.1:9090"`) |

**server_secret:** Once data has been written under a given secret, the
server writes an encryption canary to KV. Starting with a different
`server_secret` will fail the canary check and refuse to start.

### `[http]`

Plain HTTP listener. At least one of `[http]` or `[https]` must be present.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen` | string | `"0.0.0.0:8080"` | Socket address |

### `[https]`

TLS listener.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen` | string | `"0.0.0.0:443"` | Socket address |
| `tls_cert` | path | **required** | PEM certificate file |
| `tls_key` | path | **required** | PEM private key file |

### `[kv_store]` -- redb

Depot runs redb in a sharded layout, so `path` points at a
**directory**, not a single file.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | -- | Must be `"redb"` |
| `path` | path | `"depot_data"` | Directory containing sharded redb files |
| `scaling_factor` | integer | `8` | Number of shards; power of two in `[1, 256]` |
| `cache_size` | integer | `268435456` (256 MiB) | Total page cache across all shards, in bytes |

### `[kv_store]` -- DynamoDB

Requires the `dynamodb` feature flag at compile time.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | -- | Must be `"dynamodb"` |
| `table_prefix` | string | **required** | Prefix for table names |
| `region` | string | `"us-east-1"` | AWS region |
| `endpoint_url` | string | _(none)_ | Override for DynamoDB Local, ScyllaDB Alternator, etc. |
| `max_retries` | integer | `3` | Retry attempts, `0..=20` |
| `connect_timeout_secs` | integer | `3` | TCP connection timeout, `1..=300` |
| `read_timeout_secs` | integer | `10` | Per-request read timeout, `1..=300` |
| `retry_mode` | string | `"standard"` | `"standard"` or `"adaptive"` |

### `[logging]`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `otlp_endpoint` | string | _(none)_ | OTLP logs endpoint (e.g. `http://otelcol:4318`) |

Depot always writes application logs to stdout. Per-request events
(`depot.request` target) go only to OTLP when configured. For Splunk,
S3, or file archival, route the OTLP stream through an OTel collector.

Legacy `capacity`, `file_path`, `splunk_hec`, and `s3` keys (or the old
`[audit]` section name) are silently ignored with a warning.

### `[tracing]`

OpenTelemetry tracing. Enabling `otlp_endpoint` raises the global log
filter to `debug` for depot targets so KV/blob child spans reach the
exporter; the stdout layer stays at `info`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `otlp_endpoint` | string | _(none)_ | OTLP gRPC endpoint (e.g. `"http://otel-collector:4317"`) |
| `service_name` | string | `"depot"` | Service name reported to the collector |
| `deployment_environment` | string | _(none)_ | Resource attribute (e.g. `"production"`) |
| `sampling_ratio` | float | _(none)_ | Fixed ratio `[0.0, 1.0]`; ignored when `max_traces_per_sec` is set |
| `max_traces_per_sec` | integer | `100` | Adaptive rate limiter; `0` = unlimited |

### `[ldap]`

Requires the `ldap` feature flag. Also configurable at runtime via
`PUT /api/v1/settings/ldap`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | **required** | LDAP server URL |
| `bind_dn` | string | **required** | Service account DN |
| `bind_password` | string | **required** | Service account password |
| `user_base_dn` | string | **required** | Base DN for user searches |
| `user_filter` | string | `"(uid={username})"` | LDAP filter template |
| `group_base_dn` | string | **required** | Base DN for group searches |
| `group_filter` | string | `"(member={dn})"` | Group filter template |
| `group_name_attr` | string | `"cn"` | Group name attribute |
| `group_role_mapping` | map | `{}` | Map LDAP group names to Depot roles |
| `starttls` | bool | `false` | Use STARTTLS |
| `tls_skip_verify` | bool | `false` | Skip TLS cert verification |
| `fallback_to_builtin` | bool | `true` | Fall back to builtin auth when LDAP user not found |

### `[initialization]`

Declarative first-boot provisioning. Runs **only** when the KV store is
fresh (no users yet); silently ignored on already-initialized clusters.
Lets you ship a self-contained config for edge caches, demos, or
CI-runner sidecars without an external bootstrap script.

Init-declared names overwrite default-bootstrap records on conflict
(`admin` user, `admin`/`read-only` roles). Declaring an `admin` user
also suppresses the random-password generation.

See [`etc/depotd.init.example.toml`](https://github.com/artifact-depot/artifact-depot/blob/main/etc/depotd.init.example.toml)
for a fully populated example. The supported sub-tables:

| Sub-table | Shape | Description |
|-----------|-------|-------------|
| `[[initialization.roles]]` | `{ name, description, capabilities: [{ capability, repo }] }` | Create roles with glob-scoped capabilities |
| `[[initialization.stores]]` | `{ name, store_type, root, ... }` | Create blob stores (same fields as `POST /api/v1/stores`) |
| `[[initialization.repositories]]` | `{ name, repo_type, format, store, ... }` | Create repositories (same fields as `POST /api/v1/repositories`) |
| `[[initialization.users]]` | `{ username, password, roles, must_change_password? }` | Create users; passwords are Argon2-hashed before persisting |
| `[initialization.settings]` | Any subset of the [runtime settings](runtime-settings.md) | Override cluster-wide defaults at first boot |

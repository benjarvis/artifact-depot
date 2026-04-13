# Configuration Reference

Depot is configured via a TOML file, passed with `-c` (default: `depotd.toml`). All fields have sensible defaults -- an empty file (or no file at all) produces a working server on port 8080 with an embedded redb database and a local filesystem blob store.

Operational settings (CORS, rate limiting, cleanup intervals, blob stores, etc.) are managed at runtime via the REST API, not in the TOML file. See [Runtime Settings](#runtime-settings) and [Blob Store Management](#blob-store-management) below.

## Minimal Configuration

```toml
listen = "0.0.0.0:8080"
default_admin_password = "admin"

[kv_store]
type = "redb"
path = "depot.redb"
```

## Full Annotated Example

```toml
# -- Network -------------------------------------------------------
listen = "0.0.0.0:443"           # Primary listener (default: "0.0.0.0:8080")
http_listen = "0.0.0.0:80"      # Optional plain-HTTP listener (useful when TLS is enabled)

# TLS -- both must be set together, or omitted for plain HTTP.
tls_cert = "/etc/depot/cert.pem"
tls_key = "/etc/depot/key.pem"

# -- Bootstrap ------------------------------------------------------
default_admin_password = "changeme"  # Admin password on first start (default: random, printed to stderr)

# -- KV Store -------------------------------------------------------
# Option A: redb (default, embedded, single-writer)
[kv_store]
type = "redb"
path = "/var/lib/depot/depot.redb"

# Option B: DynamoDB (requires the "dynamodb" feature at compile time)
# [kv_store]
# type = "dynamodb"
# table_prefix = "depot"                  # Each table becomes "{prefix}_{name}" e.g. "depot_artifacts"
# region = "us-east-1"
# endpoint_url = "http://localhost:8000"  # Optional, for DynamoDB Local or ScyllaDB Alternator
# max_retries = 3                         # Max retry attempts, 0-20 (default: 3)
# connect_timeout_secs = 3               # TCP connection timeout (default: 3)
# read_timeout_secs = 10                 # Per-request read timeout (default: 10)
# retry_mode = "standard"                # "standard" or "adaptive" (default: "standard")

# -- Audit ----------------------------------------------------------
[audit]
capacity = 1000                          # In-memory ring buffer size (default: 1000)
file_path = "/var/log/depot/audit.jsonl" # Optional file backend (JSON lines)

# Optional Splunk HEC integration
[audit.splunk_hec]
url = "https://splunk.example.com:8088"
token = "your-hec-token"
source = "depot"                         # Default: "depot"
sourcetype = "depot:audit"               # Default: "depot:audit"
index = "depot_audit"                    # Optional
tls_skip_verify = false                  # Default: false

# -- Metrics --------------------------------------------------------
metrics_listen = "127.0.0.1:9090"        # Optional dedicated Prometheus metrics listener

# -- LDAP (requires the "ldap" feature at compile time) -------------
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
| `listen` | string | `"0.0.0.0:8080"` | Primary listener socket address |
| `http_listen` | string | _(none)_ | Optional additional plain-HTTP listener; useful alongside TLS |
| `tls_cert` | path | _(none)_ | PEM certificate file; must be paired with `tls_key` |
| `tls_key` | path | _(none)_ | PEM private key file; must be paired with `tls_cert` |
| `default_admin_password` | string | _(random)_ | Set admin password on first start instead of generating one |
| `metrics_listen` | string | _(none)_ | Dedicated Prometheus metrics listener address |

### `[kv_store]` -- redb

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | -- | Must be `"redb"` |
| `path` | path | `"depot.redb"` | Path to the redb database file |

### `[kv_store]` -- DynamoDB

Requires the `dynamodb` feature flag at compile time.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | -- | Must be `"dynamodb"` |
| `table_prefix` | string | **required** | Prefix for DynamoDB table names. Each logical table becomes `"{prefix}_{name}"` |
| `region` | string | `"us-east-1"` | AWS region |
| `endpoint_url` | string | _(none)_ | Override for DynamoDB Local, ScyllaDB Alternator, etc. |

### `[audit]`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `capacity` | integer | `1000` | In-memory audit event ring buffer size |
| `file_path` | path | _(none)_ | Append audit events as JSON lines to this file |

### `[audit.splunk_hec]`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | **required** | Splunk HEC endpoint URL |
| `token` | string | **required** | HEC authentication token |
| `source` | string | `"depot"` | Event source field |
| `sourcetype` | string | `"depot:audit"` | Event sourcetype field |
| `index` | string | _(none)_ | Optional Splunk index override |
| `tls_skip_verify` | bool | `false` | Skip TLS certificate verification |

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
| `cors` | object or null | `null` | CORS configuration (allowed_origins, allowed_methods, allowed_headers, max_age_secs) |
| `rate_limit` | object or null | `null` | Rate limiting (requests_per_second, burst_size) per client IP |
| `gc_interval_secs` | integer or null | `86400` (24h) | Blob garbage collection interval |
| `max_docker_chunk_count` | integer | `10000` | Maximum chunks per Docker chunked upload session |

## Blob Store Management

Blob stores are **not** configured in the TOML file. They are managed at runtime via the REST API:

- `POST /api/v1/stores` -- create a new blob store
- `GET /api/v1/stores` -- list all stores
- `DELETE /api/v1/stores/{name}` -- delete a store

A "default" filesystem blob store is auto-created on first boot. Each repository selects its store via the `store` field at creation time.

### File store

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | -- | Must be `"file"` |
| `root` | path | -- | Root directory for blob storage |
| `sync` | bool | `true` | Fsync files and parent directories after writes |

### S3 store

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | -- | Must be `"s3"` |
| `bucket` | string | **required** | S3 bucket name |
| `endpoint` | string | _(none)_ | Custom endpoint for S3-compatible stores (MinIO, etc.) |
| `region` | string | `"us-east-1"` | AWS region |
| `prefix` | string | _(none)_ | Key prefix within the bucket |
| `access_key` | string | _(none)_ | AWS access key; falls back to default credential chain |
| `secret_key` | string | _(none)_ | AWS secret key |

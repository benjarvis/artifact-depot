// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

pub mod bootstrap;
pub mod settings;

use serde::{Deserialize, Serialize};
#[cfg(feature = "ldap")]
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplunkHecConfig {
    /// Splunk HEC endpoint URL (e.g. "https://splunk.example.com:8088")
    pub url: String,
    /// HEC authentication token.
    pub token: String,
    /// Event source field. Default: "depot"
    #[serde(default = "default_splunk_source")]
    pub source: String,
    /// Event sourcetype field. Default: "depot:log"
    #[serde(default = "default_splunk_sourcetype")]
    pub sourcetype: String,
    /// Optional Splunk index override.
    #[serde(default)]
    pub index: Option<String>,
    /// Skip TLS certificate verification (for self-signed certs).
    #[serde(default)]
    pub tls_skip_verify: bool,
}

fn default_splunk_source() -> String {
    "depot".to_string()
}

fn default_splunk_sourcetype() -> String {
    "depot:log".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_logging_capacity")]
    pub capacity: usize,
    #[serde(default)]
    pub file_path: Option<PathBuf>,
    /// Optional Splunk HEC integration.
    #[serde(default)]
    pub splunk_hec: Option<SplunkHecConfig>,
    /// OTLP endpoint for log export (e.g. Loki "http://loki:3100/otlp").
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
}

fn default_logging_capacity() -> usize {
    1000
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            capacity: default_logging_capacity(),
            file_path: None,
            splunk_hec: None,
            otlp_endpoint: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum KvStoreConfig {
    #[serde(rename = "redb")]
    Redb {
        #[serde(default = "default_db_path")]
        path: PathBuf,
        #[serde(default = "default_scaling_factor")]
        scaling_factor: usize,
        /// Total page cache across all shards (bytes). Default 256 MiB.
        #[serde(default = "default_cache_size")]
        cache_size: usize,
    },
    #[cfg(feature = "dynamodb")]
    #[serde(rename = "dynamodb")]
    DynamoDB {
        /// Prefix for DynamoDB table names. Each logical table becomes
        /// `"{table_prefix}_{table}"` (e.g. `"depot_artifacts"`).
        table_prefix: String,
        #[serde(default = "default_dynamodb_region")]
        region: String,
        /// Override endpoint for DynamoDB Local testing.
        #[serde(default)]
        endpoint_url: Option<String>,
        /// Maximum number of retry attempts for failed DynamoDB requests.
        #[serde(default = "default_dynamodb_max_retries")]
        max_retries: u32,
        /// TCP connection timeout in seconds.
        #[serde(default = "default_dynamodb_connect_timeout_secs")]
        connect_timeout_secs: u64,
        /// Per-request read timeout in seconds.
        #[serde(default = "default_dynamodb_read_timeout_secs")]
        read_timeout_secs: u64,
        /// Retry strategy: "standard" (jittered backoff) or "adaptive"
        /// (adds client-side rate limiting).
        #[serde(default = "default_dynamodb_retry_mode")]
        retry_mode: String,
    },
}

#[cfg(feature = "dynamodb")]
fn default_dynamodb_region() -> String {
    "us-east-1".to_string()
}

#[cfg(feature = "dynamodb")]
fn default_dynamodb_max_retries() -> u32 {
    3
}

#[cfg(feature = "dynamodb")]
fn default_dynamodb_connect_timeout_secs() -> u64 {
    3
}

#[cfg(feature = "dynamodb")]
fn default_dynamodb_read_timeout_secs() -> u64 {
    10
}

#[cfg(feature = "dynamodb")]
fn default_dynamodb_retry_mode() -> String {
    "standard".to_string()
}

fn default_scaling_factor() -> usize {
    8
}

/// 256 MiB total cache, divided across all shards.
fn default_cache_size() -> usize {
    256 * 1024 * 1024
}

fn default_kv_store() -> KvStoreConfig {
    KvStoreConfig::Redb {
        path: default_db_path(),
        scaling_factor: default_scaling_factor(),
        cache_size: default_cache_size(),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct CorsConfig {
    /// Allowed origins (exact strings like "http://localhost:3000", or "*" for any).
    #[serde(default)]
    pub allowed_origins: Vec<String>,

    /// Allowed HTTP methods. Defaults to common methods.
    #[serde(default = "default_cors_methods")]
    pub allowed_methods: Vec<String>,

    /// Allowed request headers. Defaults to Authorization, Content-Type, Accept.
    #[serde(default = "default_cors_headers")]
    pub allowed_headers: Vec<String>,

    /// Preflight cache max-age in seconds. Defaults to 3600.
    #[serde(default = "default_cors_max_age")]
    pub max_age_secs: u64,
}

fn default_cors_methods() -> Vec<String> {
    ["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

fn default_cors_headers() -> Vec<String> {
    ["Authorization", "Content-Type", "Accept"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

fn default_cors_max_age() -> u64 {
    3600
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct RateLimitConfig {
    /// Sustained requests per second allowed per client IP. Default: 100.
    #[serde(default = "default_rate_limit_rps")]
    pub requests_per_second: u64,

    /// Maximum burst size above the sustained rate. Default: 50.
    #[serde(default = "default_rate_limit_burst")]
    pub burst_size: u32,
}

fn default_rate_limit_rps() -> u64 {
    100
}

fn default_rate_limit_burst() -> u32 {
    50
}

/// LDAP authentication configuration.
#[cfg(feature = "ldap")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct LdapConfig {
    /// LDAP server URL (e.g. "ldap://ldap.example.com:389")
    pub url: String,
    /// Service account DN used for user lookups.
    pub bind_dn: String,
    /// Service account password.
    pub bind_password: String,
    /// Base DN for user searches.
    pub user_base_dn: String,
    /// LDAP filter template for finding users. `{username}` is replaced with the login name.
    #[serde(default = "default_user_filter")]
    pub user_filter: String,
    /// Base DN for group searches.
    pub group_base_dn: String,
    /// LDAP filter template for finding groups a user belongs to. `{dn}` is replaced with the user DN.
    #[serde(default = "default_group_filter")]
    pub group_filter: String,
    /// Attribute that holds the group name.
    #[serde(default = "default_group_name_attr")]
    pub group_name_attr: String,
    /// Map LDAP group names to depot role names.
    #[serde(default)]
    pub group_role_mapping: HashMap<String, String>,
    /// Use STARTTLS on the connection.
    #[serde(default)]
    pub starttls: bool,
    /// Skip TLS certificate verification (insecure, for testing only).
    #[serde(default)]
    pub tls_skip_verify: bool,
    /// If true (default), fall back to builtin auth when LDAP lookup finds no matching user.
    #[serde(default = "default_true")]
    pub fallback_to_builtin: bool,
}

#[cfg(feature = "ldap")]
fn default_user_filter() -> String {
    "(uid={username})".to_string()
}

#[cfg(feature = "ldap")]
fn default_group_filter() -> String {
    "(member={dn})".to_string()
}

#[cfg(feature = "ldap")]
fn default_group_name_attr() -> String {
    "cn".to_string()
}

/// Tracing / OpenTelemetry configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
    /// OTLP gRPC endpoint for trace export (e.g. "http://localhost:4317").
    #[serde(default)]
    pub otlp_endpoint: Option<String>,

    /// Override the service name reported to the OTLP collector (default: "depot").
    #[serde(default)]
    pub service_name: Option<String>,

    /// Deployment environment label (e.g. "production", "staging").
    #[serde(default)]
    pub deployment_environment: Option<String>,

    /// Trace sampling ratio between 0.0 and 1.0 (default: 1.0 = sample everything).
    /// Ignored when `max_traces_per_sec` is set.
    #[serde(default)]
    pub sampling_ratio: Option<f64>,

    /// Maximum root traces exported per second.  At low throughput every trace
    /// is sampled; once the rate exceeds this limit, excess root traces are
    /// dropped.  Child spans of sampled traces are always kept.
    /// Default: 100.  Set to 0 to disable rate limiting (sample everything).
    #[serde(default)]
    pub max_traces_per_sec: Option<u64>,
}

/// Plain HTTP listener configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    /// Address to listen on, e.g. "0.0.0.0:8080"
    #[serde(default = "default_listen")]
    pub listen: String,
}

/// HTTPS / TLS listener configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpsConfig {
    /// Address to listen on, e.g. "0.0.0.0:443"
    #[serde(default = "default_https_listen")]
    pub listen: String,

    /// Path to TLS certificate file (PEM).
    pub tls_cert: PathBuf,

    /// Path to TLS private key file (PEM).
    pub tls_key: PathBuf,
}

fn default_https_listen() -> String {
    "0.0.0.0:443".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Plain HTTP listener. Omit to disable when `[https]` is configured.
    #[serde(default)]
    pub http: Option<HttpConfig>,

    /// HTTPS / TLS listener. When present, the server serves TLS on this address.
    #[serde(default)]
    pub https: Option<HttpsConfig>,

    /// KV store backend configuration.
    #[serde(default = "default_kv_store")]
    pub kv_store: KvStoreConfig,

    /// If set, used as the admin password on first start (for dev/test).
    /// Otherwise a random password is generated.
    #[serde(default)]
    pub default_admin_password: Option<String>,

    /// Logging endpoint configuration (replaces the old `[audit]` section).
    #[serde(default, alias = "audit")]
    pub logging: LoggingConfig,

    /// Optional address for a dedicated metrics listener (e.g. "127.0.0.1:9090").
    /// When set, `/metrics` is served on this listener instead of the main port.
    #[serde(default)]
    pub metrics_listen: Option<String>,

    /// Secret used to encrypt KV store values at rest.
    /// When omitted, a built-in fallback key is used and a warning is emitted.
    /// Changing this after data has been written will prevent the server from
    /// starting (the canary decryption check will fail).
    #[serde(default)]
    pub server_secret: Option<String>,

    /// Tracing / OpenTelemetry configuration.
    #[serde(default)]
    pub tracing: Option<TracingConfig>,

    /// Number of tokio worker threads. 0 (default) = automatic (one per CPU).
    /// Reducing this on machines with many cores can lower thread wake/park
    /// contention overhead at high request concurrency.
    #[serde(default)]
    pub worker_threads: usize,
}

#[cfg(feature = "ldap")]
fn default_true() -> bool {
    true
}

impl Config {
    pub fn tls_enabled(&self) -> bool {
        self.https.is_some()
    }

    /// Validate configuration values, returning all errors at once so the user
    /// can fix everything in a single pass.
    fn validate(&self) -> anyhow::Result<()> {
        let mut errors: Vec<String> = Vec::new();

        // [http] section validation
        if let Some(ref http) = self.http {
            if http.listen.parse::<std::net::SocketAddr>().is_err() {
                errors.push(format!(
                    "http.listen: {:?} is not a valid socket address",
                    http.listen
                ));
            }
        }

        // [https] section validation
        if let Some(ref https) = self.https {
            if https.listen.parse::<std::net::SocketAddr>().is_err() {
                errors.push(format!(
                    "https.listen: {:?} is not a valid socket address",
                    https.listen
                ));
            }
            if !https.tls_cert.exists() {
                errors.push(format!(
                    "https.tls_cert: file {:?} does not exist",
                    https.tls_cert
                ));
            }
            if !https.tls_key.exists() {
                errors.push(format!(
                    "https.tls_key: file {:?} does not exist",
                    https.tls_key
                ));
            }
        }

        // Must have at least one listener
        if self.http.is_none() && self.https.is_none() {
            errors.push("at least one of [http] or [https] must be configured".to_string());
        }

        // KV store checks
        match &self.kv_store {
            KvStoreConfig::Redb {
                path,
                scaling_factor,
                ..
            } => {
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() && !parent.exists() {
                        errors.push(format!(
                            "kv_store.redb.path: parent directory {:?} does not exist",
                            parent
                        ));
                    }
                }
                if *scaling_factor == 0
                    || *scaling_factor > 256
                    || !scaling_factor.is_power_of_two()
                {
                    errors.push(format!(
                        "kv_store.redb.scaling_factor: {} must be a power of 2 in [1, 256]",
                        scaling_factor
                    ));
                }
            }
            #[cfg(feature = "dynamodb")]
            KvStoreConfig::DynamoDB {
                table_prefix,
                max_retries,
                connect_timeout_secs,
                read_timeout_secs,
                retry_mode,
                ..
            } => {
                if table_prefix.is_empty() {
                    errors.push("kv_store.dynamodb.table_prefix must not be empty".to_string());
                }
                if *max_retries > 20 {
                    errors.push(format!(
                        "kv_store.dynamodb.max_retries: {} must be in [0, 20]",
                        max_retries
                    ));
                }
                if *connect_timeout_secs == 0 || *connect_timeout_secs > 300 {
                    errors.push(format!(
                        "kv_store.dynamodb.connect_timeout_secs: {} must be in [1, 300]",
                        connect_timeout_secs
                    ));
                }
                if *read_timeout_secs == 0 || *read_timeout_secs > 300 {
                    errors.push(format!(
                        "kv_store.dynamodb.read_timeout_secs: {} must be in [1, 300]",
                        read_timeout_secs
                    ));
                }
                if retry_mode != "standard" && retry_mode != "adaptive" {
                    errors.push(format!(
                        "kv_store.dynamodb.retry_mode: {:?} must be \"standard\" or \"adaptive\"",
                        retry_mode
                    ));
                }
            }
        }

        // Tracing config checks
        if let Some(ref tracing_cfg) = self.tracing {
            if let Some(ref ep) = tracing_cfg.otlp_endpoint {
                if ep.is_empty() {
                    errors.push("tracing.otlp_endpoint must not be empty when set".to_string());
                } else if !ep.starts_with("http://") && !ep.starts_with("https://") {
                    errors.push(format!(
                        "tracing.otlp_endpoint: {:?} must start with http:// or https://",
                        ep
                    ));
                }
            }
        }

        // Splunk HEC config checks
        if let Some(ref hec) = self.logging.splunk_hec {
            if hec.url.is_empty() {
                errors.push("logging.splunk_hec.url must not be empty".to_string());
            }
            if hec.token.is_empty() {
                errors.push("logging.splunk_hec.token must not be empty".to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!(
                "configuration has {} error(s):\n  - {}",
                errors.len(),
                errors.join("\n  - ")
            );
        }
    }
}

fn default_listen() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_db_path() -> PathBuf {
    PathBuf::from("depot_data")
}

impl Default for Config {
    fn default() -> Self {
        Self {
            http: Some(HttpConfig {
                listen: default_listen(),
            }),
            https: None,
            kv_store: default_kv_store(),
            default_admin_password: None,
            logging: LoggingConfig::default(),
            metrics_listen: None,
            server_secret: None,
            tracing: None,
            worker_threads: 0,
        }
    }
}

pub fn load(path: &Path) -> anyhow::Result<Config> {
    if path.exists() {
        let text = std::fs::read_to_string(path)?;
        let mut cfg: Config = toml::from_str(&text)?;
        // When neither [http] nor [https] is specified, default to plain HTTP.
        if cfg.http.is_none() && cfg.https.is_none() {
            cfg.http = Some(HttpConfig {
                listen: default_listen(),
            });
        }
        cfg.validate()?;
        Ok(cfg)
    } else {
        tracing::warn!(
            "config file not found at {}, using defaults",
            path.display()
        );
        Ok(Config::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_default_values() {
        let cfg = Config::default();
        let http = cfg.http.as_ref().expect("default has http");
        assert_eq!(http.listen, "0.0.0.0:8080");
        assert!(cfg.https.is_none());
        assert!(
            matches!(cfg.kv_store, KvStoreConfig::Redb { ref path, scaling_factor, .. } if *path == PathBuf::from("depot_data") && scaling_factor == 8)
        );
        assert!(cfg.default_admin_password.is_none());
    }

    #[test]
    fn test_tls_enabled_with_https() {
        let mut cfg = Config::default();
        cfg.https = Some(HttpsConfig {
            listen: "0.0.0.0:443".to_string(),
            tls_cert: PathBuf::from("cert.pem"),
            tls_key: PathBuf::from("key.pem"),
        });
        assert!(cfg.tls_enabled());
    }

    #[test]
    fn test_tls_enabled_without_https() {
        let cfg = Config::default();
        assert!(!cfg.tls_enabled());
    }

    #[test]
    fn test_load_defaults_when_file_missing() {
        let cfg = load(Path::new("/tmp/nonexistent-depot-config-12345.toml")).unwrap();
        assert_eq!(
            cfg.http.as_ref().expect("default has http").listen,
            "0.0.0.0:8080"
        );
    }

    #[test]
    fn test_load_minimal_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "[http]").unwrap();
        writeln!(f, r#"listen = "127.0.0.1:9090""#).unwrap();
        drop(f);

        let cfg = load(&path).unwrap();
        assert_eq!(cfg.http.as_ref().unwrap().listen, "127.0.0.1:9090");
        assert!(matches!(cfg.kv_store, KvStoreConfig::Redb { .. }));
    }

    #[test]
    fn test_load_full_config() {
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, "fake cert").unwrap();
        std::fs::write(&key_path, "fake key").unwrap();

        let path = dir.path().join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, r#"default_admin_password = "secret""#).unwrap();
        writeln!(f).unwrap();
        writeln!(f, "[http]").unwrap();
        writeln!(f, r#"listen = "0.0.0.0:80""#).unwrap();
        writeln!(f).unwrap();
        writeln!(f, "[https]").unwrap();
        writeln!(f, r#"listen = "0.0.0.0:443""#).unwrap();
        writeln!(f, "tls_cert = {:?}", cert_path.to_str().unwrap()).unwrap();
        writeln!(f, "tls_key = {:?}", key_path.to_str().unwrap()).unwrap();
        drop(f);

        let cfg = load(&path).unwrap();
        assert_eq!(cfg.http.as_ref().unwrap().listen, "0.0.0.0:80");
        let https = cfg.https.as_ref().unwrap();
        assert_eq!(https.listen, "0.0.0.0:443");
        assert!(cfg.tls_enabled());
        assert_eq!(cfg.default_admin_password.as_deref(), Some("secret"));
    }

    // --- Validation tests ---

    #[test]
    fn test_validate_default_config_passes() {
        Config::default().validate().unwrap();
    }

    #[test]
    fn test_validate_bad_http_listen() {
        let mut cfg = Config::default();
        cfg.http = Some(HttpConfig {
            listen: "not-an-address".to_string(),
        });
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("http.listen:"), "{err}");
        assert!(err.contains("not a valid socket address"), "{err}");
    }

    #[test]
    fn test_validate_bad_https_listen() {
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, "fake").unwrap();
        std::fs::write(&key_path, "fake").unwrap();

        let mut cfg = Config::default();
        cfg.https = Some(HttpsConfig {
            listen: "bad".to_string(),
            tls_cert: cert_path,
            tls_key: key_path,
        });
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("https.listen:"), "{err}");
    }

    #[test]
    fn test_validate_https_files_not_found() {
        let mut cfg = Config::default();
        cfg.https = Some(HttpsConfig {
            listen: "0.0.0.0:443".to_string(),
            tls_cert: PathBuf::from("/nonexistent/cert.pem"),
            tls_key: PathBuf::from("/nonexistent/key.pem"),
        });
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("https.tls_cert: file"), "{err}");
        assert!(err.contains("https.tls_key: file"), "{err}");
        assert!(err.contains("does not exist"), "{err}");
    }

    #[test]
    fn test_validate_no_listeners() {
        let mut cfg = Config::default();
        cfg.http = None;
        cfg.https = None;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("at least one of [http] or [https] must be configured"),
            "{err}"
        );
    }

    #[test]
    fn test_validate_redb_nonexistent_parent() {
        let mut cfg = Config::default();
        cfg.kv_store = KvStoreConfig::Redb {
            path: PathBuf::from("/no/such/dir/depot_data"),
            scaling_factor: 8,
            cache_size: 0,
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("parent directory"), "{err}");
    }

    #[test]
    fn test_validate_redb_bare_filename_passes() {
        let mut cfg = Config::default();
        cfg.kv_store = KvStoreConfig::Redb {
            path: PathBuf::from("depot_data"),
            scaling_factor: 8,
            cache_size: 0,
        };
        cfg.validate().unwrap();
    }

    #[test]
    fn test_validate_redb_scaling_factor_not_power_of_two() {
        let mut cfg = Config::default();
        cfg.kv_store = KvStoreConfig::Redb {
            path: PathBuf::from("depot_data"),
            scaling_factor: 3,
            cache_size: 0,
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("scaling_factor"), "{err}");
    }

    #[test]
    fn test_validate_redb_scaling_factor_zero() {
        let mut cfg = Config::default();
        cfg.kv_store = KvStoreConfig::Redb {
            path: PathBuf::from("depot_data"),
            scaling_factor: 0,
            cache_size: 0,
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("scaling_factor"), "{err}");
    }

    #[test]
    fn test_validate_redb_scaling_factor_too_large() {
        let mut cfg = Config::default();
        cfg.kv_store = KvStoreConfig::Redb {
            path: PathBuf::from("depot_data"),
            scaling_factor: 512,
            cache_size: 0,
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("scaling_factor"), "{err}");
    }

    #[test]
    fn test_validate_multiple_errors_collected() {
        let mut cfg = Config::default();
        cfg.http = Some(HttpConfig {
            listen: "bad".to_string(),
        });
        cfg.https = Some(HttpsConfig {
            listen: "0.0.0.0:443".to_string(),
            tls_cert: PathBuf::from("/nonexistent/cert.pem"),
            tls_key: PathBuf::from("/nonexistent/key.pem"),
        });
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("3 error(s)"), "{err}");
        assert!(err.contains("http.listen:"), "{err}");
        assert!(err.contains("https.tls_cert"), "{err}");
    }

    #[test]
    fn test_validate_splunk_hec_empty_url() {
        let mut cfg = Config::default();
        cfg.logging.splunk_hec = Some(SplunkHecConfig {
            url: "".to_string(),
            token: "my-token".to_string(),
            source: "depot".to_string(),
            sourcetype: "depot:log".to_string(),
            index: None,
            tls_skip_verify: false,
        });
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("logging.splunk_hec.url must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn test_validate_splunk_hec_empty_token() {
        let mut cfg = Config::default();
        cfg.logging.splunk_hec = Some(SplunkHecConfig {
            url: "https://splunk.example.com:8088".to_string(),
            token: "".to_string(),
            source: "depot".to_string(),
            sourcetype: "depot:log".to_string(),
            index: None,
            tls_skip_verify: false,
        });
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("logging.splunk_hec.token must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn test_validate_splunk_hec_valid() {
        let mut cfg = Config::default();
        cfg.logging.splunk_hec = Some(SplunkHecConfig {
            url: "https://splunk.example.com:8088".to_string(),
            token: "my-token".to_string(),
            source: "depot".to_string(),
            sourcetype: "depot:log".to_string(),
            index: Some("depot_logs".to_string()),
            tls_skip_verify: false,
        });
        cfg.validate().unwrap();
    }

    #[test]
    fn test_splunk_hec_toml_deserialization() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "[http]").unwrap();
        writeln!(f, r#"listen = "0.0.0.0:8080""#).unwrap();
        writeln!(f).unwrap();
        // Use the old [audit] key to test backward compat alias.
        writeln!(f, "[audit.splunk_hec]").unwrap();
        writeln!(f, r#"url = "https://splunk.example.com:8088""#).unwrap();
        writeln!(f, r#"token = "test-token-123""#).unwrap();
        writeln!(f, r#"index = "my_index""#).unwrap();
        drop(f);

        let cfg = load(&path).unwrap();
        let hec = cfg.logging.splunk_hec.unwrap();
        assert_eq!(hec.url, "https://splunk.example.com:8088");
        assert_eq!(hec.token, "test-token-123");
        assert_eq!(hec.source, "depot"); // default
        assert_eq!(hec.sourcetype, "depot:log"); // default
        assert_eq!(hec.index.as_deref(), Some("my_index"));
        assert!(!hec.tls_skip_verify);
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn test_validate_dynamodb_defaults_pass() {
        let mut cfg = Config::default();
        cfg.kv_store = KvStoreConfig::DynamoDB {
            table_prefix: "depot".to_string(),
            region: default_dynamodb_region(),
            endpoint_url: None,
            max_retries: default_dynamodb_max_retries(),
            connect_timeout_secs: default_dynamodb_connect_timeout_secs(),
            read_timeout_secs: default_dynamodb_read_timeout_secs(),
            retry_mode: default_dynamodb_retry_mode(),
        };
        cfg.validate().unwrap();
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn test_validate_dynamodb_bad_retry_mode() {
        let mut cfg = Config::default();
        cfg.kv_store = KvStoreConfig::DynamoDB {
            table_prefix: "depot".to_string(),
            region: default_dynamodb_region(),
            endpoint_url: None,
            max_retries: 3,
            connect_timeout_secs: 3,
            read_timeout_secs: 10,
            retry_mode: "exponential".to_string(),
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("retry_mode"), "{err}");
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn test_validate_dynamodb_max_retries_too_high() {
        let mut cfg = Config::default();
        cfg.kv_store = KvStoreConfig::DynamoDB {
            table_prefix: "depot".to_string(),
            region: default_dynamodb_region(),
            endpoint_url: None,
            max_retries: 25,
            connect_timeout_secs: 3,
            read_timeout_secs: 10,
            retry_mode: "standard".to_string(),
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("max_retries"), "{err}");
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn test_validate_dynamodb_zero_connect_timeout() {
        let mut cfg = Config::default();
        cfg.kv_store = KvStoreConfig::DynamoDB {
            table_prefix: "depot".to_string(),
            region: default_dynamodb_region(),
            endpoint_url: None,
            max_retries: 3,
            connect_timeout_secs: 0,
            read_timeout_secs: 10,
            retry_mode: "standard".to_string(),
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("connect_timeout_secs"), "{err}");
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn test_validate_dynamodb_zero_read_timeout() {
        let mut cfg = Config::default();
        cfg.kv_store = KvStoreConfig::DynamoDB {
            table_prefix: "depot".to_string(),
            region: default_dynamodb_region(),
            endpoint_url: None,
            max_retries: 3,
            connect_timeout_secs: 3,
            read_timeout_secs: 0,
            retry_mode: "standard".to_string(),
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("read_timeout_secs"), "{err}");
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn test_dynamodb_toml_deserialization() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "[kv_store]").unwrap();
        writeln!(f, r#"type = "dynamodb""#).unwrap();
        writeln!(f, r#"table_prefix = "myapp""#).unwrap();
        writeln!(f, r#"region = "eu-west-1""#).unwrap();
        writeln!(f, "max_retries = 5").unwrap();
        writeln!(f, "connect_timeout_secs = 10").unwrap();
        writeln!(f, "read_timeout_secs = 30").unwrap();
        writeln!(f, r#"retry_mode = "adaptive""#).unwrap();
        drop(f);

        let cfg = load(&path).unwrap();
        match &cfg.kv_store {
            KvStoreConfig::DynamoDB {
                table_prefix,
                region,
                max_retries,
                connect_timeout_secs,
                read_timeout_secs,
                retry_mode,
                ..
            } => {
                assert_eq!(table_prefix, "myapp");
                assert_eq!(region, "eu-west-1");
                assert_eq!(*max_retries, 5);
                assert_eq!(*connect_timeout_secs, 10);
                assert_eq!(*read_timeout_secs, 30);
                assert_eq!(retry_mode, "adaptive");
            }
            _ => panic!("expected DynamoDB variant"),
        }
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn test_dynamodb_toml_defaults_when_omitted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "[kv_store]").unwrap();
        writeln!(f, r#"type = "dynamodb""#).unwrap();
        writeln!(f, r#"table_prefix = "depot""#).unwrap();
        drop(f);

        let cfg = load(&path).unwrap();
        match &cfg.kv_store {
            KvStoreConfig::DynamoDB {
                max_retries,
                connect_timeout_secs,
                read_timeout_secs,
                retry_mode,
                ..
            } => {
                assert_eq!(*max_retries, 3);
                assert_eq!(*connect_timeout_secs, 3);
                assert_eq!(*read_timeout_secs, 10);
                assert_eq!(retry_mode, "standard");
            }
            _ => panic!("expected DynamoDB variant"),
        }
    }

    #[test]
    fn test_https_only_no_http() {
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, "fake cert").unwrap();
        std::fs::write(&key_path, "fake key").unwrap();

        let path = dir.path().join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        // Only [https] — no [http] section, but since [https] is present
        // the auto-default for [http] does not kick in.
        writeln!(f, "[https]").unwrap();
        writeln!(f, r#"listen = "0.0.0.0:443""#).unwrap();
        writeln!(f, "tls_cert = {:?}", cert_path.to_str().unwrap()).unwrap();
        writeln!(f, "tls_key = {:?}", key_path.to_str().unwrap()).unwrap();
        drop(f);

        let cfg = load(&path).unwrap();
        assert!(cfg.http.is_none());
        assert!(cfg.tls_enabled());
    }
}

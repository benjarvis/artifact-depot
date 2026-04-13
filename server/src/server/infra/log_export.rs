// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use opentelemetry::logs::AnyValue;
use opentelemetry::Key;
use opentelemetry_sdk::logs::{LogExporter, SdkLogRecord};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// RequestEvent — the structured record for per-request logging
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct RequestEvent {
    pub id: u64,
    #[schema(value_type = String, format = "date-time")]
    pub timestamp: DateTime<Utc>,
    pub request_id: String,
    pub username: String,
    pub ip: String,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub action: String,
    pub elapsed_ns: u64,
    pub bytes_recv: u64,
    pub bytes_sent: u64,
    pub direction: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Action classification
// ---------------------------------------------------------------------------

pub fn classify_action(method: &str, path: &str) -> String {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    match (method, segments.as_slice()) {
        // Auth
        ("POST", ["api", "v1", "auth", "login"]) => "auth.login".to_string(),
        ("POST", ["api", "v1", "auth", "change-password"]) => "auth.change_password".to_string(),

        // Users
        ("GET", ["api", "v1", "users"]) => "user.list".to_string(),
        ("POST", ["api", "v1", "users"]) => "user.create".to_string(),
        ("GET", ["api", "v1", "users", _]) => "user.get".to_string(),
        ("PUT", ["api", "v1", "users", _]) => "user.update".to_string(),
        ("DELETE", ["api", "v1", "users", _]) => "user.delete".to_string(),

        // Roles
        ("GET", ["api", "v1", "roles"]) => "role.list".to_string(),
        ("POST", ["api", "v1", "roles"]) => "role.create".to_string(),
        ("GET", ["api", "v1", "roles", _]) => "role.get".to_string(),
        ("PUT", ["api", "v1", "roles", _]) => "role.update".to_string(),
        ("DELETE", ["api", "v1", "roles", _]) => "role.delete".to_string(),

        // Repositories
        ("GET", ["api", "v1", "repositories"]) => "repo.list".to_string(),
        ("POST", ["api", "v1", "repositories"]) => "repo.create".to_string(),
        ("GET", ["api", "v1", "repositories", _]) => "repo.get".to_string(),
        ("DELETE", ["api", "v1", "repositories", _]) => "repo.delete".to_string(),

        // Artifacts
        ("GET", ["api", "v1", "repositories", _, "artifacts"]) => "artifact.list".to_string(),
        ("GET", ["repository", _, ..]) => "artifact.download".to_string(),
        ("PUT", ["repository", _, ..]) => "artifact.upload".to_string(),
        ("DELETE", ["repository", _, ..]) => "artifact.delete".to_string(),

        // Logging
        ("GET", ["api", "v1", "logging", "events"]) => "logging.list".to_string(),

        // Tasks
        ("GET", ["api", "v1", "tasks"]) => "task.list".to_string(),
        ("POST", ["api", "v1", "tasks", "check"]) => "task.start_check".to_string(),
        ("GET", ["api", "v1", "tasks", _]) => "task.get".to_string(),
        ("DELETE", ["api", "v1", "tasks", _]) => "task.delete".to_string(),

        // Health
        ("GET", ["api", "v1", "health"]) => "system.health".to_string(),

        // Docker V2
        ("GET", ["v2", ""]) | ("GET", ["v2"]) => "docker.check".to_string(),
        ("GET", ["v2", "_catalog"]) => "docker.catalog".to_string(),
        (_, path_segs) if path_segs.first() == Some(&"v2") => {
            let last_known = path_segs.last().copied().unwrap_or("");
            if path_segs.contains(&"manifests") {
                match method {
                    "GET" | "HEAD" => "docker.pull_manifest".to_string(),
                    "PUT" => "docker.push_manifest".to_string(),
                    "DELETE" => "docker.delete_manifest".to_string(),
                    _ => format!("{method} {path}"),
                }
            } else if path_segs.contains(&"blobs") && !path_segs.contains(&"uploads") {
                match method {
                    "GET" | "HEAD" => "docker.pull_blob".to_string(),
                    _ => format!("{method} {path}"),
                }
            } else if path_segs.contains(&"uploads") {
                match method {
                    "POST" => "docker.start_upload".to_string(),
                    "PATCH" => "docker.patch_upload".to_string(),
                    "PUT" => "docker.complete_upload".to_string(),
                    _ => format!("{method} {path}"),
                }
            } else if last_known == "list" || path_segs.contains(&"tags") {
                "docker.list_tags".to_string()
            } else {
                format!("{method} {path}")
            }
        }

        // PyPI
        ("POST", ["pypi", _, ..]) => "pypi.upload".to_string(),
        ("GET", ["pypi", _, "simple", ..]) => "pypi.simple".to_string(),
        ("GET", ["pypi", _, "packages", ..]) => "pypi.download".to_string(),

        // APT
        ("POST", ["apt", _, "upload"]) => "apt.upload".to_string(),
        ("GET", ["apt", _, ..]) => "apt.download".to_string(),

        // Catch-all
        _ => format!("{method} {path}"),
    }
}

// ---------------------------------------------------------------------------
// Tracing event emission helpers
// ---------------------------------------------------------------------------

/// Extract the current OTel trace ID from the active tracing span.
/// Returns `None` when there is no valid context or when the trace was
/// not sampled (so we don't emit dead links in the log events).
fn current_trace_id() -> Option<String> {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let cx = tracing::Span::current().context();
    let span_ref = cx.span();
    let sc = span_ref.span_context();
    if sc.is_valid() && sc.is_sampled() {
        Some(sc.trace_id().to_string())
    } else {
        None
    }
}

/// Emit a structured per-request event as an OTel LogRecord via the tracing
/// bridge. The `depot.request` target is filtered out of stderr by the fmt
/// layer, so these events only reach the configured logging endpoints.
#[allow(clippy::too_many_arguments)]
pub fn emit_request_event(
    request_id: &str,
    username: &str,
    ip: &str,
    method: &str,
    path: &str,
    status: u16,
    action: &str,
    elapsed_ns: u64,
    bytes_recv: u64,
    bytes_sent: u64,
) {
    let trace_id = current_trace_id().unwrap_or_default();
    tracing::info!(
        target: "depot.request",
        request_id,
        username,
        ip,
        method,
        path,
        status,
        action,
        elapsed_ns,
        bytes_recv,
        bytes_sent,
        direction = "downstream",
        trace_id = trace_id.as_str(),
        "request"
    );
}

/// Emit a structured upstream request event (for proxy/cache fetches).
pub fn emit_upstream_event(
    method: &str,
    url: &str,
    status: u16,
    elapsed: std::time::Duration,
    bytes_sent: u64,
    bytes_recv: u64,
    action: &str,
) {
    let peer = reqwest::Url::parse(url)
        .ok()
        .and_then(|u| {
            u.host_str().map(|h| match u.port() {
                Some(p) => format!("{h}:{p}"),
                None => h.to_string(),
            })
        })
        .unwrap_or_default();

    let path = reqwest::Url::parse(url)
        .ok()
        .map(|u| u.path().to_string())
        .unwrap_or_else(|| url.to_string());

    let trace_id = current_trace_id().unwrap_or_default();
    let elapsed_ns = elapsed.as_nanos() as u64;

    tracing::info!(
        target: "depot.request",
        request_id = "",
        username = "",
        ip = peer.as_str(),
        method,
        path = path.as_str(),
        status,
        action,
        elapsed_ns,
        bytes_recv,
        bytes_sent,
        direction = "upstream",
        trace_id = trace_id.as_str(),
        "request"
    );
}

// ---------------------------------------------------------------------------
// InMemoryLogExporter — ring buffer for dashboard / API
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct InMemoryLogExporter {
    events: tokio::sync::RwLock<VecDeque<RequestEvent>>,
    counter: AtomicU64,
    capacity: usize,
}

impl InMemoryLogExporter {
    pub fn new(capacity: usize) -> Self {
        Self {
            events: tokio::sync::RwLock::new(VecDeque::with_capacity(capacity)),
            counter: AtomicU64::new(0),
            capacity,
        }
    }

    pub async fn recent(&self, limit: usize) -> Vec<RequestEvent> {
        let events = self.events.read().await;
        events.iter().rev().take(limit).cloned().collect()
    }

    /// Record a pre-built event directly (used by the request middleware).
    pub async fn record(&self, mut event: RequestEvent) {
        event.id = self.counter.fetch_add(1, Ordering::Relaxed);
        let mut events = self.events.write().await;
        events.push_back(event);
        while events.len() > self.capacity {
            events.pop_front();
        }
    }
}

impl LogExporter for InMemoryLogExporter {
    fn export(
        &self,
        batch: opentelemetry_sdk::logs::LogBatch<'_>,
    ) -> impl std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send {
        // Convert any depot.request LogRecords to RequestEvents.
        let events: Vec<RequestEvent> = batch
            .iter()
            .filter_map(|(record, _scope)| log_record_to_request_event(record))
            .collect();

        async move {
            if !events.is_empty() {
                let mut buf = self.events.write().await;
                for mut event in events {
                    event.id = self.counter.fetch_add(1, Ordering::Relaxed);
                    buf.push_back(event);
                    while buf.len() > self.capacity {
                        buf.pop_front();
                    }
                }
            }
            Ok(())
        }
    }

    fn shutdown(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }

    fn set_resource(&mut self, _resource: &opentelemetry_sdk::Resource) {}
}

// ---------------------------------------------------------------------------
// FileLogExporter — append JSONL to a local file
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct FileLogExporter {
    writer: std::sync::Mutex<std::io::BufWriter<std::fs::File>>,
}

impl FileLogExporter {
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            writer: std::sync::Mutex::new(std::io::BufWriter::new(file)),
        })
    }
}

impl LogExporter for FileLogExporter {
    fn export(
        &self,
        batch: opentelemetry_sdk::logs::LogBatch<'_>,
    ) -> impl std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send {
        use std::io::Write;

        let events: Vec<RequestEvent> = batch
            .iter()
            .filter_map(|(record, _scope)| log_record_to_request_event(record))
            .collect();

        if let Ok(mut w) = self.writer.lock() {
            for event in &events {
                if let Ok(json) = serde_json::to_string(event) {
                    let _ = w.write_all(json.as_bytes());
                    let _ = w.write_all(b"\n");
                }
            }
            let _ = w.flush();
        }

        std::future::ready(Ok(()))
    }

    fn shutdown(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }

    fn set_resource(&mut self, _resource: &opentelemetry_sdk::Resource) {}
}

// ---------------------------------------------------------------------------
// SplunkHecLogExporter — send events to Splunk HTTP Event Collector
// ---------------------------------------------------------------------------

pub struct SplunkHecLogExporter {
    client: reqwest::Client,
    endpoint: String,
    token: String,
    source: String,
    sourcetype: String,
    index: Option<String>,
}

impl fmt::Debug for SplunkHecLogExporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SplunkHecLogExporter")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl SplunkHecLogExporter {
    pub fn new(cfg: &crate::server::config::SplunkHecConfig) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder();
        if cfg.tls_skip_verify {
            builder = builder.danger_accept_invalid_certs(true);
        }
        let client = builder.build()?;
        let endpoint = format!("{}/services/collector/event", cfg.url.trim_end_matches('/'));

        Ok(Self {
            client,
            endpoint,
            token: cfg.token.clone(),
            source: cfg.source.clone(),
            sourcetype: cfg.sourcetype.clone(),
            index: cfg.index.clone(),
        })
    }

    fn build_payload(
        event: &RequestEvent,
        source: &str,
        sourcetype: &str,
        index: Option<&str>,
    ) -> serde_json::Value {
        let mut payload = serde_json::json!({
            "time": event.timestamp.timestamp(),
            "source": source,
            "sourcetype": sourcetype,
            "event": event,
        });
        if let Some(idx) = index {
            #[allow(clippy::indexing_slicing)]
            {
                payload["index"] = serde_json::json!(idx);
            }
        }
        payload
    }
}

impl LogExporter for SplunkHecLogExporter {
    fn export(
        &self,
        batch: opentelemetry_sdk::logs::LogBatch<'_>,
    ) -> impl std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send {
        let events: Vec<RequestEvent> = batch
            .iter()
            .filter_map(|(record, _scope)| log_record_to_request_event(record))
            .collect();

        let client = self.client.clone();
        let endpoint = self.endpoint.clone();
        let token = self.token.clone();
        let source = self.source.clone();
        let sourcetype = self.sourcetype.clone();
        let index = self.index.clone();

        async move {
            for event in &events {
                let payload = Self::build_payload(event, &source, &sourcetype, index.as_deref());
                let res = client
                    .post(&endpoint)
                    .header("Authorization", format!("Splunk {token}"))
                    .json(&payload)
                    .send()
                    .await;
                match res {
                    Ok(resp) if !resp.status().is_success() => {
                        tracing::warn!(
                            status = %resp.status(),
                            "Splunk HEC rejected log event"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Splunk HEC send failed");
                    }
                    _ => {}
                }
            }
            Ok(())
        }
    }

    fn shutdown(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }

    fn set_resource(&mut self, _resource: &opentelemetry_sdk::Resource) {}
}

// ---------------------------------------------------------------------------
// LogRecord → RequestEvent conversion
// ---------------------------------------------------------------------------

fn get_attr_str(record: &SdkLogRecord, key: &str) -> String {
    record
        .attributes_iter()
        .find(|(k, _)| k.as_str() == key)
        .and_then(|(_k, v): &(Key, AnyValue)| match v {
            AnyValue::String(s) => Some(s.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

fn get_attr_u64(record: &SdkLogRecord, key: &str) -> u64 {
    record
        .attributes_iter()
        .find(|(k, _)| k.as_str() == key)
        .and_then(|(_k, v): &(Key, AnyValue)| match v {
            AnyValue::Int(n) => Some(*n as u64),
            _ => None,
        })
        .unwrap_or(0)
}

fn get_attr_u16(record: &SdkLogRecord, key: &str) -> u16 {
    get_attr_u64(record, key) as u16
}

/// Convert an OTel LogRecord to a RequestEvent if it originated from
/// `emit_request_event` / `emit_upstream_event` (i.e. has the expected
/// attributes). Returns `None` for non-request log records.
fn log_record_to_request_event(record: &SdkLogRecord) -> Option<RequestEvent> {
    let direction = get_attr_str(record, "direction");
    if direction.is_empty() {
        return None;
    }

    let timestamp = record
        .timestamp()
        .map(DateTime::from)
        .unwrap_or_else(Utc::now);

    Some(RequestEvent {
        id: 0,
        timestamp,
        request_id: get_attr_str(record, "request_id"),
        username: get_attr_str(record, "username"),
        ip: get_attr_str(record, "ip"),
        method: get_attr_str(record, "method"),
        path: get_attr_str(record, "path"),
        status: get_attr_u16(record, "status"),
        action: get_attr_str(record, "action"),
        elapsed_ns: get_attr_u64(record, "elapsed_ns"),
        bytes_recv: get_attr_u64(record, "bytes_recv"),
        bytes_sent: get_attr_u64(record, "bytes_sent"),
        direction,
        trace_id: {
            let tid = get_attr_str(record, "trace_id");
            if tid.is_empty() {
                None
            } else {
                Some(tid)
            }
        },
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_action_auth() {
        assert_eq!(classify_action("POST", "/api/v1/auth/login"), "auth.login");
        assert_eq!(
            classify_action("POST", "/api/v1/auth/change-password"),
            "auth.change_password"
        );
    }

    #[test]
    fn test_classify_action_users() {
        assert_eq!(classify_action("GET", "/api/v1/users"), "user.list");
        assert_eq!(classify_action("POST", "/api/v1/users"), "user.create");
        assert_eq!(classify_action("GET", "/api/v1/users/alice"), "user.get");
        assert_eq!(classify_action("PUT", "/api/v1/users/alice"), "user.update");
        assert_eq!(
            classify_action("DELETE", "/api/v1/users/alice"),
            "user.delete"
        );
    }

    #[test]
    fn test_classify_action_repos() {
        assert_eq!(classify_action("GET", "/api/v1/repositories"), "repo.list");
        assert_eq!(
            classify_action("POST", "/api/v1/repositories"),
            "repo.create"
        );
        assert_eq!(
            classify_action("DELETE", "/api/v1/repositories/my-repo"),
            "repo.delete"
        );
    }

    #[test]
    fn test_classify_action_artifacts() {
        assert_eq!(
            classify_action("GET", "/repository/my-repo/path/to/file"),
            "artifact.download"
        );
        assert_eq!(
            classify_action("PUT", "/repository/my-repo/path/to/file"),
            "artifact.upload"
        );
        assert_eq!(
            classify_action("DELETE", "/repository/my-repo/path/to/file"),
            "artifact.delete"
        );
    }

    #[test]
    fn test_classify_action_docker() {
        assert_eq!(
            classify_action("GET", "/v2/myimage/manifests/latest"),
            "docker.pull_manifest"
        );
        assert_eq!(
            classify_action("PUT", "/v2/myimage/manifests/latest"),
            "docker.push_manifest"
        );
        assert_eq!(
            classify_action("GET", "/v2/myimage/blobs/sha256:abc"),
            "docker.pull_blob"
        );
        assert_eq!(
            classify_action("POST", "/v2/myimage/blobs/uploads/"),
            "docker.start_upload"
        );
        assert_eq!(
            classify_action("GET", "/v2/myimage/tags/list"),
            "docker.list_tags"
        );
    }

    #[test]
    fn test_classify_action_logging() {
        assert_eq!(
            classify_action("GET", "/api/v1/logging/events"),
            "logging.list"
        );
    }

    #[test]
    fn test_classify_action_catch_all() {
        assert_eq!(
            classify_action("OPTIONS", "/unknown/path"),
            "OPTIONS /unknown/path"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_in_memory_recent() {
        let exporter = InMemoryLogExporter::new(3);
        for i in 0..5 {
            let event = RequestEvent {
                id: 0,
                timestamp: Utc::now(),
                request_id: String::new(),
                username: format!("user{i}"),
                ip: "127.0.0.1".to_string(),
                method: "GET".to_string(),
                path: "/test".to_string(),
                status: 200,
                action: "test".to_string(),
                elapsed_ns: 1_000_000,
                bytes_recv: 0,
                bytes_sent: 0,
                direction: "downstream".to_string(),
                trace_id: None,
            };
            exporter.record(event).await;
        }
        let events = exporter.recent(10).await;
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].username, "user4");
        assert_eq!(events[1].username, "user3");
        assert_eq!(events[2].username, "user2");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_in_memory_recent_limit() {
        let exporter = InMemoryLogExporter::new(100);
        for i in 0..10 {
            let event = RequestEvent {
                id: 0,
                timestamp: Utc::now(),
                request_id: String::new(),
                username: format!("user{i}"),
                ip: "127.0.0.1".to_string(),
                method: "GET".to_string(),
                path: "/test".to_string(),
                status: 200,
                action: "test".to_string(),
                elapsed_ns: 1_000_000,
                bytes_recv: 0,
                bytes_sent: 0,
                direction: "downstream".to_string(),
                trace_id: None,
            };
            exporter.record(event).await;
        }
        let events = exporter.recent(3).await;
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].username, "user9");
    }

    #[test]
    fn test_splunk_hec_payload_basic() {
        let event = RequestEvent {
            id: 42,
            timestamp: chrono::DateTime::parse_from_rfc3339("2026-03-21T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            request_id: String::new(),
            username: "alice".to_string(),
            ip: "10.0.0.1".to_string(),
            method: "PUT".to_string(),
            path: "/repository/releases/foo.tar.gz".to_string(),
            status: 201,
            action: "artifact.upload".to_string(),
            elapsed_ns: 42_000_000,
            bytes_recv: 1024,
            bytes_sent: 0,
            direction: "downstream".to_string(),
            trace_id: None,
        };
        let payload = SplunkHecLogExporter::build_payload(&event, "depot", "depot:audit", None);
        assert_eq!(payload["source"], "depot");
        assert_eq!(payload["sourcetype"], "depot:audit");
        assert_eq!(payload["time"], event.timestamp.timestamp());
        assert!(payload.get("index").is_none());
        let inner = &payload["event"];
        assert_eq!(inner["username"], "alice");
        assert_eq!(inner["action"], "artifact.upload");
        assert_eq!(inner["status"], 201);
    }

    #[test]
    fn test_splunk_hec_payload_with_index() {
        let event = RequestEvent {
            id: 1,
            timestamp: Utc::now(),
            request_id: String::new(),
            username: "bob".to_string(),
            ip: "127.0.0.1".to_string(),
            method: "GET".to_string(),
            path: "/api/v1/health".to_string(),
            status: 200,
            action: "system.health".to_string(),
            elapsed_ns: 1_000_000,
            bytes_recv: 0,
            bytes_sent: 0,
            direction: "downstream".to_string(),
            trace_id: None,
        };
        let payload = SplunkHecLogExporter::build_payload(
            &event,
            "depot",
            "depot:audit",
            Some("depot_audit"),
        );
        assert_eq!(payload["index"], "depot_audit");
    }
}

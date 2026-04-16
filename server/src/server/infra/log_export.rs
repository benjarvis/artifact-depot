// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Per-request structured event emission.
//!
//! Request events are emitted as tracing events with target `depot.request`.
//! They flow through the OpenTelemetry log bridge to the configured OTLP
//! endpoint. The fmt layer filters `depot.request` out of stdout; stdout
//! only carries application logs.

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

/// Emit a structured per-request event through the OTel log bridge.
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
    fn test_classify_action_catch_all() {
        assert_eq!(
            classify_action("OPTIONS", "/unknown/path"),
            "OPTIONS /unknown/path"
        );
    }
}

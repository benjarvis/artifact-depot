// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use axum::{
    extract::{MatchedPath, State},
    http::Request,
    middleware::Next,
    response::{IntoResponse, Response},
};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::time::Instant;

/// Histogram buckets covering microsecond-level KV ops through multi-second blob transfers.
const LATENCY_BUCKETS: &[f64] = &[
    0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
    1.0, 2.5, 5.0, 10.0,
];

/// Install the global metrics recorder and return a handle for rendering.
pub fn install_recorder() -> Result<PrometheusHandle, metrics_exporter_prometheus::BuildError> {
    PrometheusBuilder::new()
        .set_buckets(LATENCY_BUCKETS)?
        .install_recorder()
}

/// Axum middleware that records HTTP request metrics using the global `metrics` macros.
pub async fn metrics_middleware(request: Request<axum::body::Body>, next: Next) -> Response {
    let method = request.method().to_string();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_owned())
        .unwrap_or_else(|| "<unmatched>".to_string());

    let labels = [
        ("http_request_method", method.clone()),
        ("http_route", route.clone()),
    ];

    gauge!("http_requests_in_flight", &labels).increment(1.0);
    let start = Instant::now();

    let response = next.run(request).await;

    let duration = start.elapsed().as_secs_f64();
    gauge!("http_requests_in_flight", &labels).decrement(1.0);

    let status = response.status().as_u16().to_string();
    let labels_with_status = [
        ("http_request_method", method),
        ("http_route", route),
        ("http_response_status_code", status),
    ];

    counter!("http_requests_total", &labels_with_status).increment(1);
    histogram!("http_request_duration_seconds", &labels_with_status).record(duration);

    response
}

/// Handler that renders Prometheus text format metrics.
pub async fn metrics_handler(State(handle): State<PrometheusHandle>) -> impl IntoResponse {
    handle.render()
}

// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::http::{HeaderMap, Request, Response, StatusCode};
use governor::clock::DefaultClock;
use governor::state::keyed::DefaultKeyedStateStore;
use governor::{Quota, RateLimiter};

use crate::server::config::RateLimitConfig;
use depot_core::error::{ErrorBody, ErrorResponse};

type KeyedLimiter = RateLimiter<IpAddr, DefaultKeyedStateStore<IpAddr>, DefaultClock>;

/// Dynamic rate limiter that can be reconfigured without restarting the server.
///
/// Wraps a keyed `governor::RateLimiter` in an `ArcSwap` so that settings
/// changes take effect on the next request.
pub struct DynamicRateLimiter {
    inner: ArcSwap<Option<(RateLimitConfig, Arc<KeyedLimiter>)>>,
}

impl DynamicRateLimiter {
    /// Create a new dynamic rate limiter, optionally enabled.
    pub fn new(cfg: Option<&RateLimitConfig>) -> Self {
        let state = cfg.and_then(|c| build_limiter(c).map(|l| (c.clone(), Arc::new(l))));
        Self {
            inner: ArcSwap::from_pointee(state),
        }
    }

    /// Update the rate limiter if the configuration has changed.
    /// Rebuilds the governor (fresh token buckets) only when the config differs.
    pub fn update(&self, cfg: Option<&RateLimitConfig>) {
        let current = self.inner.load();
        let current_cfg = current.as_ref().as_ref().map(|(c, _)| c);

        if current_cfg == cfg {
            return;
        }

        let new_state = cfg.and_then(|c| build_limiter(c).map(|l| (c.clone(), Arc::new(l))));
        self.inner.store(Arc::new(new_state));
    }

    /// Check whether a request is allowed through.
    /// Returns `Ok(())` if allowed, or a 429 response if rate-limited.
    /// If rate limiting is disabled, always returns `Ok(())`.
    #[allow(clippy::result_large_err)]
    pub fn check<B>(&self, req: &Request<B>) -> Result<(), Response<axum::body::Body>> {
        let guard = self.inner.load();
        let Some((_, ref limiter)) = **guard else {
            return Ok(());
        };

        let ip = extract_client_ip(req.headers()).ok_or_else(|| {
            rate_limit_error("unable to determine client IP for rate limiting", 1)
        })?;

        limiter.check_key(&ip).map(|_| ()).map_err(|not_until| {
            let wait_secs = not_until
                .wait_time_from(governor::clock::Clock::now(&DefaultClock::default()))
                .as_secs();
            rate_limit_error(
                &format!("rate limit exceeded, retry after {} seconds", wait_secs),
                wait_secs,
            )
        })
    }
}

fn build_limiter(cfg: &RateLimitConfig) -> Option<KeyedLimiter> {
    let burst = NonZeroU32::new(cfg.burst_size)?;
    let rps = NonZeroU32::new(cfg.requests_per_second as u32)?;
    let quota = Quota::per_second(rps).allow_burst(burst);
    Some(RateLimiter::keyed(quota))
}

fn rate_limit_error(message: &str, wait_secs: u64) -> Response<axum::body::Body> {
    let body = ErrorResponse {
        error: ErrorBody {
            code: "RATE_LIMITED".to_string(),
            message: message.to_string(),
        },
    };

    let json = serde_json::to_string(&body).unwrap_or_else(|_| {
        r#"{"error":{"code":"RATE_LIMITED","message":"rate limit exceeded"}}"#.to_string()
    });

    let retry_after = wait_secs.max(1).to_string();
    let mut resp = Response::new(axum::body::Body::from(json));
    *resp.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    if let Ok(val) = retry_after.parse() {
        resp.headers_mut()
            .insert(axum::http::header::RETRY_AFTER, val);
    }
    resp
}

/// Extract client IP from headers (X-Forwarded-For, X-Real-IP, Forwarded),
/// mirroring the logic of `tower_governor::SmartIpKeyExtractor`.
fn extract_client_ip(headers: &HeaderMap) -> Option<IpAddr> {
    // X-Forwarded-For: first IP in the comma-separated list.
    if let Some(xff) = headers.get("x-forwarded-for") {
        if let Some(ip) = xff
            .to_str()
            .ok()
            .and_then(|s| s.split(',').find_map(|s| s.trim().parse::<IpAddr>().ok()))
        {
            return Some(ip);
        }
    }
    // X-Real-IP
    if let Some(xri) = headers.get("x-real-ip") {
        if let Some(ip) = xri
            .to_str()
            .ok()
            .and_then(|s| s.trim().parse::<IpAddr>().ok())
        {
            return Some(ip);
        }
    }
    // Forwarded: for=<ip>
    if let Some(fwd) = headers.get("forwarded") {
        if let Some(ip) = fwd.to_str().ok().and_then(parse_forwarded_for) {
            return Some(ip);
        }
    }
    None
}

/// Parse `for=<ip>` from a Forwarded header value.
fn parse_forwarded_for(value: &str) -> Option<IpAddr> {
    for part in value.split(';') {
        for kv in part.split(',') {
            let kv = kv.trim();
            if let Some(ip_str) = kv.strip_prefix("for=").or_else(|| kv.strip_prefix("FOR=")) {
                let ip_str = ip_str.trim_matches('"').trim_matches('[').trim_matches(']');
                if let Ok(ip) = ip_str.parse::<IpAddr>() {
                    return Some(ip);
                }
                // May include port: "192.168.1.1:8080"
                if let Some(ip) = ip_str.rsplit_once(':').and_then(|(h, _)| h.parse().ok()) {
                    return Some(ip);
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dynamic_rate_limiter_new_none() {
        let limiter = DynamicRateLimiter::new(None);
        let req = Request::builder()
            .header("x-forwarded-for", "1.2.3.4")
            .body(())
            .unwrap();
        assert!(limiter.check(&req).is_ok());
    }

    #[test]
    fn test_dynamic_rate_limiter_new_some() {
        let cfg = RateLimitConfig {
            requests_per_second: 10,
            burst_size: 5,
        };
        let limiter = DynamicRateLimiter::new(Some(&cfg));
        let req = Request::builder()
            .header("x-forwarded-for", "1.2.3.4")
            .body(())
            .unwrap();
        assert!(limiter.check(&req).is_ok());
    }

    #[test]
    fn test_dynamic_rate_limiter_update() {
        let limiter = DynamicRateLimiter::new(None);
        // Enable rate limiting
        let cfg = RateLimitConfig {
            requests_per_second: 100,
            burst_size: 50,
        };
        limiter.update(Some(&cfg));
        let req = Request::builder()
            .header("x-forwarded-for", "1.2.3.4")
            .body(())
            .unwrap();
        assert!(limiter.check(&req).is_ok());
        // Disable again
        limiter.update(None);
        assert!(limiter.check(&req).is_ok());
    }

    #[test]
    fn test_dynamic_rate_limiter_exhaustion() {
        let cfg = RateLimitConfig {
            requests_per_second: 1,
            burst_size: 1,
        };
        let limiter = DynamicRateLimiter::new(Some(&cfg));
        let req = Request::builder()
            .header("x-forwarded-for", "10.0.0.1")
            .body(())
            .unwrap();
        // First request should succeed
        assert!(limiter.check(&req).is_ok());
        // Second request should be rate-limited
        let result = limiter.check(&req);
        assert!(result.is_err());
        let resp = result.unwrap_err();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn test_rate_limit_error_response() {
        let resp = rate_limit_error("rate limit exceeded, retry after 42 seconds", 42);
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            resp.headers()
                .get("retry-after")
                .expect("header")
                .to_str()
                .expect("str"),
            "42"
        );
        assert_eq!(
            resp.headers()
                .get("content-type")
                .expect("header")
                .to_str()
                .expect("str"),
            "application/json"
        );
    }

    #[test]
    fn test_extract_client_ip_xff() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "1.2.3.4, 5.6.7.8".parse().unwrap());
        assert_eq!(
            extract_client_ip(&headers),
            Some("1.2.3.4".parse().unwrap())
        );
    }

    #[test]
    fn test_extract_client_ip_x_real_ip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", "10.0.0.1".parse().unwrap());
        assert_eq!(
            extract_client_ip(&headers),
            Some("10.0.0.1".parse().unwrap())
        );
    }

    #[test]
    fn test_extract_client_ip_forwarded() {
        let mut headers = HeaderMap::new();
        headers.insert("forwarded", "for=192.168.1.1".parse().unwrap());
        assert_eq!(
            extract_client_ip(&headers),
            Some("192.168.1.1".parse().unwrap())
        );
    }

    #[test]
    fn test_extract_client_ip_none() {
        let headers = HeaderMap::new();
        assert_eq!(extract_client_ip(&headers), None);
    }
}

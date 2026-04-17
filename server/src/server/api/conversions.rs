// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for validating create-request inputs and converting them
//! into persisted records.
//!
//! Used both by the HTTP handlers (`stores.rs`, `repositories.rs`) and the
//! declarative `[initialization]` config (`server::config::init`) so the
//! validation rules stay in a single place.

use depot_core::error::DepotError;
use depot_core::store::kv::{ArtifactFormat, FormatConfig, RepoKind, RepoType, StoreKind};

use crate::server::api::repositories::CreateRepoRequest;
use crate::server::api::stores::CreateStoreRequest;

/// Normalize an S3 key prefix: strip leading/trailing slashes, ensure
/// trailing slash if non-empty, return `None` if effectively empty.
pub fn normalize_s3_prefix(raw: Option<String>) -> Option<String> {
    let s = raw.unwrap_or_default();
    let trimmed = s.trim_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(format!("{trimmed}/"))
    }
}

/// Build a [`StoreKind`] from the flat [`CreateStoreRequest`] fields,
/// validating required per-type fields and numeric ranges.
pub fn build_store_kind(req: &CreateStoreRequest) -> Result<StoreKind, DepotError> {
    match req.store_type.as_str() {
        "file" => {
            let root = req
                .root
                .clone()
                .ok_or_else(|| DepotError::BadRequest("root is required for file stores".into()))?;
            if req.io_size < 4 || req.io_size > 65536 {
                return Err(DepotError::BadRequest(
                    "io_size must be between 4 and 65536 KiB".into(),
                ));
            }
            Ok(StoreKind::File {
                root,
                sync: req.sync,
                io_size: req.io_size,
                direct_io: req.direct_io,
            })
        }
        "s3" => {
            let bucket = match req.bucket.as_ref() {
                Some(b) if !b.is_empty() => b.clone(),
                _ => {
                    return Err(DepotError::BadRequest(
                        "bucket is required for s3 stores".into(),
                    ))
                }
            };
            if req.max_retries > 20 {
                return Err(DepotError::BadRequest(format!(
                    "max_retries: {} must be in [0, 20]",
                    req.max_retries
                )));
            }
            if req.connect_timeout_secs == 0 || req.connect_timeout_secs > 7200 {
                return Err(DepotError::BadRequest(format!(
                    "connect_timeout_secs: {} must be in [1, 7200]",
                    req.connect_timeout_secs
                )));
            }
            if req.read_timeout_secs > 7200 {
                return Err(DepotError::BadRequest(format!(
                    "read_timeout_secs: {} must be in [0, 7200] (0 = disabled)",
                    req.read_timeout_secs
                )));
            }
            if req.retry_mode != "standard" && req.retry_mode != "adaptive" {
                return Err(DepotError::BadRequest(format!(
                    "retry_mode: {:?} must be \"standard\" or \"adaptive\"",
                    req.retry_mode
                )));
            }
            Ok(StoreKind::S3 {
                bucket,
                endpoint: req.endpoint.clone(),
                region: req.region.clone(),
                prefix: normalize_s3_prefix(req.prefix.clone()),
                access_key: req.access_key.clone(),
                secret_key: req.secret_key.clone(),
                max_retries: req.max_retries,
                connect_timeout_secs: req.connect_timeout_secs,
                read_timeout_secs: req.read_timeout_secs,
                retry_mode: req.retry_mode.clone(),
            })
        }
        other => Err(DepotError::BadRequest(format!(
            "unknown store_type: {other}"
        ))),
    }
}

/// Validate a [`CreateRepoRequest`] and build the `(kind, format_config)` pair
/// used to populate a `RepoConfig`.
///
/// Does NOT check that the named store exists or that Yum proxy members have
/// matching `repodata_depth` — those require KV access and are done by the
/// caller.
pub fn build_repo_kind_and_format(
    req: &CreateRepoRequest,
) -> Result<(RepoKind, FormatConfig), DepotError> {
    if req.cleanup_max_age_days == Some(0) {
        return Err(DepotError::BadRequest(
            "cleanup_max_age_days must be >= 1".into(),
        ));
    }
    if req.cleanup_max_unaccessed_days == Some(0) {
        return Err(DepotError::BadRequest(
            "cleanup_max_unaccessed_days must be >= 1".into(),
        ));
    }
    if let Some(depth) = req.repodata_depth {
        if depth > 5 {
            return Err(DepotError::BadRequest("repodata_depth must be 0-5".into()));
        }
        if req.format != ArtifactFormat::Yum {
            return Err(DepotError::BadRequest(
                "repodata_depth is only valid for Yum repositories".into(),
            ));
        }
        if req.repo_type == RepoType::Cache {
            return Err(DepotError::BadRequest(
                "repodata_depth is not supported for cache repositories".into(),
            ));
        }
    }

    let format_config = req.format_config();

    let kind = match req.repo_type {
        RepoType::Hosted => RepoKind::Hosted,
        RepoType::Cache => {
            let upstream_url = req.upstream_url.clone().ok_or_else(|| {
                DepotError::BadRequest("upstream_url required for cache repos".into())
            })?;
            RepoKind::Cache {
                upstream_url,
                cache_ttl_secs: req.cache_ttl_secs.unwrap_or(0),
                upstream_auth: req.upstream_auth.clone(),
            }
        }
        RepoType::Proxy => {
            let members = match req.members.clone() {
                Some(m) if !m.is_empty() => m,
                _ => {
                    return Err(DepotError::BadRequest(
                        "members required for proxy repos".into(),
                    ))
                }
            };
            if let Some(ref wm) = req.write_member {
                if !members.contains(wm) {
                    return Err(DepotError::BadRequest(
                        "write_member must be one of the listed members".into(),
                    ));
                }
            }
            RepoKind::Proxy {
                members,
                write_member: req.write_member.clone(),
            }
        }
    };

    Ok((kind, format_config))
}

/// Ensure all Yum proxy members have the same `repodata_depth`. Non-Yum
/// members are ignored. Members that don't resolve are also ignored — the
/// caller (repo creation) already validates member existence separately.
pub async fn validate_yum_proxy_member_depth(
    kv: &dyn depot_core::store::kv::KvStore,
    members: &[String],
) -> Result<(), DepotError> {
    let mut depths: Vec<(String, u32)> = Vec::new();
    for name in members {
        if let Some(cfg) = depot_core::service::get_repo(kv, name).await? {
            if cfg.format() == ArtifactFormat::Yum {
                depths.push((name.clone(), cfg.format_config.repodata_depth()));
            }
        }
    }
    if let Some((first, rest)) = depths.split_first() {
        for (name, d) in rest {
            if *d != first.1 {
                return Err(DepotError::BadRequest(format!(
                    "all Yum proxy members must have the same repodata_depth \
                     (member '{}' has depth {}, member '{}' has depth {})",
                    first.0, first.1, name, d,
                )));
            }
        }
    }
    Ok(())
}

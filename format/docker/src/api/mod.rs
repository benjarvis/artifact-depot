// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Docker Registry HTTP API V2 implementation.
//!
//! Implements the OCI Distribution Spec endpoints for hosted, cache, and proxy repos.
//!
//! Routes come in two flavors:
//! - Single-segment: `/v2/{name}/...` — flat repos where name = image name
//! - Two-segment: `/v2/{repo}/{image}/...` — namespaced repos (proxy, cache, or hosted)
//!
//! Cache repos check local storage first, then fetch from upstream Docker V2 registry.
//! Proxy (group) repos search ordered member list, returning the first hit.
//! Writes through proxy repos are routed to the first hosted member.

pub mod blobs;
pub mod dispatch;
pub mod handlers;
pub mod helpers;
pub mod manifests;
pub mod routes;
pub mod tags;
pub mod types;
pub mod uploads;

pub use handlers::*;
pub use routes::docker_port_routes;
pub use tags::catalog;
pub use types::{
    CatalogResponse, CompleteUploadParams, DockerPaginationParams, DockerPath, FixedDockerRepo,
    TagListResponse, TokenParams, UploadInitParams,
};

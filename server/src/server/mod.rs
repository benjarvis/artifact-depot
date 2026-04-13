// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

pub mod api;
pub mod auth;
pub mod config;
pub mod infra;
pub mod repo;
pub mod store;
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc
)]
mod tests;
pub mod worker;

pub use infra::router::{build_docker_port_router, build_metrics_router, build_router};
pub use infra::state::{AppState, AuthServices, BackgroundServices, RepoServices};

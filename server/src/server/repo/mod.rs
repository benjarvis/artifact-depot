// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Convenience re-exports of depot-core's shared repository infrastructure.
//!
//! Top-level names (RepoContext, RawRepo, HostedRepo, CacheRepo, ProxyRepo,
//! ingest helpers) live in `depot_core::repo`. Format-specific logic lives
//! in the per-format crates (`depot-format-apt`, `depot-format-docker`, ...).

pub use depot_core::repo::*;
// Re-export submodules so existing code can reference
// `crate::server::repo::hosted::HostedRepo`, `crate::server::repo::proxy::MAX_PROXY_DEPTH`, etc.
pub use depot_core::repo::{cache, hosted, proxy};

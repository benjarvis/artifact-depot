// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Depot's admin and server HTTP handlers (artifacts, repositories,
//! stores, users, roles, auth, tasks, system, etc.).
//!
//! Format-specific handlers (apt, cargo, docker, golang, helm, npm, pypi, yum)
//! live in their respective `depot-format-*` crates. The depot router wires
//! them in via direct imports from those crates.
//!
//! Shared format HTTP helpers (`upload_preamble`, `validate_format_repo`,
//! `stream_multipart_field_to_blob`, ...) live in `depot_core::api_helpers`.

pub mod artifacts;
pub mod audit;
pub mod auth;
pub mod backup;
pub mod nexus_compat;
pub mod repositories;
pub mod roles;
pub mod settings;
pub mod stores;
pub mod system;
pub mod tasks;
pub mod users;

pub use depot_core::api_helpers::*;
// Macros exported by `#[macro_export]` live at the crate root, not in a module,
// so we re-export it here for `super::store_from_config_fn!(...)` lookups.
pub use depot_core::store_from_config_fn;

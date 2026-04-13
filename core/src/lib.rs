// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Foundation types and traits for Artifact Depot.
//!
//! This crate contains the core abstractions shared by all other depot
//! crates: storage traits (`KvStore`, `BlobStore`), record types, the
//! service layer, and `DepotError`. It has lightweight dependencies by
//! default; HTTP integration, OpenAPI schemas, and backend-specific error
//! conversions are behind feature flags.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::todo)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::missing_panics_doc
    )
)]

#[cfg(feature = "http")]
pub mod api_helpers;
pub mod auth;
pub mod error;
#[cfg(feature = "repo")]
pub mod format_state;
pub mod inflight;
#[cfg(feature = "repo")]
pub mod repo;
pub mod service;
pub mod store;
pub mod store_registry;
pub mod task;
pub mod update;

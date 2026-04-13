// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Yum / RPM format handler — both repository logic and HTTP routes.

pub mod api;
pub mod store;

pub use store::{build_synthetic_rpm, YumStore};

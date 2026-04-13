// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Cargo registry format handler — both repository logic and HTTP routes.

pub mod api;
pub mod store;

pub use store::CargoStore;

// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! ReDB-backed KV store implementation.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]

pub mod redb_kv;
pub mod sharded_redb;

pub use redb_kv::RedbKvStore;
pub use sharded_redb::ShardedRedbKvStore;

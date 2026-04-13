// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Depot-owned storage wrappers (encryption, instrumentation).
//!
//! Core storage traits and record types live in `depot_core::store`.
//! Concrete backend implementations live in their own crates:
//! `depot_kv_redb`, `depot_kv_dynamo`, `depot_blob_file`, `depot_blob_s3`.

pub mod encrypted;
pub mod instrumented;

pub use encrypted::EncryptedKvStore;
pub use instrumented::{InstrumentedBlobStore, InstrumentedKvStore};

// Core trait + record re-exports — saves depot internals from repeating the
// long `depot_core::store::{blob,kv}::...` path everywhere.
pub use depot_core::store::blob::BlobStore;
pub use depot_core::store::kv::{
    ArtifactRecord, BlobRecord, Capability, CapabilityGrant, KvStore, RoleRecord, StoreKind,
    StoreRecord, TreeEntry, TreeEntryKind, UserRecord,
};

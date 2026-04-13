// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Blob garbage collector.
//!
//! Each GC pass builds a bloom filter of `blob_id` values from live artifacts,
//! then performs a single unified sweep:
//!
//! 1. **KV BlobRecords** are scanned.  Records whose `blob_id` is absent from
//!    the filter are deleted immediately.  This is safe because removing a KV
//!    record never touches the underlying blob file — it only prevents future
//!    dedup against that content hash.
//!
//! 2. **Blob store files** are walked.  Files whose blob_id is absent from the
//!    filter and were already candidates from the *previous* GC pass are
//!    deleted.  Newly-discovered unreferenced files become candidates for the
//!    next pass.  Candidates whose blob_id appears in the current filter
//!    (artifact created since last pass) are cleared.
//!
//! The two-pass grace period (one full GC interval, default 12–24 h) ensures
//! that any in-flight ingestion that raced the KV record deletion has time to
//! create its artifact before the file is removed.
//!
//! Uses `Arc<RwLock<()>>` shared with the check task: GC acquires a read
//! lock, check acquires a write lock to pause GC during snapshots.

mod bloom;
mod docker_gc;
mod gc_loop;
mod gc_pass;
mod repo_cleanup;
#[cfg(test)]
mod tests;

pub use gc_loop::run_blob_reaper;
pub use gc_pass::{gc_pass, GcState, GcStats};
pub use repo_cleanup::{clean_repo_artifacts, RepoCleanStats};

pub(crate) use bloom::{bloom_empty_like, bloom_union};

/// Default GC interval: 24 hours.
const DEFAULT_GC_INTERVAL_SECS: u64 = 86400;

/// Default minimum interval between GC runs: 1 hour.
const DEFAULT_GC_MIN_INTERVAL_SECS: u64 = 3600;

/// Default cap on the orphan candidate map.
const DEFAULT_GC_MAX_ORPHAN_CANDIDATES: usize = 1_000_000;

/// Docker artifact records younger than this are immune to GC.
///
/// This grace period prevents races between concurrent Docker pushes and GC.
/// A manifest push creates blob ref, manifest, and tag records as separate
/// non-atomic KV writes.  Without a grace period, GC could delete a newly
/// created blob ref before the referencing manifest is written, or delete an
/// untagged manifest before the tag record is written.
const DOCKER_GC_GRACE_SECS: u64 = 3600;

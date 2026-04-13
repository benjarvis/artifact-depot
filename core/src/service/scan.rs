// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Batched scan helpers shared across service submodules.

use std::borrow::Cow;

use crate::error;
use crate::store::keys;
use crate::store::kv::*;

/// Maximum entries per KV scan batch.
pub const SCAN_BATCH_SIZE: usize = 1000;

/// Maximum concurrent shard scans (bounds DynamoDB connection pressure).
pub const SHARD_CONCURRENCY: usize = 64;

/// Lower shard concurrency for background workers (GC, cleanup, rebuild)
/// so they don't monopolise tokio worker threads and starve UI reads.
pub const BG_SHARD_CONCURRENCY: usize = 8;

/// Hard cap for admin entity lists (repos, users, roles).
pub(super) const ADMIN_ENTITY_CAP: usize = 10_000;

/// Compute per-shard batch size for k-way merge.
/// Fetches enough to satisfy the request with uniform distribution, with a
/// 2x margin and a floor of 4 to avoid excessive re-fetching on sparse shards.
pub(super) fn per_shard_batch(limit: usize) -> usize {
    (limit / keys::NUM_SHARDS as usize + 1).max(4) * 2
}

/// Iterate over all entries with a given (table, pk, sk_prefix) in batches.
/// Calls `callback(sk, value)` for each entry; return `false` to stop early.
pub async fn for_each_in_partition<F>(
    kv: &dyn KvStore,
    table: &str,
    pk: &str,
    sk_prefix: &str,
    batch_size: usize,
    mut callback: F,
) -> error::Result<()>
where
    F: FnMut(&str, &[u8]) -> error::Result<bool>,
{
    let sk_end = prefix_end(sk_prefix);
    let mut cursor: Option<String> = None;

    loop {
        let start = match &cursor {
            Some(c) => c.as_str(),
            None => sk_prefix,
        };
        let result = kv
            .scan_range(
                table,
                Cow::Borrowed(pk),
                Cow::Borrowed(start),
                sk_end.as_deref(),
                batch_size,
            )
            .await?;

        for (sk, value) in &result.items {
            if !sk_prefix.is_empty() && !sk.starts_with(sk_prefix) {
                return Ok(());
            }
            if !callback(sk, value)? {
                return Ok(());
            }
        }

        if result.done {
            return Ok(());
        }

        if let Some((last_sk, _)) = result.items.last() {
            cursor = Some(format!("{}\0", last_sk));
        } else {
            return Ok(());
        }

        // Yield between batches so higher-priority tasks (e.g. UI reads)
        // can be scheduled on this worker thread.
        tokio::task::yield_now().await;
    }
}

/// Compute the exclusive upper bound for a string prefix.
pub(super) fn prefix_end(prefix: &str) -> Option<String> {
    crate::store::keys::prefix_end(prefix.as_bytes()).and_then(|b| String::from_utf8(b).ok())
}

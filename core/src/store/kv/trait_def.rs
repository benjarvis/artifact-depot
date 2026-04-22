// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

use crate::error;

/// Result of a `scan_range` or `scan_prefix` call.
pub struct ScanResult {
    pub items: Vec<(String, Vec<u8>)>,
    /// True when the scan reached the end of the range/prefix — no more
    /// items exist beyond this batch.
    pub done: bool,
}

/// Non-transactional key-value store with table/partition/sort-key addressing.
///
/// Each operation is independent and immediately durable. No transactions,
/// no atomicity guarantees across multiple operations.
///
/// All keys (partition key, sort key) are human-readable UTF-8 strings.
///
/// Version stamps provide optimistic concurrency control for individual keys.
/// Each key has a monotonic version counter (starting at 1 on first write).
/// Use `get_versioned` + `put_if_version` for read-modify-write patterns.
#[async_trait::async_trait]
pub trait KvStore: Send + Sync {
    /// Get a single value by (table, partition key, sort key).
    async fn get(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>>;

    /// Get a value along with its version stamp.
    /// Returns `None` if the key doesn't exist.
    /// Version starts at 1 for newly created keys and increments on every write.
    async fn get_versioned(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<(Vec<u8>, u64)>>;

    /// Unconditionally put a value. Also increments the version stamp.
    async fn put(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        value: &[u8],
    ) -> error::Result<()>;

    /// Conditional put: succeeds only if current version matches `expected_version`.
    /// - `expected_version = None` means key must not exist (insert-only).
    /// - `expected_version = Some(v)` means current version must equal `v`.
    /// On success, sets version to `expected_version.unwrap_or(0) + 1` and returns `Ok(true)`.
    /// On mismatch, returns `Ok(false)` — caller should retry or fail.
    async fn put_if_version(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        value: &[u8],
        expected_version: Option<u64>,
    ) -> error::Result<bool>;

    /// Conditional put: only succeeds if the key does NOT already exist.
    /// Equivalent to `put_if_version(table, pk, sk, value, None)`.
    /// Returns `Ok(true)` if written, `Ok(false)` if the key already existed.
    async fn put_if_absent(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        value: &[u8],
    ) -> error::Result<bool> {
        self.put_if_version(table, pk, sk, value, None).await
    }

    /// Delete a key. Returns `true` if the key existed.
    async fn delete(&self, table: &str, pk: Cow<'_, str>, sk: Cow<'_, str>) -> error::Result<bool>;

    /// Conditional delete: succeeds only if current version matches `expected_version`.
    /// Returns `Ok(true)` if version matched and key was deleted, `Ok(false)` on mismatch.
    async fn delete_if_version(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        expected_version: u64,
    ) -> error::Result<bool>;

    /// Delete a key and return its old value, if it existed.
    async fn delete_returning(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>>;

    /// Scan sort keys within a partition that start with `sk_prefix`.
    /// Returns a [`ScanResult`] with `(sk, value)` pairs and a `done` flag
    /// indicating whether the scan reached the end of the prefix.
    async fn scan_prefix(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk_prefix: Cow<'_, str>,
        limit: usize,
    ) -> error::Result<ScanResult>;

    /// Scan sort keys in `[sk_start, sk_end)` within a partition.
    /// When `sk_end` is `None`, scans to the end of the partition.
    /// Returns a [`ScanResult`] with `(sk, value)` pairs and a `done` flag
    /// indicating whether the scan reached the end of the range.
    async fn scan_range(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk_start: Cow<'_, str>,
        sk_end: Option<&str>,
        limit: usize,
    ) -> error::Result<ScanResult>;

    /// Strongly consistent read. Returns the latest committed value,
    /// never a stale replica. Use sparingly — only for leader election,
    /// distributed locks, or other coordination where consistency matters.
    /// Default implementation falls back to `get` (correct for backends
    /// that are always consistent, like redb).
    async fn get_consistent(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        self.get(table, pk, sk).await
    }

    /// Delete multiple keys in a single batch. Returns whether each key existed.
    /// Default implementation falls back to individual deletes.
    async fn delete_batch(&self, table: &str, keys: &[(&str, &str)]) -> error::Result<Vec<bool>> {
        let mut results = Vec::with_capacity(keys.len());
        for (pk, sk) in keys {
            results.push(
                self.delete(table, Cow::Borrowed(pk), Cow::Borrowed(sk))
                    .await?,
            );
        }
        Ok(results)
    }

    /// Put multiple key-value pairs in a single batch.
    /// Default implementation falls back to individual puts.
    async fn put_batch(&self, table: &str, entries: &[(&str, &str, &[u8])]) -> error::Result<()> {
        for (pk, sk, value) in entries {
            self.put(table, Cow::Borrowed(pk), Cow::Borrowed(sk), value)
                .await?;
        }
        Ok(())
    }

    /// Returns `true` if this backend is inherently single-node (e.g. redb).
    /// Used at startup to safely clear stale leases that no other instance
    /// could legitimately hold.
    fn is_single_node(&self) -> bool {
        false
    }

    /// Release freed pages to the filesystem. Only file-backed backends
    /// (redb) implement this; DynamoDB has nothing to compact. Returns
    /// `true` if the backend performed compaction work.
    async fn compact(&self) -> error::Result<bool> {
        Ok(false)
    }
}

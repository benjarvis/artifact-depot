// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::path::Path;

use crate::redb_kv::RedbKvStore;
use depot_core::error;
use depot_core::store::kv::{KvStore, ScanResult};

pub struct ShardedRedbKvStore {
    shards: Vec<RedbKvStore>,
    mask: usize,
}

impl ShardedRedbKvStore {
    /// Open `scaling_factor` redb instances inside `dir`.
    /// `scaling_factor` must be a power of two.
    /// `total_cache_bytes` is divided equally among all shards.
    /// Opens all shards in parallel on blocking threads.
    pub async fn open(
        dir: &Path,
        scaling_factor: usize,
        total_cache_bytes: usize,
    ) -> error::Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| {
            error::DepotError::Storage(
                format!("failed to create redb directory {}: {}", dir.display(), e).into(),
                error::Retryability::Permanent,
            )
        })?;
        let per_shard_cache = if total_cache_bytes > 0 {
            total_cache_bytes / scaling_factor
        } else {
            0
        };
        let mut handles = Vec::with_capacity(scaling_factor);
        for i in 0..scaling_factor {
            let path = dir.join(format!("shard_{}.redb", i));
            handles.push(tokio::task::spawn_blocking(move || {
                RedbKvStore::open_with_cache(&path, per_shard_cache)
            }));
        }
        let mut shards = Vec::with_capacity(scaling_factor);
        for (i, handle) in handles.into_iter().enumerate() {
            let store = handle.await.map_err(|e| {
                error::DepotError::Storage(
                    format!("shard {} open task failed: {}", i, e).into(),
                    error::Retryability::Permanent,
                )
            })??;
            shards.push(store);
        }
        Ok(Self {
            shards,
            mask: scaling_factor - 1,
        })
    }

    /// Gracefully close all shards in parallel, draining their writer tasks.
    pub async fn close(&mut self) {
        let futs: Vec<_> = self.shards.iter_mut().map(|shard| shard.close()).collect();
        futures::future::join_all(futs).await;
    }

    #[allow(clippy::indexing_slicing)] // blake3 hash is always 32 bytes; usize is at most 8.
    fn shard_index(&self, pk: &str) -> usize {
        let h = blake3::hash(pk.as_bytes());
        let bytes = h.as_bytes();
        let mut buf = [0u8; std::mem::size_of::<usize>()];
        buf.copy_from_slice(&bytes[..std::mem::size_of::<usize>()]);
        usize::from_le_bytes(buf) & self.mask
    }
}

#[allow(clippy::indexing_slicing)] // All indices bounded by shard_index() mask and input lengths.
#[async_trait::async_trait]
impl KvStore for ShardedRedbKvStore {
    async fn get(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        let idx = self.shard_index(&pk);
        self.shards[idx].get(table, pk, sk).await
    }

    async fn get_versioned(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<(Vec<u8>, u64)>> {
        let idx = self.shard_index(&pk);
        self.shards[idx].get_versioned(table, pk, sk).await
    }

    async fn put(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        value: &[u8],
    ) -> error::Result<()> {
        let idx = self.shard_index(&pk);
        self.shards[idx].put(table, pk, sk, value).await
    }

    async fn put_if_version(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        value: &[u8],
        expected_version: Option<u64>,
    ) -> error::Result<bool> {
        let idx = self.shard_index(&pk);
        self.shards[idx]
            .put_if_version(table, pk, sk, value, expected_version)
            .await
    }

    async fn delete(&self, table: &str, pk: Cow<'_, str>, sk: Cow<'_, str>) -> error::Result<bool> {
        let idx = self.shard_index(&pk);
        self.shards[idx].delete(table, pk, sk).await
    }

    async fn delete_if_version(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        expected_version: u64,
    ) -> error::Result<bool> {
        let idx = self.shard_index(&pk);
        self.shards[idx]
            .delete_if_version(table, pk, sk, expected_version)
            .await
    }

    async fn delete_returning(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        let idx = self.shard_index(&pk);
        self.shards[idx].delete_returning(table, pk, sk).await
    }

    async fn delete_batch(&self, table: &str, keys: &[(&str, &str)]) -> error::Result<Vec<bool>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // Fast path: all keys route to the same shard.
        let first_shard = self.shard_index(keys[0].0);
        let all_same = keys
            .iter()
            .all(|&(pk, _)| self.shard_index(pk) == first_shard);
        if all_same {
            return self.shards[first_shard].delete_batch(table, keys).await;
        }

        // Group entries by shard, preserving original indices.
        let mut per_shard: Vec<Vec<(usize, usize)>> = vec![vec![]; self.shards.len()];
        for (i, &(pk, _)) in keys.iter().enumerate() {
            per_shard[self.shard_index(pk)].push((i, i));
        }

        let mut output = vec![false; keys.len()];
        for (shard_idx, indices) in per_shard.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let sub_keys: Vec<(&str, &str)> = indices.iter().map(|&(orig, _)| keys[orig]).collect();
            let results = self.shards[shard_idx]
                .delete_batch(table, &sub_keys)
                .await?;
            for (result, &(orig_idx, _)) in results.iter().zip(indices) {
                output[orig_idx] = *result;
            }
        }
        Ok(output)
    }

    async fn put_batch(&self, table: &str, entries: &[(&str, &str, &[u8])]) -> error::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Fast path: all entries route to the same shard.
        let first_shard = self.shard_index(entries[0].0);
        let all_same = entries
            .iter()
            .all(|&(pk, _, _)| self.shard_index(pk) == first_shard);
        if all_same {
            return self.shards[first_shard].put_batch(table, entries).await;
        }

        // Group entries by shard.
        let mut per_shard: Vec<Vec<usize>> = vec![vec![]; self.shards.len()];
        for (i, &(pk, _, _)) in entries.iter().enumerate() {
            per_shard[self.shard_index(pk)].push(i);
        }

        for (shard_idx, indices) in per_shard.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let sub_entries: Vec<(&str, &str, &[u8])> =
                indices.iter().map(|&i| entries[i]).collect();
            self.shards[shard_idx]
                .put_batch(table, &sub_entries)
                .await?;
        }
        Ok(())
    }

    async fn scan_prefix(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk_prefix: Cow<'_, str>,
        limit: usize,
    ) -> error::Result<ScanResult> {
        let idx = self.shard_index(&pk);
        self.shards[idx]
            .scan_prefix(table, pk, sk_prefix, limit)
            .await
    }

    async fn scan_range(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk_start: Cow<'_, str>,
        sk_end: Option<&str>,
        limit: usize,
    ) -> error::Result<ScanResult> {
        let idx = self.shard_index(&pk);
        self.shards[idx]
            .scan_range(table, pk, sk_start, sk_end, limit)
            .await
    }

    fn is_single_node(&self) -> bool {
        true
    }

    async fn compact(&self) -> error::Result<bool> {
        // Fan out across shards in parallel. Each shard holds its own
        // write lock independently, so transactions on shard A are not
        // blocked while shard B compacts — the window during which any
        // given partition key is stalled is the cost of one shard's
        // compaction, not N of them. Trade: peak disk usage spikes to
        // N × shard-size during compact (redb grows the file before
        // shrinking). Accepted over longer blocking windows.
        let futs = self
            .shards
            .iter()
            .enumerate()
            .map(|(i, shard)| async move { (i, shard.compact().await) });
        let results = futures::future::join_all(futs).await;
        let mut any = false;
        for (i, res) in results {
            match res {
                Ok(c) => any |= c,
                Err(e) => {
                    tracing::warn!(shard = i, error = %e, "shard compaction failed");
                }
            }
        }
        Ok(any)
    }
}

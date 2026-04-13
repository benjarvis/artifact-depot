// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! K-way merge pagination across sharded data.

use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

use serde::de::DeserializeOwned;

use crate::error;
use crate::store::kv::*;

use super::scan::prefix_end;

/// Lazy, pull-based stream over a single shard's sorted entries.
/// Fetches entries in batches via `scan_range` and optionally filters by
/// substring match (for search). Used as input to `k_way_merge`.
pub(super) struct ShardStream {
    pk: String,
    sk_prefix: String,
    sk_end: Option<String>,
    buffer: Vec<(String, Vec<u8>)>,
    buf_idx: usize,
    next_start: String,
    exhausted: bool,
    filter: Option<String>,
    batch_size: usize,
}

impl ShardStream {
    pub(super) fn new(
        pk: String,
        sk_prefix: String,
        scan_start: String,
        filter: Option<String>,
        batch_size: usize,
    ) -> Self {
        let sk_end = prefix_end(&sk_prefix);
        Self {
            pk,
            sk_prefix,
            sk_end,
            buffer: Vec::new(),
            buf_idx: 0,
            next_start: scan_start,
            exhausted: false,
            filter,
            batch_size,
        }
    }

    /// Ensure the buffer has at least one item at `buf_idx`, fetching new
    /// batches as needed. After this returns, either `peek()` yields `Some`
    /// or the shard is exhausted.
    pub(super) async fn ensure_buffered(
        &mut self,
        kv: &dyn KvStore,
        table: &str,
    ) -> error::Result<()> {
        while self.buf_idx >= self.buffer.len() && !self.exhausted {
            let result = kv
                .scan_range(
                    table,
                    Cow::Borrowed(&self.pk),
                    Cow::Borrowed(&self.next_start),
                    self.sk_end.as_deref(),
                    self.batch_size,
                )
                .await?;

            if let Some((last_sk, _)) = result.items.last() {
                self.next_start = format!("{}\0", last_sk);
            }

            // Filter to prefix-matching and query-matching entries only.
            let mut filtered: Vec<(String, Vec<u8>)> = Vec::new();
            let mut past_prefix = false;
            for (sk, v) in result.items {
                if !self.sk_prefix.is_empty() && !sk.starts_with(&self.sk_prefix) {
                    past_prefix = true;
                    break;
                }
                if let Some(ref query) = self.filter {
                    let path = sk.to_lowercase();
                    if !path.contains(query.as_str()) {
                        continue;
                    }
                }
                filtered.push((sk, v));
            }

            self.buffer = filtered;
            self.buf_idx = 0;
            self.exhausted = result.done || past_prefix;
        }
        Ok(())
    }

    pub(super) fn peek(&self) -> Option<(&str, &[u8])> {
        if self.buf_idx < self.buffer.len() {
            #[allow(clippy::indexing_slicing)] // guarded by buf_idx < buffer.len()
            let (ref sk, ref v) = self.buffer[self.buf_idx];
            Some((sk, v))
        } else {
            None
        }
    }

    pub(super) fn advance(&mut self) {
        self.buf_idx += 1;
    }
}

/// Entry in the min-heap used by `k_way_merge`. Keyed by the current
/// peek sort-key so the heap always yields the globally smallest item.
#[derive(Eq, PartialEq)]
struct HeapEntry {
    sk: String,
    stream_idx: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sk
            .cmp(&other.sk)
            .then(self.stream_idx.cmp(&other.stream_idx))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// K-way merge across shard streams. Produces up to `limit + 1` items in
/// globally sorted order, skipping items at or before `cursor_path` and
/// the first `skip` qualifying items (for offset pagination).
pub(super) async fn k_way_merge<T: DeserializeOwned>(
    kv: &dyn KvStore,
    table: &str,
    streams: &mut [ShardStream],
    cursor_path: Option<&str>,
    skip: usize,
    limit: usize,
) -> error::Result<Vec<(String, T)>> {
    // Prime all streams in parallel.
    futures::future::try_join_all(
        streams
            .iter_mut()
            .map(|stream| stream.ensure_buffered(kv, table)),
    )
    .await?;

    // Build min-heap keyed by each stream's current peek sort-key.
    let mut heap = BinaryHeap::new();
    for (i, stream) in streams.iter().enumerate() {
        if let Some((sk, _)) = stream.peek() {
            heap.push(Reverse(HeapEntry {
                sk: sk.to_string(),
                stream_idx: i,
            }));
        }
    }

    let mut results: Vec<(String, T)> = Vec::with_capacity(limit + 1);
    let mut skipped = 0usize;

    while let Some(Reverse(entry)) = heap.pop() {
        let idx = entry.stream_idx;

        // Extract path and value (owned copies, releases borrow on stream).
        // idx comes from the heap which only contains valid stream indices.
        #[allow(clippy::indexing_slicing)]
        let Some((sk, value)) = streams[idx].peek() else {
            continue;
        };
        let path = sk.to_string();
        let value = value.to_vec();

        // Advance stream and re-insert into heap if it still has data.
        #[allow(clippy::indexing_slicing)]
        streams[idx].advance();
        #[allow(clippy::indexing_slicing)]
        streams[idx].ensure_buffered(kv, table).await?;
        #[allow(clippy::indexing_slicing)]
        if let Some((next_sk, _)) = streams[idx].peek() {
            heap.push(Reverse(HeapEntry {
                sk: next_sk.to_string(),
                stream_idx: idx,
            }));
        }

        // Skip items at or before cursor.
        if let Some(cp) = cursor_path {
            if path.as_str() <= cp {
                continue;
            }
        }

        // Skip for offset pagination.
        if skipped < skip {
            skipped += 1;
            continue;
        }

        // Deserialize and collect.
        let record: T = rmp_serde::from_slice(&value)?;
        results.push((path, record));

        if results.len() > limit {
            break;
        }
    }

    Ok(results)
}

pub(super) fn decode_cursor(cursor: &Option<String>) -> Option<String> {
    use base64::Engine;
    cursor.as_ref().map(|c| {
        String::from_utf8_lossy(
            &base64::engine::general_purpose::STANDARD
                .decode(c)
                .unwrap_or_default(),
        )
        .to_string()
    })
}

pub fn paginate_results<T>(items: Vec<(String, T)>, limit: usize) -> PaginatedResult<(String, T)> {
    use base64::Engine;
    let next_cursor = items
        .get(limit.saturating_sub(1))
        .filter(|_| items.len() > limit)
        .map(|(key, _)| base64::engine::general_purpose::STANDARD.encode(key.as_bytes()));
    let items = items.into_iter().take(limit).collect();
    PaginatedResult { items, next_cursor }
}

// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Event types and sender/receiver handles for the background update worker.
//!
//! The worker loop itself (which drains and applies these events) lives in
//! the `depot` crate. Only the shared types are exposed here so downstream
//! crates can queue updates without pulling in the worker machinery.

use chrono::{DateTime, Utc};
use metrics::counter;
use tokio::sync::mpsc;

use crate::store::keys;

// ---------------------------------------------------------------------------
// Channel capacities
// ---------------------------------------------------------------------------

/// Channel capacity for dir-entry delta events.
pub const DIR_CHANNEL_CAPACITY: usize = 65_536;

/// Channel capacity for store-stats delta events.
pub const STORE_CHANNEL_CAPACITY: usize = 65_536;

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

/// Delta event for a directory entry update.
#[derive(Clone, Debug)]
pub struct DirDelta {
    pub repo: String,
    pub pk: String,
    pub dir_path: String,
    pub count_delta: i64,
    pub bytes_delta: i64,
    pub timestamp: DateTime<Utc>,
}

/// Delta event for store-level stats.
pub struct StoreDelta {
    pub store: String,
    pub count_delta: i64,
    pub bytes_delta: i64,
}

/// Queued deferred last-accessed-at update for a single artifact.
pub struct AtimeEvent {
    pub repo: String,
    pub path: String,
    pub timestamp: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// UpdateSender / UpdateReceiver
// ---------------------------------------------------------------------------

/// Cheaply cloneable sender handle for update events.
///
/// All event types use bounded `mpsc` channels. The worker drains and coalesces
/// events on the consumer side, eliminating the need for producer-side locking.
#[derive(Clone)]
pub struct UpdateSender {
    pub(crate) dir_tx: mpsc::Sender<DirDelta>,
    pub(crate) atime_tx: mpsc::Sender<AtimeEvent>,
    pub(crate) store_tx: mpsc::Sender<StoreDelta>,
}

/// Opaque receiver handle consumed by the worker.
pub struct UpdateReceiver {
    pub dir_rx: mpsc::Receiver<DirDelta>,
    pub atime_rx: mpsc::Receiver<AtimeEvent>,
    pub store_rx: mpsc::Receiver<StoreDelta>,
}

impl UpdateSender {
    /// Create a new sender/receiver pair.
    pub fn new(atime_capacity: usize) -> (Self, UpdateReceiver) {
        let (dir_tx, dir_rx) = mpsc::channel(DIR_CHANNEL_CAPACITY);
        let (atime_tx, atime_rx) = mpsc::channel(atime_capacity);
        let (store_tx, store_rx) = mpsc::channel(STORE_CHANNEL_CAPACITY);
        (
            Self {
                dir_tx,
                atime_tx,
                store_tx,
            },
            UpdateReceiver {
                dir_rx,
                atime_rx,
                store_rx,
            },
        )
    }

    /// Create a no-op sender whose events are silently discarded.
    pub fn noop() -> Self {
        let (dir_tx, _) = mpsc::channel(1);
        let (atime_tx, _) = mpsc::channel(1);
        let (store_tx, _) = mpsc::channel(1);
        Self {
            dir_tx,
            atime_tx,
            store_tx,
        }
    }

    /// Queue an atime touch. Best-effort: drops on full channel.
    pub fn touch(&self, repo: &str, path: &str, now: DateTime<Utc>) {
        let event = AtimeEvent {
            repo: repo.to_string(),
            path: path.to_string(),
            timestamp: now,
        };
        if let Err(mpsc::error::TrySendError::Full(_)) = self.atime_tx.try_send(event) {
            counter!("update_events_dropped_total").increment(1);
        }
    }

    /// Record a store-level delta (artifact added/removed/resized).
    /// Blocks if the channel is full, ensuring no deltas are silently dropped.
    pub async fn store_changed(&self, store: &str, count_delta: i64, bytes_delta: i64) {
        let _ = self
            .store_tx
            .send(StoreDelta {
                store: store.to_string(),
                count_delta,
                bytes_delta,
            })
            .await;
    }

    /// Queue a dir-entry delta for the immediate parent directory of an artifact path.
    /// The upward propagation to root happens automatically via the worker.
    pub async fn dir_changed(&self, repo: &str, path: &str, count_delta: i64, bytes_delta: i64) {
        let clean = keys::normalize_path(path);
        let dir_path = parent_dir(&clean).to_string();
        let pk = keys::sharded_pk(repo, keys::tree_parent(&dir_path));
        let _ = self
            .dir_tx
            .send(DirDelta {
                repo: repo.to_string(),
                pk,
                dir_path,
                count_delta,
                bytes_delta,
                timestamp: Utc::now(),
            })
            .await;
    }
}

/// Return the parent directory of a path.
/// `"a/b/c/file.txt"` → `"a/b/c"`, `"file.txt"` → `""`.
pub fn parent_dir(path: &str) -> &str {
    match path.rfind('/') {
        Some(pos) => path.split_at(pos).0,
        None => "",
    }
}

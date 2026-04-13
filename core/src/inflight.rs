// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Inflight request deduplication.
//!
//! Prevents concurrent cache-miss fetches for the same upstream resource from
//! stampeding the origin. The first caller becomes the "leader" and gets an
//! RAII guard; subsequent callers receive a `Notify` to await.
//!
//! The map is sharded by key hash. Each shard uses an `ArcSwap` snapshot for
//! lock-free reads (the common "is someone already fetching?" check) and a
//! `Mutex` that serialises writes (leader election and guard release).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use tokio::sync::Notify;

const SHARD_COUNT: usize = 64;

struct Shard {
    /// Lock-free snapshot for reads.
    data: ArcSwap<HashMap<String, Arc<Notify>>>,
    /// Serialises mutations (insert / remove) to prevent lost updates.
    write: Mutex<()>,
}

impl Shard {
    fn new() -> Self {
        Self {
            data: ArcSwap::from_pointee(HashMap::new()),
            write: Mutex::new(()),
        }
    }
}

/// A map of in-flight upstream fetches, keyed by an opaque string.
#[derive(Clone)]
pub struct InflightMap {
    shards: Arc<[Shard; SHARD_COUNT]>,
}

impl Default for InflightMap {
    fn default() -> Self {
        Self::new()
    }
}

impl InflightMap {
    pub fn new() -> Self {
        Self {
            shards: Arc::new(std::array::from_fn(|_| Shard::new())),
        }
    }

    fn shard(&self, key: &str) -> Option<&Shard> {
        let mut hasher = std::hash::DefaultHasher::new();
        key.hash(&mut hasher);
        let idx = hasher.finish() as usize % SHARD_COUNT;
        self.shards.get(idx)
    }

    /// Try to claim leadership for `key`.
    ///
    /// Returns `Ok(InflightGuard)` if this caller is the leader (no one else
    /// is currently fetching this key). Returns `Err(Arc<Notify>)` if another
    /// caller is already fetching — the caller should `notify.notified().await`
    /// and then retry from cache.
    pub fn try_claim(&self, key: &str) -> Result<InflightGuard, Arc<Notify>> {
        let Some(shard) = self.shard(key) else {
            // Unreachable: index is always in bounds. Return a dummy Notify
            // so the caller retries from cache.
            return Err(Arc::new(Notify::new()));
        };

        // Fast path: lock-free read to see if someone is already fetching.
        let snapshot = shard.data.load();
        if let Some(notify) = snapshot.get(key) {
            return Err(Arc::clone(notify));
        }

        // Slow path: take per-shard write lock, re-check, then insert.
        let _lock = shard.write.lock().unwrap_or_else(|e| e.into_inner());
        let snapshot = shard.data.load();
        if let Some(notify) = snapshot.get(key) {
            return Err(Arc::clone(notify));
        }
        let notify = Arc::new(Notify::new());
        let mut new = HashMap::clone(&snapshot);
        new.insert(key.to_string(), Arc::clone(&notify));
        shard.data.store(Arc::new(new));

        Ok(InflightGuard {
            shards: Arc::clone(&self.shards),
            key: key.to_string(),
            notify,
        })
    }
}

/// RAII guard: dropping removes the key from the map and wakes all waiters.
pub struct InflightGuard {
    shards: Arc<[Shard; SHARD_COUNT]>,
    key: String,
    notify: Arc<Notify>,
}

impl std::fmt::Debug for InflightGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InflightGuard")
            .field("key", &self.key)
            .finish()
    }
}

impl InflightGuard {
    fn shard(&self) -> Option<&Shard> {
        let mut hasher = std::hash::DefaultHasher::new();
        self.key.hash(&mut hasher);
        let idx = hasher.finish() as usize % SHARD_COUNT;
        self.shards.get(idx)
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if let Some(shard) = self.shard() {
            let _lock = shard.write.lock().unwrap_or_else(|e| e.into_inner());
            let snapshot = shard.data.load();
            let mut new = HashMap::clone(&snapshot);
            new.remove(&self.key);
            shard.data.store(Arc::new(new));
        }
        self.notify.notify_waiters();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn leader_claims_and_waiter_gets_notify() {
        let map = InflightMap::new();
        let guard = map.try_claim("key1").unwrap();
        // Second claim should fail with a Notify.
        let notify = map.try_claim("key1").unwrap_err();
        // Drop the guard -> key removed, waiters notified.
        drop(guard);
        // After drop, claiming again should succeed.
        let _guard2 = map.try_claim("key1").unwrap();
        // The notify should be valid (we can't easily test notification
        // without async, but we can verify the type).
        let _ = notify;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn waiter_is_notified_on_drop() {
        let map = InflightMap::new();
        let guard = map.try_claim("k").unwrap();
        let notify = map.try_claim("k").unwrap_err();

        let handle = tokio::spawn(async move {
            notify.notified().await;
            true
        });

        // Give the spawned task a moment to register the waiter.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        drop(guard);

        assert!(handle.await.unwrap());
    }
}

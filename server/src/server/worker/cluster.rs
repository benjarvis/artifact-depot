// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Cluster coordination: instance registry and distributed leases.
//!
//! Each depot instance registers itself in KV with periodic heartbeats.
//! Distributed leases provide distributed mutual exclusion — only one
//! instance can hold a given lease at a time.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use depot_core::error;
use depot_core::service;
use depot_core::store::keys;
use depot_core::store::kv::KvStore;

/// How often each instance refreshes its heartbeat (seconds).
const HEARTBEAT_INTERVAL_SECS: u64 = 30;

/// An instance is considered dead if its heartbeat is older than this (seconds).
const HEARTBEAT_TIMEOUT_SECS: i64 = 90;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceRecord {
    pub instance_id: String,
    pub started_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub hostname: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn instance_meta_key(id: &str) -> String {
    format!("instance/{}", id)
}

/// Read a msgpack-encoded record from a meta key.
async fn get_meta_record<T: serde::de::DeserializeOwned>(
    kv: &dyn KvStore,
    key: &str,
) -> error::Result<Option<T>> {
    let (table, pk, sk) = keys::meta_key(key);
    service::typed_get(kv, table, pk, sk).await
}

/// Write a msgpack-encoded record to a meta key.
async fn put_meta_record<T: serde::Serialize>(
    kv: &dyn KvStore,
    key: &str,
    record: &T,
) -> error::Result<()> {
    let (table, pk, sk) = keys::meta_key(key);
    service::typed_put(kv, table, pk, sk, record).await
}

// ---------------------------------------------------------------------------
// Instance registry
// ---------------------------------------------------------------------------

/// Register this instance in the KV store.
pub async fn register_instance(
    kv: &dyn KvStore,
    instance_id: &str,
    hostname: &str,
) -> anyhow::Result<()> {
    let now = Utc::now();
    let record = InstanceRecord {
        instance_id: instance_id.to_string(),
        started_at: now,
        last_heartbeat: now,
        hostname: hostname.to_string(),
    };
    let key = instance_meta_key(instance_id);
    put_meta_record(kv, &key, &record).await?;
    Ok(())
}

/// Update the heartbeat timestamp for this instance.
#[tracing::instrument(level = "debug", skip(kv), fields(instance_id))]
pub async fn refresh_heartbeat(kv: &dyn KvStore, instance_id: &str) -> anyhow::Result<()> {
    let key = instance_meta_key(instance_id);
    let existing: Option<InstanceRecord> = get_meta_record(kv, &key).await?;
    if let Some(mut record) = existing {
        record.last_heartbeat = Utc::now();
        put_meta_record(kv, &key, &record).await?;
    }
    Ok(())
}

/// Remove this instance's record from KV (graceful shutdown).
pub async fn deregister_instance(kv: &dyn KvStore, instance_id: &str) -> anyhow::Result<()> {
    let key = instance_meta_key(instance_id);
    service::delete_meta(kv, &key).await?;
    Ok(())
}

/// List all instances whose heartbeat is within the timeout window.
pub async fn list_live_instances(kv: &dyn KvStore) -> anyhow::Result<Vec<InstanceRecord>> {
    let (table, pk, sk_prefix) = keys::meta_key("instance/");
    let result = kv.scan_prefix(table, pk, sk_prefix, 1000).await?;
    let cutoff = Utc::now() - chrono::Duration::seconds(HEARTBEAT_TIMEOUT_SECS);
    let mut live = Vec::new();
    for (_key, value) in &result.items {
        if let Ok(record) = rmp_serde::from_slice::<InstanceRecord>(value) {
            if record.last_heartbeat >= cutoff {
                live.push(record);
            }
        }
    }
    Ok(live)
}

// ---------------------------------------------------------------------------
// Distributed leases
// ---------------------------------------------------------------------------

/// Well-known lease name for the blob garbage collector.
pub const LEASE_GC: &str = "gc";

/// Try to acquire or renew a named lease.
///
/// Returns `true` if this instance now holds the lease. The caller decides
/// the TTL — for GC this should match the GC interval so that a dead
/// holder's orphan-candidate grace period expires before anyone else takes
/// over.
///
/// Uses `get_versioned` (strongly consistent in DynamoDB) + `put_if_version`
/// to guarantee at most one holder at a time.
pub async fn try_acquire_lease(
    kv: &dyn KvStore,
    name: &str,
    instance_id: &str,
    ttl_secs: u64,
) -> anyhow::Result<bool> {
    let (table, pk, sk) = keys::lease_key(name);
    let now = Utc::now();

    match kv.get_versioned(table, pk.clone(), sk.clone()).await? {
        Some((bytes, version)) => {
            let record: depot_core::store::kv::LeaseRecord = rmp_serde::from_slice(&bytes)?;

            let expired =
                now > record.renewed_at + chrono::Duration::seconds(record.ttl_secs as i64);

            if record.holder == instance_id || expired {
                // Renew our own lease, or take over an expired one.
                let new_record = depot_core::store::kv::LeaseRecord {
                    holder: instance_id.to_string(),
                    renewed_at: now,
                    ttl_secs,
                };
                let new_bytes = rmp_serde::to_vec(&new_record)?;
                Ok(kv
                    .put_if_version(table, pk, sk, &new_bytes, Some(version))
                    .await?)
            } else {
                // Someone else holds a valid lease.
                Ok(false)
            }
        }
        None => {
            // No lease exists — try to create one.
            let record = depot_core::store::kv::LeaseRecord {
                holder: instance_id.to_string(),
                renewed_at: now,
                ttl_secs,
            };
            let bytes = rmp_serde::to_vec(&record)?;
            Ok(kv.put_if_absent(table, pk, sk, &bytes).await?)
        }
    }
}

/// Release a lease if held by this instance.
pub async fn release_lease(
    kv: &dyn KvStore,
    name: &str,
    instance_id: &str,
) -> anyhow::Result<bool> {
    let (table, pk, sk) = keys::lease_key(name);

    match kv.get_versioned(table, pk.clone(), sk.clone()).await? {
        Some((bytes, _version)) => {
            let record: depot_core::store::kv::LeaseRecord = rmp_serde::from_slice(&bytes)?;
            if record.holder == instance_id {
                kv.delete(table, pk, sk).await?;
                Ok(true)
            } else {
                Ok(false)
            }
        }
        None => Ok(false),
    }
}

// ---------------------------------------------------------------------------
// Background heartbeat loop
// ---------------------------------------------------------------------------

/// Background loop: refreshes heartbeat periodically.
pub async fn run_heartbeat_loop(
    kv: Arc<dyn KvStore>,
    instance_id: String,
    cancel: CancellationToken,
) {
    let mut interval =
        tokio::time::interval(std::time::Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
    interval.tick().await; // skip immediate tick

    loop {
        tokio::select! {
            _ = interval.tick() => {
                async {
                    if let Err(e) = refresh_heartbeat(kv.as_ref(), &instance_id).await {
                        tracing::warn!("heartbeat refresh failed: {}", e);
                    }
                }
                .instrument(tracing::debug_span!("heartbeat"))
                .await;
            }
            _ = cancel.cancelled() => {
                tracing::info!("heartbeat loop shutting down");
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RAII guard for releasing a lease on all exit paths
// ---------------------------------------------------------------------------

/// RAII guard that releases a named lease when dropped.
pub struct LeaseGuard {
    kv: Arc<dyn KvStore>,
    name: String,
    instance_id: String,
    released: bool,
}

impl LeaseGuard {
    pub fn new(kv: Arc<dyn KvStore>, name: &str, instance_id: &str) -> Self {
        Self {
            kv,
            name: name.to_string(),
            instance_id: instance_id.to_string(),
            released: false,
        }
    }

    /// Explicitly release the lease (preferred over relying on Drop).
    pub async fn release(&mut self) -> anyhow::Result<()> {
        if !self.released {
            self.released = true;
            release_lease(self.kv.as_ref(), &self.name, &self.instance_id).await?;
        }
        Ok(())
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if !self.released {
            // Best-effort async cleanup from sync Drop context.
            let kv = self.kv.clone();
            let name = self.name.clone();
            let instance_id = self.instance_id.clone();
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(async move {
                        if let Err(e) = release_lease(kv.as_ref(), &name, &instance_id).await {
                            tracing::warn!("failed to release lease '{}' on drop: {}", name, e);
                        }
                    });
                }
                Err(_) => {
                    tracing::warn!("runtime gone, lease '{}' will expire via TTL", name);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use depot_kv_redb::sharded_redb::ShardedRedbKvStore;

    async fn test_kv() -> (Arc<dyn KvStore>, tempfile::TempDir) {
        let dir = crate::server::infra::test_support::test_tempdir();
        let kv = Arc::new(
            ShardedRedbKvStore::open(&dir.path().join("kv"), 2, 0)
                .await
                .unwrap(),
        ) as Arc<dyn KvStore>;
        (kv, dir)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_register_and_list_instances() {
        let (kv, _dir) = test_kv().await;

        register_instance(kv.as_ref(), "inst-1", "host-a")
            .await
            .unwrap();
        register_instance(kv.as_ref(), "inst-2", "host-b")
            .await
            .unwrap();
        register_instance(kv.as_ref(), "inst-3", "host-c")
            .await
            .unwrap();

        let live = list_live_instances(kv.as_ref()).await.unwrap();
        assert_eq!(live.len(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_heartbeat_expiry() {
        let (kv, _dir) = test_kv().await;

        // Register an instance with an old heartbeat.
        let old_time = Utc::now() - chrono::Duration::seconds(200);
        let record = InstanceRecord {
            instance_id: "old-inst".to_string(),
            started_at: old_time,
            last_heartbeat: old_time,
            hostname: "host-old".to_string(),
        };
        let key = instance_meta_key("old-inst");
        put_meta_record(kv.as_ref(), &key, &record).await.unwrap();

        // Register a fresh instance.
        register_instance(kv.as_ref(), "fresh-inst", "host-fresh")
            .await
            .unwrap();

        let live = list_live_instances(kv.as_ref()).await.unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].instance_id, "fresh-inst");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_deregister_instance() {
        let (kv, _dir) = test_kv().await;

        register_instance(kv.as_ref(), "inst-x", "host-x")
            .await
            .unwrap();
        let live = list_live_instances(kv.as_ref()).await.unwrap();
        assert_eq!(live.len(), 1);

        deregister_instance(kv.as_ref(), "inst-x").await.unwrap();
        let live = list_live_instances(kv.as_ref()).await.unwrap();
        assert_eq!(live.len(), 0);
    }

    // --- Lease tests ---

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lease_acquire() {
        let (kv, _dir) = test_kv().await;

        // First acquire succeeds.
        let got = try_acquire_lease(kv.as_ref(), "gc", "inst-a", 3600)
            .await
            .unwrap();
        assert!(got);

        // Same holder can renew.
        let got = try_acquire_lease(kv.as_ref(), "gc", "inst-a", 3600)
            .await
            .unwrap();
        assert!(got);

        // Different holder is blocked.
        let got = try_acquire_lease(kv.as_ref(), "gc", "inst-b", 3600)
            .await
            .unwrap();
        assert!(!got);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lease_expired_takeover() {
        let (kv, _dir) = test_kv().await;

        // Write an already-expired lease.
        let expired_record = depot_core::store::kv::LeaseRecord {
            holder: "inst-dead".to_string(),
            renewed_at: Utc::now() - chrono::Duration::seconds(7200),
            ttl_secs: 3600,
        };
        let (table, pk, sk) = keys::lease_key("gc");
        let bytes = rmp_serde::to_vec(&expired_record).unwrap();
        kv.put(table, pk.clone(), sk.clone(), &bytes).await.unwrap();

        // Another instance can take over.
        let got = try_acquire_lease(kv.as_ref(), "gc", "inst-new", 3600)
            .await
            .unwrap();
        assert!(got);

        // Verify new holder.
        let (val, _ver) = kv.get_versioned(table, pk, sk).await.unwrap().unwrap();
        let rec: depot_core::store::kv::LeaseRecord = rmp_serde::from_slice(&val).unwrap();
        assert_eq!(rec.holder, "inst-new");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lease_release() {
        let (kv, _dir) = test_kv().await;

        try_acquire_lease(kv.as_ref(), "gc", "inst-a", 3600)
            .await
            .unwrap();

        // Release by holder succeeds.
        let released = release_lease(kv.as_ref(), "gc", "inst-a").await.unwrap();
        assert!(released);

        // Another instance can now acquire.
        let got = try_acquire_lease(kv.as_ref(), "gc", "inst-b", 3600)
            .await
            .unwrap();
        assert!(got);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lease_release_wrong_holder() {
        let (kv, _dir) = test_kv().await;

        try_acquire_lease(kv.as_ref(), "gc", "inst-a", 3600)
            .await
            .unwrap();

        // Release by non-holder fails.
        let released = release_lease(kv.as_ref(), "gc", "inst-b").await.unwrap();
        assert!(!released);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_independent_leases() {
        let (kv, _dir) = test_kv().await;

        // Different lease names don't interfere.
        let got_gc = try_acquire_lease(kv.as_ref(), "gc", "inst-a", 3600)
            .await
            .unwrap();
        let got_other = try_acquire_lease(kv.as_ref(), "rebuild", "inst-b", 3600)
            .await
            .unwrap();
        assert!(got_gc);
        assert!(got_other);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lease_same_instance_different_jobs() {
        let (kv, _dir) = test_kv().await;

        // Simulate the GC loop holder on instance "inst-a".
        let got = try_acquire_lease(kv.as_ref(), "gc", "inst-a:gc", 3600)
            .await
            .unwrap();
        assert!(got, "gc job should acquire lease");

        // A check job on the same instance must be blocked.
        let got = try_acquire_lease(kv.as_ref(), "gc", "inst-a:check", 3600)
            .await
            .unwrap();
        assert!(!got, "check job must not acquire lease held by gc job");

        // After release, check can acquire.
        release_lease(kv.as_ref(), "gc", "inst-a:gc").await.unwrap();
        let got = try_acquire_lease(kv.as_ref(), "gc", "inst-a:check", 3600)
            .await
            .unwrap();
        assert!(got, "check should acquire after gc releases");

        // And gc is now blocked.
        let got = try_acquire_lease(kv.as_ref(), "gc", "inst-a:gc", 3600)
            .await
            .unwrap();
        assert!(!got, "gc must not acquire lease held by check");
    }

    // -----------------------------------------------------------------------
    // Error injection tests
    // -----------------------------------------------------------------------

    use depot_core::error::Retryability;
    use depot_core::store::test_mock::{FailingKvStore, KvOp};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lease_acquire_get_versioned_error() {
        let (real_kv, _dir) = test_kv().await;
        let kv = FailingKvStore::wrap(real_kv);
        // Fail the get_versioned call used by try_acquire_lease.
        kv.fail_on(KvOp::GetVersioned, Some(1), Retryability::Transient);

        let result = try_acquire_lease(kv.as_ref(), "gc", "inst-1", 3600).await;
        assert!(result.is_err(), "should propagate KV error");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lease_acquire_put_if_version_error() {
        let (real_kv, _dir) = test_kv().await;
        let kv = FailingKvStore::wrap(real_kv);
        // Let get_versioned succeed (returns None → new lease), fail put_if_version.
        kv.fail_on(KvOp::PutIfVersion, Some(1), Retryability::Permanent);

        let result = try_acquire_lease(kv.as_ref(), "gc", "inst-1", 3600).await;
        assert!(result.is_err(), "should propagate put_if_version error");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lease_release_kv_error() {
        let (real_kv, _dir) = test_kv().await;
        let kv = FailingKvStore::wrap(real_kv);

        // First acquire successfully.
        let acquired = try_acquire_lease(kv.as_ref(), "gc", "inst-1", 3600)
            .await
            .unwrap();
        assert!(acquired);

        // Now fail get_versioned (used by release_lease to find the lease).
        kv.fail_on(KvOp::GetVersioned, None, Retryability::Permanent);
        let result = release_lease(kv.as_ref(), "gc", "inst-1").await;
        assert!(result.is_err(), "should propagate release KV error");
    }
}

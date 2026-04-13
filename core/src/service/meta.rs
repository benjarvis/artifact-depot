// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Application metadata key-value storage.

use crate::error;
use crate::store::keys;
use crate::store::kv::*;

pub async fn get_meta(kv: &dyn KvStore, key: &str) -> error::Result<Option<Vec<u8>>> {
    let (table, pk, sk) = keys::meta_key(key);
    kv.get(table, pk, sk).await
}

pub async fn put_meta(kv: &dyn KvStore, key: &str, value: &[u8]) -> error::Result<()> {
    let (table, pk, sk) = keys::meta_key(key);
    kv.put(table, pk, sk, value).await
}

pub async fn delete_meta(kv: &dyn KvStore, key: &str) -> error::Result<()> {
    let (table, pk, sk) = keys::meta_key(key);
    kv.delete(table, pk, sk).await?;
    Ok(())
}

/// Read the persisted GC last-started timestamp.
/// Returns `None` on fresh installs or pre-upgrade KV stores.
pub async fn get_gc_last_started_at(
    kv: &dyn KvStore,
) -> error::Result<Option<chrono::DateTime<chrono::Utc>>> {
    match get_meta(kv, "gc_last_started_at").await? {
        Some(data) => Ok(rmp_serde::from_slice(&data).ok()),
        None => Ok(None),
    }
}

/// Persist the GC last-started timestamp.
pub async fn set_gc_last_started_at(
    kv: &dyn KvStore,
    time: chrono::DateTime<chrono::Utc>,
) -> error::Result<()> {
    let data = rmp_serde::to_vec(&time).map_err(|e| {
        error::DepotError::Internal(format!("Failed to serialize gc_last_started_at: {e}"))
    })?;
    put_meta(kv, "gc_last_started_at", &data).await
}

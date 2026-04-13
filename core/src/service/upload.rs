// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Upload session CRUD and cleanup.

use std::borrow::Cow;

use crate::error;
use crate::store::keys;
use crate::store::kv::*;

use super::scan::ADMIN_ENTITY_CAP;
use super::typed::{typed_get, typed_get_versioned, typed_list, typed_put};

pub async fn put_upload_session(
    kv: &dyn KvStore,
    record: &UploadSessionRecord,
) -> error::Result<()> {
    let (table, pk, sk) = keys::upload_session_key(&record.uuid);
    typed_put(kv, table, pk, sk, record).await
}

pub async fn get_upload_session(
    kv: &dyn KvStore,
    uuid: &str,
) -> error::Result<Option<UploadSessionRecord>> {
    let (table, pk, sk) = keys::upload_session_key(uuid);
    typed_get(kv, table, pk, sk).await
}

pub async fn get_upload_session_versioned(
    kv: &dyn KvStore,
    uuid: &str,
) -> error::Result<Option<(UploadSessionRecord, u64)>> {
    let (table, pk, sk) = keys::upload_session_key(uuid);
    typed_get_versioned(kv, table, pk, sk).await
}

pub async fn put_upload_session_if_version(
    kv: &dyn KvStore,
    record: &UploadSessionRecord,
    expected_version: u64,
) -> error::Result<bool> {
    let (table, pk, sk) = keys::upload_session_key(&record.uuid);
    let bytes = rmp_serde::to_vec(record)?;
    kv.put_if_version(table, pk, sk, &bytes, Some(expected_version))
        .await
}

pub async fn delete_upload_session(kv: &dyn KvStore, uuid: &str) -> error::Result<()> {
    let (table, pk, sk) = keys::upload_session_key(uuid);
    kv.delete(table, pk, sk).await?;
    Ok(())
}

pub async fn list_upload_sessions(kv: &dyn KvStore) -> error::Result<Vec<UploadSessionRecord>> {
    typed_list(
        kv,
        keys::TABLE_UPLOAD_SESSIONS,
        Cow::Borrowed(keys::SINGLE_PK),
        Cow::Borrowed(""),
        ADMIN_ENTITY_CAP,
    )
    .await
}

/// Scan all upload session records older than `max_age` and clean them up.
#[tracing::instrument(level = "debug", skip(kv, stores))]
pub async fn cleanup_upload_sessions(
    kv: &dyn KvStore,
    stores: &crate::store_registry::StoreRegistry,
    max_age: std::time::Duration,
) -> error::Result<usize> {
    let sessions = list_upload_sessions(kv).await?;
    let cutoff = chrono::Utc::now() - chrono::Duration::from_std(max_age).unwrap_or_default();
    let expired: Vec<_> = sessions
        .into_iter()
        .filter(|s| s.updated_at < cutoff)
        .collect();
    let count = expired.len();
    for session in &expired {
        if let Some(blob_store) = stores.get(&session.store).await {
            for chunk_id in &session.chunk_blob_ids {
                let _ = blob_store.delete(chunk_id).await;
            }
            let _ = blob_store.delete(&session.blob_id).await;
        }
    }
    let del_keys: Vec<_> = expired
        .iter()
        .map(|s| keys::upload_session_key(&s.uuid))
        .collect();
    let del_refs: Vec<(&str, &str)> = del_keys.iter().map(|(_, pk, sk)| (&**pk, &**sk)).collect();
    if !del_refs.is_empty() {
        kv.delete_batch(keys::TABLE_UPLOAD_SESSIONS, &del_refs)
            .await?;
    }
    Ok(count)
}

// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Store configuration CRUD.

use std::borrow::Cow;

use crate::error;
use crate::store::keys;
use crate::store::kv::*;

use super::scan::ADMIN_ENTITY_CAP;
use super::typed::{typed_get, typed_list, typed_put};

pub async fn put_store(kv: &dyn KvStore, record: &StoreRecord) -> error::Result<()> {
    let (table, pk, sk) = keys::store_key(&record.name);
    typed_put(kv, table, pk, sk, record).await
}

pub async fn get_store(kv: &dyn KvStore, name: &str) -> error::Result<Option<StoreRecord>> {
    let (table, pk, sk) = keys::store_key(name);
    typed_get(kv, table, pk, sk).await
}

pub async fn list_stores(kv: &dyn KvStore) -> error::Result<Vec<StoreRecord>> {
    typed_list(
        kv,
        keys::TABLE_STORES,
        Cow::Borrowed(keys::SINGLE_PK),
        Cow::Borrowed(""),
        ADMIN_ENTITY_CAP,
    )
    .await
}

pub async fn delete_store(kv: &dyn KvStore, name: &str) -> error::Result<bool> {
    let (table, pk, sk) = keys::store_key(name);
    kv.delete(table, pk, sk).await
}

// --- Store stats ---

pub async fn put_store_stats(
    kv: &dyn KvStore,
    name: &str,
    stats: &StoreStatsRecord,
) -> error::Result<()> {
    let (table, pk, sk) = keys::store_stats_key(name);
    typed_put(kv, table, pk, sk, stats).await
}

pub async fn get_store_stats(
    kv: &dyn KvStore,
    name: &str,
) -> error::Result<Option<StoreStatsRecord>> {
    let (table, pk, sk) = keys::store_stats_key(name);
    typed_get(kv, table, pk, sk).await
}

pub async fn delete_store_stats(kv: &dyn KvStore, name: &str) -> error::Result<bool> {
    let (table, pk, sk) = keys::store_stats_key(name);
    kv.delete(table, pk, sk).await
}

// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Generic typed helpers over the raw-bytes KvStore trait.
//!
//! These wrap the common msgpack serialize/deserialize pattern so that
//! service functions don't need to repeat `rmp_serde::to_vec` / `from_slice`
//! at every call site.

use std::borrow::Cow;

use serde::{de::DeserializeOwned, Serialize};

use crate::error;
use crate::store::kv::{KvStore, ScanResult};

/// Deserialize a single record by key, returning `None` if absent.
pub async fn typed_get<T: DeserializeOwned>(
    kv: &dyn KvStore,
    table: &str,
    pk: Cow<'_, str>,
    sk: Cow<'_, str>,
) -> error::Result<Option<T>> {
    match kv.get(table, pk, sk).await? {
        Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
        None => Ok(None),
    }
}

/// Serialize and unconditionally put a record.
pub async fn typed_put<T: Serialize>(
    kv: &dyn KvStore,
    table: &str,
    pk: Cow<'_, str>,
    sk: Cow<'_, str>,
    record: &T,
) -> error::Result<()> {
    let bytes = rmp_serde::to_vec_named(record)?;
    kv.put(table, pk, sk, &bytes).await
}

/// Deserialize a single record with its version stamp.
pub async fn typed_get_versioned<T: DeserializeOwned>(
    kv: &dyn KvStore,
    table: &str,
    pk: Cow<'_, str>,
    sk: Cow<'_, str>,
) -> error::Result<Option<(T, u64)>> {
    match kv.get_versioned(table, pk, sk).await? {
        Some((bytes, version)) => Ok(Some((rmp_serde::from_slice(&bytes)?, version))),
        None => Ok(None),
    }
}

/// Serialize and put a record only if the key does not already exist.
/// Returns `Ok(true)` if written, `Ok(false)` if the key already existed.
pub async fn typed_put_if_absent<T: Serialize>(
    kv: &dyn KvStore,
    table: &str,
    pk: Cow<'_, str>,
    sk: Cow<'_, str>,
    record: &T,
) -> error::Result<bool> {
    let bytes = rmp_serde::to_vec_named(record)?;
    kv.put_if_absent(table, pk, sk, &bytes).await
}

/// Scan a prefix and deserialize all results.
pub async fn typed_list<T: DeserializeOwned>(
    kv: &dyn KvStore,
    table: &str,
    pk: Cow<'_, str>,
    sk_prefix: Cow<'_, str>,
    limit: usize,
) -> error::Result<Vec<T>> {
    let result: ScanResult = kv.scan_prefix(table, pk, sk_prefix, limit).await?;
    result
        .items
        .iter()
        .map(|(_, v)| Ok(rmp_serde::from_slice(v)?))
        .collect()
}

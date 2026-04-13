// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Test-only mock wrappers for KvStore and BlobStore with error injection.
//!
//! Both wrappers delegate to a real inner store by default. Error injection
//! is configured via `fail_on` / `fail_on_table` which add rules checked
//! before each delegation. Rules match on operation type, optional call
//! count, and optional table name.

#![allow(clippy::unwrap_used)]

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{self, DepotError, Retryability};
use crate::store::blob::{BlobReader, BlobStore, BlobWriter};
use crate::store::kv::{KvStore, ScanResult};

// ---------------------------------------------------------------------------
// KvOp / BlobOp enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KvOp {
    Get,
    GetVersioned,
    GetConsistent,
    Put,
    PutIfVersion,
    Delete,
    DeleteReturning,
    DeleteBatch,
    ScanPrefix,
    ScanRange,
    PutBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlobOp {
    Put,
    Get,
    OpenRead,
    Exists,
    Delete,
    CreateWriter,
}

// ---------------------------------------------------------------------------
// Error rules
// ---------------------------------------------------------------------------

struct ErrorRule {
    op: KvOp,
    /// Fire on this call number (1-indexed). `None` = every call.
    on_call: Option<u64>,
    retryability: Retryability,
    /// Only match when the table argument equals this value.
    table_filter: Option<String>,
}

struct BlobErrorRule {
    op: BlobOp,
    on_call: Option<u64>,
    retryability: Retryability,
}

// ---------------------------------------------------------------------------
// FailingKvStore
// ---------------------------------------------------------------------------

struct KvErrorState {
    rules: Vec<ErrorRule>,
    call_counts: HashMap<(KvOp, Option<String>), u64>,
}

pub struct FailingKvStore {
    inner: Arc<dyn KvStore>,
    state: std::sync::Mutex<KvErrorState>,
}

impl FailingKvStore {
    /// Wrap a real KvStore. All operations delegate unless error rules match.
    pub fn wrap(inner: Arc<dyn KvStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            state: std::sync::Mutex::new(KvErrorState {
                rules: Vec::new(),
                call_counts: HashMap::new(),
            }),
        })
    }

    /// Inject an error: the Nth call (1-indexed) to `op` will fail.
    /// If `on_call` is `None`, ALL calls to `op` fail.
    pub fn fail_on(&self, op: KvOp, on_call: Option<u64>, retryability: Retryability) {
        self.state.lock().unwrap().rules.push(ErrorRule {
            op,
            on_call,
            retryability,
            table_filter: None,
        });
    }

    /// Inject an error only when the table argument matches.
    pub fn fail_on_table(
        &self,
        op: KvOp,
        table: &str,
        on_call: Option<u64>,
        retryability: Retryability,
    ) {
        self.state.lock().unwrap().rules.push(ErrorRule {
            op,
            on_call,
            retryability,
            table_filter: Some(table.to_string()),
        });
    }

    /// Remove all injected error rules.
    pub fn clear_errors(&self) {
        let mut s = self.state.lock().unwrap();
        s.rules.clear();
    }

    /// Reset call counters (useful between test phases).
    pub fn reset_counters(&self) {
        self.state.lock().unwrap().call_counts.clear();
    }

    /// Check whether an error should be injected for this operation.
    fn check_error(&self, op: KvOp, table: Option<&str>) -> Option<DepotError> {
        let mut s = self.state.lock().unwrap();

        // Increment counter keyed by (op, table).
        let key = (op, table.map(|t| t.to_string()));
        let count = s.call_counts.entry(key).or_insert(0);
        *count += 1;
        let current = *count;

        for rule in &s.rules {
            if rule.op != op {
                continue;
            }
            if let Some(ref filter) = rule.table_filter {
                if table.is_none_or(|t| t != filter.as_str()) {
                    continue;
                }
            }
            let matches = match rule.on_call {
                Some(n) => current == n,
                None => true,
            };
            if matches {
                return Some(DepotError::Storage(
                    Box::new(std::io::Error::other(format!(
                        "injected {op:?} error (call #{current})"
                    ))),
                    rule.retryability,
                ));
            }
        }
        None
    }
}

#[async_trait::async_trait]
impl KvStore for FailingKvStore {
    async fn get(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        if let Some(e) = self.check_error(KvOp::Get, Some(table)) {
            return Err(e);
        }
        self.inner.get(table, pk, sk).await
    }

    async fn get_versioned(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<(Vec<u8>, u64)>> {
        if let Some(e) = self.check_error(KvOp::GetVersioned, Some(table)) {
            return Err(e);
        }
        self.inner.get_versioned(table, pk, sk).await
    }

    async fn get_consistent(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        if let Some(e) = self.check_error(KvOp::GetConsistent, Some(table)) {
            return Err(e);
        }
        self.inner.get_consistent(table, pk, sk).await
    }

    async fn put(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        value: &[u8],
    ) -> error::Result<()> {
        if let Some(e) = self.check_error(KvOp::Put, Some(table)) {
            return Err(e);
        }
        self.inner.put(table, pk, sk, value).await
    }

    async fn put_if_version(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        value: &[u8],
        expected_version: Option<u64>,
    ) -> error::Result<bool> {
        if let Some(e) = self.check_error(KvOp::PutIfVersion, Some(table)) {
            return Err(e);
        }
        self.inner
            .put_if_version(table, pk, sk, value, expected_version)
            .await
    }

    async fn delete(&self, table: &str, pk: Cow<'_, str>, sk: Cow<'_, str>) -> error::Result<bool> {
        if let Some(e) = self.check_error(KvOp::Delete, Some(table)) {
            return Err(e);
        }
        self.inner.delete(table, pk, sk).await
    }

    async fn delete_if_version(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        expected_version: u64,
    ) -> error::Result<bool> {
        if let Some(e) = self.check_error(KvOp::Delete, Some(table)) {
            return Err(e);
        }
        self.inner
            .delete_if_version(table, pk, sk, expected_version)
            .await
    }

    async fn delete_returning(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        if let Some(e) = self.check_error(KvOp::DeleteReturning, Some(table)) {
            return Err(e);
        }
        self.inner.delete_returning(table, pk, sk).await
    }

    async fn delete_batch(&self, table: &str, keys: &[(&str, &str)]) -> error::Result<Vec<bool>> {
        if let Some(e) = self.check_error(KvOp::DeleteBatch, Some(table)) {
            return Err(e);
        }
        self.inner.delete_batch(table, keys).await
    }

    async fn scan_prefix(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk_prefix: Cow<'_, str>,
        limit: usize,
    ) -> error::Result<ScanResult> {
        if let Some(e) = self.check_error(KvOp::ScanPrefix, Some(table)) {
            return Err(e);
        }
        self.inner.scan_prefix(table, pk, sk_prefix, limit).await
    }

    async fn scan_range(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk_start: Cow<'_, str>,
        sk_end: Option<&str>,
        limit: usize,
    ) -> error::Result<ScanResult> {
        if let Some(e) = self.check_error(KvOp::ScanRange, Some(table)) {
            return Err(e);
        }
        self.inner
            .scan_range(table, pk, sk_start, sk_end, limit)
            .await
    }

    async fn put_batch(&self, table: &str, entries: &[(&str, &str, &[u8])]) -> error::Result<()> {
        if let Some(e) = self.check_error(KvOp::PutBatch, Some(table)) {
            return Err(e);
        }
        self.inner.put_batch(table, entries).await
    }
}

// ---------------------------------------------------------------------------
// FailingBlobStore
// ---------------------------------------------------------------------------

struct BlobErrorState {
    rules: Vec<BlobErrorRule>,
    call_counts: HashMap<BlobOp, u64>,
}

pub struct FailingBlobStore {
    inner: Arc<dyn BlobStore>,
    state: std::sync::Mutex<BlobErrorState>,
}

impl FailingBlobStore {
    pub fn wrap(inner: Arc<dyn BlobStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            state: std::sync::Mutex::new(BlobErrorState {
                rules: Vec::new(),
                call_counts: HashMap::new(),
            }),
        })
    }

    pub fn fail_on(&self, op: BlobOp, on_call: Option<u64>, retryability: Retryability) {
        self.state.lock().unwrap().rules.push(BlobErrorRule {
            op,
            on_call,
            retryability,
        });
    }

    pub fn clear_errors(&self) {
        self.state.lock().unwrap().rules.clear();
    }

    pub fn reset_counters(&self) {
        self.state.lock().unwrap().call_counts.clear();
    }

    fn check_error(&self, op: BlobOp) -> Option<DepotError> {
        let mut s = self.state.lock().unwrap();
        let count = s.call_counts.entry(op).or_insert(0);
        *count += 1;
        let current = *count;

        for rule in &s.rules {
            if rule.op != op {
                continue;
            }
            let matches = match rule.on_call {
                Some(n) => current == n,
                None => true,
            };
            if matches {
                return Some(DepotError::Storage(
                    Box::new(std::io::Error::other(format!(
                        "injected {op:?} error (call #{current})"
                    ))),
                    rule.retryability,
                ));
            }
        }
        None
    }
}

#[async_trait::async_trait]
impl BlobStore for FailingBlobStore {
    async fn put(&self, blob_id: &str, data: &[u8]) -> error::Result<u64> {
        if let Some(e) = self.check_error(BlobOp::Put) {
            return Err(e);
        }
        self.inner.put(blob_id, data).await
    }

    async fn get(&self, blob_id: &str) -> error::Result<Option<Vec<u8>>> {
        if let Some(e) = self.check_error(BlobOp::Get) {
            return Err(e);
        }
        self.inner.get(blob_id).await
    }

    async fn open_read(&self, blob_id: &str) -> error::Result<Option<(BlobReader, u64)>> {
        if let Some(e) = self.check_error(BlobOp::OpenRead) {
            return Err(e);
        }
        self.inner.open_read(blob_id).await
    }

    async fn exists(&self, blob_id: &str) -> error::Result<bool> {
        if let Some(e) = self.check_error(BlobOp::Exists) {
            return Err(e);
        }
        self.inner.exists(blob_id).await
    }

    async fn delete(&self, blob_id: &str) -> error::Result<()> {
        if let Some(e) = self.check_error(BlobOp::Delete) {
            return Err(e);
        }
        self.inner.delete(blob_id).await
    }

    fn blob_path(&self, blob_id: &str) -> Option<std::path::PathBuf> {
        self.inner.blob_path(blob_id)
    }

    async fn create_writer(
        &self,
        blob_id: &str,
        content_length_hint: Option<u64>,
    ) -> error::Result<Box<dyn BlobWriter>> {
        if let Some(e) = self.check_error(BlobOp::CreateWriter) {
            return Err(e);
        }
        self.inner.create_writer(blob_id, content_length_hint).await
    }

    async fn scan_all_blobs(&self) -> error::Result<crate::store::blob::BlobScanStream<'_>> {
        self.inner.scan_all_blobs().await
    }

    async fn health_check(&self) -> error::Result<()> {
        self.inner.health_check().await
    }

    async fn browse_prefix(
        &self,
        prefix: &str,
    ) -> error::Result<Vec<crate::store::blob::BrowseEntry>> {
        self.inner.browse_prefix(prefix).await
    }
}

// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::path::PathBuf;
use std::time::Instant;

use metrics::{counter, gauge, histogram};
use tracing::Instrument;

use depot_core::error;
use depot_core::store::blob::{BlobReader, BlobScanStream, BlobStore, BlobWriter, BrowseEntry};
use depot_core::store::kv::{KvStore, ScanResult};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn record_kv(op: &'static str, start: Instant, ok: bool) {
    gauge!("kv_store_ops_in_flight").decrement(1.0);
    histogram!("kv_store_operation_duration_seconds", "op" => op)
        .record(start.elapsed().as_secs_f64());
    counter!("kv_store_operations_total", "op" => op, "result" => if ok { "ok" } else { "error" })
        .increment(1);
}

fn start_kv() -> Instant {
    gauge!("kv_store_ops_in_flight").increment(1.0);
    Instant::now()
}

fn record_blob(op: &'static str, start: Instant, ok: bool) {
    gauge!("blob_store_ops_in_flight").decrement(1.0);
    histogram!("blob_store_operation_duration_seconds", "op" => op)
        .record(start.elapsed().as_secs_f64());
    counter!("blob_store_operations_total", "op" => op, "result" => if ok { "ok" } else { "error" })
        .increment(1);
}

fn start_blob() -> Instant {
    gauge!("blob_store_ops_in_flight").increment(1.0);
    Instant::now()
}

// ---------------------------------------------------------------------------
// InstrumentedKvStore
// ---------------------------------------------------------------------------

pub struct InstrumentedKvStore<T> {
    inner: T,
    db_system: &'static str,
}

impl<T> InstrumentedKvStore<T> {
    pub fn new(inner: T, db_system: &'static str) -> Self {
        // Ensure the gauge exists at zero from startup.
        gauge!("kv_store_ops_in_flight").set(0.0);
        Self { inner, db_system }
    }
}

#[async_trait::async_trait]
impl<T: KvStore> KvStore for InstrumentedKvStore<T> {
    async fn get(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        let span =
            tracing::debug_span!("kv.get", "db.system" = self.db_system, table, pk = %pk, sk = %sk);
        let start = start_kv();
        let result = self.inner.get(table, pk, sk).instrument(span).await;
        record_kv("get", start, result.is_ok());
        result
    }

    async fn get_versioned(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<(Vec<u8>, u64)>> {
        let span = tracing::debug_span!("kv.get_versioned", "db.system" = self.db_system, table, pk = %pk, sk = %sk);
        let start = start_kv();
        let result = self
            .inner
            .get_versioned(table, pk, sk)
            .instrument(span)
            .await;
        record_kv("get_versioned", start, result.is_ok());
        result
    }

    async fn put(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        value: &[u8],
    ) -> error::Result<()> {
        let span =
            tracing::debug_span!("kv.put", "db.system" = self.db_system, table, pk = %pk, sk = %sk);
        let start = start_kv();
        let result = self.inner.put(table, pk, sk, value).instrument(span).await;
        record_kv("put", start, result.is_ok());
        result
    }

    async fn put_if_version(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        value: &[u8],
        expected_version: Option<u64>,
    ) -> error::Result<bool> {
        let span = tracing::debug_span!("kv.put_if_version", "db.system" = self.db_system, table, pk = %pk, sk = %sk, expected_version);
        let start = start_kv();
        let result = self
            .inner
            .put_if_version(table, pk, sk, value, expected_version)
            .instrument(span)
            .await;
        record_kv("put_if_version", start, result.is_ok());
        result
    }

    async fn delete(&self, table: &str, pk: Cow<'_, str>, sk: Cow<'_, str>) -> error::Result<bool> {
        let span = tracing::debug_span!("kv.delete", "db.system" = self.db_system, table, pk = %pk, sk = %sk);
        let start = start_kv();
        let result = self.inner.delete(table, pk, sk).instrument(span).await;
        record_kv("delete", start, result.is_ok());
        result
    }

    async fn delete_if_version(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        expected_version: u64,
    ) -> error::Result<bool> {
        let span = tracing::debug_span!("kv.delete_if_version", "db.system" = self.db_system, table, pk = %pk, sk = %sk);
        let start = start_kv();
        let result = self
            .inner
            .delete_if_version(table, pk, sk, expected_version)
            .instrument(span)
            .await;
        record_kv("delete_if_version", start, result.is_ok());
        result
    }

    async fn delete_returning(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        let span = tracing::debug_span!("kv.delete_returning", "db.system" = self.db_system, table, pk = %pk, sk = %sk);
        let start = start_kv();
        let result = self
            .inner
            .delete_returning(table, pk, sk)
            .instrument(span)
            .await;
        record_kv("delete_returning", start, result.is_ok());
        result
    }

    async fn delete_batch(&self, table: &str, keys: &[(&str, &str)]) -> error::Result<Vec<bool>> {
        let span = tracing::debug_span!(
            "kv.delete_batch",
            "db.system" = self.db_system,
            table,
            count = keys.len()
        );
        let start = start_kv();
        let result = self.inner.delete_batch(table, keys).instrument(span).await;
        record_kv("delete_batch", start, result.is_ok());
        result
    }

    async fn put_batch(&self, table: &str, entries: &[(&str, &str, &[u8])]) -> error::Result<()> {
        let span = tracing::debug_span!(
            "kv.put_batch",
            "db.system" = self.db_system,
            table,
            count = entries.len()
        );
        let start = start_kv();
        let result = self.inner.put_batch(table, entries).instrument(span).await;
        record_kv("put_batch", start, result.is_ok());
        result
    }

    async fn scan_prefix(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk_prefix: Cow<'_, str>,
        limit: usize,
    ) -> error::Result<ScanResult> {
        let span = tracing::debug_span!("kv.scan_prefix", "db.system" = self.db_system, table, pk = %pk, limit);
        let start = start_kv();
        let result = self
            .inner
            .scan_prefix(table, pk, sk_prefix, limit)
            .instrument(span)
            .await;
        record_kv("scan_prefix", start, result.is_ok());
        result
    }

    async fn scan_range(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk_start: Cow<'_, str>,
        sk_end: Option<&str>,
        limit: usize,
    ) -> error::Result<ScanResult> {
        let span = tracing::debug_span!("kv.scan_range", "db.system" = self.db_system, table, pk = %pk, limit);
        let start = start_kv();
        let result = self
            .inner
            .scan_range(table, pk, sk_start, sk_end, limit)
            .instrument(span)
            .await;
        record_kv("scan_range", start, result.is_ok());
        result
    }

    async fn get_consistent(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        let span = tracing::debug_span!("kv.get_consistent", "db.system" = self.db_system, table, pk = %pk, sk = %sk);
        let start = start_kv();
        let result = self
            .inner
            .get_consistent(table, pk, sk)
            .instrument(span)
            .await;
        record_kv("get_consistent", start, result.is_ok());
        result
    }

    fn is_single_node(&self) -> bool {
        self.inner.is_single_node()
    }

    async fn compact(&self) -> error::Result<bool> {
        let span = tracing::debug_span!("kv.compact", "db.system" = self.db_system);
        let start = start_kv();
        let result = self.inner.compact().instrument(span).await;
        record_kv("compact", start, result.is_ok());
        result
    }
}

// ---------------------------------------------------------------------------
// InstrumentedBlobStore
// ---------------------------------------------------------------------------

pub struct InstrumentedBlobStore<T> {
    inner: T,
}

impl<T> InstrumentedBlobStore<T> {
    pub fn new(inner: T) -> Self {
        gauge!("blob_store_ops_in_flight").set(0.0);
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<T: BlobStore> BlobStore for InstrumentedBlobStore<T> {
    async fn put(&self, blob_id: &str, data: &[u8]) -> error::Result<u64> {
        let span = tracing::debug_span!("blob.put", blob_id);
        let start = start_blob();
        let result = self.inner.put(blob_id, data).instrument(span).await;
        gauge!("blob_store_ops_in_flight").decrement(1.0);
        histogram!("blob_store_operation_duration_seconds", "op" => "put")
            .record(start.elapsed().as_secs_f64());
        match &result {
            Ok(0) => counter!("blob_store_operations_total", "op" => "put", "result" => "dedup")
                .increment(1),
            Ok(n) => {
                counter!("blob_store_operations_total", "op" => "put", "result" => "ok")
                    .increment(1);
                counter!("blob_store_written_bytes_total").increment(*n);
            }
            Err(_) => counter!("blob_store_operations_total", "op" => "put", "result" => "error")
                .increment(1),
        }
        result
    }

    async fn get(&self, blob_id: &str) -> error::Result<Option<Vec<u8>>> {
        let span = tracing::debug_span!("blob.get", blob_id);
        let start = start_blob();
        let result = self.inner.get(blob_id).instrument(span).await;
        gauge!("blob_store_ops_in_flight").decrement(1.0);
        histogram!("blob_store_operation_duration_seconds", "op" => "get")
            .record(start.elapsed().as_secs_f64());
        match &result {
            Ok(Some(data)) => {
                counter!("blob_store_operations_total", "op" => "get", "result" => "ok")
                    .increment(1);
                counter!("blob_store_read_bytes_total").increment(data.len() as u64);
            }
            Ok(None) => counter!("blob_store_operations_total", "op" => "get", "result" => "ok")
                .increment(1),
            Err(_) => counter!("blob_store_operations_total", "op" => "get", "result" => "error")
                .increment(1),
        }
        result
    }

    async fn open_read(&self, blob_id: &str) -> error::Result<Option<(BlobReader, u64)>> {
        let span = tracing::debug_span!("blob.open_read", blob_id);
        let start = start_blob();
        let result = self.inner.open_read(blob_id).instrument(span).await;
        record_blob("open_read", start, result.is_ok());
        result
    }

    async fn exists(&self, blob_id: &str) -> error::Result<bool> {
        let span = tracing::debug_span!("blob.exists", blob_id);
        let start = start_blob();
        let result = self.inner.exists(blob_id).instrument(span).await;
        record_blob("exists", start, result.is_ok());
        result
    }

    async fn delete(&self, blob_id: &str) -> error::Result<()> {
        let span = tracing::debug_span!("blob.delete", blob_id);
        let start = start_blob();
        let result = self.inner.delete(blob_id).instrument(span).await;
        record_blob("delete", start, result.is_ok());
        result
    }

    fn blob_path(&self, blob_id: &str) -> Option<PathBuf> {
        self.inner.blob_path(blob_id)
    }

    async fn create_writer(
        &self,
        blob_id: &str,
        content_length_hint: Option<u64>,
    ) -> error::Result<Box<dyn BlobWriter>> {
        let span = tracing::debug_span!("blob.create_writer", blob_id, content_length_hint);
        self.inner
            .create_writer(blob_id, content_length_hint)
            .instrument(span)
            .await
    }

    async fn scan_all_blobs(&self) -> error::Result<BlobScanStream<'_>> {
        // Passed through without per-item instrumentation — scans emit huge
        // numbers of items and instrumenting each would be very noisy.
        self.inner.scan_all_blobs().await
    }

    async fn health_check(&self) -> error::Result<()> {
        let span = tracing::debug_span!("blob.health_check");
        let start = start_blob();
        let result = self.inner.health_check().instrument(span).await;
        gauge!("blob_store_ops_in_flight").decrement(1.0);
        histogram!("blob_store_operation_duration_seconds", "op" => "health_check")
            .record(start.elapsed().as_secs_f64());
        let outcome = if result.is_ok() { "ok" } else { "error" };
        counter!("blob_store_operations_total", "op" => "health_check", "result" => outcome)
            .increment(1);
        result
    }

    async fn browse_prefix(&self, prefix: &str) -> error::Result<Vec<BrowseEntry>> {
        let span = tracing::debug_span!("blob.browse_prefix", prefix);
        self.inner.browse_prefix(prefix).instrument(span).await
    }
}

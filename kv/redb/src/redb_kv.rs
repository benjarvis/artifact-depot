// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

use depot_core::error::{self, DepotError, Retryability};
use depot_core::store::kv::ScanResult;
use redb::{Database, ReadableTable, TableDefinition};
use std::path::Path;
use std::sync::Arc;

/// Maximum number of write operations to coalesce into a single transaction.
/// Under load, the channel naturally accumulates operations while the
/// previous batch commits, so no intentional delay is needed.
const BATCH_CAP: usize = 64;

/// Envelope format tag for the current version.
const ENVELOPE_V1: u8 = 0x01;
/// Size of the envelope header: 1 byte format + 8 bytes version.
const ENVELOPE_HEADER_SIZE: usize = 9;

// ---------------------------------------------------------------------------
// Writer-thread protocol
// ---------------------------------------------------------------------------

enum WriteOp {
    Put {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<error::Result<()>>,
    },
    PutIfVersion {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
        expected_version: Option<u64>,
        reply: tokio::sync::oneshot::Sender<error::Result<bool>>,
    },
    Delete {
        table: String,
        key: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<error::Result<bool>>,
    },
    DeleteReturning {
        table: String,
        key: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<error::Result<Option<Vec<u8>>>>,
    },
    DeleteIfVersion {
        table: String,
        key: Vec<u8>,
        expected_version: u64,
        reply: tokio::sync::oneshot::Sender<error::Result<bool>>,
    },
    DeleteBatch {
        table: String,
        keys: Vec<Vec<u8>>,
        reply: tokio::sync::oneshot::Sender<error::Result<Vec<bool>>>,
    },
    PutBatch {
        table: String,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        reply: tokio::sync::oneshot::Sender<error::Result<()>>,
    },
}

pub struct RedbKvStore {
    db: Arc<Database>,
    write_tx: Option<tokio::sync::mpsc::Sender<WriteOp>>,
    writer_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for RedbKvStore {
    fn drop(&mut self) {
        // Drop the sender to close the channel — the writer task will see
        // `None` from recv() and exit after draining any in-flight batch,
        // allowing redb to shut down cleanly without WAL recovery on restart.
        self.write_tx.take();
    }
}

impl RedbKvStore {
    pub fn open(path: &Path) -> error::Result<Self> {
        Self::open_with_cache(path, 0)
    }

    pub fn open_with_cache(path: &Path, cache_bytes: usize) -> error::Result<Self> {
        let db = if cache_bytes > 0 {
            Database::builder()
                .set_cache_size(cache_bytes)
                .create(path)
                .map_err(DepotError::storage_permanent)?
        } else {
            Database::create(path).map_err(DepotError::storage_permanent)?
        };

        // Pre-create all known tables so reads never hit TableDoesNotExist.
        let txn = db.begin_write().map_err(DepotError::storage_permanent)?;
        for name in depot_core::store::keys::ALL_TABLES {
            let def = TableDefinition::<&[u8], &[u8]>::new(name);
            let _ = txn.open_table(def).map_err(DepotError::storage_permanent)?;
        }
        txn.commit().map_err(DepotError::storage_permanent)?;

        let db = Arc::new(db);
        let writer_db = Arc::clone(&db);

        let (write_tx, write_rx) = tokio::sync::mpsc::channel::<WriteOp>(4096);

        let writer_handle = tokio::spawn(writer_task(writer_db, write_rx));

        Ok(Self {
            db,
            write_tx: Some(write_tx),
            writer_handle: Some(writer_handle),
        })
    }

    /// Gracefully shut down the writer task and wait for it to finish.
    /// Must be called from an async context. This ensures the redb database
    /// file lock is released before the store is re-opened.
    pub async fn close(&mut self) {
        self.write_tx.take();
        if let Some(h) = self.writer_handle.take() {
            let _ = h.await;
        }
    }

    /// Send a write operation and await its result.
    fn send_write<T: Send + 'static>(
        &self,
        op_fn: impl FnOnce(tokio::sync::oneshot::Sender<error::Result<T>>) -> WriteOp,
    ) -> tokio::sync::oneshot::Receiver<error::Result<T>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let op = op_fn(tx);
        let sent = self
            .write_tx
            .as_ref()
            .map(|s| s.try_send(op).is_ok())
            .unwrap_or(false);
        if !sent {
            let (tx2, rx2) = tokio::sync::oneshot::channel();
            let _ = tx2.send(Err(DepotError::Storage(
                "redb writer channel full or closed".into(),
                Retryability::Permanent,
            )));
            return rx2;
        }
        rx
    }
}

/// Async writer task.  Receives write ops via a tokio channel (no kernel
/// futex on the send path), batches them, and executes the redb transaction
/// on a blocking thread.
///
/// Batching is opportunistic: execute immediately with whatever is available,
/// no intentional delay.  While the transaction commits on the blocking
/// thread, new ops accumulate in the channel and form the next batch
/// naturally.
async fn writer_task(db: Arc<Database>, mut rx: tokio::sync::mpsc::Receiver<WriteOp>) {
    loop {
        // Wait for at least one operation (no busy-spin when idle).
        let first = match rx.recv().await {
            Some(op) => op,
            None => return, // Channel closed — shut down.
        };

        let mut batch = Vec::with_capacity(BATCH_CAP);
        batch.push(first);

        // Drain whatever else is already queued, up to BATCH_CAP.
        while batch.len() < BATCH_CAP {
            match rx.try_recv() {
                Ok(op) => batch.push(op),
                Err(_) => break,
            }
        }

        let db = Arc::clone(&db);
        // Run the redb transaction on a blocking thread so we don't
        // block the tokio runtime during the (synchronous) commit.
        let _ = tokio::task::spawn_blocking(move || execute_batch(&db, batch)).await;
    }
}

/// Execute a batch of write operations in a single redb write transaction.
fn execute_batch(db: &Database, batch: Vec<WriteOp>) {
    // Attempt to run all operations in one transaction.
    let txn = match db.begin_write() {
        Ok(t) => t,
        Err(e) => {
            let err_msg = e.to_string();
            send_errors(batch, &err_msg);
            return;
        }
    };

    // Collect per-operation results while the transaction is open.
    let mut results: Vec<OpResult> = Vec::with_capacity(batch.len());

    // Helper macro to open table by name within the transaction.
    macro_rules! open_table {
        ($table_name:expr, $err_variant:ident) => {
            match txn.open_table(TableDefinition::<&[u8], &[u8]>::new($table_name)) {
                Ok(t) => t,
                Err(e) => {
                    results.push(OpResult::$err_variant(Err(e.to_string())));
                    continue;
                }
            }
        };
    }

    for op in &batch {
        let r = match op {
            WriteOp::Put {
                table, key, value, ..
            } => {
                let mut t = open_table!(table, Unit);
                // Read current version, increment, wrap with envelope.
                let current_version = match t.get(key.as_slice()) {
                    Ok(Some(v)) => read_version(v.value()),
                    Ok(None) => 0,
                    Err(e) => {
                        results.push(OpResult::Unit(Err(e.to_string())));
                        continue;
                    }
                };
                let wrapped = wrap_envelope(current_version + 1, value);
                let r = match t.insert(key.as_slice(), wrapped.as_slice()) {
                    Ok(_) => OpResult::Unit(Ok(())),
                    Err(e) => OpResult::Unit(Err(e.to_string())),
                };
                r
            }
            WriteOp::PutIfVersion {
                table,
                key,
                value,
                expected_version,
                ..
            } => {
                let mut t = open_table!(table, Bool);
                // Read current version, check against expected.
                let current_version = match t.get(key.as_slice()) {
                    Ok(Some(v)) => Some(read_version(v.value())),
                    Ok(None) => None,
                    Err(e) => {
                        results.push(OpResult::Bool(Err(e.to_string())));
                        continue;
                    }
                };

                let ok = match (*expected_version, current_version) {
                    (None, None) => true,
                    (None, Some(_)) => false,
                    (Some(exp), Some(act)) if exp == act => true,
                    (Some(_), _) => false,
                };

                if !ok {
                    OpResult::Bool(Ok(false))
                } else {
                    let new_version = expected_version.unwrap_or(0) + 1;
                    let wrapped = wrap_envelope(new_version, value);
                    let r = match t.insert(key.as_slice(), wrapped.as_slice()) {
                        Ok(_) => OpResult::Bool(Ok(true)),
                        Err(e) => OpResult::Bool(Err(e.to_string())),
                    };
                    r
                }
            }
            WriteOp::Delete { table, key, .. } => {
                let mut t = open_table!(table, Bool);
                let r = match t.remove(key.as_slice()) {
                    Ok(guard) => OpResult::Bool(Ok(guard.is_some())),
                    Err(e) => OpResult::Bool(Err(e.to_string())),
                };
                r
            }
            WriteOp::DeleteIfVersion {
                table,
                key,
                expected_version,
                ..
            } => {
                let mut t = open_table!(table, Bool);
                let current_version = match t.get(key.as_slice()) {
                    Ok(Some(v)) => Some(read_version(v.value())),
                    Ok(None) => None,
                    Err(e) => {
                        results.push(OpResult::Bool(Err(e.to_string())));
                        continue;
                    }
                };

                if current_version != Some(*expected_version) {
                    OpResult::Bool(Ok(false))
                } else {
                    match t.remove(key.as_slice()) {
                        Ok(guard) => OpResult::Bool(Ok(guard.is_some())),
                        Err(e) => OpResult::Bool(Err(e.to_string())),
                    }
                }
            }
            WriteOp::DeleteReturning { table, key, .. } => {
                let mut t = open_table!(table, OptBytes);
                let result = match t.remove(key.as_slice()) {
                    Ok(Some(guard)) => {
                        let raw = guard.value();
                        let (payload, _version) = unwrap_envelope(raw);
                        Ok(Some(payload.to_vec()))
                    }
                    Ok(None) => Ok(None),
                    Err(e) => Err(e.to_string()),
                };
                OpResult::OptBytes(result)
            }
            WriteOp::DeleteBatch { table, keys, .. } => {
                let mut t = open_table!(table, Bools);
                let mut existed = Vec::with_capacity(keys.len());
                let mut err = None;
                for key in keys {
                    match t.remove(key.as_slice()) {
                        Ok(guard) => existed.push(guard.is_some()),
                        Err(e) => {
                            err = Some(e.to_string());
                            break;
                        }
                    }
                }
                match err {
                    Some(e) => OpResult::Bools(Err(e)),
                    None => OpResult::Bools(Ok(existed)),
                }
            }
            WriteOp::PutBatch { table, entries, .. } => {
                let mut t = open_table!(table, Unit);
                let mut err = None;
                for (key, value) in entries {
                    // Read current version, increment, wrap with envelope.
                    let current_version = match t.get(key.as_slice()) {
                        Ok(Some(v)) => read_version(v.value()),
                        Ok(None) => 0,
                        Err(e) => {
                            err = Some(e.to_string());
                            break;
                        }
                    };
                    let wrapped = wrap_envelope(current_version + 1, value);
                    if let Err(e) = t.insert(key.as_slice(), wrapped.as_slice()) {
                        err = Some(e.to_string());
                        break;
                    }
                }
                match err {
                    Some(e) => OpResult::Unit(Err(e)),
                    None => OpResult::Unit(Ok(())),
                }
            }
        };
        results.push(r);
    }

    if let Err(e) = txn.commit() {
        let err_msg = e.to_string();
        send_errors(batch, &err_msg);
        return;
    }

    // Send individual results back.
    for (op, result) in batch.into_iter().zip(results) {
        match (op, result) {
            (WriteOp::Put { reply, .. }, OpResult::Unit(r)) => {
                let _ = reply.send(r.map_err(storage_err));
            }
            (WriteOp::PutIfVersion { reply, .. }, OpResult::Bool(r)) => {
                let _ = reply.send(r.map_err(storage_err));
            }
            (WriteOp::Delete { reply, .. }, OpResult::Bool(r)) => {
                let _ = reply.send(r.map_err(storage_err));
            }
            (WriteOp::DeleteIfVersion { reply, .. }, OpResult::Bool(r)) => {
                let _ = reply.send(r.map_err(storage_err));
            }
            (WriteOp::DeleteReturning { reply, .. }, OpResult::OptBytes(r)) => {
                let _ = reply.send(r.map_err(storage_err));
            }
            (WriteOp::DeleteBatch { reply, .. }, OpResult::Bools(r)) => {
                let _ = reply.send(r.map_err(storage_err));
            }
            (WriteOp::PutBatch { reply, .. }, OpResult::Unit(r)) => {
                let _ = reply.send(r.map_err(storage_err));
            }
            _ => {
                tracing::error!("mismatched op/result types in write batch");
            }
        }
    }
}

enum OpResult {
    Unit(Result<(), String>),
    Bool(Result<bool, String>),
    Bools(Result<Vec<bool>, String>),
    OptBytes(Result<Option<Vec<u8>>, String>),
}

fn storage_err(msg: String) -> DepotError {
    DepotError::Storage(msg.into(), Retryability::Permanent)
}

fn send_errors(batch: Vec<WriteOp>, err_msg: &str) {
    for op in batch {
        match op {
            WriteOp::Put { reply, .. } => {
                let _ = reply.send(Err(storage_err(err_msg.to_string())));
            }
            WriteOp::PutIfVersion { reply, .. } => {
                let _ = reply.send(Err(storage_err(err_msg.to_string())));
            }
            WriteOp::Delete { reply, .. } => {
                let _ = reply.send(Err(storage_err(err_msg.to_string())));
            }
            WriteOp::DeleteIfVersion { reply, .. } => {
                let _ = reply.send(Err(storage_err(err_msg.to_string())));
            }
            WriteOp::DeleteReturning { reply, .. } => {
                let _ = reply.send(Err(storage_err(err_msg.to_string())));
            }
            WriteOp::DeleteBatch { reply, .. } => {
                let _ = reply.send(Err(storage_err(err_msg.to_string())));
            }
            WriteOp::PutBatch { reply, .. } => {
                let _ = reply.send(Err(storage_err(err_msg.to_string())));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Flat-key helpers
// ---------------------------------------------------------------------------

/// Build the internal flat key from (pk, sk).
/// Table dimension is now handled by separate redb tables.
fn flat_key(pk: &str, sk: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(pk.len() + 1 + sk.len());
    k.extend_from_slice(pk.as_bytes());
    k.push(0x00);
    k.extend_from_slice(sk.as_bytes());
    k
}

/// Compute the prefix for scanning all sort keys within a partition.
fn partition_prefix(pk: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(pk.len() + 1);
    k.extend_from_slice(pk.as_bytes());
    k.push(0x00);
    k
}

use depot_core::store::keys::prefix_end as prefix_range_end;

/// Extract the sort key from a flat key given the known partition prefix length.
fn extract_sk(flat: &[u8], prefix_len: usize) -> String {
    let bytes = flat.get(prefix_len..).unwrap_or(&[]);
    String::from_utf8(bytes.to_vec()).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Envelope helpers
// ---------------------------------------------------------------------------

/// Wrap payload bytes with the envelope header: [format][version LE].
fn wrap_envelope(version: u64, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(ENVELOPE_HEADER_SIZE + payload.len());
    buf.push(ENVELOPE_V1);
    buf.extend_from_slice(&version.to_le_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Unwrap envelope, returning (payload, version).
/// Returns the raw bytes and version 0 if no envelope header is present (shouldn't happen).
fn unwrap_envelope(raw: &[u8]) -> (&[u8], u64) {
    #[allow(clippy::indexing_slicing)] // all slices guarded by raw.len() >= ENVELOPE_HEADER_SIZE
    if raw.len() >= ENVELOPE_HEADER_SIZE && raw[0] == ENVELOPE_V1 {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&raw[1..9]);
        let version = u64::from_le_bytes(bytes);
        (&raw[ENVELOPE_HEADER_SIZE..], version)
    } else {
        // No envelope — treat as version 0 (pre-envelope data).
        (raw, 0)
    }
}

/// Read the version from an existing raw value, or 0 if not present.
fn read_version(raw: &[u8]) -> u64 {
    unwrap_envelope(raw).1
}

// ---------------------------------------------------------------------------
// KvStore implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl depot_core::store::kv::KvStore for RedbKvStore {
    async fn get(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        let key = flat_key(&pk, &sk);
        let txn = self
            .db
            .begin_read()
            .map_err(DepotError::storage_permanent)?;
        let t = txn
            .open_table(TableDefinition::<&[u8], &[u8]>::new(table))
            .map_err(DepotError::storage_permanent)?;
        let r = t
            .get(key.as_slice())
            .map_err(DepotError::storage_permanent)?
            .map(|v| unwrap_envelope(v.value()).0.to_vec());
        Ok(r)
    }

    async fn get_versioned(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<(Vec<u8>, u64)>> {
        let key = flat_key(&pk, &sk);
        let txn = self
            .db
            .begin_read()
            .map_err(DepotError::storage_permanent)?;
        let t = txn
            .open_table(TableDefinition::<&[u8], &[u8]>::new(table))
            .map_err(DepotError::storage_permanent)?;
        let r = t
            .get(key.as_slice())
            .map_err(DepotError::storage_permanent)?
            .map(|v| {
                let (payload, version) = unwrap_envelope(v.value());
                (payload.to_vec(), version)
            });
        Ok(r)
    }

    async fn put(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        value: &[u8],
    ) -> error::Result<()> {
        let table = table.to_string();
        let key = flat_key(&pk, &sk);
        let value = value.to_vec();
        let rx = self.send_write(|reply| WriteOp::Put {
            table,
            key,
            value,
            reply,
        });
        rx.await.map_err(|_| storage_err("writer dropped".into()))?
    }

    async fn put_if_version(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        value: &[u8],
        expected_version: Option<u64>,
    ) -> error::Result<bool> {
        let table = table.to_string();
        let key = flat_key(&pk, &sk);
        let value = value.to_vec();
        let rx = self.send_write(|reply| WriteOp::PutIfVersion {
            table,
            key,
            value,
            expected_version,
            reply,
        });
        rx.await.map_err(|_| storage_err("writer dropped".into()))?
    }

    async fn delete(&self, table: &str, pk: Cow<'_, str>, sk: Cow<'_, str>) -> error::Result<bool> {
        let table = table.to_string();
        let key = flat_key(&pk, &sk);
        let rx = self.send_write(|reply| WriteOp::Delete { table, key, reply });
        rx.await.map_err(|_| storage_err("writer dropped".into()))?
    }

    async fn delete_if_version(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        expected_version: u64,
    ) -> error::Result<bool> {
        let table = table.to_string();
        let key = flat_key(&pk, &sk);
        let rx = self.send_write(|reply| WriteOp::DeleteIfVersion {
            table,
            key,
            expected_version,
            reply,
        });
        rx.await.map_err(|_| storage_err("writer dropped".into()))?
    }

    async fn delete_returning(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        let table = table.to_string();
        let key = flat_key(&pk, &sk);
        let rx = self.send_write(|reply| WriteOp::DeleteReturning { table, key, reply });
        rx.await.map_err(|_| storage_err("writer dropped".into()))?
    }

    async fn delete_batch(&self, table: &str, keys: &[(&str, &str)]) -> error::Result<Vec<bool>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let table = table.to_string();
        let flat_keys: Vec<Vec<u8>> = keys.iter().map(|(pk, sk)| flat_key(pk, sk)).collect();
        let rx = self.send_write(|reply| WriteOp::DeleteBatch {
            table,
            keys: flat_keys,
            reply,
        });
        rx.await.map_err(|_| storage_err("writer dropped".into()))?
    }

    async fn put_batch(&self, table: &str, entries: &[(&str, &str, &[u8])]) -> error::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let table = table.to_string();
        let flat_entries: Vec<(Vec<u8>, Vec<u8>)> = entries
            .iter()
            .map(|(pk, sk, value)| (flat_key(pk, sk), value.to_vec()))
            .collect();
        let rx = self.send_write(|reply| WriteOp::PutBatch {
            table,
            entries: flat_entries,
            reply,
        });
        rx.await.map_err(|_| storage_err("writer dropped".into()))?
    }

    async fn scan_prefix(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk_prefix: Cow<'_, str>,
        limit: usize,
    ) -> error::Result<ScanResult> {
        let prefix = {
            let mut p = partition_prefix(&pk);
            p.extend_from_slice(sk_prefix.as_bytes());
            p
        };
        let part_prefix_len = partition_prefix(&pk).len();
        let txn = self
            .db
            .begin_read()
            .map_err(DepotError::storage_permanent)?;
        let t = txn
            .open_table(TableDefinition::<&[u8], &[u8]>::new(table))
            .map_err(DepotError::storage_permanent)?;
        let mut results = Vec::new();
        let mut hit_limit = false;
        let end = prefix_range_end(&prefix);
        let range = if let Some(ref end_bytes) = end {
            t.range::<&[u8]>(prefix.as_slice()..end_bytes.as_slice())
                .map_err(DepotError::storage_permanent)?
        } else {
            t.range::<&[u8]>(prefix.as_slice()..)
                .map_err(DepotError::storage_permanent)?
        };
        for entry in range {
            if results.len() >= limit {
                hit_limit = true;
                break;
            }
            let (k, v) = entry.map_err(DepotError::storage_permanent)?;
            let flat = k.value();
            if !flat.starts_with(&prefix) {
                break;
            }
            let sk = extract_sk(flat, part_prefix_len);
            let (payload, _version) = unwrap_envelope(v.value());
            results.push((sk, payload.to_vec()));
        }
        Ok(ScanResult {
            items: results,
            done: !hit_limit,
        })
    }

    async fn scan_range(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk_start: Cow<'_, str>,
        sk_end: Option<&str>,
        limit: usize,
    ) -> error::Result<ScanResult> {
        let start = flat_key(&pk, &sk_start);
        let part_prefix_len = partition_prefix(&pk).len();
        let end = match sk_end {
            Some(end_str) => Some(flat_key(&pk, end_str)),
            None => {
                // Open-ended: scan to end of partition.
                prefix_range_end(&partition_prefix(&pk))
            }
        };
        let txn = self
            .db
            .begin_read()
            .map_err(DepotError::storage_permanent)?;
        let t = txn
            .open_table(TableDefinition::<&[u8], &[u8]>::new(table))
            .map_err(DepotError::storage_permanent)?;
        let mut results = Vec::new();
        let mut hit_limit = false;
        let range = if let Some(ref end_bytes) = end {
            t.range::<&[u8]>(start.as_slice()..end_bytes.as_slice())
                .map_err(DepotError::storage_permanent)?
        } else {
            t.range::<&[u8]>(start.as_slice()..)
                .map_err(DepotError::storage_permanent)?
        };
        for entry in range {
            if results.len() >= limit {
                hit_limit = true;
                break;
            }
            let (k, v) = entry.map_err(DepotError::storage_permanent)?;
            let sk = extract_sk(k.value(), part_prefix_len);
            let (payload, _version) = unwrap_envelope(v.value());
            results.push((sk, payload.to_vec()));
        }
        Ok(ScanResult {
            items: results,
            done: !hit_limit,
        })
    }

    fn is_single_node(&self) -> bool {
        true
    }
}

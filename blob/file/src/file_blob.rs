// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::io::AsyncWriteExt;

use crate::direct_io::{
    open_direct_read, open_direct_write, probe_direct_io, AlignedBufPool, DirectReader,
    DirectWriter,
};
use depot_core::error;
use depot_core::store::blob::{
    blob_id_to_path, BlobReader, BlobScanStream, BlobStore, BlobWriter, BrowseEntry,
};
use futures::stream::StreamExt;

pub struct FileBlobStore {
    root: PathBuf,
    sync: bool,
    io_size: usize,
    /// When `Some`, O_DIRECT is active and aligned buffers are recycled here.
    pool: Option<Arc<AlignedBufPool>>,
}

impl FileBlobStore {
    pub fn new(root: &Path, sync: bool, io_size: usize, direct_io: bool) -> error::Result<Self> {
        std::fs::create_dir_all(root)?;

        let pool = if direct_io {
            if probe_direct_io(root) {
                tracing::info!(root = %root.display(), "O_DIRECT enabled for blob store");
                Some(AlignedBufPool::new(io_size, 32))
            } else {
                tracing::warn!(
                    root = %root.display(),
                    "O_DIRECT not supported on this filesystem, falling back to buffered I/O"
                );
                None
            }
        } else {
            None
        };

        Ok(Self {
            root: root.to_path_buf(),
            sync,
            io_size,
            pool,
        })
    }
}

/// Round `n` up to the next multiple of 4096.
fn round_up_align(n: usize) -> usize {
    (n + 4095) & !4095
}

#[async_trait::async_trait]
impl BlobStore for FileBlobStore {
    async fn put(&self, blob_id: &str, data: &[u8]) -> error::Result<u64> {
        let root = self.root.clone();
        let sync = self.sync;
        let blob_id = blob_id.to_string();
        let data = data.to_vec();

        if let Some(pool) = &self.pool {
            let pool = Arc::clone(pool);
            let io_size = self.io_size;
            tokio::task::spawn_blocking(move || {
                let path = blob_id_to_path(&root, &blob_id)?;
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let f = open_direct_write(&path)?;
                let total = data.len();
                let mut offset = 0usize;

                while offset < total {
                    let remaining = total - offset;
                    let chunk = remaining.min(io_size);
                    let mut buf = pool.get()?;
                    let src = data.get(offset..offset + chunk).ok_or_else(|| {
                        error::DepotError::Internal("put: data slice OOB".to_string())
                    })?;
                    let dst = buf.get_mut(..chunk).ok_or_else(|| {
                        error::DepotError::Internal("put: buf slice OOB".to_string())
                    })?;
                    dst.copy_from_slice(src);
                    let write_size = round_up_align(chunk);
                    // Zero only the pad region between data and aligned boundary.
                    if let Some(pad) = buf.get_mut(chunk..write_size) {
                        pad.fill(0);
                    }
                    let write_src = buf.get(..write_size).ok_or_else(|| {
                        error::DepotError::Internal("put: write slice OOB".to_string())
                    })?;
                    f.write_all_at(write_src, offset as u64)?;
                    pool.put(buf);
                    offset += chunk;
                }

                f.set_len(total as u64)?;
                if sync {
                    f.sync_all()?;
                }
                Ok(total as u64)
            })
            .await?
        } else {
            tokio::task::spawn_blocking(move || {
                let path = blob_id_to_path(&root, &blob_id)?;
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let mut f = File::create(&path)?;
                f.write_all(&data)?;
                if sync {
                    f.sync_all()?;
                }
                Ok(data.len() as u64)
            })
            .await?
        }
    }

    async fn get(&self, blob_id: &str) -> error::Result<Option<Vec<u8>>> {
        let root = self.root.clone();
        let blob_id = blob_id.to_string();

        if let Some(pool) = &self.pool {
            let pool = Arc::clone(pool);
            let io_size = self.io_size;
            tokio::task::spawn_blocking(move || {
                let path = blob_id_to_path(&root, &blob_id)?;
                let f = match File::open(&path) {
                    Ok(f) => f,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => return Err(e.into()),
                };
                // Re-open with O_DIRECT for the actual read.
                let df = match open_direct_read(&path) {
                    Ok(f) => f,
                    Err(_) => {
                        // Fallback: read without O_DIRECT.
                        drop(f);
                        return Ok(Some(std::fs::read(&path)?));
                    }
                };
                let size = f.metadata()?.len() as usize;
                drop(f);
                let mut result = vec![0u8; size];
                let mut offset = 0usize;

                while offset < size {
                    let remaining = size - offset;
                    let chunk = remaining.min(io_size);
                    let read_size = round_up_align(chunk);
                    let mut buf = pool.get()?;
                    let read_dst = buf.get_mut(..read_size).ok_or_else(|| {
                        error::DepotError::Internal("get: read buf slice OOB".to_string())
                    })?;
                    let n = df.read_at(read_dst, offset as u64)?;
                    let valid = n.min(remaining);
                    let src = buf.get(..valid).ok_or_else(|| {
                        error::DepotError::Internal("get: src slice OOB".to_string())
                    })?;
                    let dst = result.get_mut(offset..offset + valid).ok_or_else(|| {
                        error::DepotError::Internal("get: result slice OOB".to_string())
                    })?;
                    dst.copy_from_slice(src);
                    pool.put(buf);
                    offset += valid;
                }

                Ok(Some(result))
            })
            .await?
        } else {
            tokio::task::spawn_blocking(move || {
                let path = blob_id_to_path(&root, &blob_id)?;
                if !path.exists() {
                    return Ok(None);
                }
                let data = std::fs::read(&path)?;
                Ok(Some(data))
            })
            .await?
        }
    }

    async fn open_read(&self, blob_id: &str) -> error::Result<Option<(BlobReader, u64)>> {
        let root = self.root.clone();
        let blob_id = blob_id.to_string();
        let io_size = self.io_size;

        if let Some(pool) = &self.pool {
            let pool = Arc::clone(pool);
            tokio::task::spawn_blocking(move || {
                let path = blob_id_to_path(&root, &blob_id)?;
                match open_direct_read(&path) {
                    Ok(f) => {
                        let size = f.metadata()?.len();
                        let buf = pool.get()?;
                        let reader = DirectReader::new(f, size, pool, io_size, buf);
                        Ok(Some((Box::new(reader) as BlobReader, size)))
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e.into()),
                }
            })
            .await?
        } else {
            tokio::task::spawn_blocking(move || {
                let path = blob_id_to_path(&root, &blob_id)?;
                match File::open(&path) {
                    Ok(f) => {
                        let meta = f.metadata()?;
                        let size = meta.len();
                        let tokio_file = tokio::fs::File::from_std(f);
                        let buffered = tokio::io::BufReader::with_capacity(io_size, tokio_file);
                        Ok(Some((Box::new(buffered) as BlobReader, size)))
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e.into()),
                }
            })
            .await?
        }
    }

    async fn exists(&self, blob_id: &str) -> error::Result<bool> {
        let root = self.root.clone();
        let blob_id = blob_id.to_string();
        tokio::task::spawn_blocking(move || {
            let path = blob_id_to_path(&root, &blob_id)?;
            Ok(path.exists())
        })
        .await?
    }

    async fn delete(&self, blob_id: &str) -> error::Result<()> {
        let root = self.root.clone();
        let blob_id = blob_id.to_string();
        tokio::task::spawn_blocking(move || {
            let path = blob_id_to_path(&root, &blob_id)?;
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            Ok(())
        })
        .await?
    }

    fn blob_path(&self, blob_id: &str) -> Option<PathBuf> {
        blob_id_to_path(&self.root, blob_id).ok()
    }

    async fn create_writer(
        &self,
        blob_id: &str,
        _content_length_hint: Option<u64>,
    ) -> error::Result<Box<dyn BlobWriter>> {
        let path = blob_id_to_path(&self.root, blob_id)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        if let Some(pool) = &self.pool {
            let file = tokio::task::spawn_blocking({
                let path = path.clone();
                move || open_direct_write(&path)
            })
            .await
            .map_err(|e| error::DepotError::Internal(e.to_string()))??;

            let buf = pool.get()?;
            Ok(Box::new(DirectWriter::new(
                file,
                path,
                Arc::clone(pool),
                self.io_size,
                self.sync,
                buf,
            )))
        } else {
            let file = tokio::fs::File::create(&path).await?;
            let writer = tokio::io::BufWriter::with_capacity(self.io_size, file);
            Ok(Box::new(FileBlobWriter {
                path,
                writer,
                size: 0,
                sync: self.sync,
            }))
        }
    }

    async fn scan_all_blobs(&self) -> error::Result<BlobScanStream<'_>> {
        let root = self.root.clone();

        // Walk the 2-level directory structure (`<root>/ab/cd/<blob_id>`) in a
        // blocking task — std::fs is fine and walking is CPU+IO bound in
        // predictable ways. Stream items back through an mpsc channel so the
        // caller can process each blob as it's discovered.
        let (tx, rx) = tokio::sync::mpsc::channel::<error::Result<(String, u64)>>(1024);

        tokio::task::spawn_blocking(move || {
            const DIR_DEPTH: usize = 2;

            fn walk_level(
                dir: &Path,
                depth_remaining: usize,
                prefixes: &mut Vec<String>,
                tx: &tokio::sync::mpsc::Sender<error::Result<(String, u64)>>,
            ) {
                let entries = match std::fs::read_dir(dir) {
                    Ok(e) => e,
                    Err(_) => return,
                };

                if depth_remaining == 0 {
                    for entry in entries.flatten() {
                        let fname = entry.file_name();
                        let blob_id = fname.to_string_lossy();
                        let valid = prefixes.iter().enumerate().all(|(i, prefix)| {
                            let start = i * 2;
                            blob_id.get(start..start + 2) == Some(prefix.as_str())
                        });
                        if !valid {
                            continue;
                        }
                        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                        if tx.blocking_send(Ok((blob_id.into_owned(), size))).is_err() {
                            return;
                        }
                    }
                } else {
                    for entry in entries.flatten() {
                        let name = entry.file_name();
                        let name_str = name.to_string_lossy();
                        if name_str.len() != 2 || !entry.path().is_dir() {
                            continue;
                        }
                        prefixes.push(name_str.into_owned());
                        walk_level(&entry.path(), depth_remaining - 1, prefixes, tx);
                        prefixes.pop();
                    }
                }
            }

            let mut prefixes = Vec::with_capacity(DIR_DEPTH);
            walk_level(&root, DIR_DEPTH, &mut prefixes, &tx);
        });

        Ok(tokio_stream::wrappers::ReceiverStream::new(rx).boxed())
    }

    async fn health_check(&self) -> error::Result<()> {
        let check_id = format!("_depot_health_{}", uuid::Uuid::new_v4().simple());
        let test_data = b"depot-health-check";

        // 1. Put
        self.put(&check_id, test_data)
            .await
            .map_err(|e| error::DepotError::Internal(format!("health_check put: {e}")))?;

        // From here on, best-effort delete on failure.
        let inner = self.health_check_inner(&check_id, test_data).await;
        if inner.is_err() {
            let _ = self.delete(&check_id).await;
        }
        inner
    }

    async fn browse_prefix(&self, prefix: &str) -> error::Result<Vec<BrowseEntry>> {
        let base = self.root.join(prefix.trim_start_matches('/'));
        let mut result = Vec::new();
        let mut entries = match tokio::fs::read_dir(&base).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(result),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            let name = match entry.file_name().into_string() {
                Ok(n) => n,
                Err(_) => continue,
            };
            let meta = entry.metadata().await.ok();
            let is_dir = meta.as_ref().is_some_and(|m| m.is_dir());
            let size = if is_dir { None } else { meta.map(|m| m.len()) };
            result.push(BrowseEntry { name, is_dir, size });
        }
        result.sort_by(|a, b| b.is_dir.cmp(&a.is_dir).then_with(|| a.name.cmp(&b.name)));
        Ok(result)
    }
}

impl FileBlobStore {
    async fn health_check_inner(&self, check_id: &str, test_data: &[u8]) -> error::Result<()> {
        // 2. Get and verify
        match self.get(check_id).await {
            Ok(Some(data)) if data == test_data => {}
            Ok(Some(_)) => {
                return Err(error::DepotError::Internal(
                    "health_check get: returned unexpected data".into(),
                ))
            }
            Ok(None) => {
                return Err(error::DepotError::Internal(
                    "health_check get: blob not found after successful put".into(),
                ))
            }
            Err(e) => {
                return Err(error::DepotError::Internal(format!(
                    "health_check get: {e}"
                )))
            }
        }

        // 3. List and verify the blob appears in a directory listing.
        let (d1, d2) = match (check_id.get(..2), check_id.get(2..4)) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                return Err(error::DepotError::Internal(
                    "health_check list: blob id too short for directory layout".into(),
                ))
            }
        };
        let dir_path = format!("{d1}/{d2}/");
        let entries = self
            .browse_prefix(&dir_path)
            .await
            .map_err(|e| error::DepotError::Internal(format!("health_check list: {e}")))?;
        if !entries.iter().any(|e| e.name == check_id) {
            return Err(error::DepotError::Internal(format!(
                "health_check list: blob not found in directory listing ({} entries)",
                entries.len()
            )));
        }

        // 4. Delete
        self.delete(check_id)
            .await
            .map_err(|e| error::DepotError::Internal(format!("health_check delete: {e}")))?;

        // 5. Verify deletion
        match self.exists(check_id).await {
            Ok(false) => Ok(()),
            Ok(true) => Err(error::DepotError::Internal(
                "health_check delete verify: blob still exists".into(),
            )),
            Err(e) => Err(error::DepotError::Internal(format!(
                "health_check delete verify: {e}"
            ))),
        }
    }
}

/// Streaming writer that writes directly to the final blob path.
struct FileBlobWriter {
    path: PathBuf,
    writer: tokio::io::BufWriter<tokio::fs::File>,
    size: u64,
    sync: bool,
}

#[async_trait::async_trait]
impl BlobWriter for FileBlobWriter {
    async fn write_chunk(&mut self, data: &[u8]) -> error::Result<()> {
        self.writer.write_all(data).await?;
        self.size += data.len() as u64;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> error::Result<u64> {
        self.writer.flush().await?;
        drop(self.writer);

        if self.sync {
            let path = self.path.clone();
            tokio::task::spawn_blocking(move || {
                let f = File::open(&path)?;
                f.sync_all()?;
                Ok::<(), std::io::Error>(())
            })
            .await??;
        }

        Ok(self.size)
    }

    async fn abort(self: Box<Self>) -> error::Result<()> {
        let _ = tokio::fs::remove_file(&self.path).await;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn make_store(sync: bool) -> (FileBlobStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), sync, 1048576, false).unwrap();
        (store, dir)
    }

    fn test_blob_id() -> String {
        // Use a UUID-like string with enough chars for the path split
        "a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_put_get_roundtrip() {
        let (store, _dir) = make_store(false);
        let id = test_blob_id();
        let written = store.put(&id, b"test data").await.unwrap();
        assert_eq!(written, 9);
        let data = store.get(&id).await.unwrap().unwrap();
        assert_eq!(data, b"test data");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_get_missing_returns_none() {
        let (store, _dir) = make_store(false);
        let result = store.get(&test_blob_id()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_open_read_success() {
        let (store, _dir) = make_store(false);
        let id = test_blob_id();
        store.put(&id, b"test data").await.unwrap();
        let (mut reader, size) = store.open_read(&id).await.unwrap().unwrap();
        assert_eq!(size, 9);
        let mut buf = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buf)
            .await
            .unwrap();
        assert_eq!(buf, b"test data");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_open_read_missing_returns_none() {
        let (store, _dir) = make_store(false);
        let result = store.open_read(&test_blob_id()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_exists() {
        let (store, _dir) = make_store(false);
        let id = test_blob_id();
        assert!(!store.exists(&id).await.unwrap());
        store.put(&id, b"test data").await.unwrap();
        assert!(store.exists(&id).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_delete_existing() {
        let (store, _dir) = make_store(false);
        let id = test_blob_id();
        store.put(&id, b"test data").await.unwrap();
        store.delete(&id).await.unwrap();
        assert!(!store.exists(&id).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_delete_missing() {
        let (store, _dir) = make_store(false);
        store.delete(&test_blob_id()).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_blob_path() {
        let (store, _dir) = make_store(false);
        let id = test_blob_id();
        let path = store.blob_path(&id).unwrap();
        assert!(path.to_string_lossy().contains(&id));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_put_with_sync() {
        let (store, _dir) = make_store(true);
        let id = test_blob_id();
        let written = store.put(&id, b"test data").await.unwrap();
        assert_eq!(written, 9);
        let data = store.get(&id).await.unwrap().unwrap();
        assert_eq!(data, b"test data");
    }
}

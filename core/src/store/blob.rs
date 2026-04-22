#![deny(clippy::string_slice)]

// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use futures::stream::{self, BoxStream, StreamExt};

use crate::error;

/// One entry in a browse-style prefix listing.
///
/// `name` is a leaf component (no prefix, no trailing delimiter).
/// `is_dir` is `true` for a common-prefix / directory group. Directory
/// groups have no size.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(as = StoreBrowseEntry))]
pub struct BrowseEntry {
    pub name: String,
    pub is_dir: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

/// Streaming result of `BlobStore::scan_all_blobs`: pairs of
/// `(blob_id, size_bytes)` for every blob in the store.
pub type BlobScanStream<'a> = BoxStream<'a, error::Result<(String, u64)>>;

/// A boxed async reader for streaming blob content.
pub type BlobReader = Box<dyn tokio::io::AsyncRead + Send + Unpin>;

/// Streaming writer for blob content.
#[async_trait::async_trait]
pub trait BlobWriter: Send {
    /// Write a chunk of data.
    async fn write_chunk(&mut self, data: &[u8]) -> error::Result<()>;
    /// Finish writing and persist the blob. Returns the total size in bytes.
    async fn finish(self: Box<Self>) -> error::Result<u64>;
    /// Abort the write and clean up any partial data.
    async fn abort(self: Box<Self>) -> error::Result<()>;
}

/// Trait for content-addressed blob storage.
#[async_trait::async_trait]
pub trait BlobStore: Send + Sync {
    /// Store bytes under the given blob_id. Returns the number of bytes written.
    async fn put(&self, blob_id: &str, data: &[u8]) -> error::Result<u64>;

    /// Retrieve the bytes for a blob by blob_id.
    async fn get(&self, blob_id: &str) -> error::Result<Option<Vec<u8>>>;

    /// Open a blob for streaming reads. Returns None if the blob doesn't exist.
    /// The caller receives an async reader and the blob size, and can stream
    /// without holding any store locks or buffering the entire blob in memory.
    async fn open_read(&self, blob_id: &str) -> error::Result<Option<(BlobReader, u64)>>;

    /// Check whether a blob exists.
    async fn exists(&self, blob_id: &str) -> error::Result<bool>;

    /// Delete a blob. A no-op if the blob does not exist.
    async fn delete(&self, blob_id: &str) -> error::Result<()>;

    /// Return the filesystem path for a blob (if applicable).
    fn blob_path(&self, blob_id: &str) -> Option<std::path::PathBuf>;

    /// Create a streaming writer for a new blob.
    ///
    /// `content_length_hint` is an optional hint for the total size of the blob.
    /// Backends may use this to optimise the upload strategy (e.g. avoiding
    /// multipart uploads for small, known-size blobs on S3).
    async fn create_writer(
        &self,
        blob_id: &str,
        content_length_hint: Option<u64>,
    ) -> error::Result<Box<dyn BlobWriter>>;

    /// Stream every blob in the store as `(blob_id, size)` pairs.
    ///
    /// Used by GC bloom-filter building and orphan sweeps. Ordering is
    /// unspecified, duplicates must not occur. Implementations may
    /// parallelise internally (e.g. S3 shards 256 ways across hex prefixes).
    async fn scan_all_blobs(&self) -> error::Result<BlobScanStream<'_>>;

    /// Number of independent shards this store exposes for parallel scans.
    ///
    /// Returns 1 if sharded scanning is not supported. Blob GC uses this
    /// count to fan out `scan_blobs_sharded` calls across cores.
    fn scan_shard_count(&self) -> u16 {
        1
    }

    /// Stream blobs in the given shard, where `shard` is in
    /// `0..scan_shard_count()`. The union of all shard streams covers every
    /// blob exactly once; individual shard streams must be disjoint.
    ///
    /// Default routes shard 0 to `scan_all_blobs` and yields empty for
    /// other shards, preserving existing semantics for implementations that
    /// don't override.
    async fn scan_blobs_sharded(&self, shard: u16) -> error::Result<BlobScanStream<'_>> {
        if shard == 0 {
            self.scan_all_blobs().await
        } else {
            Ok(stream::empty().boxed())
        }
    }

    /// Put/get/delete/list round trip to verify the store is reachable and
    /// the caller has the required permissions.
    ///
    /// Used when creating a store (POST /stores) and on the explicit
    /// POST /stores/{name}/check endpoint.
    async fn health_check(&self) -> error::Result<()>;

    /// Browse-style prefix listing for the admin UI. Returns the immediate
    /// children of `prefix` with directory groups collapsed by `/`.
    ///
    /// `prefix` is a virtual path inside the store's own namespace — on
    /// a filesystem it's relative to the blob root, on S3 it's relative to
    /// the configured bucket prefix plus the blobs subdirectory.
    async fn browse_prefix(&self, prefix: &str) -> error::Result<Vec<BrowseEntry>>;
}

/// Blob path layout using 2-character directory prefixes at depth 2.
///
/// Layout: `<root>/<id[0..2]>/<id[2..4]>/<id>`
pub fn blob_id_to_path(root: &Path, blob_id: &str) -> error::Result<std::path::PathBuf> {
    const DIR_DEPTH: usize = 2;
    let needed = DIR_DEPTH * 2;
    if blob_id.len() < needed {
        return Err(error::DepotError::DataIntegrity(format!(
            "blob_id too short: {blob_id}"
        )));
    }
    let mut path = root.to_path_buf();
    for i in 0..DIR_DEPTH {
        let start = i * 2;
        let prefix = blob_id.get(start..start + 2).ok_or_else(|| {
            error::DepotError::DataIntegrity(format!("blob_id too short: {blob_id}"))
        })?;
        path.push(prefix);
    }
    path.push(blob_id);
    Ok(path)
}

// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Registry of named blob stores, backed by ArcSwap for wait-free reads.

use std::sync::Arc;

use crate::server::store::{BlobStore, InstrumentedBlobStore};
use depot_blob_file::FileBlobStore;
use depot_blob_s3::S3BlobStore;
use depot_core::error::{self, DepotError};
use depot_core::store::kv::StoreKind;
use depot_core::store_registry::StoreRegistry;

/// Instantiate a BlobStore from a StoreKind descriptor.
pub async fn instantiate_store(kind: &StoreKind) -> anyhow::Result<Arc<dyn BlobStore>> {
    match kind {
        StoreKind::File {
            root,
            sync,
            io_size,
            direct_io,
        } => {
            let store = FileBlobStore::new(
                std::path::Path::new(root),
                *sync,
                *io_size * 1024,
                *direct_io,
            )?;
            Ok(Arc::new(InstrumentedBlobStore::new(store)))
        }
        StoreKind::S3 {
            bucket,
            endpoint,
            region,
            prefix,
            access_key,
            secret_key,
            max_retries,
            connect_timeout_secs,
            read_timeout_secs,
            ref retry_mode,
        } => {
            let store = S3BlobStore::new(
                bucket.clone(),
                endpoint.clone(),
                region.clone(),
                prefix.clone(),
                access_key.clone(),
                secret_key.clone(),
                *max_retries,
                *connect_timeout_secs,
                *read_timeout_secs,
                retry_mode,
            )
            .await?;
            Ok(Arc::new(InstrumentedBlobStore::new(store)))
        }
    }
}

/// Helper on AppState to resolve a blob store by name.
pub async fn resolve_blob_store(
    stores: &Arc<StoreRegistry>,
    store_name: &str,
) -> error::Result<Arc<dyn BlobStore>> {
    stores
        .get(store_name)
        .await
        .ok_or_else(|| DepotError::NotFound(format!("blob store '{}' not found", store_name)))
}

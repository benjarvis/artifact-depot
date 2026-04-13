// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! State available to format HTTP handlers.
//!
//! `FormatState` is the subset of the depot `AppState` that artifact format
//! crates need. It carries storage access, HTTP client for upstream fetching,
//! and a thin permission-checking trait. It deliberately excludes auth
//! backends, config, workers, and other server internals so format crates
//! can depend only on `depot-core`.

use std::sync::Arc;

use crate::error::Result;
use crate::inflight::InflightMap;
use crate::store::blob::BlobStore;
use crate::store::kv::{Capability, KvStore, RoleRecord, UserRecord};
use crate::store_registry::StoreRegistry;
use crate::update::UpdateSender;

/// Auth surface that format handlers need.
///
/// This covers the common identity and authorisation operations that format
/// protocols perform: permission checks on upload/download, username/password
/// authentication (for Docker login and npm adduser), and JWT-style token
/// issuance. Format crates interact only with this trait — the concrete
/// AuthBackend, JWT secret handling, and settings all live in the depot
/// server crate behind an implementation of this trait.
#[async_trait::async_trait]
pub trait PermissionChecker: Send + Sync {
    /// Check whether `user` has the required capability on `repo`.
    async fn check_permission(&self, user: &str, repo: &str, required: Capability) -> Result<()>;

    /// Verify credentials. Returns the UserRecord on success, None when the
    /// user does not exist or the password is wrong.
    async fn authenticate(&self, username: &str, password: &str) -> Result<Option<UserRecord>>;

    /// Fetch the user record + all resolved roles. Used by endpoints like the
    /// Docker catalog that filter the response by per-repo permissions.
    async fn resolve_user_roles(
        &self,
        username: &str,
    ) -> Result<Option<(UserRecord, Vec<RoleRecord>)>>;

    /// Issue a JWT bearer token for the given user. The concrete implementation
    /// chooses the signing key and expiry policy.
    async fn create_user_token(&self, user: &UserRecord) -> Result<String>;
}

/// Format-handler-visible settings subset.
///
/// Kept small and focused: only flags that format HTTP handlers actually
/// read during request handling. Backed by an `ArcSwap` in depot so that
/// settings changes are visible without a restart.
#[async_trait::async_trait]
pub trait FormatSettings: Send + Sync {
    /// If set, single-segment Docker paths (`/v2/{name}/...`) are treated
    /// as an image inside this repository. Returns the current value.
    fn default_docker_repo(&self) -> Option<String>;

    /// Maximum number of chunks allowed per Docker chunked upload session.
    fn max_docker_chunk_count(&self) -> u32;
}

/// State available to format HTTP handlers.
///
/// Contains everything a format handler needs to serve requests: storage
/// access, HTTP client for upstream fetching, and permission checking.
/// Does NOT include auth backends, config, workers, or other server internals.
#[derive(Clone)]
pub struct FormatState {
    pub kv: Arc<dyn KvStore>,
    pub stores: Arc<StoreRegistry>,
    pub http: reqwest::Client,
    pub inflight: InflightMap,
    pub updater: UpdateSender,
    pub auth: Arc<dyn PermissionChecker>,
    pub settings: Arc<dyn FormatSettings>,
}

impl FormatState {
    /// Resolve the blob store for a given store name.
    ///
    /// This helper does a simple lookup against the registry and returns a
    /// `NotFound` error if the named store is not registered. The depot crate
    /// may wrap this with instrumentation or additional checks on top.
    pub async fn blob_store(&self, store_name: &str) -> Result<Arc<dyn BlobStore>> {
        self.stores.get(store_name).await.ok_or_else(|| {
            crate::error::DepotError::NotFound(format!("blob store '{}' not found", store_name))
        })
    }
}

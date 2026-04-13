// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use super::{make_raw_repo_with_depth, ArtifactContent, RawRepo, RepoContext};
use crate::error;
use crate::inflight::InflightMap;
use crate::service::{self, pagination::paginate_results};
use crate::store::blob::BlobStore;
use crate::store::kv::{ArtifactRecord, DirEntry, KvStore, PaginatedResult, Pagination};
use crate::update::UpdateSender;

/// Maximum recursion depth for proxy repo resolution.
/// Prevents stack overflow from circular membership (e.g. A -> B -> A).
pub const MAX_PROXY_DEPTH: u8 = 8;

/// A proxy (group) repository that searches an ordered list of member repositories.
///
/// On GET: iterate through members in order, return the first hit.
/// Members can be hosted, cache, or other proxy repos (recursive resolution).
pub struct ProxyRepo {
    pub name: String,
    pub members: Vec<String>,
    pub kv: Arc<dyn KvStore>,
    pub blobs: Arc<dyn BlobStore>,
    pub store: String,
    pub http: reqwest::Client,
    pub inflight: InflightMap,
    pub updater: UpdateSender,
    pub depth: u8,
}

impl ProxyRepo {
    pub fn head<'b>(
        &'b self,
        path: &'b str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = error::Result<Option<ArtifactRecord>>> + Send + 'b>,
    > {
        Box::pin(async move { self.head_inner(path).await })
    }

    #[tracing::instrument(level = "debug", skip(self), fields(repo = %self.name, path))]
    async fn head_inner(&self, path: &str) -> error::Result<Option<ArtifactRecord>> {
        if self.depth > MAX_PROXY_DEPTH {
            tracing::warn!(
                proxy = %self.name,
                depth = self.depth,
                "proxy recursion depth exceeded, returning not found"
            );
            return Ok(None);
        }
        for member_name in &self.members {
            let member_config = service::get_repo(self.kv.as_ref(), member_name).await?;
            let member_config = match member_config {
                Some(c) => c,
                None => {
                    tracing::warn!(
                        proxy = %self.name,
                        member = member_name,
                        "member repo not found, skipping"
                    );
                    continue;
                }
            };

            let ctx = RepoContext {
                kv: Arc::clone(&self.kv),
                blobs: Arc::clone(&self.blobs),
                http: self.http.clone(),
                inflight: self.inflight.clone(),
                updater: self.updater.clone(),
            };
            let member_repo = make_raw_repo_with_depth(ctx, &member_config, self.depth + 1);
            let result = member_repo.head(path).await;

            match result {
                Ok(Some(record)) => {
                    tracing::debug!(
                        proxy = %self.name,
                        member = member_name,
                        path,
                        "found artifact (HEAD)"
                    );
                    return Ok(Some(record));
                }
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(proxy = %self.name, member = member_name, path, error = %e, "error on HEAD from member, trying next");
                    continue;
                }
            }
        }
        Ok(None)
    }

    pub fn get<'b>(
        &'b self,
        path: &'b str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = error::Result<Option<ArtifactContent>>> + Send + 'b>,
    > {
        Box::pin(async move { self.get_inner(path).await })
    }

    #[tracing::instrument(level = "debug", skip(self), fields(repo = %self.name, path))]
    async fn get_inner(&self, path: &str) -> error::Result<Option<ArtifactContent>> {
        if self.depth > MAX_PROXY_DEPTH {
            tracing::warn!(
                proxy = %self.name,
                depth = self.depth,
                "proxy recursion depth exceeded, returning not found"
            );
            return Ok(None);
        }
        for member_name in &self.members {
            let member_config = service::get_repo(self.kv.as_ref(), member_name).await?;
            let member_config = match member_config {
                Some(c) => c,
                None => {
                    tracing::warn!(
                        proxy = %self.name,
                        member = member_name,
                        "member repo not found, skipping"
                    );
                    continue;
                }
            };

            let ctx = RepoContext {
                kv: Arc::clone(&self.kv),
                blobs: Arc::clone(&self.blobs),
                http: self.http.clone(),
                inflight: self.inflight.clone(),
                updater: self.updater.clone(),
            };
            let member_repo = make_raw_repo_with_depth(ctx, &member_config, self.depth + 1);
            let result = member_repo.get(path).await;

            match result {
                Ok(Some(content)) => {
                    tracing::debug!(
                        proxy = %self.name,
                        member = member_name,
                        path,
                        "found artifact"
                    );
                    return Ok(Some(content));
                }
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(proxy = %self.name, member = member_name, path, error = %e, "error fetching from member, trying next");
                    continue;
                }
            }
        }
        Ok(None)
    }

    /// List artifacts across all members (union of cached/local artifacts).
    pub async fn list(
        &self,
        prefix: &str,
        pagination: &Pagination,
    ) -> error::Result<PaginatedResult<(String, ArtifactRecord)>> {
        self.paginated_merge(pagination, MergeOp::List { prefix })
            .await
    }

    /// Search artifacts across all members.
    pub async fn search(
        &self,
        query: &str,
        pagination: &Pagination,
    ) -> error::Result<PaginatedResult<(String, ArtifactRecord)>> {
        self.paginated_merge(pagination, MergeOp::Search { query })
            .await
    }

    /// Shared merge-sort pagination for list and search operations.
    ///
    /// Uses a single path-based cursor shared across all members. Each member's
    /// items are sorted by path (the KV sort key), so a BTreeMap merge gives us
    /// globally sorted, deduplicated results with first-member-wins priority.
    async fn paginated_merge(
        &self,
        pagination: &Pagination,
        op: MergeOp<'_>,
    ) -> error::Result<PaginatedResult<(String, ArtifactRecord)>> {
        if self.members.is_empty() {
            return Ok(PaginatedResult {
                items: Vec::new(),
                next_cursor: None,
            });
        }

        // Every member receives the same cursor (a base64-encoded path).
        // Each member's list/search skips items at or before that path,
        // so previous-page items are never re-emitted — no cross-page dedup needed.
        let per_member_limit = pagination.limit + 1;

        // Fetch from each member. Members are iterated in priority order.
        let mut merged: BTreeMap<String, ArtifactRecord> = BTreeMap::new();
        for member_name in &self.members {
            let member_pagination = Pagination {
                cursor: pagination.cursor.clone(),
                offset: 0,
                limit: per_member_limit,
            };

            let result = match &op {
                MergeOp::List { prefix } => {
                    service::list_artifacts(
                        self.kv.as_ref(),
                        member_name,
                        prefix,
                        &member_pagination,
                    )
                    .await
                }
                MergeOp::Search { query } => {
                    service::search_artifacts(
                        self.kv.as_ref(),
                        member_name,
                        query,
                        &member_pagination,
                    )
                    .await
                }
            };

            match result {
                Ok(page) => {
                    for (path, record) in page.items {
                        // or_insert keeps the first insertion = highest-priority member.
                        merged.entry(path).or_insert(record);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        proxy = %self.name,
                        member = member_name,
                        error = %e,
                        "error fetching from member, skipping"
                    );
                }
            }
        }

        // Apply offset on first page, then collect up to limit+1 items.
        let skip = if pagination.cursor.is_none() {
            pagination.offset
        } else {
            0
        };
        let items: Vec<(String, ArtifactRecord)> = merged
            .into_iter()
            .skip(skip)
            .take(per_member_limit)
            .collect();

        Ok(paginate_results(items, pagination.limit))
    }

    /// List children across all members with dir aggregation.
    pub async fn list_children(
        &self,
        prefix: &str,
    ) -> error::Result<crate::store::kv::ChildrenResult> {
        let mut dir_map = std::collections::HashMap::<String, DirEntry>::new();
        let mut file_seen = std::collections::HashSet::new();
        let mut files = Vec::new();

        for member_name in &self.members {
            if let Ok((member_dirs, member_files)) =
                service::list_children(self.kv.as_ref(), member_name, prefix).await
            {
                for (name, de) in member_dirs {
                    dir_map
                        .entry(name)
                        .and_modify(|existing| {
                            existing.artifact_count += de.artifact_count;
                            existing.total_bytes += de.total_bytes;
                            existing.last_modified_at =
                                existing.last_modified_at.max(de.last_modified_at);
                            existing.last_accessed_at =
                                existing.last_accessed_at.max(de.last_accessed_at);
                        })
                        .or_insert(de);
                }
                for (name, record) in member_files {
                    if file_seen.insert(name.clone()) {
                        files.push((name, record));
                    }
                }
            }
        }

        let mut dirs: Vec<(String, DirEntry)> = dir_map.into_iter().collect();
        dirs.sort_by(|a, b| a.0.cmp(&b.0));
        files.sort_by(|a, b| a.0.cmp(&b.0));
        Ok((dirs, files))
    }
}

enum MergeOp<'a> {
    List { prefix: &'a str },
    Search { query: &'a str },
}

#[async_trait::async_trait]
impl RawRepo for ProxyRepo {
    async fn head(&self, path: &str) -> error::Result<Option<ArtifactRecord>> {
        self.head(path).await
    }

    async fn get(&self, path: &str) -> error::Result<Option<ArtifactContent>> {
        self.get(path).await
    }

    async fn search(
        &self,
        query: &str,
        pagination: &Pagination,
    ) -> error::Result<PaginatedResult<(String, ArtifactRecord)>> {
        self.search(query, pagination).await
    }

    async fn list_children(&self, prefix: &str) -> error::Result<crate::store::kv::ChildrenResult> {
        self.list_children(prefix).await
    }
}

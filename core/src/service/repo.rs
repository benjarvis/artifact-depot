// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Repository configuration CRUD.

use std::borrow::Cow;

use crate::error;
use crate::store::keys;
use crate::store::kv::*;

use super::scan::{ADMIN_ENTITY_CAP, SCAN_BATCH_SIZE, SHARD_CONCURRENCY};
use super::typed::{typed_get, typed_get_versioned, typed_list, typed_put, typed_put_if_absent};

pub async fn put_repo(kv: &dyn KvStore, config: &RepoConfig) -> error::Result<()> {
    let (table, pk, sk) = keys::repo_key(&config.name);
    typed_put(kv, table, pk, sk, config).await
}

/// Get a repo config, excluding repos marked for deletion.
#[tracing::instrument(level = "debug", name = "get_repo", skip(kv), fields(repo = name))]
pub async fn get_repo(kv: &dyn KvStore, name: &str) -> error::Result<Option<RepoConfig>> {
    match get_repo_raw(kv, name).await? {
        Some(config) if config.deleting => Ok(None),
        other => Ok(other),
    }
}

/// Get a repo config including repos marked for deletion.
/// Used by cleanup workers that need to drain deleting repos.
pub async fn get_repo_raw(kv: &dyn KvStore, name: &str) -> error::Result<Option<RepoConfig>> {
    let (table, pk, sk) = keys::repo_key(name);
    typed_get(kv, table, pk, sk).await
}

pub async fn list_repos(kv: &dyn KvStore) -> error::Result<Vec<RepoConfig>> {
    let repos: Vec<RepoConfig> = typed_list(
        kv,
        keys::TABLE_REPOS,
        Cow::Borrowed(keys::SINGLE_PK),
        Cow::Borrowed(""),
        ADMIN_ENTITY_CAP,
    )
    .await?;
    Ok(repos.into_iter().filter(|r| !r.deleting).collect())
}

/// List all repos including those marked for deletion. Used by cleanup workers.
pub async fn list_repos_raw(kv: &dyn KvStore) -> error::Result<Vec<RepoConfig>> {
    typed_list(
        kv,
        keys::TABLE_REPOS,
        Cow::Borrowed(keys::SINGLE_PK),
        Cow::Borrowed(""),
        ADMIN_ENTITY_CAP,
    )
    .await
}

/// Propagate a stale flag from `source_repo` to all ancestor proxy repos
/// of the given format using BFS. Handles nested proxies (proxy-of-proxy).
///
/// `stale_path` is the KV path for the stale flag artifact, e.g.
/// `"_apt/metadata_stale/stable"` or `"_yum/metadata_stale"`.
pub async fn propagate_staleness(
    kv: &dyn KvStore,
    source_repo: &str,
    format: ArtifactFormat,
    stale_path: &str,
) -> error::Result<()> {
    use std::collections::{HashMap, HashSet, VecDeque};

    let repos = list_repos(kv).await?;

    // Build reverse adjacency map: member name → parent proxy names.
    let mut member_to_proxies: HashMap<&str, Vec<&str>> = HashMap::new();
    for config in &repos {
        if config.format() != format {
            continue;
        }
        if let RepoKind::Proxy { ref members, .. } = config.kind {
            for member in members {
                member_to_proxies
                    .entry(member.as_str())
                    .or_default()
                    .push(&config.name);
            }
        }
    }

    // BFS from source_repo upward through proxy parents.
    let now = chrono::Utc::now();
    let flag = ArtifactRecord {
        schema_version: CURRENT_RECORD_VERSION,
        id: String::new(),
        size: 0,
        content_type: "text/plain".to_string(),
        created_at: now,
        updated_at: now,
        last_accessed_at: now,
        path: String::new(),
        blob_id: None,
        content_hash: None,
        etag: None,
        kind: stale_kind_for_format(format),
        internal: true,
    };

    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(source_repo);
    let max_depth: u8 = 8;
    let mut depth = 0u8;
    while !queue.is_empty() && depth < max_depth {
        let level_size = queue.len();
        for _ in 0..level_size {
            let Some(name) = queue.pop_front() else {
                break;
            };
            if let Some(parents) = member_to_proxies.get(name) {
                for &parent in parents {
                    if visited.insert(parent) {
                        let _ =
                            super::artifact::put_artifact_if_absent(kv, parent, stale_path, &flag)
                                .await;
                        queue.push_back(parent);
                    }
                }
            }
        }
        depth += 1;
    }

    Ok(())
}

/// Map an artifact format to the appropriate ArtifactKind for stale flags.
fn stale_kind_for_format(format: ArtifactFormat) -> ArtifactKind {
    match format {
        ArtifactFormat::Apt => ArtifactKind::AptMetadata,
        ArtifactFormat::Yum => ArtifactKind::YumMetadata,
        ArtifactFormat::Helm => ArtifactKind::HelmMetadata,
        ArtifactFormat::Npm => ArtifactKind::NpmMetadata,
        // PyPI and others use Raw for internal metadata artifacts.
        _ => ArtifactKind::Raw,
    }
}

/// Mark a repo for deletion by setting `deleting = true`.
/// Actual artifact cleanup is handled by the background cleanup worker.
pub async fn delete_repo(kv: &dyn KvStore, name: &str) -> error::Result<bool> {
    let (table, pk, sk) = keys::repo_key(name);
    let (mut config, version): (RepoConfig, u64) =
        match typed_get_versioned(kv, table, pk.clone(), sk.clone()).await? {
            Some(v) => v,
            None => return Ok(false),
        };
    if config.deleting {
        // Already marked for deletion.
        return Ok(true);
    }
    config.deleting = true;
    let new_bytes = rmp_serde::to_vec(&config)?;
    // Use conditional write — if someone else modified the repo config concurrently,
    // this will fail and the caller can retry.
    kv.put_if_version(table, pk, sk, &new_bytes, Some(version))
        .await
}

/// Result of a clone operation with copy counts.
pub struct CloneStats {
    pub copied_artifacts: u64,
    pub copied_entries: u64,
}

/// Clone a repository: create a new repo config and copy all artifacts
/// and directory entries from the source repo to the new name.
/// Returns `Ok(Some(stats))` on success, `Ok(None)` if `new_name` was
/// already taken (conflict).
///
/// When `progress_tx` is provided, progress is reported via
/// `total_artifacts` / `checked_artifacts` on `TaskProgress`.
/// When `cancel` is provided, the operation can be cancelled mid-copy.
pub async fn clone_repo(
    kv: &dyn KvStore,
    source: &RepoConfig,
    new_name: &str,
    total_artifacts: u64,
    cancel: Option<&tokio_util::sync::CancellationToken>,
    progress_tx: Option<&tokio::sync::watch::Sender<crate::task::TaskProgress>>,
) -> error::Result<Option<CloneStats>> {
    use futures::stream::{self, StreamExt, TryStreamExt};
    use std::sync::atomic::{AtomicU64, Ordering};

    // Step 1: Create new repo config (reserves the name atomically).
    let mut new_config = source.clone();
    new_config.name = new_name.to_string();
    new_config.created_at = chrono::Utc::now();
    new_config.deleting = false;
    let (table, pk, sk) = keys::repo_key(new_name);
    if !typed_put_if_absent(kv, table, pk, sk, &new_config).await? {
        return Ok(None); // Name already taken.
    }

    if let Some(tx) = progress_tx {
        tx.send_modify(|p| {
            p.total_artifacts = total_artifacts as usize;
            p.phase = "Copying artifacts".to_string();
        });
    }

    // Step 2: Copy all artifacts across all 256 shards in parallel.
    let artifact_counter = AtomicU64::new(0);
    stream::iter(0..keys::NUM_SHARDS)
        .map(|shard| {
            let counter = &artifact_counter;
            async move {
                if cancel.is_some_and(|c| c.is_cancelled()) {
                    return Ok(());
                }
                let src_pk = keys::artifact_shard_pk(&source.name, shard);
                let dst_pk = keys::artifact_shard_pk(new_name, shard);
                copy_partition(kv, keys::TABLE_ARTIFACTS, &src_pk, &dst_pk, |n| {
                    let total = counter.fetch_add(n, Ordering::Relaxed) + n;
                    if let Some(tx) = progress_tx {
                        tx.send_modify(|p| {
                            p.checked_artifacts = total as usize;
                        });
                    }
                })
                .await
            }
        })
        .buffer_unordered(SHARD_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    if cancel.is_some_and(|c| c.is_cancelled()) {
        return Err(error::DepotError::Internal("Clone cancelled".to_string()));
    }

    if let Some(tx) = progress_tx {
        tx.send_modify(|p| {
            p.phase = "Copying directory entries".to_string();
        });
    }

    // Step 3: Copy all directory entries across all 256 shards in parallel.
    let entry_counter = AtomicU64::new(0);
    stream::iter(0..keys::NUM_SHARDS)
        .map(|shard| {
            let counter = &entry_counter;
            async move {
                if cancel.is_some_and(|c| c.is_cancelled()) {
                    return Ok(());
                }
                let src_pk = keys::tree_shard_pk(&source.name, shard);
                let dst_pk = keys::tree_shard_pk(new_name, shard);
                copy_partition(kv, keys::TABLE_DIR_ENTRIES, &src_pk, &dst_pk, |n| {
                    counter.fetch_add(n, Ordering::Relaxed);
                })
                .await
            }
        })
        .buffer_unordered(SHARD_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    Ok(Some(CloneStats {
        copied_artifacts: artifact_counter.load(Ordering::Relaxed),
        copied_entries: entry_counter.load(Ordering::Relaxed),
    }))
}

/// Copy all entries in one partition to a new partition key (same sort keys
/// and values). Calls `on_batch(count)` after each batch write for progress.
async fn copy_partition(
    kv: &dyn KvStore,
    table: &str,
    src_pk: &str,
    dst_pk: &str,
    mut on_batch: impl FnMut(u64),
) -> error::Result<()> {
    let mut cursor: Option<String> = None;
    loop {
        let start = match &cursor {
            Some(c) => Cow::Owned(c.clone()),
            None => Cow::Borrowed(""),
        };
        let batch = kv
            .scan_range(table, Cow::Borrowed(src_pk), start, None, SCAN_BATCH_SIZE)
            .await?;
        if batch.items.is_empty() {
            break;
        }
        let count = batch.items.len() as u64;
        let entries: Vec<(&str, &str, &[u8])> = batch
            .items
            .iter()
            .map(|(sk, v)| (dst_pk, sk.as_str(), v.as_slice()))
            .collect();
        kv.put_batch(table, &entries).await?;
        on_batch(count);
        if batch.done {
            break;
        }
        // Advance cursor past the last key seen.
        if let Some((last_sk, _)) = batch.items.last() {
            let mut next = last_sk.clone();
            next.push('\0');
            cursor = Some(next);
        }
        tokio::task::yield_now().await;
    }
    Ok(())
}

/// Drain all artifacts and dir entries for a repo marked as deleting,
/// then delete the repo config. Called by the background cleanup worker.
pub async fn drain_deleting_repo(kv: &dyn KvStore, name: &str) -> error::Result<()> {
    use futures::stream::{self, StreamExt, TryStreamExt};

    // Delete all artifacts across all shards in parallel.
    stream::iter(0..keys::NUM_SHARDS)
        .map(|shard| async move {
            let pk = keys::artifact_shard_pk(name, shard);
            loop {
                let result = kv
                    .scan_prefix(
                        keys::TABLE_ARTIFACTS,
                        Cow::Borrowed(pk.as_str()),
                        Cow::Borrowed(""),
                        SCAN_BATCH_SIZE,
                    )
                    .await?;
                if result.items.is_empty() {
                    break;
                }
                let del_keys: Vec<(&str, &str)> = result
                    .items
                    .iter()
                    .map(|(sk, _)| (pk.as_str(), sk.as_str()))
                    .collect();
                kv.delete_batch(keys::TABLE_ARTIFACTS, &del_keys).await?;
            }
            Ok::<_, error::DepotError>(())
        })
        .buffer_unordered(SHARD_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    // Delete all dir entries across all shards in parallel.
    stream::iter(0..keys::NUM_SHARDS)
        .map(|shard| async move {
            let pk = keys::tree_shard_pk(name, shard);
            loop {
                let result = kv
                    .scan_prefix(
                        keys::TABLE_DIR_ENTRIES,
                        Cow::Borrowed(pk.as_str()),
                        Cow::Borrowed(""),
                        SCAN_BATCH_SIZE,
                    )
                    .await?;
                if result.items.is_empty() {
                    break;
                }
                let del_keys: Vec<(&str, &str)> = result
                    .items
                    .iter()
                    .map(|(sk, _)| (pk.as_str(), sk.as_str()))
                    .collect();
                kv.delete_batch(keys::TABLE_DIR_ENTRIES, &del_keys).await?;
            }
            Ok::<_, error::DepotError>(())
        })
        .buffer_unordered(SHARD_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    // Delete the repo config itself.
    let (table, pk, sk) = keys::repo_key(name);
    kv.delete(table, pk, sk).await?;
    Ok(())
}

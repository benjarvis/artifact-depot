// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Directory browsing and repository stats.

use crate::error;
use crate::store::keys;
use crate::store::kv::*;

use super::scan::{for_each_in_partition, SCAN_BATCH_SIZE};

/// Get directory stats via a single point read in the tree.
pub async fn get_dir(
    kv: &dyn KvStore,
    repo: &str,
    dir_path: &str,
) -> error::Result<Option<DirEntry>> {
    let clean = dir_path.trim_matches('/');
    let (_, pk, sk) = keys::tree_entry_key(repo, clean);
    match kv.get(keys::TABLE_DIR_ENTRIES, pk, sk).await? {
        Some(bytes) => {
            let te: TreeEntry = rmp_serde::from_slice(&bytes)?;
            match te.kind {
                TreeEntryKind::Dir {
                    artifact_count,
                    total_bytes,
                } => Ok(Some(DirEntry {
                    schema_version: CURRENT_RECORD_VERSION,
                    artifact_count,
                    total_bytes,
                    last_modified_at: te.last_modified_at,
                    last_accessed_at: te.last_accessed_at,
                })),
                TreeEntryKind::File { .. } => Ok(None),
            }
        }
        None => Ok(None),
    }
}

/// List immediate children (directories and files) under a directory.
///
/// Scans a single partition in the tree table — all children of `prefix`
/// share the same partition key, so no fan-out across shards is needed.
pub async fn list_children(
    kv: &dyn KvStore,
    repo: &str,
    prefix: &str,
) -> error::Result<ChildrenResult> {
    let clean = prefix.trim_end_matches('/');
    let pk = keys::tree_children_pk(repo, clean);
    let sk_prefix = keys::tree_children_sk_prefix(clean);

    let mut dirs: Vec<(String, DirEntry)> = Vec::new();
    let mut files: Vec<(String, TreeEntry)> = Vec::new();

    for_each_in_partition(
        kv,
        keys::TABLE_DIR_ENTRIES,
        &pk,
        &sk_prefix,
        SCAN_BATCH_SIZE,
        |sk, value| {
            let name = keys::tree_sk_name(sk);
            let te: TreeEntry = rmp_serde::from_slice(value)?;
            match &te.kind {
                TreeEntryKind::Dir {
                    artifact_count,
                    total_bytes,
                } => {
                    dirs.push((
                        name.to_string(),
                        DirEntry {
                            schema_version: CURRENT_RECORD_VERSION,
                            artifact_count: *artifact_count,
                            total_bytes: *total_bytes,
                            last_modified_at: te.last_modified_at,
                            last_accessed_at: te.last_accessed_at,
                        },
                    ));
                }
                TreeEntryKind::File { .. } => {
                    files.push((name.to_string(), te));
                }
            }
            Ok(true)
        },
    )
    .await?;

    dirs.sort_by(|a, b| a.0.cmp(&b.0));
    files.sort_by(|a, b| a.0.cmp(&b.0));

    Ok((dirs, files))
}

pub async fn get_repo_stats(kv: &dyn KvStore, config: &RepoConfig) -> error::Result<RepoStats> {
    match &config.kind {
        RepoKind::Proxy { members, .. } => {
            let mut total = RepoStats::default();
            let mut visited = std::collections::HashSet::new();
            aggregate_proxy_stats(kv, members, &mut total, &mut visited).await?;
            Ok(total)
        }
        _ => match get_dir(kv, &config.name, "").await? {
            Some(de) => Ok(RepoStats {
                artifact_count: de.artifact_count,
                total_bytes: de.total_bytes,
            }),
            None => Ok(RepoStats::default()),
        },
    }
}

/// Recursively aggregate root stats from proxy members, deduplicating by repo
/// name to handle cycles and diamond topologies.
async fn aggregate_proxy_stats(
    kv: &dyn KvStore,
    members: &[String],
    total: &mut RepoStats,
    visited: &mut std::collections::HashSet<String>,
) -> error::Result<()> {
    for member_name in members {
        if !visited.insert(member_name.clone()) {
            continue;
        }
        if let Some(member_config) = super::get_repo(kv, member_name).await? {
            match &member_config.kind {
                RepoKind::Proxy {
                    members: nested, ..
                } => {
                    Box::pin(aggregate_proxy_stats(kv, nested, total, visited)).await?;
                }
                _ => {
                    if let Some(de) = get_dir(kv, member_name, "").await? {
                        total.artifact_count += de.artifact_count;
                        total.total_bytes += de.total_bytes;
                    }
                }
            }
        }
    }
    Ok(())
}

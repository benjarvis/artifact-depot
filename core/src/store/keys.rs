// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Table/partition/sort-key encoding for the partitioned KV store.
//!
//! Large tables use hash-based partition sharding (256 shards) to distribute
//! data evenly.  Small administrative tables use a single empty partition key.

use std::borrow::Cow;
use std::fmt::Write;

// --- Table name constants ---

pub const TABLE_REPOS: &str = "repos";
pub const TABLE_ARTIFACTS: &str = "artifacts";
pub const TABLE_BLOBS: &str = "blobs";
pub const TABLE_USERS: &str = "users";
pub const TABLE_ROLES: &str = "roles";
pub const TABLE_META: &str = "meta";
pub const TABLE_STORES: &str = "stores";
pub const TABLE_DIR_ENTRIES: &str = "dir_entries";
pub const TABLE_UPLOAD_SESSIONS: &str = "upload_sessions";
pub const TABLE_LEASES: &str = "leases";
pub const TABLE_TASKS: &str = "tasks";
pub const TABLE_TASK_EVENTS: &str = "task_events";

/// All known table names, used by backends to pre-create tables on startup.
pub const ALL_TABLES: &[&str] = &[
    TABLE_REPOS,
    TABLE_ARTIFACTS,
    TABLE_BLOBS,
    TABLE_USERS,
    TABLE_ROLES,
    TABLE_META,
    TABLE_STORES,
    TABLE_DIR_ENTRIES,
    TABLE_UPLOAD_SESSIONS,
    TABLE_LEASES,
    TABLE_TASKS,
    TABLE_TASK_EVENTS,
];

// --- Key triple type ---

/// `(table, partition_key, sort_key)` — borrows where possible to avoid
/// allocations on hot paths.
pub type KeyTriple<'a> = (&'static str, Cow<'a, str>, Cow<'a, str>);

// --- Shard helpers ---

pub const NUM_SHARDS: u16 = 256;

/// Compute shard index from sort key (first 2 bytes of blake3 hash).
pub fn shard_of(sk: &str) -> u16 {
    let h = blake3::hash(sk.as_bytes());
    let bytes = h.as_bytes();
    u16::from_be_bytes([bytes[0], bytes[1]]) % NUM_SHARDS
}

/// Build a sharded partition key: `"{scope}/{shard}"`.
pub fn sharded_pk(scope: &str, sk: &str) -> String {
    let shard = shard_of(sk);
    let mut buf = String::with_capacity(scope.len() + 4);
    let _ = write!(buf, "{}/{}", scope, shard);
    buf
}

/// Generate all 256 partition keys for a scope (for fan-out scans).
pub fn all_sharded_pks(scope: &str) -> Vec<String> {
    (0..NUM_SHARDS)
        .map(|s| {
            let mut buf = String::with_capacity(scope.len() + 4);
            let _ = write!(buf, "{}/{}", scope, s);
            buf
        })
        .collect()
}

/// Partition key for unsharded tables (single partition, like shard 0).
pub const SINGLE_PK: &str = "0";

// --- Sharded key constructors ---

/// Artifact key: sharded by repo, SK = normalized path.
pub fn artifact_key(repo: &str, path: &str) -> KeyTriple<'static> {
    let clean = normalize_path(path);
    (
        TABLE_ARTIFACTS,
        Cow::Owned(sharded_pk(repo, &clean)),
        Cow::Owned(clean.into_owned()),
    )
}

/// Artifact prefix for scanning all artifacts in a specific shard of a repo.
pub fn artifact_shard_pk(repo: &str, shard: u16) -> String {
    let mut buf = String::with_capacity(repo.len() + 4);
    let _ = write!(buf, "{}/{}", repo, shard);
    buf
}

/// Blob key: sharded by store, SK = hash.
pub fn blob_key<'a>(store: &str, hash: &'a str) -> KeyTriple<'a> {
    (
        TABLE_BLOBS,
        Cow::Owned(sharded_pk(store, hash)),
        Cow::Borrowed(hash),
    )
}

/// Blob shard PK for scanning all blobs in a specific shard.
pub fn blob_shard_pk(store: &str, shard: u16) -> String {
    let mut buf = String::with_capacity(store.len() + 4);
    let _ = write!(buf, "{}/{}", store, shard);
    buf
}

// --- Path normalization ---

/// Normalize an artifact/directory path: collapse runs of `/`, strip
/// leading and trailing `/`.  Returns a `Cow::Borrowed` when the input
/// is already clean (common case) to avoid allocation.
pub fn normalize_path(path: &str) -> Cow<'_, str> {
    let trimmed = path.trim_matches('/');
    if !trimmed.contains("//") {
        return Cow::Borrowed(trimmed);
    }
    // Slow path: collapse double slashes.
    let mut out = String::with_capacity(trimmed.len());
    let mut prev_slash = false;
    for ch in trimmed.chars() {
        if ch == '/' {
            if !prev_slash {
                out.push('/');
            }
            prev_slash = true;
        } else {
            out.push(ch);
            prev_slash = false;
        }
    }
    Cow::Owned(out)
}

// --- Tree (directory) key constructors ---

/// Sentinel parent scope for the repo root entry.
const ROOT_PARENT: &str = "_root";

/// Separator between the parent path and name in tree sort keys.
/// Disambiguates entries from different parents that hash to the same shard.
const TREE_SK_SEP: char = '\0';

/// Return the parent scope for a canonical path.
/// Non-root paths use `parent_dir(path)`; root (`""`) uses the `_root` sentinel.
pub fn tree_parent(path: &str) -> &str {
    if path.is_empty() {
        ROOT_PARENT
    } else {
        match path.rfind('/') {
            Some(pos) => path.split_at(pos).0,
            None => "",
        }
    }
}

/// Return the name segment of a canonical path.
/// `"foo/bar"` → `"bar"`, `"foo"` → `"foo"`, `""` → `""`.
pub fn tree_name(path: &str) -> &str {
    if path.is_empty() {
        ""
    } else {
        path.rsplit('/').next().unwrap_or(path)
    }
}

/// Build the composite sort key for a tree entry: `"{parent}\0{name}"`.
///
/// Including the parent path in the SK ensures entries from different
/// directories that happen to hash to the same shard are distinguishable
/// via prefix scan.
pub fn tree_sort_key(path: &str) -> String {
    let parent = tree_parent(path);
    let name = tree_name(path);
    let mut sk = String::with_capacity(parent.len() + 1 + name.len());
    sk.push_str(parent);
    sk.push(TREE_SK_SEP);
    sk.push_str(name);
    sk
}

/// Extract the entry name from a composite sort key `"{parent}\0{name}"`.
pub fn tree_sk_name(sk: &str) -> &str {
    // TREE_SK_SEP is '\0' which is a single-byte UTF-8 character,
    // so splitting at the byte boundary is always safe.
    match sk.rsplit_once(TREE_SK_SEP) {
        Some((_, name)) => name,
        None => sk, // legacy key without separator
    }
}

/// Key for a single tree entry at `path`.
/// PK = `"{repo}/{shard_of(parent)}"`, SK = `"{parent}\0{name}"`.
/// The path is normalized (leading/trailing/double slashes collapsed).
pub fn tree_entry_key(repo: &str, path: &str) -> KeyTriple<'static> {
    let clean = normalize_path(path);
    (
        TABLE_DIR_ENTRIES,
        Cow::Owned(sharded_pk(repo, tree_parent(&clean))),
        Cow::Owned(tree_sort_key(&clean)),
    )
}

/// PK for scanning all immediate children of `parent_path`.
pub fn tree_children_pk(repo: &str, parent_path: &str) -> String {
    sharded_pk(repo, parent_path)
}

/// SK prefix for scanning immediate children of `parent_path`.
/// Returns `"{parent_path}\0"` so `scan_prefix` only matches entries
/// whose parent is exactly `parent_path`.
pub fn tree_children_sk_prefix(parent_path: &str) -> String {
    let mut prefix = String::with_capacity(parent_path.len() + 1);
    prefix.push_str(parent_path);
    prefix.push(TREE_SK_SEP);
    prefix
}

/// Tree shard PK for scanning entries in a specific shard (used by full_scan cleanup).
pub fn tree_shard_pk(repo: &str, shard: u16) -> String {
    let mut buf = String::with_capacity(repo.len() + 4);
    let _ = write!(buf, "{}/{}", repo, shard);
    buf
}

// --- Unsharded key constructors ---

/// Repo config key.
pub fn repo_key(name: &str) -> KeyTriple<'_> {
    (TABLE_REPOS, Cow::Borrowed(SINGLE_PK), Cow::Borrowed(name))
}

/// User record key.
pub fn user_key(username: &str) -> KeyTriple<'_> {
    (
        TABLE_USERS,
        Cow::Borrowed(SINGLE_PK),
        Cow::Borrowed(username),
    )
}

/// Role record key.
pub fn role_key(name: &str) -> KeyTriple<'_> {
    (TABLE_ROLES, Cow::Borrowed(SINGLE_PK), Cow::Borrowed(name))
}

/// Metadata key.
pub fn meta_key(key: &str) -> KeyTriple<'_> {
    (TABLE_META, Cow::Borrowed(SINGLE_PK), Cow::Borrowed(key))
}

/// Store config key.
pub fn store_key(name: &str) -> KeyTriple<'_> {
    (TABLE_STORES, Cow::Borrowed(SINGLE_PK), Cow::Borrowed(name))
}

/// Store stats key (blob count / total bytes).
pub fn store_stats_key(name: &str) -> KeyTriple<'static> {
    (
        TABLE_META,
        Cow::Borrowed(SINGLE_PK),
        Cow::Owned(format!("store_stats/{}", name)),
    )
}

/// Upload session key.
pub fn upload_session_key(uuid: &str) -> KeyTriple<'_> {
    (
        TABLE_UPLOAD_SESSIONS,
        Cow::Borrowed(SINGLE_PK),
        Cow::Borrowed(uuid),
    )
}

/// Lease key.
pub fn lease_key(name: &str) -> KeyTriple<'_> {
    (TABLE_LEASES, Cow::Borrowed(SINGLE_PK), Cow::Borrowed(name))
}

/// Task summary key: PK = SINGLE_PK, SK = task UUID. Unsharded (low volume).
pub fn task_key(id: &str) -> KeyTriple<'_> {
    (TABLE_TASKS, Cow::Borrowed(SINGLE_PK), Cow::Borrowed(id))
}

/// Task event key: PK = task UUID, SK = zero-padded 20-digit seq.
/// Per-task partition, append-only with single writer (the task owner).
pub fn task_event_key(task_id: &str, seq: u64) -> KeyTriple<'static> {
    (
        TABLE_TASK_EVENTS,
        Cow::Owned(task_id.to_string()),
        Cow::Owned(format!("{:020}", seq)),
    )
}

/// Partition key for all events belonging to a task.
pub fn task_event_pk(task_id: &str) -> String {
    task_id.to_string()
}

// --- Prefix range helpers ---

/// Compute the exclusive upper bound for a byte-prefix range scan.
/// Returns `None` if the prefix is empty or all-0xFF.
pub fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bytes = prefix.to_vec();
    while let Some(last) = bytes.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(bytes);
        }
        bytes.pop();
    }
    None
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_deterministic() {
        let s1 = shard_of("hello");
        let s2 = shard_of("hello");
        assert_eq!(s1, s2);
        assert!(s1 < NUM_SHARDS);
    }

    #[test]
    fn shard_distributes() {
        // Different inputs should (usually) produce different shards
        let s1 = shard_of("path/a.txt");
        let s2 = shard_of("path/b.txt");
        // Not guaranteed to differ, but at least both are valid
        assert!(s1 < NUM_SHARDS);
        assert!(s2 < NUM_SHARDS);
    }

    #[test]
    fn sharded_pk_format() {
        let pk = sharded_pk("my-repo", "some/path");
        assert!(pk.starts_with("my-repo/"));
        let shard: u16 = pk.strip_prefix("my-repo/").unwrap().parse().unwrap();
        assert!(shard < NUM_SHARDS);
    }

    #[test]
    fn all_sharded_pks_count() {
        let pks = all_sharded_pks("repo");
        assert_eq!(pks.len(), NUM_SHARDS as usize);
    }

    #[test]
    fn artifact_key_structure() {
        let (table, pk, sk) = artifact_key("repo", "path/file.txt");
        assert_eq!(table, TABLE_ARTIFACTS);
        assert_eq!(&*sk, "path/file.txt");
        assert!(pk.starts_with("repo/"));
    }

    #[test]
    fn blob_key_structure() {
        let (table, pk, sk) = blob_key("default", "abc123hash");
        assert_eq!(table, TABLE_BLOBS);
        assert_eq!(&*sk, "abc123hash");
        assert!(pk.starts_with("default/"));
    }

    #[test]
    fn unsharded_key_structure() {
        let (table, pk, sk) = repo_key("my-repo");
        assert_eq!(table, TABLE_REPOS);
        assert_eq!(&*pk, SINGLE_PK);
        assert_eq!(&*sk, "my-repo");
    }

    #[test]
    fn user_key_structure() {
        let (table, pk, sk) = user_key("admin");
        assert_eq!(table, TABLE_USERS);
        assert_eq!(&*pk, SINGLE_PK);
        assert_eq!(&*sk, "admin");
    }

    #[test]
    fn tree_parent_nested() {
        assert_eq!(tree_parent("a/b/c"), "a/b");
        assert_eq!(tree_parent("a/b"), "a");
        assert_eq!(tree_parent("a"), "");
        assert_eq!(tree_parent(""), "_root");
    }

    #[test]
    fn tree_name_segments() {
        assert_eq!(tree_name("a/b/c"), "c");
        assert_eq!(tree_name("a/b"), "b");
        assert_eq!(tree_name("a"), "a");
        assert_eq!(tree_name(""), "");
    }

    #[test]
    fn tree_entry_key_structure() {
        let (table, pk, sk) = tree_entry_key("repo", "foo/bar");
        assert_eq!(table, TABLE_DIR_ENTRIES);
        // PK is sharded by parent "foo"
        assert!(pk.starts_with("repo/"));
        // SK is composite: "foo\0bar"
        assert_eq!(&*sk, "foo\0bar");
        assert_eq!(tree_sk_name(&sk), "bar");
    }

    #[test]
    fn tree_entry_key_root() {
        let (table, pk, sk) = tree_entry_key("repo", "");
        assert_eq!(table, TABLE_DIR_ENTRIES);
        // Root entry uses _root sentinel as parent
        assert!(pk.starts_with("repo/"));
        assert_eq!(&*sk, "_root\0");
        assert_eq!(tree_sk_name(&sk), "");
    }

    #[test]
    fn tree_children_pk_consistency() {
        // Children of "foo" should be in same partition as tree_entry_key for "foo/X"
        let children_pk = tree_children_pk("repo", "foo");
        let (_, entry_pk, _) = tree_entry_key("repo", "foo/anything");
        assert_eq!(children_pk, *entry_pk);
    }

    #[test]
    fn tree_children_sk_prefix_filters() {
        // SK prefix for "foo" should match "foo\0X" but not "bar\0X"
        let prefix = tree_children_sk_prefix("foo");
        assert_eq!(prefix, "foo\0");
        let (_, _, sk_child) = tree_entry_key("repo", "foo/bar");
        assert!(sk_child.starts_with(&prefix));
        // Entry from different parent in same shard should NOT match
        let (_, _, sk_other) = tree_entry_key("repo", "baz/bar");
        assert!(!sk_other.starts_with(&prefix));
    }

    #[test]
    fn normalize_path_clean() {
        assert_eq!(normalize_path("a/b/c"), "a/b/c");
        assert_eq!(normalize_path("file.txt"), "file.txt");
        assert_eq!(normalize_path(""), "");
    }

    #[test]
    fn normalize_path_slashes() {
        assert_eq!(normalize_path("//a//b///c//"), "a/b/c");
        assert_eq!(normalize_path("/a/b/"), "a/b");
        assert_eq!(normalize_path("a//b"), "a/b");
    }
}

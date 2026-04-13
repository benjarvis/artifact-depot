// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::metadata::*;

/// Current schema version stamped on every new record.
pub const CURRENT_RECORD_VERSION: u32 = 5;

fn default_io_size() -> usize {
    1024
}

fn default_s3_max_retries() -> u32 {
    3
}

fn default_s3_connect_timeout_secs() -> u64 {
    3
}

fn default_s3_read_timeout_secs() -> u64 {
    0
}

fn default_s3_retry_mode() -> String {
    "standard".to_string()
}

/// The physical storage backend type for a blob store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StoreKind {
    File {
        root: String,
        sync: bool,
        /// Read/write buffer size in KiB (default 1024 = 1 MiB).
        #[serde(default = "default_io_size")]
        io_size: usize,
        /// Use O_DIRECT for reads and writes (bypasses page cache).
        #[serde(default)]
        direct_io: bool,
    },
    S3 {
        bucket: String,
        endpoint: Option<String>,
        region: String,
        prefix: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        /// Maximum number of retry attempts for failed S3 requests.
        #[serde(default = "default_s3_max_retries")]
        max_retries: u32,
        /// TCP connection timeout in seconds.
        #[serde(default = "default_s3_connect_timeout_secs")]
        connect_timeout_secs: u64,
        /// Per-request read timeout in seconds.
        #[serde(default = "default_s3_read_timeout_secs")]
        read_timeout_secs: u64,
        /// Retry strategy: "standard" (jittered backoff) or "adaptive"
        /// (adds client-side rate limiting).
        #[serde(default = "default_s3_retry_mode")]
        retry_mode: String,
    },
}

impl StoreKind {
    /// A short label for display ("file" or "s3").
    pub fn type_name(&self) -> &'static str {
        match self {
            StoreKind::File { .. } => "file",
            StoreKind::S3 { .. } => "s3",
        }
    }
}

/// A named blob store configuration persisted in the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreRecord {
    #[serde(default)]
    pub schema_version: u32,
    pub name: String,
    pub kind: StoreKind,
    pub created_at: DateTime<Utc>,
}

/// Aggregate blob-count and byte-total for a single blob store.
///
/// Written by GC after sweeping each store; initialised to zero on creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreStatsRecord {
    pub blob_count: u64,
    pub total_bytes: u64,
    pub updated_at: DateTime<Utc>,
}

/// Metadata record for a stored artifact (keyed by repo+path).
///
/// Access tracking (`last_accessed_at`) lives on the `TreeEntry::File` in the
/// browse tree, not here — this avoids a versioned RMW on every read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub schema_version: u32,
    pub id: String,
    pub size: u64,
    pub content_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Legacy field kept for backward-compatible deserialization of existing
    /// records. New writes set this to `updated_at`. The authoritative access
    /// time lives on the tree file entry.
    #[serde(default = "DateTime::default")]
    pub last_accessed_at: DateTime<Utc>,
    pub path: String,
    pub kind: ArtifactKind,
    /// Internal artifacts are excluded from cleanup.
    #[serde(default)]
    pub internal: bool,
    /// Blob store UUID key (present for blob-backed artifacts).
    pub blob_id: Option<String>,
    /// BLAKE3 content hash (present for blob-backed artifacts).
    pub content_hash: Option<String>,
    /// HTTP ETag value. For hosted uploads this equals the BLAKE3 hash.
    /// For cache artifacts this may be the upstream ETag or a synthesized value.
    /// V2 records lack this field; use `effective_etag()` for fallback logic.
    #[serde(default)]
    pub etag: Option<String>,
}

impl ArtifactRecord {
    /// The effective ETag for HTTP caching.
    /// V3+ records have an explicit `etag` field.
    /// V2 records fall back to `content_hash`, then `blob_id`.
    pub fn effective_etag(&self) -> Option<&str> {
        self.etag
            .as_deref()
            .filter(|e| !e.is_empty())
            .or_else(|| self.content_hash.as_deref().filter(|h| !h.is_empty()))
            .or_else(|| self.blob_id.as_deref().filter(|b| !b.is_empty()))
    }
}

/// Format-specific data for an artifact record.
///
/// Common fields (blob_id, content_hash) live on `ArtifactRecord` itself.
/// This enum carries only format-specific metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ArtifactKind {
    Raw,
    DockerBlob { docker_digest: String },
    DockerManifest { docker_digest: String },
    DockerTag { digest: String, tag: String },
    AptPackage { apt: AptPackageMeta },
    AptMetadata,
    PypiPackage { pypi: PypiPackageMeta },
    GoModule { golang: GoModuleMeta },
    HelmChart { helm: HelmChartMeta },
    HelmMetadata,
    CargoCrate { cargo: CrateVersionMeta },
    YumPackage { rpm: RpmPackageMeta },
    YumMetadata,
    NpmPackage { npm: NpmPackageMeta },
    NpmMetadata,
}

/// Directory entry metadata (aggregate stats for a directory node).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntry {
    #[serde(default)]
    pub schema_version: u32,
    pub artifact_count: u64,
    pub total_bytes: u64,
    pub last_modified_at: DateTime<Utc>,
    pub last_accessed_at: DateTime<Utc>,
}

/// A tree entry stored in the directory tree table.
///
/// Each entry is keyed by `(parent_dir_hash, child_name)` so that listing
/// children of any directory is a single-partition scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeEntry {
    pub kind: TreeEntryKind,
    pub last_modified_at: DateTime<Utc>,
    pub last_accessed_at: DateTime<Utc>,
}

/// Discriminant for directory vs file entries in the tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TreeEntryKind {
    Dir {
        artifact_count: u64,
        total_bytes: u64,
    },
    File {
        size: u64,
        content_type: String,
        artifact_kind: Box<ArtifactKind>,
        #[serde(alias = "blob_hash")]
        blob_id: Option<String>,
        #[serde(default)]
        content_hash: Option<String>,
        created_at: DateTime<Utc>,
    },
}

/// Result type for `list_children`: (directories, files).
pub type ChildrenResult = (Vec<(String, DirEntry)>, Vec<(String, TreeEntry)>);

/// Per-repository artifact count and total bytes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RepoStats {
    pub artifact_count: u64,
    pub total_bytes: u64,
}

/// Record tracking a content-addressed blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobRecord {
    #[serde(default)]
    pub schema_version: u32,
    /// UUID — the BlobStore key.
    pub blob_id: String,
    /// BLAKE3 hash — KV key for dedup lookup.
    #[serde(alias = "hash")]
    pub hash: String,
    pub size: u64,
    pub created_at: DateTime<Utc>,
    /// Which named store this blob lives in.
    pub store: String,
}

/// Record tracking an in-progress Docker chunked upload session.
///
/// Persisted to KV so that upload state survives server restarts and is
/// accessible from any node in a scale-out deployment. Each PATCH chunk
/// is stored as an ordinary blob; the IDs are tracked here and assembled
/// into the final blob at PUT (finalize) time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadSessionRecord {
    pub uuid: String,
    pub repo: String,
    pub image: Option<String>,
    /// Final blob ID (used for pending blob tracking and the assembled result).
    pub blob_id: String,
    pub store: String,
    pub size: u64,
    /// Ordered list of chunk blob UUIDs, assembled at finalize time.
    pub chunk_blob_ids: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A distributed lease record for leader election.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseRecord {
    /// Instance ID of the current holder.
    pub holder: String,
    /// When the lease was last acquired or renewed.
    pub renewed_at: DateTime<Utc>,
    /// Lease duration in seconds. The lease is considered expired when
    /// `renewed_at + ttl_secs < now`.
    pub ttl_secs: u64,
}

/// Persisted task summary: one record per task, written by the owning
/// instance. The `state_scanner` on every instance polls the set of
/// `TaskRecord`s in KV to present cluster-wide task state to UI clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    #[serde(default)]
    pub schema_version: u32,
    pub id: String,
    pub kind: crate::task::TaskKind,
    pub status: crate::task::TaskStatus,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
    /// Bumped every time the record is rewritten — lets the scanner detect
    /// progress without comparing every field.
    #[serde(default)]
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub progress: crate::task::TaskProgress,
    #[serde(default)]
    pub result: Option<crate::task::TaskResult>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub summary: String,
    /// UUID of the instance running the task. Used by the cleanup sweep for
    /// orphan detection when the owning instance dies mid-task.
    #[serde(default)]
    pub owner_instance_id: String,
    /// Set when a user has initiated deletion. The cleanup sweep drains
    /// associated events and then removes this record.
    #[serde(default)]
    pub deleting: bool,
    /// Monotonic count of appended log events. Lets the scanner fetch only
    /// newly-appended events via range scan on `TABLE_TASK_EVENTS`.
    #[serde(default)]
    pub event_seq: u64,
}

/// Append-only task log entry: one record per log line.
/// PK = task UUID; SK = zero-padded 20-digit seq so lexicographic ordering
/// matches numeric ordering for efficient range scans.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEventRecord {
    #[serde(default)]
    pub schema_version: u32,
    pub task_id: String,
    pub seq: u64,
    pub timestamp: DateTime<Utc>,
    pub message: String,
}

/// Credentials for authenticating to an upstream server.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct UpstreamAuth {
    pub username: String,
    pub password: Option<String>,
}

/// Type-specific configuration for a repository.
///
/// Encodes type-specific fields so the compiler enforces valid combinations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RepoKind {
    Hosted,
    Cache {
        upstream_url: String,
        #[serde(default)]
        cache_ttl_secs: u64,
        #[serde(default)]
        upstream_auth: Option<UpstreamAuth>,
    },
    Proxy {
        members: Vec<String>,
        #[serde(default)]
        write_member: Option<String>,
    },
}

impl RepoKind {
    pub fn repo_type(&self) -> RepoType {
        match self {
            RepoKind::Hosted => RepoType::Hosted,
            RepoKind::Cache { .. } => RepoType::Cache,
            RepoKind::Proxy { .. } => RepoType::Proxy,
        }
    }
}

/// Controls the `Content-Disposition` header when serving raw artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum ContentDisposition {
    Attachment,
    Inline,
}

/// Format-specific repository configuration.
///
/// Encodes per-format fields so the compiler enforces valid combinations,
/// following the same pattern as [`RepoKind`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FormatConfig {
    Raw {
        /// Controls the Content-Disposition header when serving artifacts.
        /// V3 records will deserialize with `None`, treated as `Attachment`.
        #[serde(default)]
        content_disposition: Option<ContentDisposition>,
    },
    Docker {
        /// Optional dedicated listen address (e.g. "0.0.0.0:5000").
        /// When set, depot spawns a dedicated port where image names map
        /// directly to this repo without requiring a repo prefix in the path.
        #[serde(default)]
        listen: Option<String>,
        /// Periodically delete manifests not referenced by any tag.
        #[serde(default)]
        cleanup_untagged_manifests: Option<bool>,
    },
    #[serde(rename = "pypi")]
    PyPI {},
    Apt {
        /// Retained for backward-compatible deserialization of existing KV
        /// records. Not used by any code path.
        #[serde(default, skip_serializing)]
        #[allow(dead_code)]
        apt_components: Option<Vec<String>>,
    },
    #[serde(rename = "golang")]
    Golang {},
    Helm {},
    Cargo {},
    Yum {
        /// Number of directory levels at which repodata is generated.
        /// V4 records lack this field; accessor defaults to 0 (root).
        #[serde(default)]
        repodata_depth: Option<u32>,
    },
    #[serde(rename = "npm")]
    Npm {},
}

impl FormatConfig {
    /// Return the [`ArtifactFormat`] discriminant for this config.
    pub fn artifact_format(&self) -> ArtifactFormat {
        match self {
            FormatConfig::Raw { .. } => ArtifactFormat::Raw,
            FormatConfig::Docker { .. } => ArtifactFormat::Docker,
            FormatConfig::PyPI {} => ArtifactFormat::PyPI,
            FormatConfig::Apt { .. } => ArtifactFormat::Apt,
            FormatConfig::Golang {} => ArtifactFormat::Golang,
            FormatConfig::Helm {} => ArtifactFormat::Helm,
            FormatConfig::Cargo {} => ArtifactFormat::Cargo,
            FormatConfig::Yum { .. } => ArtifactFormat::Yum,
            FormatConfig::Npm {} => ArtifactFormat::Npm,
        }
    }

    /// Docker dedicated listen address, if any.
    pub fn listen(&self) -> Option<&str> {
        match self {
            FormatConfig::Docker { listen, .. } => listen.as_deref(),
            _ => None,
        }
    }

    /// Whether Docker untagged-manifest cleanup is enabled.
    pub fn cleanup_untagged_manifests(&self) -> bool {
        matches!(
            self,
            FormatConfig::Docker {
                cleanup_untagged_manifests: Some(true),
                ..
            }
        )
    }

    /// Repodata directory depth for Yum repos.
    /// V5+ records have an explicit field; older records default to 0 (root).
    pub fn repodata_depth(&self) -> u32 {
        match self {
            FormatConfig::Yum { repodata_depth } => repodata_depth.unwrap_or(0),
            _ => 0,
        }
    }

    /// Content-Disposition policy for raw artifact downloads.
    /// V4+ records have an explicit field; older records default to Attachment.
    pub fn content_disposition(&self) -> ContentDisposition {
        match self {
            FormatConfig::Raw {
                content_disposition,
            } => content_disposition.unwrap_or(ContentDisposition::Attachment),
            _ => ContentDisposition::Attachment,
        }
    }
}

/// Repository configuration stored in the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoConfig {
    #[serde(default)]
    pub schema_version: u32,
    pub name: String,
    pub kind: RepoKind,
    pub format_config: FormatConfig,
    /// Which named blob store this repo uses.
    pub store: String,
    pub created_at: DateTime<Utc>,
    /// Cleanup: delete artifacts not accessed within this many days.
    #[serde(default)]
    pub cleanup_max_unaccessed_days: Option<u64>,
    /// Cleanup: delete artifacts older than this many days.
    #[serde(default)]
    pub cleanup_max_age_days: Option<u64>,
    /// When true, the repo is marked for deletion. Reads return 404;
    /// background cleanup drains artifacts before removing the config.
    #[serde(default)]
    pub deleting: bool,
}

impl RepoConfig {
    /// Convenience accessor for the repo type discriminant.
    pub fn repo_type(&self) -> RepoType {
        self.kind.repo_type()
    }

    /// Convenience accessor for the artifact format discriminant.
    pub fn format(&self) -> ArtifactFormat {
        self.format_config.artifact_format()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum RepoType {
    Hosted,
    Cache,
    Proxy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum ArtifactFormat {
    Raw,
    Docker,
    #[serde(rename = "pypi")]
    PyPI,
    Apt,
    #[serde(rename = "golang")]
    Golang,
    Helm,
    Cargo,
    Yum,
    #[serde(rename = "npm")]
    Npm,
}

impl std::fmt::Display for RepoType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RepoType::Hosted => write!(f, "hosted"),
            RepoType::Cache => write!(f, "cache"),
            RepoType::Proxy => write!(f, "proxy"),
        }
    }
}

impl std::fmt::Display for ArtifactFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArtifactFormat::Raw => write!(f, "raw"),
            ArtifactFormat::Docker => write!(f, "docker"),
            ArtifactFormat::PyPI => write!(f, "pypi"),
            ArtifactFormat::Apt => write!(f, "apt"),
            ArtifactFormat::Golang => write!(f, "golang"),
            ArtifactFormat::Helm => write!(f, "helm"),
            ArtifactFormat::Cargo => write!(f, "cargo"),
            ArtifactFormat::Yum => write!(f, "yum"),
            ArtifactFormat::Npm => write!(f, "npm"),
        }
    }
}

/// Default auth source for backward-compatible deserialization.
fn default_auth_source() -> String {
    "builtin".to_string()
}

/// A user account record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRecord {
    #[serde(default)]
    pub schema_version: u32,
    pub username: String,
    pub password_hash: String,
    #[serde(default)]
    pub roles: Vec<String>,
    /// Where this user authenticates: "builtin" (Argon2 in KvStore) or "ldap".
    #[serde(default = "default_auth_source")]
    pub auth_source: String,
    /// Per-user secret used in JWT key derivation. Rotating this revokes the
    /// user's outstanding tokens. Empty for legacy records (treated as `&[]`).
    #[serde(default)]
    pub token_secret: Vec<u8>,
    /// When true the user must change their password on next login.
    #[serde(default)]
    pub must_change_password: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A permission capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum Capability {
    Read,
    Write,
    Delete,
}

/// A capability granted on a specific repo (or `"*"` for all repos).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CapabilityGrant {
    pub capability: Capability,
    pub repo: String,
}

/// A named role with a set of capability grants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleRecord {
    #[serde(default)]
    pub schema_version: u32,
    pub name: String,
    pub description: String,
    pub capabilities: Vec<CapabilityGrant>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Pagination parameters for list/search operations.
#[derive(Debug, Clone)]
pub struct Pagination {
    pub cursor: Option<String>,
    pub offset: usize,
    pub limit: usize,
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            cursor: None,
            offset: 0,
            limit: 1000,
        }
    }
}

/// Result of a paginated query.
#[derive(Debug, Clone, Serialize)]
pub struct PaginatedResult<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

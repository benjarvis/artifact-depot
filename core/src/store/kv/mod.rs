// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

mod metadata;
mod records;
mod trait_def;

pub use metadata::{
    AptPackageMeta, CrateDep, CrateVersionMeta, GoModuleMeta, HelmChartMeta, NpmPackageMeta,
    PypiPackageMeta, PypiProjectIndex, PypiProjectLink, RpmChangelog, RpmDep, RpmFileEntry,
    RpmPackageMeta,
};
pub use records::{
    ArtifactFormat, ArtifactKind, ArtifactRecord, BlobRecord, Capability, CapabilityGrant,
    ChildrenResult, ContentDisposition, DirEntry, FormatConfig, LeaseRecord, PaginatedResult,
    Pagination, RepoConfig, RepoKind, RepoStats, RepoType, RoleRecord, StoreKind, StoreRecord,
    StoreStatsRecord, TaskEventRecord, TaskRecord, TreeEntry, TreeEntryKind, UploadSessionRecord,
    UpstreamAuth, UserRecord, CURRENT_RECORD_VERSION,
};
pub use trait_def::{KvStore, ScanResult};

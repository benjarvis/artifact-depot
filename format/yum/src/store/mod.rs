// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! YUM (RPM) package storage logic.
//!
//! Maps YUM concepts onto our generic KV + blob store:
//!
//! - RPM packages stored at `Packages/{directory}/{filename}` (user-specified directory)
//! - `repodata/repomd.xml` at mutable path (single commit point)
//! - Content-addressed files at `repodata/{sha256}-{type}.xml.gz` (immutable)
//! - `repomd.xml.asc` generated on demand from signing key
//! - Stale flag at `_yum/metadata_stale` triggers rebuild on next metadata fetch
//! - GPG signing key at `_yum/signing_key`, public key at `_yum/public_key`

mod metadata;
mod parse;
#[allow(clippy::module_inception)]
mod store;
mod synthetic;

pub use metadata::{
    generate_repomd_xml, gzip_compress, merge_filelists_xml, merge_other_xml, merge_primary_xml,
    parse_repomd_locations, RepoDataEntry,
};
pub use parse::parse_rpm;
pub use store::YumStore;
pub use synthetic::build_synthetic_rpm;

/// Hex-encode helper.
pub(crate) mod hex {
    pub fn encode(data: impl AsRef<[u8]>) -> String {
        data.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[cfg(test)]
mod tests;

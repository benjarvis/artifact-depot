// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Filesystem-backed blob store with optional O_DIRECT I/O.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]

pub mod direct_io;
pub mod file_blob;

pub use file_blob::FileBlobStore;

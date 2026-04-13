// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PyPI package format handler — both repository logic and HTTP routes.

pub mod api;
pub mod store;

pub use store::{
    extract_version_from_filename, fetch_upstream_project_page, fetch_upstream_response,
    guess_filetype_pub, normalize_name, package_path, parse_simple_html, PackageLink, PypiStore,
    PypiUploadMeta,
};

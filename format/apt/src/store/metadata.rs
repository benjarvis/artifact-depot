// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use depot_core::store::kv::{ArtifactKind, ArtifactRecord};

/// An entry in the Release file's SHA256 checksums section.
pub struct ReleaseFileEntry {
    pub path: String,
    pub sha256: String,
    pub size: u64,
}

impl ReleaseFileEntry {
    pub fn new(path: String, sha256: String, size: u64) -> Self {
        Self { path, sha256, size }
    }
}

/// Extract the plain Release text from an InRelease (clearsigned PGP message).
///
/// If the text is a PGP clearsigned message, strips the header, signature block,
/// and undoes dash-escaping. If it's not PGP-wrapped (unsigned), returns as-is.
pub fn extract_release_from_inrelease(inrelease: &str) -> String {
    if !inrelease.starts_with("-----BEGIN PGP SIGNED MESSAGE-----") {
        return inrelease.to_string();
    }
    let mut result = String::new();
    let mut in_body = false;
    for line in inrelease.lines() {
        if !in_body {
            // Skip the PGP header; body starts after the first blank line.
            if line.is_empty() {
                in_body = true;
            }
            continue;
        }
        if line.starts_with("-----BEGIN PGP SIGNATURE-----") {
            break;
        }
        // Undo dash-escaping: "- " prefix is removed.
        let unescaped = line.strip_prefix("- ").unwrap_or(line);
        result.push_str(unescaped);
        result.push('\n');
    }
    result
}

/// Parse the SHA256 section of a Release file, returning (rel_path, sha256, size) tuples.
pub fn parse_release_sha256(release_text: &str) -> Vec<(String, String, u64)> {
    let mut entries = Vec::new();
    let mut in_sha256 = false;
    for line in release_text.lines() {
        if line.starts_with("SHA256:") {
            in_sha256 = true;
            continue;
        }
        if in_sha256 {
            if !line.starts_with(' ') {
                break;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if let (Some(sha), Some(sz), Some(p)) = (parts.first(), parts.get(1), parts.get(2)) {
                let sha256 = (*sha).to_string();
                let size = sz.parse::<u64>().unwrap_or(0);
                let path = (*p).to_string();
                entries.push((path, sha256, size));
            }
        }
    }
    entries
}

/// Adjust Suite and Codename in a Release file to a new distribution name.
pub(super) fn adjust_release_suite(release_text: &str, new_name: &str) -> String {
    let mut result = String::new();
    for line in release_text.lines() {
        if line.starts_with("Suite:") {
            result.push_str(&format!("Suite: {new_name}\n"));
        } else if line.starts_with("Codename:") {
            result.push_str(&format!("Codename: {new_name}\n"));
        } else {
            result.push_str(line);
            result.push('\n');
        }
    }
    result
}

/// Generate a Packages stanza for one `.deb` artifact.
pub(super) fn generate_packages_stanza(
    record: &ArtifactRecord,
    pool_path: &str,
    _distribution: &str,
) -> String {
    let (
        package,
        version,
        arch,
        maintainer,
        depends,
        section,
        priority,
        installed_size,
        description,
        sha256,
    ) = match &record.kind {
        ArtifactKind::AptPackage { apt, .. } => (
            apt.package.as_str(),
            apt.version.as_str(),
            apt.architecture.as_str(),
            apt.maintainer.as_str(),
            apt.depends.as_str(),
            apt.section.as_str(),
            apt.priority.as_str(),
            apt.installed_size.as_str(),
            apt.description.as_str(),
            apt.sha256.as_str(),
        ),
        _ => (
            "unknown", "0.0.0", "amd64", "", "", "misc", "optional", "0", "", "",
        ),
    };

    // The Filename field points to the pool path relative to the repo root.
    // Handle both new "pool/" and legacy "_pool/" prefixes.
    let filename = if let Some(rest) = pool_path.strip_prefix("_pool/") {
        format!("pool/{rest}")
    } else {
        pool_path.to_string()
    };

    let mut stanza = String::new();
    stanza.push_str(&format!("Package: {package}\n"));
    stanza.push_str(&format!("Version: {version}\n"));
    stanza.push_str(&format!("Architecture: {arch}\n"));
    if !maintainer.is_empty() {
        stanza.push_str(&format!("Maintainer: {maintainer}\n"));
    }
    if !depends.is_empty() {
        stanza.push_str(&format!("Depends: {depends}\n"));
    }
    if !section.is_empty() {
        stanza.push_str(&format!("Section: {section}\n"));
    }
    stanza.push_str(&format!("Priority: {priority}\n"));
    if installed_size != "0" && !installed_size.is_empty() {
        stanza.push_str(&format!("Installed-Size: {installed_size}\n"));
    }
    stanza.push_str(&format!("Filename: {filename}\n"));
    stanza.push_str(&format!("Size: {}\n", record.size));
    stanza.push_str(&format!("SHA256: {sha256}\n"));
    if !description.is_empty() {
        stanza.push_str(&format!("Description: {description}\n"));
    }

    stanza
}

/// Generate a Release file with checksums of all Packages files.
pub fn generate_release(
    distribution: &str,
    components: &[String],
    architectures: &[String],
    checksums: &[ReleaseFileEntry],
) -> String {
    let date = chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S UTC");
    let comp_str = components.join(" ");
    let arch_str = architectures.join(" ");

    let mut release = String::new();
    release.push_str(&format!("Suite: {distribution}\n"));
    release.push_str(&format!("Codename: {distribution}\n"));
    release.push_str(&format!("Components: {comp_str}\n"));
    release.push_str(&format!("Architectures: {arch_str}\n"));
    release.push_str(&format!("Date: {date}\n"));
    release.push_str("Acquire-By-Hash: yes\n");
    release.push_str("SHA256:\n");

    for entry in checksums {
        release.push_str(&format!(
            " {} {:>16} {}\n",
            entry.sha256, entry.size, entry.path
        ));
    }

    release
}

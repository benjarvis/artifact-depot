// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use depot_core::store::kv::{ArtifactKind, ArtifactRecord, RpmDep};

/// Gzip compress data.
pub fn gzip_compress(data: &[u8]) -> Vec<u8> {
    use flate2::write::GzEncoder;
    use flate2::Compression;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    // Writing to an in-memory Vec<u8> via GzEncoder cannot fail.
    let _ = std::io::Write::write_all(&mut encoder, data);
    encoder.finish().unwrap_or_default()
}

/// Escape XML special characters.
pub(super) fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Generate primary.xml content.
///
/// `repodata_prefix` is the directory prefix for the repodata group (e.g. `"p1/7/x86_64"`).
/// Location hrefs are made relative to this prefix so that a client using
/// `baseurl=.../p1/7/x86_64/` can resolve packages correctly. When the prefix is empty
/// (depth=0), hrefs are relative to the repo root — matching the previous behavior.
pub(super) fn generate_primary_xml(
    packages: &[(String, ArtifactRecord)],
    repodata_prefix: &str,
) -> String {
    let mut xml = String::new();
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str(&format!(
        "<metadata xmlns=\"http://linux.duke.edu/metadata/common\" xmlns:rpm=\"http://linux.duke.edu/metadata/rpm\" packages=\"{}\">\n",
        packages.len()
    ));

    for (path, record) in packages {
        let rpm = match &record.kind {
            ArtifactKind::YumPackage { rpm, .. } => rpm,
            _ => continue,
        };

        // Strip the repodata prefix so hrefs are relative to the base URL.
        let location_href = if !repodata_prefix.is_empty() {
            path.strip_prefix(repodata_prefix)
                .unwrap_or(path)
                .trim_start_matches('/')
                .to_string()
        } else {
            path.to_string()
        };

        xml.push_str("<package type=\"rpm\">\n");
        xml.push_str(&format!("  <name>{}</name>\n", xml_escape(&rpm.name)));
        xml.push_str(&format!("  <arch>{}</arch>\n", xml_escape(&rpm.arch)));
        xml.push_str(&format!(
            "  <version epoch=\"{}\" ver=\"{}\" rel=\"{}\"/>\n",
            rpm.epoch,
            xml_escape(&rpm.version),
            xml_escape(&rpm.release)
        ));
        xml.push_str(&format!(
            "  <checksum type=\"sha256\" pkgid=\"YES\">{}</checksum>\n",
            rpm.sha256
        ));
        xml.push_str(&format!(
            "  <summary>{}</summary>\n",
            xml_escape(&rpm.summary)
        ));
        xml.push_str(&format!(
            "  <description>{}</description>\n",
            xml_escape(&rpm.description)
        ));
        xml.push_str(&format!(
            "  <packager>{}</packager>\n",
            xml_escape(&rpm.packager)
        ));
        xml.push_str(&format!("  <url>{}</url>\n", xml_escape(&rpm.url)));
        xml.push_str(&format!(
            "  <time file=\"{}\" build=\"{}\"/>\n",
            record.created_at.timestamp(),
            rpm.buildtime
        ));
        xml.push_str(&format!(
            "  <size package=\"{}\" installed=\"{}\" archive=\"{}\"/>\n",
            record.size, rpm.installed_size, rpm.archive_size
        ));
        xml.push_str(&format!(
            "  <location href=\"{}\"/>\n",
            xml_escape(&location_href)
        ));
        xml.push_str("  <format>\n");
        xml.push_str(&format!(
            "    <rpm:license>{}</rpm:license>\n",
            xml_escape(&rpm.license)
        ));
        xml.push_str(&format!(
            "    <rpm:vendor>{}</rpm:vendor>\n",
            xml_escape(&rpm.vendor)
        ));
        xml.push_str(&format!(
            "    <rpm:group>{}</rpm:group>\n",
            xml_escape(&rpm.group)
        ));
        xml.push_str(&format!(
            "    <rpm:buildhost>{}</rpm:buildhost>\n",
            xml_escape(&rpm.buildhost)
        ));

        // Dependencies
        write_deps_xml(&mut xml, "rpm:requires", &rpm.requires);
        write_deps_xml(&mut xml, "rpm:provides", &rpm.provides);
        write_deps_xml(&mut xml, "rpm:conflicts", &rpm.conflicts);
        write_deps_xml(&mut xml, "rpm:obsoletes", &rpm.obsoletes);

        xml.push_str("  </format>\n");
        xml.push_str("</package>\n");
    }

    xml.push_str("</metadata>\n");
    xml
}

fn write_deps_xml(xml: &mut String, tag: &str, deps: &[RpmDep]) {
    if deps.is_empty() {
        return;
    }
    xml.push_str(&format!("    <{tag}>\n"));
    for dep in deps {
        if dep.flags.is_empty() {
            xml.push_str(&format!(
                "      <rpm:entry name=\"{}\"/>\n",
                xml_escape(&dep.name)
            ));
        } else {
            xml.push_str(&format!(
                "      <rpm:entry name=\"{}\" flags=\"{}\" epoch=\"{}\" ver=\"{}\" rel=\"{}\"/>\n",
                xml_escape(&dep.name),
                xml_escape(&dep.flags),
                xml_escape(&dep.epoch),
                xml_escape(&dep.ver),
                xml_escape(&dep.rel),
            ));
        }
    }
    xml.push_str(&format!("    </{tag}>\n"));
}

/// Generate filelists.xml content (minimal — package identity only).
pub(super) fn generate_filelists_xml(packages: &[(String, ArtifactRecord)]) -> String {
    let mut xml = String::new();
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str(&format!(
        "<filelists xmlns=\"http://linux.duke.edu/metadata/filelists\" packages=\"{}\">\n",
        packages.len()
    ));

    for (_path, record) in packages {
        let rpm = match &record.kind {
            ArtifactKind::YumPackage { rpm, .. } => rpm,
            _ => continue,
        };
        xml.push_str(&format!(
            "<package pkgid=\"{}\" name=\"{}\" arch=\"{}\">\n",
            rpm.sha256,
            xml_escape(&rpm.name),
            xml_escape(&rpm.arch)
        ));
        xml.push_str(&format!(
            "  <version epoch=\"{}\" ver=\"{}\" rel=\"{}\"/>\n",
            rpm.epoch,
            xml_escape(&rpm.version),
            xml_escape(&rpm.release)
        ));
        for file in &rpm.files {
            match file.file_type.as_str() {
                "dir" => xml.push_str(&format!(
                    "  <file type=\"dir\">{}</file>\n",
                    xml_escape(&file.path)
                )),
                "ghost" => xml.push_str(&format!(
                    "  <file type=\"ghost\">{}</file>\n",
                    xml_escape(&file.path)
                )),
                _ => xml.push_str(&format!("  <file>{}</file>\n", xml_escape(&file.path))),
            }
        }
        xml.push_str("</package>\n");
    }

    xml.push_str("</filelists>\n");
    xml
}

/// Generate other.xml content (minimal — package identity only).
pub(super) fn generate_other_xml(packages: &[(String, ArtifactRecord)]) -> String {
    let mut xml = String::new();
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str(&format!(
        "<otherdata xmlns=\"http://linux.duke.edu/metadata/other\" packages=\"{}\">\n",
        packages.len()
    ));

    for (_path, record) in packages {
        let rpm = match &record.kind {
            ArtifactKind::YumPackage { rpm, .. } => rpm,
            _ => continue,
        };
        xml.push_str(&format!(
            "<package pkgid=\"{}\" name=\"{}\" arch=\"{}\">\n",
            rpm.sha256,
            xml_escape(&rpm.name),
            xml_escape(&rpm.arch)
        ));
        xml.push_str(&format!(
            "  <version epoch=\"{}\" ver=\"{}\" rel=\"{}\"/>\n",
            rpm.epoch,
            xml_escape(&rpm.version),
            xml_escape(&rpm.release)
        ));
        for cl in &rpm.changelogs {
            xml.push_str(&format!(
                "  <changelog author=\"{}\" date=\"{}\">{}</changelog>\n",
                xml_escape(&cl.author),
                cl.timestamp,
                xml_escape(&cl.text)
            ));
        }
        xml.push_str("</package>\n");
    }

    xml.push_str("</otherdata>\n");
    xml
}

/// Parse repomd.xml to extract the `<location href="..."/>` values.
/// Returns paths like `"repodata/{sha256}-primary.xml.gz"`.
pub fn parse_repomd_locations(repomd_xml: &str) -> Vec<String> {
    let mut locations = Vec::new();
    for line in repomd_xml.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("<location href=\"") {
            if let Some(href) = rest.strip_suffix("\"/>") {
                locations.push(href.to_string());
            }
        }
    }
    locations
}

/// Merge multiple primary.xml documents by extracting and combining `<package>` elements.
pub fn merge_primary_xml(members: &[String]) -> String {
    let mut packages = Vec::new();
    for xml in members {
        packages.extend(extract_xml_elements(xml, "<package ", "</package>"));
    }
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str(&format!(
        "<metadata xmlns=\"http://linux.duke.edu/metadata/common\" xmlns:rpm=\"http://linux.duke.edu/metadata/rpm\" packages=\"{}\">\n",
        packages.len()
    ));
    for pkg in &packages {
        out.push_str(pkg);
        out.push('\n');
    }
    out.push_str("</metadata>\n");
    out
}

/// Merge multiple filelists.xml documents.
pub fn merge_filelists_xml(members: &[String]) -> String {
    let mut packages = Vec::new();
    for xml in members {
        packages.extend(extract_xml_elements(xml, "<package ", "</package>"));
    }
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str(&format!(
        "<filelists xmlns=\"http://linux.duke.edu/metadata/filelists\" packages=\"{}\">\n",
        packages.len()
    ));
    for pkg in &packages {
        out.push_str(pkg);
        out.push('\n');
    }
    out.push_str("</filelists>\n");
    out
}

/// Merge multiple other.xml documents.
pub fn merge_other_xml(members: &[String]) -> String {
    let mut packages = Vec::new();
    for xml in members {
        packages.extend(extract_xml_elements(xml, "<package ", "</package>"));
    }
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str(&format!(
        "<otherdata xmlns=\"http://linux.duke.edu/metadata/other\" packages=\"{}\">\n",
        packages.len()
    ));
    for pkg in &packages {
        out.push_str(pkg);
        out.push('\n');
    }
    out.push_str("</otherdata>\n");
    out
}

/// Extract XML elements that start with `open_tag` prefix and end with `close_tag`.
fn extract_xml_elements(xml: &str, open_prefix: &str, close_tag: &str) -> Vec<String> {
    let mut elements = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(open_prefix) {
        let after_start = rest.get(start..).unwrap_or("");
        let close_with_newline = format!("{close_tag}\n");
        let end_offset = if let Some(pos) = after_start.find(&close_with_newline) {
            pos + close_with_newline.len()
        } else if let Some(pos) = after_start.find(close_tag) {
            pos + close_tag.len()
        } else {
            break;
        };
        if let Some(element) = after_start.get(..end_offset) {
            elements.push(element.to_string());
        }
        rest = after_start.get(end_offset..).unwrap_or("");
    }
    elements
}

/// Metadata for a single repodata entry (primary, filelists, or other).
pub struct RepoDataEntry<'a> {
    pub filename: &'a str,
    pub checksum: &'a str,
    pub size: u64,
    pub open_checksum: &'a str,
    pub open_size: u64,
}

/// Generate repomd.xml content.
pub fn generate_repomd_xml(
    revision: i64,
    primary: &RepoDataEntry<'_>,
    filelists: &RepoDataEntry<'_>,
    other: &RepoDataEntry<'_>,
    timestamp: i64,
) -> String {
    let mut xml = String::new();
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<repomd xmlns=\"http://linux.duke.edu/metadata/repo\">\n");
    xml.push_str(&format!("  <revision>{revision}</revision>\n"));

    for (data_type, entry) in [
        ("primary", primary),
        ("filelists", filelists),
        ("other", other),
    ] {
        xml.push_str(&format!("  <data type=\"{data_type}\">\n"));
        xml.push_str(&format!(
            "    <checksum type=\"sha256\">{}</checksum>\n",
            entry.checksum
        ));
        xml.push_str(&format!(
            "    <open-checksum type=\"sha256\">{}</open-checksum>\n",
            entry.open_checksum
        ));
        xml.push_str(&format!(
            "    <location href=\"repodata/{}\"/>\n",
            entry.filename
        ));
        xml.push_str(&format!("    <timestamp>{timestamp}</timestamp>\n"));
        xml.push_str(&format!("    <size>{}</size>\n", entry.size));
        xml.push_str(&format!("    <open-size>{}</open-size>\n", entry.open_size));
        xml.push_str("  </data>\n");
    }

    xml.push_str("</repomd>\n");
    xml
}

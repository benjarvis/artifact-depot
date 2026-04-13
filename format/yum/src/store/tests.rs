// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use super::metadata::{generate_repomd_xml, xml_escape, RepoDataEntry};
use super::parse::parse_evr;
use super::*;

#[test]
fn test_build_and_parse_synthetic_rpm() {
    let rpm_data =
        build_synthetic_rpm("test-pkg", "1.0.0", "1", "x86_64", "A test package").unwrap();
    let meta = parse_rpm(
        std::io::Cursor::new(&rpm_data),
        "test",
        rpm_data.len() as u64,
    )
    .unwrap();
    assert_eq!(meta.name, "test-pkg");
    assert_eq!(meta.version, "1.0.0");
    assert_eq!(meta.release, "1");
}

#[test]
fn test_xml_escape() {
    assert_eq!(xml_escape("<hello>"), "&lt;hello&gt;");
    assert_eq!(xml_escape("a&b"), "a&amp;b");
    assert_eq!(xml_escape("normal"), "normal");
}

#[test]
fn test_parse_evr() {
    let (e, v, r) = parse_evr("1:2.0-3");
    assert_eq!(e, "1");
    assert_eq!(v, "2.0");
    assert_eq!(r, "3");

    let (e, v, r) = parse_evr("2.0-3");
    assert_eq!(e, "0");
    assert_eq!(v, "2.0");
    assert_eq!(r, "3");

    let (e, v, r) = parse_evr("2.0");
    assert_eq!(e, "0");
    assert_eq!(v, "2.0");
    assert_eq!(r, "");
}

#[test]
fn test_generate_repomd_xml() {
    let xml = generate_repomd_xml(
        1000,
        &RepoDataEntry {
            filename: "abc-primary.xml.gz",
            checksum: "abc",
            size: 100,
            open_checksum: "def",
            open_size: 200,
        },
        &RepoDataEntry {
            filename: "ghi-filelists.xml.gz",
            checksum: "ghi",
            size: 50,
            open_checksum: "jkl",
            open_size: 150,
        },
        &RepoDataEntry {
            filename: "mno-other.xml.gz",
            checksum: "mno",
            size: 30,
            open_checksum: "pqr",
            open_size: 80,
        },
        1000,
    );
    assert!(xml.contains("<revision>1000</revision>"));
    assert!(xml.contains("type=\"primary\""));
    assert!(xml.contains("type=\"filelists\""));
    assert!(xml.contains("type=\"other\""));
    assert!(xml.contains("abc-primary.xml.gz"));
}

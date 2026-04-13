// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use super::metadata::{generate_release, ReleaseFileEntry};
use super::parse::{parse_control_file, pool_path, pool_prefix};
use super::synthetic::build_synthetic_deb;
use super::*;

#[test]
fn test_pool_prefix() {
    assert_eq!(pool_prefix("apt"), "a");
    assert_eq!(pool_prefix("zlib"), "z");
    assert_eq!(pool_prefix("libc6"), "libc");
    assert_eq!(pool_prefix("libssl"), "libs");
}

#[test]
fn test_pool_path() {
    assert_eq!(
        pool_path("main", "hello", "hello_1.0_amd64.deb"),
        "pool/main/h/hello/hello_1.0_amd64.deb"
    );
    assert_eq!(
        pool_path("main", "libssl", "libssl_3.0_amd64.deb"),
        "pool/main/libs/libssl/libssl_3.0_amd64.deb"
    );
}

#[test]
fn test_parse_control_file() {
    let control = "\
Package: hello
Version: 2.10-1
Architecture: amd64
Maintainer: Test <test@test>
Depends: libc6
Section: devel
Priority: optional
Installed-Size: 280
Description: A test package
 Long description line.
";
    let parsed = parse_control_file(control).unwrap();
    assert_eq!(parsed.package, "hello");
    assert_eq!(parsed.version, "2.10-1");
    assert_eq!(parsed.architecture, "amd64");
    assert_eq!(parsed.maintainer, "Test <test@test>");
    assert_eq!(parsed.depends, "libc6");
    assert_eq!(parsed.section, "devel");
    assert_eq!(parsed.priority, "optional");
    assert_eq!(parsed.installed_size, "280");
}

#[test]
fn test_build_and_parse_synthetic_deb() {
    let deb = build_synthetic_deb("test-pkg", "1.0.0", "amd64", "A test package").unwrap();
    let control = parse_deb(std::io::Cursor::new(&deb)).unwrap();
    assert_eq!(control.package, "test-pkg");
    assert_eq!(control.version, "1.0.0");
    assert_eq!(control.architecture, "amd64");
}

#[test]
fn test_generate_release() {
    let checksums = vec![
        ReleaseFileEntry {
            path: "main/binary-amd64/Packages".to_string(),
            sha256: "abc123".to_string(),
            size: 1234,
        },
        ReleaseFileEntry {
            path: "main/binary-amd64/Packages.gz".to_string(),
            sha256: "def456".to_string(),
            size: 567,
        },
    ];
    let release = generate_release(
        "stable",
        &["main".to_string()],
        &["amd64".to_string()],
        &checksums,
    );
    assert!(release.contains("Suite: stable"));
    assert!(release.contains("Components: main"));
    assert!(release.contains("Architectures: amd64"));
    assert!(release.contains("abc123"));
    assert!(release.contains("main/binary-amd64/Packages"));
}

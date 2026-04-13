// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Cargo demo data seeding: create a Cargo repo and publish synthetic crates.

use anyhow::Result;

use crate::client::{CreateRepoRequest, DepotClient};

/// Seed demo Cargo data: create a hosted Cargo repo and publish synthetic crates.
pub async fn seed_cargo_demo(client: &DepotClient) -> Result<()> {
    let repo_name = "cargo-hosted";
    tracing::info!("Creating Cargo repo: {}", repo_name);

    let _ = client
        .create_repo(&CreateRepoRequest {
            name: repo_name.to_string(),
            repo_type: "hosted".to_string(),
            format: "cargo".to_string(),
            store: "default".to_string(),
            upstream_url: None,
            cache_ttl_secs: None,
            members: None,
            listen: None,
        })
        .await;

    let crates = [
        ("hello-world", &["0.1.0", "0.2.0", "1.0.0"][..]),
        ("my-utils", &["0.1.0", "0.1.1"][..]),
        ("demo-lib", &["1.0.0", "1.1.0", "2.0.0"][..]),
        ("test-crate", &["0.1.0"][..]),
        ("example-macro", &["0.1.0", "0.2.0"][..]),
    ];

    for (name, versions) in &crates {
        for version in *versions {
            let crate_data = build_minimal_crate(name, version);
            match publish_crate(client, repo_name, name, version, &crate_data).await {
                Ok(()) => {}
                Err(e) => tracing::warn!("Cargo publish {name}@{version} failed: {e}"),
            }
        }
        tracing::info!("  Published {} versions of {}", versions.len(), name);
    }

    Ok(())
}

/// Build a minimal .crate tarball (gzipped tar with Cargo.toml + src/lib.rs).
fn build_minimal_crate(name: &str, version: &str) -> Vec<u8> {
    use flate2::write::GzEncoder;
    use flate2::Compression;

    let prefix = format!("{name}-{version}");
    let cargo_toml = format!(
        r#"[package]
name = "{name}"
version = "{version}"
edition = "2021"
"#
    );
    let lib_rs = b"// auto-generated demo crate\n";

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    {
        let mut tar = tar::Builder::new(&mut encoder);

        // Add Cargo.toml
        let cargo_toml_bytes = cargo_toml.as_bytes();
        let mut header = tar::Header::new_gnu();
        header.set_path(format!("{prefix}/Cargo.toml")).unwrap();
        header.set_size(cargo_toml_bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append(&header, cargo_toml_bytes).unwrap();

        // Add src/lib.rs
        let mut header = tar::Header::new_gnu();
        header.set_path(format!("{prefix}/src/lib.rs")).unwrap();
        header.set_size(lib_rs.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append(&header, &lib_rs[..]).unwrap();

        tar.finish().unwrap();
    }
    encoder.finish().unwrap()
}

/// Publish a crate to the depot via the Cargo publish wire format.
async fn publish_crate(
    client: &DepotClient,
    repo: &str,
    name: &str,
    version: &str,
    crate_data: &[u8],
) -> Result<()> {
    let meta = serde_json::json!({
        "name": name,
        "vers": version,
        "deps": [],
        "features": {},
        "links": serde_json::Value::Null,
    });
    let meta_bytes = serde_json::to_vec(&meta)?;

    // Build binary wire format
    let mut body = Vec::new();
    body.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
    body.extend_from_slice(&meta_bytes);
    body.extend_from_slice(&(crate_data.len() as u32).to_le_bytes());
    body.extend_from_slice(crate_data);

    client
        .cargo_publish(repo, body)
        .await
        .map_err(|e| anyhow::anyhow!("publish failed: {e}"))?;

    Ok(())
}

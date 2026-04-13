// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! APT demo data seeding: create an APT repo and upload synthetic .deb packages.

use anyhow::Result;

use crate::client::DepotClient;
use depot_format_apt::build_synthetic_deb;

/// Seed demo APT data: create a hosted APT repo and upload synthetic .deb packages.
pub async fn seed_apt_demo(client: &DepotClient) -> Result<()> {
    use crate::client::CreateRepoRequest;

    let repo_name = "apt-packages";
    tracing::info!("Creating APT repo: {}", repo_name);

    let _ = client
        .create_repo(&CreateRepoRequest {
            name: repo_name.to_string(),
            repo_type: "hosted".to_string(),
            format: "apt".to_string(),
            store: "default".to_string(),
            upstream_url: None,
            cache_ttl_secs: None,
            members: None,
            listen: None,
        })
        .await;

    // Upload a few synthetic .deb packages
    let packages = [
        ("hello", "1.0.0", "amd64", "A hello world package"),
        ("goodbye", "2.1.0", "amd64", "A goodbye package"),
        ("libfoo", "0.3.1", "amd64", "A library package"),
        ("hello", "1.0.0", "arm64", "A hello world package for ARM"),
    ];

    for (name, version, arch, desc) in &packages {
        let deb = build_synthetic_deb(name, version, arch, desc)?;
        let filename = format!("{name}_{version}_{arch}.deb");

        match client.apt_upload(repo_name, "main", &filename, deb).await {
            Ok(()) => {}
            Err(e) => tracing::warn!("APT upload {filename} failed: {e}"),
        }
    }
    tracing::info!(
        "  Uploaded {} .deb packages to {}",
        packages.len(),
        repo_name
    );

    Ok(())
}

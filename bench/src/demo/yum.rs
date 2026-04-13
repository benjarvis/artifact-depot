// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! YUM demo data seeding: create a YUM repo and upload synthetic RPM packages.

use anyhow::Result;

use crate::client::DepotClient;
use depot_format_yum::build_synthetic_rpm;

/// Seed demo YUM data: create a hosted YUM repo and upload synthetic RPM packages.
pub async fn seed_yum_demo(client: &DepotClient) -> Result<()> {
    use crate::client::CreateRepoRequest;

    let repo_name = "yum-packages";
    tracing::info!("Creating YUM repo: {}", repo_name);

    let _ = client
        .create_repo(&CreateRepoRequest {
            name: repo_name.to_string(),
            repo_type: "hosted".to_string(),
            format: "yum".to_string(),
            store: "default".to_string(),
            upstream_url: None,
            cache_ttl_secs: None,
            members: None,
            listen: None,
        })
        .await;

    let packages = [
        ("hello", "1.0.0", "1", "x86_64", "A hello world package"),
        ("goodbye", "2.1.0", "1", "x86_64", "A goodbye package"),
        ("libfoo", "0.3.1", "1", "x86_64", "A library package"),
        (
            "hello",
            "1.0.0",
            "1",
            "noarch",
            "A hello world package for all arches",
        ),
    ];

    for (name, version, release, arch, desc) in &packages {
        let rpm = build_synthetic_rpm(name, version, release, arch, desc)?;
        let filename = format!("{name}-{version}-{release}.{arch}.rpm");

        match client.yum_upload(repo_name, &filename, rpm, None).await {
            Ok(()) => {}
            Err(e) => tracing::warn!("YUM upload {filename} failed: {e}"),
        }
    }
    tracing::info!(
        "  Uploaded {} RPM packages to {}",
        packages.len(),
        repo_name
    );

    Ok(())
}

// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! npm demo data seeding: create an npm repo and publish synthetic packages.

use crate::client::DepotClient;
use anyhow::Result;

/// Seed demo npm data: create a hosted npm repo and publish synthetic packages.
pub async fn seed_npm_demo(client: &DepotClient) -> Result<()> {
    use crate::client::CreateRepoRequest;

    let repo_name = "npm-packages";
    tracing::info!("Creating npm repo: {}", repo_name);

    let _ = client
        .create_repo(&CreateRepoRequest {
            name: repo_name.to_string(),
            repo_type: "hosted".to_string(),
            format: "npm".to_string(),
            store: "default".to_string(),
            upstream_url: None,
            cache_ttl_secs: None,
            members: None,
            listen: None,
        })
        .await;

    // Publish a few synthetic packages
    let packages: Vec<(&str, &[&str])> = vec![
        ("hello-world", &["1.0.0", "1.1.0", "2.0.0"]),
        ("utils", &["0.1.0", "0.2.0"]),
        ("@demo/widget", &["1.0.0", "1.0.1"]),
    ];

    for (name, versions) in &packages {
        for version in *versions {
            let tarball = build_synthetic_tarball(name, version);
            match client.publish_npm(repo_name, name, version, &tarball).await {
                Ok(()) => {}
                Err(e) => tracing::warn!("npm publish {name}@{version} failed: {e}"),
            }
        }
        tracing::info!("  Published {} versions of {}", versions.len(), name);
    }

    Ok(())
}

/// Build a minimal synthetic tarball (just raw bytes for demo purposes).
fn build_synthetic_tarball(name: &str, version: &str) -> Vec<u8> {
    let content = format!(
        r#"{{"name":"{}","version":"{}","description":"A demo package"}}"#,
        name, version
    );
    content.into_bytes()
}

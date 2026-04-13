// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Helm demo data seeding: create a Helm repo and upload synthetic charts.

use anyhow::Result;

use crate::client::DepotClient;
use depot_format_helm::build_synthetic_chart;

/// Seed demo Helm data: create a hosted Helm repo and upload synthetic charts.
pub async fn seed_helm_demo(client: &DepotClient) -> Result<()> {
    use crate::client::CreateRepoRequest;

    let repo_name = "helm-charts";
    tracing::info!("Creating Helm repo: {}", repo_name);

    let _ = client
        .create_repo(&CreateRepoRequest {
            name: repo_name.to_string(),
            repo_type: "hosted".to_string(),
            format: "helm".to_string(),
            store: "default".to_string(),
            upstream_url: None,
            cache_ttl_secs: None,
            members: None,
            listen: None,
        })
        .await;

    let charts = [
        ("nginx", "1.0.0", "An NGINX web server chart"),
        ("nginx", "1.1.0", "An NGINX web server chart"),
        ("redis", "2.0.0", "A Redis database chart"),
        ("redis", "2.1.0", "A Redis database chart"),
        ("postgresql", "3.0.0", "A PostgreSQL database chart"),
    ];

    for (name, version, desc) in &charts {
        let tgz = build_synthetic_chart(name, version, desc)?;
        let filename = format!("{name}-{version}.tgz");

        match client.helm_upload(repo_name, &filename, tgz).await {
            Ok(()) => {}
            Err(e) => tracing::warn!("Helm upload {filename} failed: {e}"),
        }
    }
    tracing::info!("  Uploaded {} charts to {}", charts.len(), repo_name);

    Ok(())
}

// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

pub mod apt;
pub mod cargo;
pub mod docker;
pub mod golang;
pub mod helm;
pub mod names;
pub mod npm;
pub mod raw;
pub mod trickle;
pub mod yum;

use anyhow::Result;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use crate::client::{CreateRepoRequest, DepotClient};

pub struct DemoConfig {
    pub repos: usize,
    pub docker_repos: usize,
    pub artifacts: usize,
    pub images: usize,
    pub tags: usize,
    pub clean: bool,
    /// Blob store root path configured on the depot server. Use an absolute
    /// path when invoking `demo::run` from tests so the store lives outside
    /// the calling crate's CWD. `make demo` (via `scripts/demo.sh`) passes
    /// the workspace-rooted `build/demo/blobs` relative path.
    pub blob_root: String,
}

pub async fn run(client: &DepotClient, cfg: DemoConfig) -> Result<()> {
    let mut rng = ChaCha8Rng::seed_from_u64(42);

    client.login().await?;
    tracing::info!("Authenticated with depot instance");

    client
        .create_store("default", "file", &cfg.blob_root)
        .await?;
    tracing::info!("Ensured 'default' blob store exists");

    // Clean existing repos if requested
    if cfg.clean {
        tracing::info!("Cleaning existing repositories...");
        let repos = client.list_repos().await?;
        for repo in &repos {
            client.delete_repo(&repo.name).await?;
            tracing::info!("  Deleted {}", repo.name);
        }
    }

    // Create and populate raw repos
    let raw_defs = names::raw_repo_defs(cfg.repos);
    for def in &raw_defs {
        tracing::info!("Creating raw repo: {} ({})", def.name, def.description);
        let _ = client
            .create_repo(&CreateRepoRequest {
                name: def.name.to_string(),
                repo_type: "hosted".to_string(),
                format: "raw".to_string(),
                store: "default".to_string(),
                upstream_url: None,
                cache_ttl_secs: None,
                members: None,
                listen: None,
            })
            .await;

        for i in 0..cfg.artifacts {
            let (path, content_type) = (def.path_gen)(&mut rng, i);
            let content = raw::generate_content(&mut rng, content_type, &path);
            match client
                .upload_raw(def.name, &path, content, content_type)
                .await
            {
                Ok(()) => {}
                Err(e) => tracing::warn!("upload {}/{} failed: {}", def.name, path, e),
            }
        }
        tracing::info!("  Uploaded {} artifacts to {}", cfg.artifacts, def.name);
    }

    // Create and populate Docker repos
    let docker_names = names::docker_repo_names(cfg.docker_repos);
    for repo_name in &docker_names {
        tracing::info!("Creating Docker repo: {}", repo_name);
        let _ = client
            .create_repo(&CreateRepoRequest {
                name: repo_name.to_string(),
                repo_type: "hosted".to_string(),
                format: "docker".to_string(),
                store: "default".to_string(),
                upstream_url: None,
                cache_ttl_secs: None,
                members: None,
                listen: None,
            })
            .await;

        let image_names = names::docker_image_names(repo_name, cfg.images);
        for image in &image_names {
            let tags = names::docker_tags(&mut rng, repo_name, cfg.tags);
            for tag in &tags {
                match docker::push_image(client, repo_name, image, tag, &mut rng).await {
                    Ok(()) => {}
                    Err(e) => tracing::warn!(
                        "push {}/{}/{}:{} failed: {}",
                        repo_name,
                        image,
                        image,
                        tag,
                        e
                    ),
                }
            }
            tracing::info!("  Pushed {} tags for {}/{}", cfg.tags, repo_name, image);
        }
    }

    // Create and populate APT repos
    if let Err(e) = apt::seed_apt_demo(client).await {
        tracing::warn!("APT demo seeding failed: {e}");
    }

    // Create and populate Golang repos
    if let Err(e) = golang::seed_golang_demo(client).await {
        tracing::warn!("Golang demo seeding failed: {e}");
    }

    // Create and populate Helm repos
    if let Err(e) = helm::seed_helm_demo(client).await {
        tracing::warn!("Helm demo seeding failed: {e}");
    }

    // Create and populate Cargo repos
    if let Err(e) = cargo::seed_cargo_demo(client).await {
        tracing::warn!("Cargo demo seeding failed: {e}");
    }

    // Create and populate YUM repos
    if let Err(e) = yum::seed_yum_demo(client).await {
        tracing::warn!("YUM demo seeding failed: {e}");
    }

    // Create and populate npm repos
    if let Err(e) = npm::seed_npm_demo(client).await {
        tracing::warn!("npm demo seeding failed: {e}");
    }

    // Create a dedicated user for background trickle activity so it
    // survives the admin password change on first UI login.
    client.create_user("trickle", "trickle", &["admin"]).await?;
    tracing::info!("Created 'trickle' service user");

    tracing::info!("Demo seeding complete!");
    Ok(())
}

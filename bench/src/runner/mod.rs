// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

pub mod report;
pub mod scenario;
pub mod stats;
pub mod worker;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use tokio::sync::mpsc;

use crate::client::{CreateRepoRequest, DepotClient};
use crate::demo;

pub struct BenchConfig {
    pub scenario: String,
    pub concurrency: usize,
    pub duration: u64,
    pub count: Option<u64>,
    pub artifact_size: usize,
    pub layer_size: usize,
    pub warmup: u64,
    pub json: bool,
    pub store: String,
    pub random_data: bool,
    pub pipeline: usize,
}

pub async fn run(client: &DepotClient, cfg: BenchConfig) -> Result<()> {
    client.login().await?;

    let scenarios = scenario::get_scenarios(&cfg.scenario);
    if scenarios.is_empty() {
        bail!(
            "Unknown scenario '{}'. Valid: raw-upload, raw-download, raw-mixed, docker-push, docker-pull, docker-mixed, all",
            cfg.scenario
        );
    }

    for sc in &scenarios {
        run_scenario(client, &cfg, sc).await?;
    }

    Ok(())
}

async fn run_scenario(
    client: &DepotClient,
    cfg: &BenchConfig,
    sc: &scenario::Scenario,
) -> Result<()> {
    let raw_repo = "bench-raw";
    let docker_repo = "bench-docker";
    let docker_image = "bench-image";

    // Setup phase: create repos
    let _ = client
        .create_repo(&CreateRepoRequest {
            name: raw_repo.to_string(),
            repo_type: "hosted".to_string(),
            format: "raw".to_string(),
            store: cfg.store.clone(),
            upstream_url: None,
            cache_ttl_secs: None,
            members: None,
            listen: None,
        })
        .await;

    let _ = client
        .create_repo(&CreateRepoRequest {
            name: docker_repo.to_string(),
            repo_type: "hosted".to_string(),
            format: "docker".to_string(),
            store: cfg.store.clone(),
            upstream_url: None,
            cache_ttl_secs: None,
            members: None,
            listen: None,
        })
        .await;

    // Seed data for read scenarios
    let mut seeded_paths = Vec::new();
    let mut seeded_tags = Vec::new();

    if sc.needs_raw_seed {
        if !cfg.json {
            tracing::info!("Seeding raw artifacts for {}...", sc.name);
        }
        for i in 0..20 {
            let path = format!("seed/artifact-{}.bin", i);
            let data = vec![0x42u8; cfg.artifact_size];
            let _ = client
                .upload_raw(raw_repo, &path, data, "application/octet-stream")
                .await;
            seeded_paths.push(path);
        }
        // Also collect any existing artifacts
        if let Ok(artifacts) = client.list_artifacts(raw_repo).await {
            for a in artifacts {
                if !seeded_paths.contains(&a.path) {
                    seeded_paths.push(a.path);
                }
            }
        }
    }

    if sc.needs_docker_seed {
        if !cfg.json {
            tracing::info!("Seeding Docker images for {}...", sc.name);
        }
        let mut rng = ChaCha8Rng::seed_from_u64(88);
        for i in 0..3 {
            let tag = format!("seed-{}", i);
            let _ =
                demo::docker::push_image(client, docker_repo, docker_image, &tag, &mut rng).await;
            seeded_tags.push(tag);
        }
    }

    let seeded_paths = Arc::new(seeded_paths);
    let seeded_tags = Arc::new(seeded_tags);

    // Warmup + measure
    let total_duration = Duration::from_secs(cfg.duration + cfg.warmup);
    let warmup_duration = Duration::from_secs(cfg.warmup);

    let cancel = Arc::new(AtomicBool::new(false));
    let op_counter = Arc::new(AtomicU64::new(0));
    let (tx, mut rx) = mpsc::unbounded_channel::<stats::OpResult>();

    if !cfg.json {
        tracing::info!("Running {} ({} workers)...", sc.name, cfg.concurrency);
    }

    // Spawn workers — each gets its own HTTP connection pool so that
    // HTTP/2 connections are not shared (avoids TCP window contention).
    let mut handles = Vec::new();
    for w in 0..cfg.concurrency {
        let worker_client = client.fork().unwrap_or_else(|_| client.clone());
        let worker_cfg = worker::WorkerConfig {
            worker_id: w,
            ops: sc.ops.clone(),
            client: worker_client,
            repo_name: raw_repo.to_string(),
            docker_repo: docker_repo.to_string(),
            docker_image: docker_image.to_string(),
            artifact_size: cfg.artifact_size,
            layer_size: cfg.layer_size,
            cancel: cancel.clone(),
            op_counter: op_counter.clone(),
            max_ops: cfg.count.map(|c| c / cfg.concurrency as u64),
            seeded_paths: seeded_paths.clone(),
            seeded_tags: seeded_tags.clone(),
            random_data: cfg.random_data,
            pipeline: cfg.pipeline,
        };
        let worker_tx = tx.clone();
        handles.push(tokio::spawn(worker::run_worker(worker_cfg, worker_tx)));
    }
    drop(tx); // Close our sender so rx completes when workers are done

    let start = Instant::now();

    // Spawn cancellation timer
    let cancel_clone = cancel.clone();
    let timer = tokio::spawn(async move {
        tokio::time::sleep(total_duration).await;
        cancel_clone.store(true, Ordering::Relaxed);
    });

    // Collect results
    let mut collector = stats::StatsCollector::new();
    while let Some(result) = rx.recv().await {
        let elapsed = start.elapsed();
        // Skip warmup results
        if elapsed >= warmup_duration {
            collector.record(result);
        }
    }

    timer.abort();

    // Wait for workers
    for h in handles {
        let _ = h.await;
    }

    let measure_duration = start.elapsed().saturating_sub(warmup_duration);
    let all_stats = collector.compute(measure_duration);

    if cfg.json {
        report::print_json(&sc.name, cfg.duration, cfg.concurrency, &all_stats);
    } else {
        report::print_table(&sc.name, cfg.duration, cfg.concurrency, &all_stats);
    }

    Ok(())
}

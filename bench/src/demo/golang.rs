// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Go module demo data seeding: create a Golang repo and upload synthetic modules.

use anyhow::Result;

use crate::client::DepotClient;

/// Seed demo Go module data: create a hosted golang repo and upload synthetic modules.
pub async fn seed_golang_demo(client: &DepotClient) -> Result<()> {
    use crate::client::CreateRepoRequest;

    let repo_name = "go-modules";
    tracing::info!("Creating Golang repo: {}", repo_name);

    let _ = client
        .create_repo(&CreateRepoRequest {
            name: repo_name.to_string(),
            repo_type: "hosted".to_string(),
            format: "golang".to_string(),
            store: "default".to_string(),
            upstream_url: None,
            cache_ttl_secs: None,
            members: None,
            listen: None,
        })
        .await;

    // Upload synthetic modules
    let modules = [
        ("example.com/hello", "v1.0.0"),
        ("example.com/hello", "v1.1.0"),
        ("example.com/greet", "v0.1.0"),
    ];

    for (module, version) in &modules {
        let info = format!("{{\"Version\":\"{version}\",\"Time\":\"2024-01-01T00:00:00Z\"}}");
        let gomod = format!("module {module}\n\ngo 1.21\n");
        // Build a minimal zip with the go.mod file
        let zip = build_module_zip(module, version, &gomod);

        match client
            .golang_upload(
                repo_name,
                module,
                version,
                info.as_bytes(),
                gomod.as_bytes(),
                &zip,
            )
            .await
        {
            Ok(()) => {}
            Err(e) => tracing::warn!("Go module upload {module}@{version} failed: {e}"),
        }
    }
    tracing::info!(
        "  Uploaded {} Go module versions to {}",
        modules.len(),
        repo_name
    );

    Ok(())
}

/// Build a minimal module zip file. Files are prefixed with `{module}@{version}/`.
fn build_module_zip(module: &str, version: &str, gomod: &str) -> Vec<u8> {
    use std::io::Write;
    let buf = std::io::Cursor::new(Vec::new());
    let mut zip = zip::ZipWriter::new(buf);
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    let prefix = format!("{module}@{version}/");
    zip.start_file(format!("{prefix}go.mod"), options).unwrap();
    zip.write_all(gomod.as_bytes()).unwrap();
    // Add a simple main.go
    zip.start_file(format!("{prefix}main.go"), options).unwrap();
    zip.write_all(b"package main\n\nfunc main() {}\n").unwrap();
    zip.finish().unwrap().into_inner()
}

// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Background worker that reconciles dedicated Docker port listeners with repo
//! configs every 30 seconds, spawning new listeners and stopping stale ones.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::server::AppState;
use depot_core::service;

/// A running Docker port listener.
struct ActiveListener {
    addr: SocketAddr,
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

/// Periodically reconciles dedicated Docker port listeners against repo configs.
///
/// On each tick the loop compares the set of Docker repos that have a `listen`
/// address configured against the set of currently running listeners, starting
/// new ones and stopping stale ones as needed.
pub async fn run_docker_listener_refresh(
    state: AppState,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    cancel: CancellationToken,
) {
    let mut registry: HashMap<String, ActiveListener> = HashMap::new();
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    // First tick fires immediately so listeners bind at startup.

    loop {
        tokio::select! {
            _ = interval.tick() => {
                reconcile(&state, &tls_cert, &tls_key, &cancel, &mut registry).await;
            }
            _ = cancel.cancelled() => {
                tracing::info!(
                    count = registry.len(),
                    "docker listener manager shutting down"
                );
                for (name, active) in registry.drain() {
                    active.cancel.cancel();
                    tracing::info!(repo = %name, "stopped docker port listener");
                }
                return;
            }
        }
    }
}

#[tracing::instrument(level = "debug", name = "docker_listener_refresh", skip_all)]
async fn reconcile(
    state: &AppState,
    tls_cert: &Option<PathBuf>,
    tls_key: &Option<PathBuf>,
    cancel: &CancellationToken,
    registry: &mut HashMap<String, ActiveListener>,
) {
    // Sweep finished tasks (crashed / failed bind) so they can be retried.
    registry.retain(|name, active| {
        if active.handle.is_finished() {
            tracing::warn!(repo = %name, "docker port listener exited unexpectedly");
            false
        } else {
            true
        }
    });

    // Build desired state from KV.
    let repos = match service::list_repos(state.repo.kv.as_ref()).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "docker listener refresh: failed to list repos");
            return;
        }
    };

    let mut desired: HashMap<String, SocketAddr> = HashMap::new();
    for repo in &repos {
        if let Some(listen_addr) = repo.format_config.listen() {
            match listen_addr.parse::<SocketAddr>() {
                Ok(addr) => {
                    desired.insert(repo.name.clone(), addr);
                }
                Err(e) => {
                    tracing::warn!(
                        repo = %repo.name,
                        listen = %listen_addr,
                        error = %e,
                        "invalid docker listen address"
                    );
                }
            }
        }
    }

    // Stop listeners whose repo was removed or whose address changed.
    let stale: Vec<String> = registry
        .keys()
        .filter(|name| {
            desired
                .get(*name)
                .is_none_or(|addr| registry[*name].addr != *addr)
        })
        .cloned()
        .collect();
    for name in stale {
        if let Some(active) = registry.remove(&name) {
            active.cancel.cancel();
            tracing::info!(repo = %name, addr = %active.addr, "stopped docker port listener");
        }
    }

    // Start listeners for new or re-addressed repos.
    for (name, addr) in &desired {
        if registry.contains_key(name) {
            continue;
        }
        if let Some(active) = spawn_listener(state, name, *addr, tls_cert, tls_key, cancel) {
            tracing::info!(repo = %name, addr = %addr, "started docker port listener");
            registry.insert(name.clone(), active);
        }
    }
}

fn spawn_listener(
    state: &AppState,
    repo_name: &str,
    addr: SocketAddr,
    tls_cert: &Option<PathBuf>,
    tls_key: &Option<PathBuf>,
    cancel: &CancellationToken,
) -> Option<ActiveListener> {
    let token = cancel.child_token();
    let docker_app = crate::server::build_docker_port_router(state.clone(), repo_name.to_owned());

    let handle = if let (Some(cert), Some(key)) = (tls_cert.as_ref(), tls_key.as_ref()) {
        let cert = cert.clone();
        let key = key.clone();
        let listener_cancel = token.clone();
        tokio::spawn(async move {
            let tls_config = match axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert, &key)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(addr = %addr, "failed to load TLS config for docker port: {}", e);
                    return;
                }
            };
            let handle = axum_server::Handle::new();
            let h = handle.clone();
            tokio::spawn(async move {
                listener_cancel.cancelled().await;
                h.graceful_shutdown(Some(Duration::from_secs(5)));
            });
            if let Err(e) = axum_server::bind_rustls(addr, tls_config)
                .handle(handle)
                .serve(docker_app.into_make_service_with_connect_info::<SocketAddr>())
                .await
            {
                tracing::error!(addr = %addr, "docker port listener error: {}", e);
            }
        })
    } else {
        let listener_cancel = token.clone();
        tokio::spawn(async move {
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(addr = %addr, "failed to bind docker port: {}", e);
                    return;
                }
            };
            if let Err(e) = axum::serve(listener, docker_app)
                .with_graceful_shutdown(listener_cancel.cancelled_owned())
                .await
            {
                tracing::error!(addr = %addr, "docker port listener error: {}", e);
            }
        })
    };

    Some(ActiveListener {
        addr,
        cancel: token,
        handle,
    })
}

// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Backup & Restore API — export/import full system configuration as JSON.

use axum::{extract::State, http::header, response::IntoResponse, Extension, Json};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::server::api::repositories::RepoResponse;
use crate::server::api::roles::RoleResponse;
use crate::server::api::stores::StoreResponse;
use crate::server::api::users::UserResponse;
use crate::server::config::settings;
use crate::server::infra::event_bus::{ModelEvent, Topic};
use crate::server::AppState;
use depot_core::auth::AuthenticatedUser;
use depot_core::error::DepotError;
use depot_core::service;
use depot_core::store::kv::{RepoConfig, RoleRecord, StoreRecord, UserRecord};

/// Top-level backup document containing all system configuration.
#[derive(Debug, Serialize, Deserialize)]
pub struct BackupDocument {
    pub version: u32,
    pub created_at: DateTime<Utc>,
    pub settings: settings::Settings,
    #[cfg(feature = "ldap")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ldap_config: Option<crate::server::config::LdapConfig>,
    pub stores: Vec<StoreRecord>,
    pub repositories: Vec<RepoConfig>,
    pub roles: Vec<RoleRecord>,
    pub users: Vec<UserRecord>,
}

/// Summary returned after a restore operation.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct RestoreSummary {
    pub stores: usize,
    pub repositories: usize,
    pub roles: usize,
    pub users: usize,
    pub settings: bool,
    #[cfg(feature = "ldap")]
    pub ldap_config: bool,
}

/// GET /api/v1/backup — export full system configuration (admin only).
#[utoipa::path(
    get,
    path = "/api/v1/backup",
    responses(
        (status = 200, description = "Full system configuration export as JSON"),
        (status = 403, description = "Admin access required")
    ),
    tag = "backup"
)]
pub async fn get_backup(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
) -> impl IntoResponse {
    if let Err(e) = state.auth.backend.require_admin(&user.0).await {
        return e.into_response();
    }

    let kv = state.repo.kv.as_ref();

    let (stores, repos, users, roles, current_settings) = match tokio::try_join!(
        async { service::list_stores(kv).await.map_err(anyhow::Error::from) },
        async {
            service::list_repos_raw(kv)
                .await
                .map_err(anyhow::Error::from)
        },
        async { service::list_users(kv).await.map_err(anyhow::Error::from) },
        async { service::list_roles(kv).await.map_err(anyhow::Error::from) },
        async { settings::load_settings_from_kv(kv).await },
    ) {
        Ok(v) => v,
        Err(e) => {
            return DepotError::Internal(format!("backup failed: {e}")).into_response();
        }
    };

    #[cfg(feature = "ldap")]
    let ldap_config = match settings::load_ldap_config(kv).await {
        Ok(c) => c,
        Err(e) => {
            return DepotError::Internal(format!("backup failed loading LDAP config: {e}"))
                .into_response();
        }
    };

    let now = Utc::now();
    let doc = BackupDocument {
        version: 1,
        created_at: now,
        settings: current_settings.unwrap_or_default(),
        #[cfg(feature = "ldap")]
        ldap_config,
        stores,
        repositories: repos,
        roles,
        users,
    };

    let timestamp = now.format("%Y%m%d-%H%M%S");
    let filename = format!("depot-backup-{timestamp}.json");

    (
        [
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
            (header::CONTENT_TYPE, "application/json".to_string()),
        ],
        Json(doc),
    )
        .into_response()
}

/// POST /api/v1/restore — import system configuration from backup JSON (admin only).
#[utoipa::path(
    post,
    path = "/api/v1/restore",
    request_body(content = String, description = "Backup document JSON (same format as GET /api/v1/backup response)", content_type = "application/json"),
    responses(
        (status = 200, description = "Restore completed", body = RestoreSummary),
        (status = 400, description = "Invalid backup document"),
        (status = 403, description = "Admin access required")
    ),
    tag = "backup"
)]
pub async fn post_restore(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Json(doc): Json<BackupDocument>,
) -> impl IntoResponse {
    if let Err(e) = state.auth.backend.require_admin(&user.0).await {
        return e.into_response();
    }

    let kv = state.repo.kv.as_ref();

    let bus = &state.bg.event_bus;

    // Restore stores first (repos reference stores).
    for store in &doc.stores {
        if let Err(e) = service::put_store(kv, store).await {
            return DepotError::Internal(format!("restore store '{}': {e}", store.name))
                .into_response();
        }
        let mut resp = StoreResponse::from(store.clone());
        if let Ok(Some(stats)) = service::get_store_stats(kv, &store.name).await {
            resp.blob_count = Some(stats.blob_count);
            resp.total_bytes = Some(stats.total_bytes);
        }
        bus.publish(Topic::Stores, ModelEvent::StoreCreated { store: resp });
    }

    // Restore roles before users (users reference roles).
    for role in &doc.roles {
        if let Err(e) = service::put_role(kv, role).await {
            return DepotError::Internal(format!("restore role '{}': {e}", role.name))
                .into_response();
        }
        bus.publish(
            Topic::Roles,
            ModelEvent::RoleCreated {
                role: RoleResponse::from(role.clone()),
            },
        );
    }

    // Restore users.
    for u in &doc.users {
        if let Err(e) = service::put_user(kv, u).await {
            return DepotError::Internal(format!("restore user '{}': {e}", u.username))
                .into_response();
        }
        bus.publish(
            Topic::Users,
            ModelEvent::UserCreated {
                user: UserResponse::from(u.clone()),
            },
        );
    }

    // Restore repositories.
    for repo in &doc.repositories {
        if let Err(e) = service::put_repo(kv, repo).await {
            return DepotError::Internal(format!("restore repo '{}': {e}", repo.name))
                .into_response();
        }
        bus.publish(
            Topic::Repos,
            ModelEvent::RepoCreated {
                repo: RepoResponse::from(repo.clone()),
            },
        );
    }

    // Restore settings.
    if let Err(e) = settings::save_settings_to_kv(kv, &doc.settings).await {
        return DepotError::Internal(format!("restore settings: {e}")).into_response();
    }
    bus.publish(
        Topic::Settings,
        ModelEvent::SettingsUpdated {
            settings: doc.settings.clone(),
        },
    );
    state.rate_limiter.update(doc.settings.rate_limit.as_ref());
    state
        .settings
        .store(std::sync::Arc::new(doc.settings.clone()));

    // Restore LDAP config if present.
    #[cfg(feature = "ldap")]
    let ldap_restored = if let Some(ref ldap) = doc.ldap_config {
        match settings::save_ldap_config(kv, ldap).await {
            Ok(()) => {
                state.auth.ldap_config.store(Some(ldap.clone()));
                true
            }
            Err(e) => {
                return DepotError::Internal(format!("restore LDAP config: {e}")).into_response();
            }
        }
    } else {
        false
    };

    // Invalidate auth caches so restored users/roles take effect immediately.
    state.auth.backend.invalidate_all();

    let summary = RestoreSummary {
        stores: doc.stores.len(),
        repositories: doc.repositories.len(),
        roles: doc.roles.len(),
        users: doc.users.len(),
        settings: true,
        #[cfg(feature = "ldap")]
        ldap_config: ldap_restored,
    };

    Json(summary).into_response()
}

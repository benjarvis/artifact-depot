// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::server::AppState;
use depot_core::auth::AuthenticatedUser;
use depot_core::error::DepotError;
use depot_core::service;
use depot_core::store::kv::RoleRecord;
use depot_core::store::kv::{CapabilityGrant, CURRENT_RECORD_VERSION};

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RoleResponse {
    pub name: String,
    pub description: String,
    pub capabilities: Vec<CapabilityGrant>,
    #[schema(value_type = String, format = "date-time")]
    pub created_at: chrono::DateTime<chrono::Utc>,
    #[schema(value_type = String, format = "date-time")]
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<RoleRecord> for RoleResponse {
    fn from(r: RoleRecord) -> Self {
        Self {
            name: r.name,
            description: r.description,
            capabilities: r.capabilities,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateRoleRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub capabilities: Vec<CapabilityGrant>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateRoleRequest {
    pub description: Option<String>,
    pub capabilities: Option<Vec<CapabilityGrant>>,
}

/// List all roles.
#[utoipa::path(
    get,
    path = "/api/v1/roles",
    responses(
        (status = 200, description = "List of roles", body = Vec<RoleResponse>),
        (status = 403, description = "Forbidden")
    ),
    tag = "roles"
)]
pub async fn list_roles(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
) -> impl IntoResponse {
    if let Err(e) = state.auth.backend.require_admin(&user.0).await {
        return e.into_response();
    }
    match service::list_roles(state.repo.kv.as_ref()).await {
        Ok(roles) => {
            let resp: Vec<RoleResponse> = roles.into_iter().map(RoleResponse::from).collect();
            Json(resp).into_response()
        }
        Err(e) => e.into_response(),
    }
}

/// Get a single role.
#[utoipa::path(
    get,
    path = "/api/v1/roles/{name}",
    params(("name" = String, Path, description = "Role name")),
    responses(
        (status = 200, description = "Role details", body = RoleResponse),
        (status = 404, description = "Role not found"),
        (status = 403, description = "Forbidden")
    ),
    tag = "roles"
)]
pub async fn get_role(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = state.auth.backend.require_admin(&user.0).await {
        return e.into_response();
    }
    let name_owned = name.clone();
    match service::get_role(state.repo.kv.as_ref(), &name_owned).await {
        Ok(Some(record)) => Json(RoleResponse::from(record)).into_response(),
        Ok(None) => DepotError::NotFound(format!("role '{}' not found", name)).into_response(),
        Err(e) => e.into_response(),
    }
}

/// Create a new role.
#[utoipa::path(
    post,
    path = "/api/v1/roles",
    request_body = CreateRoleRequest,
    responses(
        (status = 201, description = "Role created", body = RoleResponse),
        (status = 409, description = "Role already exists"),
        (status = 403, description = "Forbidden")
    ),
    tag = "roles"
)]
pub async fn create_role(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Json(req): Json<CreateRoleRequest>,
) -> impl IntoResponse {
    if let Err(e) = state.auth.backend.require_admin(&user.0).await {
        return e.into_response();
    }
    if req.name.is_empty() {
        return DepotError::BadRequest("name is required".into()).into_response();
    }
    let now = crate::server::repo::now_utc();
    let record = RoleRecord {
        schema_version: CURRENT_RECORD_VERSION,
        name: req.name,
        description: req.description,
        capabilities: req.capabilities,
        created_at: now,
        updated_at: now,
    };
    if service::get_role(state.repo.kv.as_ref(), &record.name)
        .await
        .ok()
        .flatten()
        .is_some()
    {
        return DepotError::Conflict("role already exists".into()).into_response();
    }
    match service::put_role(state.repo.kv.as_ref(), &record).await {
        Ok(()) => {
            state.auth.backend.invalidate_role(&record.name);
            let resp = RoleResponse::from(record);
            state.bg.event_bus.publish(
                crate::server::infra::event_bus::Topic::Roles,
                crate::server::infra::event_bus::ModelEvent::RoleCreated { role: resp.clone() },
            );
            (StatusCode::CREATED, Json(resp)).into_response()
        }
        Err(e) => e.into_response(),
    }
}

/// Update an existing role.
#[utoipa::path(
    put,
    path = "/api/v1/roles/{name}",
    params(("name" = String, Path, description = "Role name")),
    request_body = UpdateRoleRequest,
    responses(
        (status = 200, description = "Role updated", body = RoleResponse),
        (status = 404, description = "Role not found"),
        (status = 403, description = "Forbidden")
    ),
    tag = "roles"
)]
pub async fn update_role(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(name): Path<String>,
    Json(req): Json<UpdateRoleRequest>,
) -> impl IntoResponse {
    if let Err(e) = state.auth.backend.require_admin(&user.0).await {
        return e.into_response();
    }
    let new_description = req.description;
    let new_capabilities = req.capabilities;
    let now = crate::server::repo::now_utc();
    let mut record = match service::get_role(state.repo.kv.as_ref(), &name).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            return DepotError::NotFound(format!("role '{}' not found", name)).into_response()
        }
        Err(e) => return e.into_response(),
    };
    if let Some(description) = new_description {
        record.description = description;
    }
    if let Some(capabilities) = new_capabilities {
        record.capabilities = capabilities;
    }
    record.updated_at = now;
    match service::put_role(state.repo.kv.as_ref(), &record).await {
        Ok(()) => {
            state.auth.backend.invalidate_role(&name);
            let resp = RoleResponse::from(record);
            state.bg.event_bus.publish(
                crate::server::infra::event_bus::Topic::Roles,
                crate::server::infra::event_bus::ModelEvent::RoleUpdated { role: resp.clone() },
            );
            Json(resp).into_response()
        }
        Err(e) => e.into_response(),
    }
}

/// Delete a role.
#[utoipa::path(
    delete,
    path = "/api/v1/roles/{name}",
    params(("name" = String, Path, description = "Role name")),
    responses(
        (status = 204, description = "Role deleted"),
        (status = 404, description = "Role not found"),
        (status = 400, description = "Cannot delete the admin role"),
        (status = 403, description = "Forbidden")
    ),
    tag = "roles"
)]
pub async fn delete_role(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = state.auth.backend.require_admin(&user.0).await {
        return e.into_response();
    }
    if name == "admin" {
        return DepotError::BadRequest("cannot delete the admin role".into()).into_response();
    }
    let name_owned = name.clone();
    match service::delete_role(state.repo.kv.as_ref(), &name_owned).await {
        Ok(true) => {
            state.auth.backend.invalidate_role(&name);
            state.bg.event_bus.publish(
                crate::server::infra::event_bus::Topic::Roles,
                crate::server::infra::event_bus::ModelEvent::RoleDeleted { name: name_owned },
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => DepotError::NotFound(format!("role '{}' not found", name)).into_response(),
        Err(e) => e.into_response(),
    }
}

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

use crate::server::auth::{self, hash_password};
use crate::server::AppState;
use depot_core::auth::AuthenticatedUser;
use depot_core::error::DepotError;
use depot_core::service;
use depot_core::store::kv::UserRecord;
use depot_core::store::kv::CURRENT_RECORD_VERSION;

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UserResponse {
    pub username: String,
    pub roles: Vec<String>,
    pub must_change_password: bool,
    #[schema(value_type = String, format = "date-time")]
    pub created_at: chrono::DateTime<chrono::Utc>,
    #[schema(value_type = String, format = "date-time")]
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<UserRecord> for UserResponse {
    fn from(r: UserRecord) -> Self {
        Self {
            username: r.username,
            roles: r.roles,
            must_change_password: r.must_change_password,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub must_change_password: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateUserRequest {
    pub password: Option<String>,
    pub roles: Option<Vec<String>>,
    pub must_change_password: Option<bool>,
}

/// List all users.
#[utoipa::path(
    get,
    path = "/api/v1/users",
    responses(
        (status = 200, description = "List of users", body = Vec<UserResponse>),
        (status = 403, description = "Forbidden")
    ),
    tag = "users"
)]
pub async fn list_users(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
) -> impl IntoResponse {
    if let Err(e) = state.auth.backend.require_admin(&user.0).await {
        return e.into_response();
    }
    match service::list_users(state.repo.kv.as_ref()).await {
        Ok(users) => {
            let resp: Vec<UserResponse> = users.into_iter().map(UserResponse::from).collect();
            Json(resp).into_response()
        }
        Err(e) => e.into_response(),
    }
}

/// Get a single user.
#[utoipa::path(
    get,
    path = "/api/v1/users/{username}",
    params(("username" = String, Path, description = "Username")),
    responses(
        (status = 200, description = "User details", body = UserResponse),
        (status = 404, description = "User not found"),
        (status = 403, description = "Forbidden")
    ),
    tag = "users"
)]
pub async fn get_user(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(username): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = state.auth.backend.require_admin(&user.0).await {
        return e.into_response();
    }
    let username_owned = username.clone();
    match service::get_user(state.repo.kv.as_ref(), &username_owned).await {
        Ok(Some(record)) => Json(UserResponse::from(record)).into_response(),
        Ok(None) => DepotError::NotFound(format!("user '{}' not found", username)).into_response(),
        Err(e) => e.into_response(),
    }
}

/// Create a new user.
#[utoipa::path(
    post,
    path = "/api/v1/users",
    request_body = CreateUserRequest,
    responses(
        (status = 201, description = "User created", body = UserResponse),
        (status = 409, description = "User already exists"),
        (status = 403, description = "Forbidden")
    ),
    tag = "users"
)]
pub async fn create_user(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Json(req): Json<CreateUserRequest>,
) -> impl IntoResponse {
    if let Err(e) = state.auth.backend.require_admin(&user.0).await {
        return e.into_response();
    }
    if req.username.is_empty() {
        return DepotError::BadRequest("username is required".into()).into_response();
    }
    let now = crate::server::repo::now_utc();
    let password_hash = match hash_password(req.password.clone()).await {
        Ok(h) => h,
        Err(e) => return e.into_response(),
    };
    let record = UserRecord {
        schema_version: CURRENT_RECORD_VERSION,
        username: req.username,
        password_hash,
        roles: req.roles,
        auth_source: "builtin".to_string(),
        token_secret: auth::generate_token_secret(),
        must_change_password: req.must_change_password,
        created_at: now,
        updated_at: now,
    };
    if service::get_user(state.repo.kv.as_ref(), &record.username)
        .await
        .ok()
        .flatten()
        .is_some()
    {
        return DepotError::Conflict("user already exists".into()).into_response();
    }
    match service::put_user(state.repo.kv.as_ref(), &record).await {
        Ok(()) => {
            state.auth.backend.invalidate_user(&record.username);
            let resp = UserResponse::from(record);
            state.bg.event_bus.publish(
                crate::server::infra::event_bus::Topic::Users,
                crate::server::infra::event_bus::ModelEvent::UserCreated { user: resp.clone() },
            );
            (StatusCode::CREATED, Json(resp)).into_response()
        }
        Err(e) => e.into_response(),
    }
}

/// Update an existing user.
#[utoipa::path(
    put,
    path = "/api/v1/users/{username}",
    params(("username" = String, Path, description = "Username")),
    request_body = UpdateUserRequest,
    responses(
        (status = 200, description = "User updated", body = UserResponse),
        (status = 404, description = "User not found"),
        (status = 403, description = "Forbidden")
    ),
    tag = "users"
)]
pub async fn update_user(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(username): Path<String>,
    Json(req): Json<UpdateUserRequest>,
) -> impl IntoResponse {
    if let Err(e) = state.auth.backend.require_admin(&user.0).await {
        return e.into_response();
    }
    let has_password_change = req.password.is_some();
    let new_password_hash = match req.password {
        Some(password) => Some(match hash_password(password).await {
            Ok(h) => h,
            Err(e) => return e.into_response(),
        }),
        None => None,
    };
    let new_roles = req.roles;
    let now = crate::server::repo::now_utc();
    let mut record = match service::get_user(state.repo.kv.as_ref(), &username).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            return DepotError::NotFound(format!("user '{}' not found", username)).into_response()
        }
        Err(e) => return e.into_response(),
    };
    if has_password_change && record.auth_source != "builtin" {
        return DepotError::BadRequest(
            "cannot change password for externally authenticated user".into(),
        )
        .into_response();
    }
    if let Some(ph) = new_password_hash {
        record.password_hash = ph;
        record.token_secret = auth::generate_token_secret();
    }
    if let Some(roles) = new_roles {
        record.roles = roles;
    }
    if let Some(mcp) = req.must_change_password {
        record.must_change_password = mcp;
    }
    record.updated_at = now;
    match service::put_user(state.repo.kv.as_ref(), &record).await {
        Ok(()) => {
            state.auth.backend.invalidate_user(&username);
            let resp = UserResponse::from(record);
            state.bg.event_bus.publish(
                crate::server::infra::event_bus::Topic::Users,
                crate::server::infra::event_bus::ModelEvent::UserUpdated { user: resp.clone() },
            );
            Json(resp).into_response()
        }
        Err(e) => e.into_response(),
    }
}

/// Delete a user.
#[utoipa::path(
    delete,
    path = "/api/v1/users/{username}",
    params(("username" = String, Path, description = "Username")),
    responses(
        (status = 204, description = "User deleted"),
        (status = 404, description = "User not found"),
        (status = 400, description = "Cannot delete last admin"),
        (status = 403, description = "Forbidden")
    ),
    tag = "users"
)]
pub async fn delete_user(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(username): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = state.auth.backend.require_admin(&user.0).await {
        return e.into_response();
    }
    // Prevent deleting the last admin user.
    if let Ok(Some(target)) = service::get_user(state.repo.kv.as_ref(), &username).await {
        if target.roles.iter().any(|r| r == "admin") {
            if let Ok(users) = service::list_users(state.repo.kv.as_ref()).await {
                let admin_count = users
                    .iter()
                    .filter(|u| u.roles.iter().any(|r| r == "admin"))
                    .count();
                if admin_count <= 1 {
                    return DepotError::BadRequest("cannot delete the last admin user".into())
                        .into_response();
                }
            }
        }
    }
    match service::delete_user(state.repo.kv.as_ref(), &username).await {
        Ok(true) => {
            state.auth.backend.invalidate_user(&username);
            state.bg.event_bus.publish(
                crate::server::infra::event_bus::Topic::Users,
                crate::server::infra::event_bus::ModelEvent::UserDeleted {
                    username: username.clone(),
                },
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => DepotError::NotFound(format!("user '{}' not found", username)).into_response(),
        Err(e) => e.into_response(),
    }
}

/// Revoke all outstanding JWT tokens for a specific user by rotating their
/// per-user token secret. Requires admin role or the user revoking their own.
#[utoipa::path(
    post,
    path = "/api/v1/users/{username}/revoke-tokens",
    params(("username" = String, Path, description = "Username")),
    responses(
        (status = 204, description = "User tokens revoked"),
        (status = 403, description = "Not permitted"),
        (status = 404, description = "User not found")
    ),
    tag = "users"
)]
pub async fn revoke_user_tokens(
    State(state): State<AppState>,
    Extension(caller): Extension<AuthenticatedUser>,
    Path(username): Path<String>,
) -> impl IntoResponse {
    // Allow self-service or admin.
    if caller.0 != username {
        if let Err(e) = state.auth.backend.require_admin(&caller.0).await {
            return e.into_response();
        }
    }

    let mut record = match service::get_user(state.repo.kv.as_ref(), &username).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            return DepotError::NotFound(format!("user '{}' not found", username)).into_response()
        }
        Err(e) => return e.into_response(),
    };

    record.token_secret = auth::generate_token_secret();
    record.updated_at = crate::server::repo::now_utc();

    match service::put_user(state.repo.kv.as_ref(), &record).await {
        Ok(()) => {
            state.auth.backend.invalidate_user(&username);
            tracing::warn!(target_user = %username, revoked_by = %caller.0, "audit: user tokens revoked");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => e.into_response(),
    }
}

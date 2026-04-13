// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use axum::{extract::State, Extension, Json};
use serde::Deserialize;

use crate::server::infra::log_export::RequestEvent;
use crate::server::AppState;
use depot_core::auth::AuthenticatedUser;
use depot_core::error::DepotError;

#[derive(Deserialize)]
pub struct ListParams {
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    100
}

/// List recent request events (admin only).
#[utoipa::path(
    get,
    path = "/api/v1/logging/events",
    params(
        ("limit" = Option<usize>, Query, description = "Maximum number of events to return (default 100, max 1000)")
    ),
    responses(
        (status = 200, description = "List of request events", body = Vec<crate::server::infra::log_export::RequestEvent>),
        (status = 403, description = "Admin access required")
    ),
    tag = "logging"
)]
pub async fn list_request_events(
    State(state): State<AppState>,
    Extension(caller): Extension<AuthenticatedUser>,
    params: axum::extract::Query<ListParams>,
) -> Result<Json<Vec<RequestEvent>>, DepotError> {
    state.auth.backend.require_admin(&caller.0).await?;
    let limit = params.limit.min(1000);
    let events = state.in_memory_log.recent(limit).await;
    Ok(Json(events))
}

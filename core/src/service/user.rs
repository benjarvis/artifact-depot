// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! User and role CRUD.

use std::borrow::Cow;

use crate::error;
use crate::store::keys;
use crate::store::kv::*;

use super::scan::ADMIN_ENTITY_CAP;
use super::typed::{typed_get, typed_list, typed_put};

// --- User CRUD ---

pub async fn put_user(kv: &dyn KvStore, record: &UserRecord) -> error::Result<()> {
    let (table, pk, sk) = keys::user_key(&record.username);
    typed_put(kv, table, pk, sk, record).await
}

pub async fn get_user(kv: &dyn KvStore, username: &str) -> error::Result<Option<UserRecord>> {
    let (table, pk, sk) = keys::user_key(username);
    typed_get(kv, table, pk, sk).await
}

pub async fn list_users(kv: &dyn KvStore) -> error::Result<Vec<UserRecord>> {
    typed_list(
        kv,
        keys::TABLE_USERS,
        Cow::Borrowed(keys::SINGLE_PK),
        Cow::Borrowed(""),
        ADMIN_ENTITY_CAP,
    )
    .await
}

pub async fn delete_user(kv: &dyn KvStore, username: &str) -> error::Result<bool> {
    let (table, pk, sk) = keys::user_key(username);
    kv.delete(table, pk, sk).await
}

// --- Role CRUD ---

pub async fn put_role(kv: &dyn KvStore, record: &RoleRecord) -> error::Result<()> {
    let (table, pk, sk) = keys::role_key(&record.name);
    typed_put(kv, table, pk, sk, record).await
}

pub async fn get_role(kv: &dyn KvStore, name: &str) -> error::Result<Option<RoleRecord>> {
    let (table, pk, sk) = keys::role_key(name);
    typed_get(kv, table, pk, sk).await
}

pub async fn list_roles(kv: &dyn KvStore) -> error::Result<Vec<RoleRecord>> {
    typed_list(
        kv,
        keys::TABLE_ROLES,
        Cow::Borrowed(keys::SINGLE_PK),
        Cow::Borrowed(""),
        ADMIN_ENTITY_CAP,
    )
    .await
}

pub async fn delete_role(kv: &dyn KvStore, name: &str) -> error::Result<bool> {
    let (table, pk, sk) = keys::role_key(name);
    kv.delete(table, pk, sk).await
}

pub async fn get_user_with_roles(
    kv: &dyn KvStore,
    username: &str,
) -> error::Result<Option<(UserRecord, Vec<RoleRecord>)>> {
    let user = match get_user(kv, username).await? {
        Some(u) => u,
        None => return Ok(None),
    };
    let mut roles = Vec::with_capacity(user.roles.len());
    for role_name in &user.roles {
        if let Ok(Some(role)) = get_role(kv, role_name).await {
            roles.push(role);
        }
    }
    Ok(Some((user, roles)))
}

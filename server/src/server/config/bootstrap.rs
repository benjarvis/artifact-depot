// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! First-start bootstrap: create default roles and users when the KV store is empty.

use crate::server::{auth, repo, store};
use depot_core::error;
use depot_core::service;
use depot_core::store::kv::CURRENT_RECORD_VERSION;

/// Creates "admin" and "read-only" roles if the role table is empty.
/// Returns `true` if roles were created.
pub async fn bootstrap_roles(kv: &dyn store::KvStore) -> error::Result<bool> {
    let roles_empty = service::list_roles(kv).await?.is_empty();

    if !roles_empty {
        return Ok(false);
    }

    let now = repo::now_utc();

    let admin_role = store::RoleRecord {
        schema_version: CURRENT_RECORD_VERSION,
        name: "admin".to_string(),
        description: "Full access to all repositories and admin operations".to_string(),
        capabilities: vec![
            store::CapabilityGrant {
                capability: store::Capability::Read,
                repo: "*".to_string(),
            },
            store::CapabilityGrant {
                capability: store::Capability::Write,
                repo: "*".to_string(),
            },
            store::CapabilityGrant {
                capability: store::Capability::Delete,
                repo: "*".to_string(),
            },
        ],
        created_at: now,
        updated_at: now,
    };

    let readonly_role = store::RoleRecord {
        schema_version: CURRENT_RECORD_VERSION,
        name: "read-only".to_string(),
        description: "Read-only access to all repositories".to_string(),
        capabilities: vec![store::CapabilityGrant {
            capability: store::Capability::Read,
            repo: "*".to_string(),
        }],
        created_at: now,
        updated_at: now,
    };

    service::put_role(kv, &admin_role).await?;
    service::put_role(kv, &readonly_role).await?;

    Ok(true)
}

/// Creates "admin" and "anonymous" users if the user table is empty.
///
/// When `skip_admin` is `true`, only the `anonymous` user is created and
/// `None` is returned — the caller is expected to create `admin` itself (e.g.
/// from the `[initialization]` config section), so we don't want to print a
/// throwaway random password here.
///
/// If `default_password` is `Some`, uses it for the created admin; otherwise
/// a random one is generated. Returns `Some(cleartext_password)` when a
/// random admin password was used, `None` otherwise.
pub async fn bootstrap_users(
    kv: &dyn store::KvStore,
    default_password: Option<String>,
    skip_admin: bool,
) -> error::Result<Option<String>> {
    let users_empty = service::list_users(kv).await?.is_empty();

    if !users_empty {
        return Ok(None);
    }

    let now = repo::now_utc();

    let anon_user = store::UserRecord {
        schema_version: CURRENT_RECORD_VERSION,
        username: "anonymous".to_string(),
        password_hash: String::new(),
        roles: vec![],
        auth_source: "builtin".to_string(),
        token_secret: vec![],
        must_change_password: false,
        created_at: now,
        updated_at: now,
    };

    service::put_user(kv, &anon_user).await?;

    if skip_admin {
        return Ok(None);
    }

    let password = default_password.unwrap_or_else(auth::generate_random_password);
    let hash = auth::hash_password(password.clone()).await?;

    let admin_user = store::UserRecord {
        schema_version: CURRENT_RECORD_VERSION,
        username: "admin".to_string(),
        password_hash: hash,
        roles: vec!["admin".to_string()],
        auth_source: "builtin".to_string(),
        token_secret: auth::generate_token_secret(),
        must_change_password: true,
        created_at: now,
        updated_at: now,
    };

    service::put_user(kv, &admin_user).await?;

    Ok(Some(password))
}

#[cfg(test)]
mod tests {
    use super::*;
    use depot_kv_redb::ShardedRedbKvStore;
    use std::sync::Arc;

    async fn open_kv(dir: &tempfile::TempDir) -> Arc<dyn store::KvStore> {
        Arc::new(
            ShardedRedbKvStore::open(&dir.path().join("kv"), 2, 0)
                .await
                .unwrap(),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bootstrap_roles_creates_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let kv = open_kv(&dir).await;

        let created = bootstrap_roles(kv.as_ref()).await.unwrap();
        assert!(created);

        // Verify roles exist with correct capabilities.
        let roles = service::list_roles(kv.as_ref()).await.unwrap();
        assert_eq!(roles.len(), 2);

        let admin = roles.iter().find(|r| r.name == "admin").unwrap();
        assert_eq!(admin.capabilities.len(), 3);

        let readonly = roles.iter().find(|r| r.name == "read-only").unwrap();
        assert_eq!(readonly.capabilities.len(), 1);
        assert_eq!(readonly.capabilities[0].capability, store::Capability::Read);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bootstrap_roles_skips_when_populated() {
        let dir = tempfile::tempdir().unwrap();
        let kv = open_kv(&dir).await;

        // Pre-insert a role.
        let now = repo::now_utc();
        let role = store::RoleRecord {
            schema_version: CURRENT_RECORD_VERSION,
            name: "existing".to_string(),
            description: "pre-existing".to_string(),
            capabilities: vec![],
            created_at: now,
            updated_at: now,
        };
        service::put_role(kv.as_ref(), &role).await.unwrap();

        let created = bootstrap_roles(kv.as_ref()).await.unwrap();
        assert!(!created);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bootstrap_users_creates_with_provided_password() {
        let dir = tempfile::tempdir().unwrap();
        let kv = open_kv(&dir).await;

        // Roles must exist first for a realistic bootstrap.
        bootstrap_roles(kv.as_ref()).await.unwrap();

        let result = bootstrap_users(kv.as_ref(), Some("test123".to_string()), false)
            .await
            .unwrap();
        assert_eq!(result, Some("test123".to_string()));

        let users = service::list_users(kv.as_ref()).await.unwrap();
        assert_eq!(users.len(), 2);
        assert!(users.iter().any(|u| u.username == "admin"));
        assert!(users.iter().any(|u| u.username == "anonymous"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bootstrap_users_generates_random_password() {
        let dir = tempfile::tempdir().unwrap();
        let kv = open_kv(&dir).await;

        let result = bootstrap_users(kv.as_ref(), None, false).await.unwrap();
        assert!(result.is_some());
        assert!(!result.unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bootstrap_users_skip_admin_creates_only_anonymous() {
        let dir = tempfile::tempdir().unwrap();
        let kv = open_kv(&dir).await;

        let result = bootstrap_users(kv.as_ref(), Some("unused".to_string()), true)
            .await
            .unwrap();
        assert!(result.is_none());

        let users = service::list_users(kv.as_ref()).await.unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].username, "anonymous");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bootstrap_users_skips_when_populated() {
        let dir = tempfile::tempdir().unwrap();
        let kv = open_kv(&dir).await;

        // Pre-insert a user.
        let now = repo::now_utc();
        let user = store::UserRecord {
            schema_version: CURRENT_RECORD_VERSION,
            username: "existing".to_string(),
            password_hash: String::new(),
            roles: vec![],
            auth_source: "builtin".to_string(),
            token_secret: vec![],
            must_change_password: false,
            created_at: now,
            updated_at: now,
        };
        service::put_user(kv.as_ref(), &user).await.unwrap();

        let result = bootstrap_users(kv.as_ref(), None, false).await.unwrap();
        assert!(result.is_none());
    }
}

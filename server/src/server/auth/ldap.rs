// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LDAP authentication backend.
//!
//! Authenticates users via LDAP bind, queries group membership, maps groups
//! to depot roles, and JIT-provisions a `UserRecord` in the KvStore.
//!
//! The LDAP config is stored in the KV store and can be changed at runtime
//! via the settings API. The backend reads the current config on each
//! authentication attempt via an `ArcSwap`.

use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::server::auth::backend::{AuthBackend, BuiltinAuthBackend};
use crate::server::config::LdapConfig;
use depot_core::error::DepotError;
use depot_core::service;
use depot_core::store::kv::KvStore;
use depot_core::store::kv::{Capability, RoleRecord, UserRecord, CURRENT_RECORD_VERSION};

/// Sentinel password hash for LDAP-sourced users. Can never match Argon2 verify.
const EXTERNAL_PASSWORD_HASH: &str = "!EXTERNAL";

/// Thread-safe handle to the current LDAP config, swappable at runtime.
pub struct LdapConfigHandle(ArcSwap<Option<LdapConfig>>);

impl LdapConfigHandle {
    pub fn new(initial: Option<LdapConfig>) -> Self {
        Self(ArcSwap::from_pointee(initial))
    }

    /// Read the current LDAP config snapshot.
    pub fn load(&self) -> arc_swap::Guard<Arc<Option<LdapConfig>>> {
        self.0.load()
    }

    /// Replace the current LDAP config.
    pub fn store(&self, new: Option<LdapConfig>) {
        self.0.store(Arc::new(new));
    }
}

pub struct LdapAuthBackend {
    pub(crate) ldap_config: Arc<LdapConfigHandle>,
    pub(crate) kv: Arc<dyn KvStore>,
    builtin: BuiltinAuthBackend,
}

impl LdapAuthBackend {
    pub fn new(ldap_config: Arc<LdapConfigHandle>, kv: Arc<dyn KvStore>) -> Self {
        let builtin = BuiltinAuthBackend::new(kv.clone());
        Self {
            ldap_config,
            kv,
            builtin,
        }
    }
}

/// Connect to the LDAP server and return an authenticated (service-bind) connection.
async fn connect(config: &LdapConfig) -> anyhow::Result<ldap3::Ldap> {
    let mut settings = ldap3::LdapConnSettings::new();
    if config.starttls {
        settings = settings.set_starttls(true);
    }
    if config.tls_skip_verify {
        settings = settings.set_no_tls_verify(true);
    }

    let (conn, mut ldap) = ldap3::LdapConnAsync::with_settings(settings, &config.url).await?;
    ldap3::drive!(conn);

    // Service-account bind
    ldap.simple_bind(&config.bind_dn, &config.bind_password)
        .await?
        .success()?;

    Ok(ldap)
}

/// Search for the user's DN using the configured filter template.
async fn find_user_dn(
    config: &LdapConfig,
    ldap: &mut ldap3::Ldap,
    username: &str,
) -> anyhow::Result<Option<String>> {
    let filter = config
        .user_filter
        .replace("{username}", &ldap3::ldap_escape(username));

    let (entries, _) = ldap
        .search(
            &config.user_base_dn,
            ldap3::Scope::Subtree,
            &filter,
            vec!["dn"],
        )
        .await?
        .success()?;

    if let Some(entry) = entries.into_iter().next() {
        let se = ldap3::SearchEntry::construct(entry);
        Ok(Some(se.dn))
    } else {
        Ok(None)
    }
}

/// Query LDAP groups for the given user DN.
async fn query_groups(
    config: &LdapConfig,
    ldap: &mut ldap3::Ldap,
    user_dn: &str,
) -> anyhow::Result<Vec<String>> {
    let filter = config
        .group_filter
        .replace("{dn}", &ldap3::ldap_escape(user_dn));

    let (entries, _) = ldap
        .search(
            &config.group_base_dn,
            ldap3::Scope::Subtree,
            &filter,
            vec![&config.group_name_attr],
        )
        .await?
        .success()?;

    let mut groups = Vec::new();
    for entry in entries {
        let se = ldap3::SearchEntry::construct(entry);
        if let Some(vals) = se.attrs.get(&config.group_name_attr) {
            for v in vals {
                groups.push(v.clone());
            }
        }
    }
    Ok(groups)
}

/// Map LDAP group names to depot role names using the configured mapping.
pub fn map_groups_to_roles(config: &LdapConfig, groups: &[String]) -> Vec<String> {
    let mut roles = Vec::new();
    for group in groups {
        if let Some(role) = config.group_role_mapping.get(group) {
            if !roles.contains(role) {
                roles.push(role.clone());
            }
        }
    }
    roles
}

/// JIT-provision (or update) a UserRecord in the KvStore for an LDAP user.
async fn provision_user(
    kv: &dyn KvStore,
    username: &str,
    roles: Vec<String>,
) -> anyhow::Result<UserRecord> {
    let now = crate::server::repo::now_utc();
    let uname = username.to_string();

    let existing = service::with_read_txn(kv, {
        let uname = uname.clone();
        async move |txn| service::get_user(txn, &uname).await
    })
    .await?;

    let record = if let Some(mut existing) = existing {
        // Update roles if they changed
        if existing.roles != roles {
            existing.roles = roles;
            existing.updated_at = now;
            let rec = existing.clone();
            service::with_txn(kv, async move |txn| service::put_user(txn, &rec).await).await?;
        }
        existing
    } else {
        let record = UserRecord {
            schema_version: CURRENT_RECORD_VERSION,
            username: uname.clone(),
            password_hash: EXTERNAL_PASSWORD_HASH.to_string(),
            roles,
            auth_source: "ldap".to_string(),
            token_secret: vec![],
            must_change_password: false,
            created_at: now,
            updated_at: now,
        };
        let rec = record.clone();
        service::with_txn(kv, async move |txn| service::put_user(txn, &rec).await).await?;
        record
    };

    Ok(record)
}

/// Perform LDAP authentication against the given config.
async fn ldap_authenticate(
    config: &LdapConfig,
    kv: &dyn KvStore,
    username: &str,
    password: &str,
) -> anyhow::Result<Option<UserRecord>> {
    let mut ldap = connect(config).await?;

    // Find user DN
    let user_dn = match find_user_dn(config, &mut ldap, username).await? {
        Some(dn) => dn,
        None => return Ok(None),
    };

    // Bind as the user to verify password
    let bind_result = ldap.simple_bind(&user_dn, password).await?;
    if bind_result.rc != 0 {
        return Ok(None);
    }

    // Re-bind as service account for group queries
    ldap.simple_bind(&config.bind_dn, &config.bind_password)
        .await?
        .success()?;

    // Query groups and map to roles
    let groups = query_groups(config, &mut ldap, &user_dn).await?;
    let roles = map_groups_to_roles(config, &groups);

    let _ = ldap.unbind().await;

    // JIT provision user
    let user = provision_user(kv, username, roles).await?;
    Ok(Some(user))
}

#[async_trait::async_trait]
impl AuthBackend for LdapAuthBackend {
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> anyhow::Result<Option<UserRecord>> {
        // Read current LDAP config snapshot
        let config_guard = self.ldap_config.load();
        let config = match config_guard.as_ref() {
            Some(cfg) => cfg.clone(),
            None => {
                // No LDAP config — fall back to builtin
                return self.builtin.authenticate(username, password).await;
            }
        };
        drop(config_guard);

        let fallback = config.fallback_to_builtin;

        match ldap_authenticate(&config, self.kv.as_ref(), username, password).await {
            Ok(Some(user)) => Ok(Some(user)),
            Ok(None) => {
                if fallback {
                    self.builtin.authenticate(username, password).await
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "LDAP authentication error, attempting fallback");
                if fallback {
                    self.builtin.authenticate(username, password).await
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn check_permission(
        &self,
        username: &str,
        repo: &str,
        required: Capability,
    ) -> Result<(), DepotError> {
        self.builtin
            .check_permission(username, repo, required)
            .await
    }

    async fn require_admin(&self, username: &str) -> Result<(), DepotError> {
        self.builtin.require_admin(username).await
    }

    async fn resolve_user_roles(
        &self,
        username: &str,
    ) -> Result<Option<(UserRecord, Vec<RoleRecord>)>, DepotError> {
        self.builtin.resolve_user_roles(username).await
    }

    async fn lookup_user(&self, username: &str) -> Result<Option<UserRecord>, DepotError> {
        self.builtin.lookup_user(username).await
    }

    fn invalidate_user(&self, _username: &str) {}
    fn invalidate_role(&self, _name: &str) {}
    fn invalidate_all(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> LdapConfig {
        LdapConfig {
            url: "ldap://localhost".into(),
            bind_dn: "cn=admin".into(),
            bind_password: "secret".into(),
            user_base_dn: "ou=people,dc=test".into(),
            user_filter: "(uid={username})".into(),
            group_base_dn: "ou=groups,dc=test".into(),
            group_filter: "(member={dn})".into(),
            group_name_attr: "cn".into(),
            group_role_mapping: Default::default(),
            starttls: false,
            tls_skip_verify: false,
            fallback_to_builtin: true,
        }
    }

    #[test]
    fn test_map_groups_to_roles_basic() {
        let mut config = test_config();
        config.group_role_mapping = [
            ("depot-admins".to_string(), "admin".to_string()),
            ("depot-readers".to_string(), "read-only".to_string()),
        ]
        .into_iter()
        .collect();

        // Matching groups
        let roles = map_groups_to_roles(
            &config,
            &[
                "depot-admins".to_string(),
                "depot-readers".to_string(),
                "unrelated-group".to_string(),
            ],
        );
        assert_eq!(roles, vec!["admin", "read-only"]);

        // No matching groups
        let roles = map_groups_to_roles(&config, &["other".to_string()]);
        assert!(roles.is_empty());

        // Empty groups
        let roles = map_groups_to_roles(&config, &[]);
        assert!(roles.is_empty());
    }

    #[test]
    fn test_map_groups_deduplicates() {
        let mut config = test_config();
        config.group_role_mapping = [
            ("group-a".to_string(), "admin".to_string()),
            ("group-b".to_string(), "admin".to_string()),
        ]
        .into_iter()
        .collect();

        let roles = map_groups_to_roles(&config, &["group-a".to_string(), "group-b".to_string()]);
        assert_eq!(roles, vec!["admin"]);
    }

    #[test]
    fn test_filter_interpolation() {
        let filter = "(uid={username})";
        let result = filter.replace("{username}", &ldap3::ldap_escape("john.doe"));
        assert_eq!(result, "(uid=john.doe)");

        // Special characters are escaped
        let result = filter.replace("{username}", &ldap3::ldap_escape("user*()\\"));
        assert!(result.contains("\\2a")); // * escaped
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_jit_provision_creates_user() {
        let tmp = crate::server::infra::test_support::test_tempdir();
        let kv: Arc<dyn KvStore> = Arc::new(
            depot_kv_redb::ShardedRedbKvStore::open(&tmp.path().join("kv"), 2, 0)
                .await
                .unwrap(),
        );

        let user = provision_user(kv.as_ref(), "ldap-user", vec!["read-only".to_string()])
            .await
            .unwrap();

        assert_eq!(user.username, "ldap-user");
        assert_eq!(user.auth_source, "ldap");
        assert_eq!(user.password_hash, EXTERNAL_PASSWORD_HASH);
        assert_eq!(user.roles, vec!["read-only"]);

        // Verify it's in KvStore
        let stored = service::with_read_txn(kv.as_ref(), async |txn| {
            service::get_user(txn, "ldap-user").await
        })
        .await
        .unwrap();
        assert!(stored.is_some());
        assert_eq!(stored.unwrap().auth_source, "ldap");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_jit_provision_updates_roles() {
        let tmp = crate::server::infra::test_support::test_tempdir();
        let kv: Arc<dyn KvStore> = Arc::new(
            depot_kv_redb::ShardedRedbKvStore::open(&tmp.path().join("kv"), 2, 0)
                .await
                .unwrap(),
        );

        // First provision
        provision_user(kv.as_ref(), "ldap-user2", vec!["read-only".to_string()])
            .await
            .unwrap();

        // Re-provision with different roles
        let user = provision_user(kv.as_ref(), "ldap-user2", vec!["admin".to_string()])
            .await
            .unwrap();

        assert_eq!(user.roles, vec!["admin"]);
    }

    #[test]
    fn test_ldap_config_handle_swap() {
        let handle = LdapConfigHandle::new(None);
        assert!(handle.load().is_none());

        handle.store(Some(test_config()));
        assert!(handle.load().is_some());

        handle.store(None);
        assert!(handle.load().is_none());
    }
}

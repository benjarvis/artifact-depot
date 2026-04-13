// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use moka::sync::Cache as MokaCache;

use crate::server::auth::{dummy_hash, verify_password};
use depot_core::error::{DepotError, Retryability};
use depot_core::service;
use depot_core::store::kv::KvStore;
use depot_core::store::kv::{Capability, RoleRecord, UserRecord};

/// Async trait abstracting authentication and authorization.
#[async_trait::async_trait]
pub trait AuthBackend: Send + Sync {
    /// Verify credentials. Returns UserRecord on success, None if user not found
    /// or wrong password.
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> anyhow::Result<Option<UserRecord>>;

    /// Check whether user has required capability on repo.
    async fn check_permission(
        &self,
        username: &str,
        repo: &str,
        required: Capability,
    ) -> Result<(), DepotError>;

    /// Check whether user has the "admin" role.
    async fn require_admin(&self, username: &str) -> Result<(), DepotError>;

    /// Fetch user + all resolved roles. For batch permission checks (catalog, list_repos).
    async fn resolve_user_roles(
        &self,
        username: &str,
    ) -> Result<Option<(UserRecord, Vec<RoleRecord>)>, DepotError>;

    /// Fetch user record by username (cached when available).
    /// Used by JWT validation to retrieve the per-user token secret.
    async fn lookup_user(&self, username: &str) -> Result<Option<UserRecord>, DepotError>;

    /// Cache invalidation hook -- remove cached user entry.
    fn invalidate_user(&self, username: &str);

    /// Cache invalidation hook -- remove cached role entry.
    fn invalidate_role(&self, name: &str);

    /// Cache invalidation hook -- clear all cached entries.
    fn invalidate_all(&self);
}

pub use depot_core::auth::check_grants;

/// Check whether the user has the "admin" role.
pub fn is_admin(user: &UserRecord) -> bool {
    user.roles.iter().any(|r| r == "admin")
}

// =============================================================================
// BuiltinAuthBackend -- delegates to KvStore
// =============================================================================

pub struct BuiltinAuthBackend {
    kv: std::sync::Arc<dyn KvStore>,
}

impl BuiltinAuthBackend {
    pub fn new(kv: std::sync::Arc<dyn KvStore>) -> Self {
        Self { kv }
    }
}

#[async_trait::async_trait]
impl AuthBackend for BuiltinAuthBackend {
    #[tracing::instrument(
        level = "debug",
        name = "resolve_identity",
        skip(self, password),
        fields(username)
    )]
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> anyhow::Result<Option<UserRecord>> {
        let uname = username.to_string();
        let (user, hash) = match service::get_user(&*self.kv, &uname).await? {
            Some(u) => {
                let h = u.password_hash.clone();
                (Some(u), h)
            }
            None => (None, dummy_hash().to_string()),
        };

        let valid = verify_password(password.to_string(), hash).await;
        Ok(if valid { user } else { None })
    }

    #[tracing::instrument(
        level = "debug",
        name = "check_permission",
        skip(self),
        fields(username, repo)
    )]
    async fn check_permission(
        &self,
        username: &str,
        repo: &str,
        required: Capability,
    ) -> Result<(), DepotError> {
        let uname = username.to_string();
        let (_, roles) = service::get_user_with_roles(&*self.kv, &uname)
            .await
            .map_err(|e| DepotError::Storage(Box::new(e), Retryability::Permanent))?
            .ok_or_else(|| DepotError::Forbidden("access denied".into()))?;

        check_grants(&roles, repo, required)
    }

    async fn require_admin(&self, username: &str) -> Result<(), DepotError> {
        let uname = username.to_string();
        let user = service::get_user(&*self.kv, &uname)
            .await
            .map_err(|e| DepotError::Storage(Box::new(e), Retryability::Permanent))?
            .ok_or_else(|| DepotError::Forbidden("access denied".into()))?;

        if is_admin(&user) {
            Ok(())
        } else {
            Err(DepotError::Forbidden("access denied".into()))
        }
    }

    async fn resolve_user_roles(
        &self,
        username: &str,
    ) -> Result<Option<(UserRecord, Vec<RoleRecord>)>, DepotError> {
        let uname = username.to_string();
        service::get_user_with_roles(&*self.kv, &uname)
            .await
            .map_err(|e| DepotError::Storage(Box::new(e), Retryability::Permanent))
    }

    #[tracing::instrument(
        level = "debug",
        name = "resolve_identity",
        skip(self),
        fields(username)
    )]
    async fn lookup_user(&self, username: &str) -> Result<Option<UserRecord>, DepotError> {
        let uname = username.to_string();
        service::get_user(&*self.kv, &uname)
            .await
            .map_err(|e| DepotError::Storage(Box::new(e), Retryability::Permanent))
    }

    fn invalidate_user(&self, _username: &str) {}
    fn invalidate_role(&self, _name: &str) {}
    fn invalidate_all(&self) {}
}

// =============================================================================
// CachedAuthBackend -- decorator with TTL cache (moka)
// =============================================================================

fn build_cache<V: Clone + Send + Sync + 'static>(ttl: Duration) -> MokaCache<String, Option<V>> {
    MokaCache::builder()
        .max_capacity(1024)
        .time_to_live(ttl)
        .build()
}

pub struct CachedAuthBackend<T: AuthBackend> {
    inner: T,
    user_cache: MokaCache<String, Option<UserRecord>>,
    role_cache: MokaCache<String, Option<RoleRecord>>,
    auth_cache: MokaCache<String, Option<VerifiedCredential>>,
}

impl<T: AuthBackend> CachedAuthBackend<T> {
    pub fn new(inner: T, ttl: Duration) -> Self {
        Self {
            inner,
            user_cache: build_cache(ttl),
            role_cache: build_cache(ttl),
            auth_cache: build_cache(ttl),
        }
    }
}

impl<T: AuthBackend> CachedAuthBackend<T> {
    fn get_cached_user(&self, username: &str) -> Option<Option<UserRecord>> {
        self.user_cache.get(&username.to_string())
    }

    fn put_cached_user(&self, username: &str, value: Option<UserRecord>) {
        self.user_cache.insert(username.to_string(), value);
    }

    fn get_cached_role(&self, name: &str) -> Option<Option<RoleRecord>> {
        self.role_cache.get(&name.to_string())
    }

    fn put_cached_role(&self, name: &str, value: Option<RoleRecord>) {
        self.role_cache.insert(name.to_string(), value);
    }
}

/// Cached result of a successful Argon2 password verification.
/// Allows subsequent requests with the same credentials to skip Argon2.
#[derive(Clone)]
struct VerifiedCredential {
    /// The Argon2 hash from UserRecord at the time of verification.
    /// If the user's password changes, this won't match and we re-verify.
    password_hash: String,
    /// BLAKE3 digest of the plaintext password that was verified.
    password_digest: [u8; 32],
}

/// Internal helper: resolve a user from cache or delegate to inner backend's KV.
/// This is used by CachedAuthBackend methods that need a UserRecord.
async fn resolve_user_cached<T: AuthBackend>(
    cached: &CachedAuthBackend<T>,
    kv: &dyn KvStore,
    username: &str,
) -> Result<UserRecord, DepotError> {
    if let Some(maybe_user) = cached.get_cached_user(username) {
        return maybe_user.ok_or_else(|| DepotError::Forbidden("access denied".into()));
    }
    let uname = username.to_string();
    let user = service::get_user(kv, &uname)
        .await
        .map_err(|e| DepotError::Storage(Box::new(e), Retryability::Permanent))?;
    cached.put_cached_user(username, user.clone());
    user.ok_or_else(|| DepotError::Forbidden("access denied".into()))
}

/// Internal helper: resolve user + all roles, populating both caches.
/// On user cache miss, fetches user + roles in a single transaction.
async fn resolve_user_and_roles_cached<T: AuthBackend>(
    cached: &CachedAuthBackend<T>,
    kv: &dyn KvStore,
    username: &str,
) -> Result<(UserRecord, Vec<RoleRecord>), DepotError> {
    // Check user cache
    let user = match cached.get_cached_user(username) {
        Some(maybe) => maybe.ok_or_else(|| DepotError::Forbidden("access denied".into()))?,
        None => {
            // User not cached — fetch user + roles in one txn, populate both caches
            let uname = username.to_string();
            let result = service::get_user_with_roles(kv, &uname)
                .await
                .map_err(|e| DepotError::Storage(Box::new(e), Retryability::Permanent))?;
            match result {
                Some((user, roles)) => {
                    cached.put_cached_user(username, Some(user.clone()));
                    for role in &roles {
                        cached.put_cached_role(&role.name, Some(role.clone()));
                    }
                    return Ok((user, roles));
                }
                None => {
                    cached.put_cached_user(username, None);
                    return Err(DepotError::Forbidden("access denied".into()));
                }
            }
        }
    };
    // User was cached — resolve roles individually (they may also be cached)
    let mut roles = Vec::with_capacity(user.roles.len());
    let mut uncached: Vec<String> = Vec::new();
    for role_name in &user.roles {
        match cached.get_cached_role(role_name) {
            Some(Some(role)) => roles.push(role),
            Some(None) => {} // negatively cached
            None => uncached.push(role_name.clone()),
        }
    }
    if !uncached.is_empty() {
        // Fetch all uncached roles
        let fetched = {
            let mut result = Vec::new();
            for name in &uncached {
                let role = service::get_role(kv, name)
                    .await
                    .map_err(|e| DepotError::Storage(Box::new(e), Retryability::Permanent))?;
                result.push((name.clone(), role));
            }
            result
        };
        for (name, role) in fetched {
            cached.put_cached_role(&name, role.clone());
            if let Some(r) = role {
                roles.push(r);
            }
        }
    }
    Ok((user, roles))
}

/// Trait for auth backends that expose a KvStore reference.
/// Needed by `CachedAuthBackend` for direct user/role lookups.
pub trait HasKvStore {
    fn kv(&self) -> &dyn KvStore;
}

impl HasKvStore for BuiltinAuthBackend {
    fn kv(&self) -> &dyn KvStore {
        self.kv.as_ref()
    }
}

#[cfg(feature = "ldap")]
impl HasKvStore for crate::server::auth::ldap::LdapAuthBackend {
    fn kv(&self) -> &dyn KvStore {
        self.kv.as_ref()
    }
}

impl<T: AuthBackend + HasKvStore> CachedAuthBackend<T> {
    fn kv(&self) -> &dyn KvStore {
        self.inner.kv()
    }
}

#[async_trait::async_trait]
impl<T: AuthBackend + HasKvStore> AuthBackend for CachedAuthBackend<T> {
    #[tracing::instrument(
        level = "debug",
        name = "resolve_identity",
        skip(self, password),
        fields(username)
    )]
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> anyhow::Result<Option<UserRecord>> {
        // Use cached UserRecord for hash lookup.
        let (user, hash) = if let Some(maybe_user) = self.get_cached_user(username) {
            match maybe_user {
                Some(u) => {
                    let h = u.password_hash.clone();
                    (Some(u), h)
                }
                None => (None, dummy_hash().to_string()),
            }
        } else {
            let uname = username.to_string();
            let fetched = service::get_user(self.kv(), &uname).await?;
            self.put_cached_user(username, fetched.clone());
            match fetched {
                Some(u) => {
                    let h = u.password_hash.clone();
                    (Some(u), h)
                }
                None => (None, dummy_hash().to_string()),
            }
        };

        // For known users, check the verification cache to skip Argon2.
        if user.is_some() {
            let digest: [u8; 32] = blake3::hash(password.as_bytes()).into();
            let cache_hit = self
                .auth_cache
                .get(&username.to_string())
                .flatten()
                .is_some_and(|v| v.password_hash == hash && v.password_digest == digest);
            if cache_hit {
                return Ok(user);
            }
        }

        let valid = verify_password(password.to_string(), hash.clone()).await;
        if valid {
            // Cache the successful verification so subsequent calls skip Argon2.
            if user.is_some() {
                let digest: [u8; 32] = blake3::hash(password.as_bytes()).into();
                self.auth_cache.insert(
                    username.to_string(),
                    Some(VerifiedCredential {
                        password_hash: hash,
                        password_digest: digest,
                    }),
                );
            }
            Ok(user)
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(
        level = "debug",
        name = "check_permission",
        skip(self),
        fields(username, repo)
    )]
    async fn check_permission(
        &self,
        username: &str,
        repo: &str,
        required: Capability,
    ) -> Result<(), DepotError> {
        let (_, roles) = resolve_user_and_roles_cached(self, self.kv(), username).await?;
        check_grants(&roles, repo, required)
    }

    async fn require_admin(&self, username: &str) -> Result<(), DepotError> {
        let user = resolve_user_cached(self, self.kv(), username).await?;
        if is_admin(&user) {
            Ok(())
        } else {
            Err(DepotError::Forbidden("access denied".into()))
        }
    }

    async fn resolve_user_roles(
        &self,
        username: &str,
    ) -> Result<Option<(UserRecord, Vec<RoleRecord>)>, DepotError> {
        match resolve_user_and_roles_cached(self, self.kv(), username).await {
            Ok(pair) => Ok(Some(pair)),
            Err(DepotError::Forbidden(_)) => {
                // User not found (negative cache or missing) — return None
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    #[tracing::instrument(
        level = "debug",
        name = "resolve_identity",
        skip(self),
        fields(username)
    )]
    async fn lookup_user(&self, username: &str) -> Result<Option<UserRecord>, DepotError> {
        match resolve_user_cached(self, self.kv(), username).await {
            Ok(user) => Ok(Some(user)),
            Err(DepotError::Forbidden(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn invalidate_user(&self, username: &str) {
        self.user_cache.invalidate(&username.to_string());
        self.auth_cache.invalidate(&username.to_string());
    }

    fn invalidate_role(&self, name: &str) {
        self.role_cache.invalidate(&name.to_string());
    }

    fn invalidate_all(&self) {
        self.user_cache.invalidate_all();
        self.role_cache.invalidate_all();
        self.auth_cache.invalidate_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use depot_core::store::kv::{CapabilityGrant, RoleRecord, UserRecord, CURRENT_RECORD_VERSION};

    fn make_user(username: &str, roles: Vec<&str>) -> UserRecord {
        UserRecord {
            schema_version: CURRENT_RECORD_VERSION,
            username: username.to_string(),
            password_hash: String::new(),
            roles: roles.into_iter().map(|s| s.to_string()).collect(),
            auth_source: "builtin".to_string(),
            token_secret: vec![],
            must_change_password: false,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    fn make_role(name: &str, grants: Vec<(Capability, &str)>) -> RoleRecord {
        RoleRecord {
            schema_version: CURRENT_RECORD_VERSION,
            name: name.to_string(),
            description: String::new(),
            capabilities: grants
                .into_iter()
                .map(|(cap, repo)| CapabilityGrant {
                    capability: cap,
                    repo: repo.to_string(),
                })
                .collect(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_check_grants_wildcard() {
        let roles = vec![make_role("admin", vec![(Capability::Read, "*")])];
        assert!(check_grants(&roles, "my-repo", Capability::Read).is_ok());
    }

    #[test]
    fn test_check_grants_specific_repo() {
        let roles = vec![make_role("dev", vec![(Capability::Write, "my-repo")])];
        assert!(check_grants(&roles, "my-repo", Capability::Write).is_ok());
        assert!(check_grants(&roles, "other-repo", Capability::Write).is_err());
    }

    #[test]
    fn test_check_grants_wrong_capability() {
        let roles = vec![make_role("reader", vec![(Capability::Read, "*")])];
        assert!(check_grants(&roles, "any", Capability::Write).is_err());
    }

    #[test]
    fn test_check_grants_empty_roles() {
        let roles: Vec<RoleRecord> = vec![];
        assert!(check_grants(&roles, "repo", Capability::Read).is_err());
    }

    #[test]
    fn test_check_grants_multiple_roles() {
        let roles = vec![
            make_role("reader", vec![(Capability::Read, "*")]),
            make_role("writer", vec![(Capability::Write, "prod")]),
        ];
        assert!(check_grants(&roles, "prod", Capability::Read).is_ok());
        assert!(check_grants(&roles, "prod", Capability::Write).is_ok());
        assert!(check_grants(&roles, "dev", Capability::Write).is_err());
    }

    #[test]
    fn test_is_admin_true() {
        let user = make_user("alice", vec!["admin", "dev"]);
        assert!(is_admin(&user));
    }

    #[test]
    fn test_is_admin_false() {
        let user = make_user("bob", vec!["reader"]);
        assert!(!is_admin(&user));
    }

    #[test]
    fn test_is_admin_empty_roles() {
        let user = make_user("anon", vec![]);
        assert!(!is_admin(&user));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cached_backend_cache_hit_and_expiry() {
        use depot_kv_redb::ShardedRedbKvStore;

        let tmp = crate::server::infra::test_support::test_tempdir();
        let db_path = tmp.path().join("kv");
        let kv: std::sync::Arc<dyn KvStore> =
            std::sync::Arc::new(ShardedRedbKvStore::open(&db_path, 2, 0).await.unwrap());

        let now = chrono::Utc::now();
        let user_rec = UserRecord {
            schema_version: CURRENT_RECORD_VERSION,
            username: "alice".to_string(),
            password_hash: String::new(),
            roles: vec!["admin".to_string()],
            auth_source: "builtin".to_string(),
            token_secret: vec![],
            must_change_password: false,
            created_at: now,
            updated_at: now,
        };
        service::put_user(kv.as_ref(), &user_rec.clone())
            .await
            .unwrap();
        let role_rec = RoleRecord {
            schema_version: CURRENT_RECORD_VERSION,
            name: "admin".to_string(),
            description: String::new(),
            capabilities: vec![CapabilityGrant {
                capability: Capability::Read,
                repo: "*".to_string(),
            }],
            created_at: now,
            updated_at: now,
        };
        service::put_role(kv.as_ref(), &role_rec.clone())
            .await
            .unwrap();

        let backend =
            CachedAuthBackend::new(BuiltinAuthBackend::new(kv.clone()), Duration::from_secs(5));

        // First call populates cache.
        assert!(backend
            .check_permission("alice", "repo", Capability::Read)
            .await
            .is_ok());

        // Should be cached now.
        assert!(backend.get_cached_user("alice").is_some());
        assert!(backend.get_cached_role("admin").is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cached_backend_invalidation() {
        use depot_kv_redb::ShardedRedbKvStore;

        let tmp = crate::server::infra::test_support::test_tempdir();
        let db_path = tmp.path().join("kv");
        let kv: std::sync::Arc<dyn KvStore> =
            std::sync::Arc::new(ShardedRedbKvStore::open(&db_path, 2, 0).await.unwrap());

        let now = chrono::Utc::now();
        let user_rec = UserRecord {
            schema_version: CURRENT_RECORD_VERSION,
            username: "bob".to_string(),
            password_hash: String::new(),
            roles: vec!["dev".to_string()],
            auth_source: "builtin".to_string(),
            token_secret: vec![],
            must_change_password: false,
            created_at: now,
            updated_at: now,
        };
        service::put_user(kv.as_ref(), &user_rec.clone())
            .await
            .unwrap();
        let role_rec = RoleRecord {
            schema_version: CURRENT_RECORD_VERSION,
            name: "dev".to_string(),
            description: String::new(),
            capabilities: vec![CapabilityGrant {
                capability: Capability::Read,
                repo: "*".to_string(),
            }],
            created_at: now,
            updated_at: now,
        };
        service::put_role(kv.as_ref(), &role_rec.clone())
            .await
            .unwrap();

        let backend = CachedAuthBackend::new(BuiltinAuthBackend::new(kv), Duration::from_secs(60));

        // Populate cache.
        backend
            .check_permission("bob", "repo", Capability::Read)
            .await
            .unwrap();
        assert!(backend.get_cached_user("bob").is_some());
        assert!(backend.get_cached_role("dev").is_some());

        // Invalidate user.
        backend.invalidate_user("bob");
        assert!(backend.get_cached_user("bob").is_none());

        // Invalidate role.
        backend.invalidate_role("dev");
        assert!(backend.get_cached_role("dev").is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cached_backend_negative_cache() {
        use depot_kv_redb::ShardedRedbKvStore;

        let tmp = crate::server::infra::test_support::test_tempdir();
        let db_path = tmp.path().join("kv");
        let kv: std::sync::Arc<dyn KvStore> =
            std::sync::Arc::new(ShardedRedbKvStore::open(&db_path, 2, 0).await.unwrap());

        let backend = CachedAuthBackend::new(BuiltinAuthBackend::new(kv), Duration::from_secs(5));

        // User doesn't exist -- should cache None.
        assert!(backend
            .check_permission("ghost", "repo", Capability::Read)
            .await
            .is_err());

        // Negative entry should be cached.
        let cached = backend.get_cached_user("ghost");
        assert!(cached.is_some()); // entry exists
        assert!(cached.unwrap().is_none()); // but value is None
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_resolve_user_roles() {
        use depot_kv_redb::ShardedRedbKvStore;

        let tmp = crate::server::infra::test_support::test_tempdir();
        let db_path = tmp.path().join("kv");
        let kv: std::sync::Arc<dyn KvStore> =
            std::sync::Arc::new(ShardedRedbKvStore::open(&db_path, 2, 0).await.unwrap());

        let now = chrono::Utc::now();
        let user_rec = UserRecord {
            schema_version: CURRENT_RECORD_VERSION,
            username: "carol".to_string(),
            password_hash: String::new(),
            roles: vec!["admin".to_string(), "dev".to_string()],
            auth_source: "builtin".to_string(),
            token_secret: vec![],
            must_change_password: false,
            created_at: now,
            updated_at: now,
        };
        service::put_user(kv.as_ref(), &user_rec.clone())
            .await
            .unwrap();
        let admin_role = RoleRecord {
            schema_version: CURRENT_RECORD_VERSION,
            name: "admin".to_string(),
            description: String::new(),
            capabilities: vec![CapabilityGrant {
                capability: Capability::Read,
                repo: "*".to_string(),
            }],
            created_at: now,
            updated_at: now,
        };
        service::put_role(kv.as_ref(), &admin_role.clone())
            .await
            .unwrap();
        let dev_role = RoleRecord {
            schema_version: CURRENT_RECORD_VERSION,
            name: "dev".to_string(),
            description: String::new(),
            capabilities: vec![CapabilityGrant {
                capability: Capability::Write,
                repo: "dev-repo".to_string(),
            }],
            created_at: now,
            updated_at: now,
        };
        service::put_role(kv.as_ref(), &dev_role.clone())
            .await
            .unwrap();

        let backend = CachedAuthBackend::new(BuiltinAuthBackend::new(kv), Duration::from_secs(5));

        let result = backend.resolve_user_roles("carol").await.unwrap();
        assert!(result.is_some());
        let (user, roles) = result.unwrap();
        assert_eq!(user.username, "carol");
        assert_eq!(roles.len(), 2);

        // Nonexistent user.
        let result = backend.resolve_user_roles("nobody").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cached_backend_auth_verification_cache() {
        use crate::server::auth::hash_password;
        use depot_kv_redb::ShardedRedbKvStore;

        let tmp = crate::server::infra::test_support::test_tempdir();
        let db_path = tmp.path().join("kv");
        let kv: std::sync::Arc<dyn KvStore> =
            std::sync::Arc::new(ShardedRedbKvStore::open(&db_path, 2, 0).await.unwrap());

        let now = chrono::Utc::now();
        let pw_hash = hash_password("secret".to_string()).await.unwrap();
        let user_rec = UserRecord {
            schema_version: CURRENT_RECORD_VERSION,
            username: "eve".to_string(),
            password_hash: pw_hash,
            roles: vec!["admin".to_string()],
            auth_source: "builtin".to_string(),
            token_secret: vec![],
            must_change_password: false,
            created_at: now,
            updated_at: now,
        };
        service::put_user(kv.as_ref(), &user_rec.clone())
            .await
            .unwrap();

        let backend =
            CachedAuthBackend::new(BuiltinAuthBackend::new(kv.clone()), Duration::from_secs(60));

        // First authenticate — runs Argon2, populates verification cache.
        let result = backend.authenticate("eve", "secret").await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().username, "eve");

        // Verification cache should have an entry now.
        assert!(backend.auth_cache.contains_key(&"eve".to_string()));

        // Second authenticate — should hit verification cache (skip Argon2).
        let result = backend.authenticate("eve", "secret").await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().username, "eve");

        // Wrong password should fail (not served from cache).
        let result = backend.authenticate("eve", "wrong").await.unwrap();
        assert!(result.is_none());

        // Invalidate user clears the verification cache.
        backend.invalidate_user("eve");
        assert!(!backend.auth_cache.contains_key(&"eve".to_string()));

        // Unknown user should not get a verification cache entry.
        let result = backend.authenticate("ghost", "secret").await.unwrap();
        assert!(result.is_none());
        assert!(!backend.auth_cache.contains_key(&"ghost".to_string()));
    }
}

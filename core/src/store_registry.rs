// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Registry of named blob stores, backed by ArcSwap for wait-free reads.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::store::blob::BlobStore;

/// Thread-safe registry mapping store names to instantiated BlobStore backends.
///
/// Reads are wait-free via `ArcSwap::load()`. Writes clone-mutate-swap the
/// inner map, which is fine because stores are added/removed only via admin API.
pub struct StoreRegistry {
    map: ArcSwap<HashMap<String, Arc<dyn BlobStore>>>,
}

impl Default for StoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl StoreRegistry {
    pub fn new() -> Self {
        Self {
            map: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    pub async fn get(&self, name: &str) -> Option<Arc<dyn BlobStore>> {
        self.map.load().get(name).cloned()
    }

    pub async fn add(&self, name: &str, store: Arc<dyn BlobStore>) {
        let mut new_map = HashMap::clone(&self.map.load());
        new_map.insert(name.to_string(), store);
        self.map.store(Arc::new(new_map));
    }

    pub async fn remove(&self, name: &str) -> bool {
        let old = self.map.load();
        if !old.contains_key(name) {
            return false;
        }
        let mut new_map = HashMap::clone(&old);
        let removed = new_map.remove(name).is_some();
        self.map.store(Arc::new(new_map));
        removed
    }

    pub async fn list_names(&self) -> Vec<String> {
        self.map.load().keys().cloned().collect()
    }
}

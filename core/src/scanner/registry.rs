// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Name-keyed registry for scanner backends.

use std::collections::HashMap;
use std::sync::Arc;

use super::Scanner;

/// Runtime registry mapping scanner name → backend instance.
///
/// Populated at startup based on configuration. The worker loop looks up
/// backends by name when draining the scan queue so multiple backends can
/// coexist in a single deployment.
#[derive(Clone, Default)]
pub struct ScannerRegistry {
    inner: Arc<HashMap<String, Arc<dyn Scanner>>>,
}

impl ScannerRegistry {
    /// Create an empty registry. Useful for tests where no scanner is needed.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build a registry from an iterator of named backends. Later entries
    /// with duplicate names overwrite earlier ones.
    pub fn from_backends<I>(backends: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn Scanner>>,
    {
        let mut map: HashMap<String, Arc<dyn Scanner>> = HashMap::new();
        for b in backends {
            map.insert(b.name().to_string(), b);
        }
        Self {
            inner: Arc::new(map),
        }
    }

    /// Look up a scanner by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn Scanner>> {
        self.inner.get(name).cloned()
    }

    /// Return the single registered scanner when exactly one is configured;
    /// otherwise `None`. Callers should prefer [`get`](Self::get) once
    /// multi-scanner deployments are supported.
    pub fn only(&self) -> Option<Arc<dyn Scanner>> {
        if self.inner.len() == 1 {
            self.inner.values().next().cloned()
        } else {
            None
        }
    }

    /// Iterate registered scanner names in unspecified order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.inner.keys().map(String::as_str)
    }

    /// Number of registered backends.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether no backends are registered.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

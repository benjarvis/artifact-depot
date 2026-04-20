---
title: Configuration
nav_order: 3
has_children: true
---

# Configuration

Depot's configuration is split into two layers:

- **[TOML config file](toml-reference.md)** — static settings read at
  startup: network listeners, KV backend, logging/tracing endpoints,
  TLS, LDAP, and the optional `[initialization]` section for
  self-contained first-boot provisioning.

- **[Runtime settings](runtime-settings.md)** — operational knobs
  stored in the KV store and managed via the REST API (or the Settings
  page in the web UI). Changes propagate to every cluster instance
  within 30 seconds. Covers upload limits, GC intervals, Docker
  defaults, CORS, rate limiting, JWT lifetimes, and blob store
  management.

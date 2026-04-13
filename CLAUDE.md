# CLAUDE.md

## Project Overview

Artifact Depot is a Rust-native, scale-out artifact repository manager. It replaces Sonatype Nexus at Quantum. It ships two binaries: `depot` (the server) and `depot-bench` (demo seeding and benchmarking).

## Build & Run

Run `make` to lint, build, and test everything. This must succeed cleanly without warnings to declare success. The Vue frontend (`depot/frontend/`) is built automatically by `build.rs`, so just `make` is sufficient.

We are supporting production installations so any on-disk change to the KV store records or semantic changes in the blob store need to be called out clearly and upgrade and migration needs to be considered.

Since the UI frontend is served by the backend server, backwards compatibility between the API and UI is not a concern.  However, breaking changes to the OpenAPI APIs need to called out and should be avoided when possible.

If your task is to implement a feature, always do your utmost to make sure the new functionality is fully covered by the built in automated tests!   
If your task is to fix a bug found elsewhere, ask why it wasn't detected by the tests!  If possible, fix the tests to reproduce the issue first, make sure coverage of the relevant area is complete, and then establish root cause.

These tests can run depot end to end, inclusive of the UI via playwright.  We run in a privileged docker container and can do much more than just run cargo test.  Tests can take the form of rust native test code and also arbitrary
third party applications running in network namespaces and/or dockerfiles.   Use dockerfiles extensively for format compatibility tests.  Compatibility with the applications that use the repository format is critical.

## Architecture

The workspace has two crates: `depot/` (the server) and `depot-bench/` (benchmarks and demo data seeding, depends on `depot`).

### Artifact Formats

Hosted, cache (write-through proxy), and group (virtual) repository types are supported for: Raw, Docker, PyPI, APT, Go modules, Helm, Cargo, Yum, and NPM.

### Storage

- **KV store** (`KvStore` trait): redb for single-node, DynamoDB for distributed deployments. String keys, JSON-serialized values.
- **Blob store** (`BlobStore` trait): local filesystem or S3. Content-addressable with BLAKE3 hashing. Blobs are deduplicated by content hash.
- **Dual hash for Docker**: BLAKE3 is the blob store key; SHA-256 digests (required by the Docker/OCI protocol) are stored as metadata.

### Consistency Model

The system is **eventually consistent by default**. Regular KV reads may return stale values. Strong consistency (`get_consistent()`) is used only for coordination paths: distributed leases, version-checked updates (`get_versioned()` + `put_if_version()` for optimistic concurrency), and the check-pause protocol. The redb backend is inherently strongly consistent (single-node); the DynamoDB backend uses eventually consistent reads by default and strongly consistent reads where required.

### Distributed Coordination

- **Instance registry** with heartbeat-based liveness detection (30s heartbeat, 90s timeout).
- **Distributed leases** for at-most-one semantics (e.g., only one instance runs blob GC).
- **Check-pause protocol** pauses cleanup/reaper across all instances during integrity checks, with automatic stale-pause clearing if the initiator crashes.
- **Blob GC** uses a two-pass grace period: unreferenced blobs are candidates in pass N and deleted in pass N+2, tolerating in-flight ingestions that race with KV deletion.

### Docker Registry

Docker V2 / OCI Distribution Spec. Supports Docker V2, Docker manifest list (multi-arch), OCI, and OCI index manifest types. Routes come in single-segment (`/v2/{name}/...`) and two-segment (`/v2/{repo}/{image}/...`) flavors. No body size limit on upload routes. Chunked upload state is tracked in-memory.

### Vue Frontend

Embedded via rust-embed. Provides repository, user, role, and store management, audit log, API docs, and settings.

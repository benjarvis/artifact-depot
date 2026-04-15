---
title: Home
nav_order: 1
---

# Artifact Depot Documentation

**Artifact Depot** is a safe-Rust, scale-out artifact repository manager.
It is a modern replacement for Sonatype Nexus and JFrog Artifactory at
datacenter scale, but it is equally well suited as an edge-deployed
artifact cache in CI pipelines and dev containers, or as an in-cluster
Docker cache alongside a Kubernetes control plane. It has no SQL
dependency -- it requires only eventually consistent key-value semantics,
so it is web-scale capable out of the box. Depot ships as a single
statically-linked binary or a rootless `FROM scratch` Docker image, with
the web UI embedded in the binary.

## Reference docs

- [Architecture](architecture.md) -- storage, consistency model,
  clustering, GC, audit, observability, and auth.
- [Configuration](configuration.md) -- TOML config reference, runtime
  settings, and blob store management.
- [Usage](usage.md) -- CLI reference, supported formats, REST API, and
  benchmarking.

## Source

The site you are reading is built with [Jekyll] and the
[just-the-docs] theme from the markdown under
[`docs/`](https://github.com/artifact-depot/artifact-depot/tree/main/docs).
The "Edit this page on GitHub" link in the top-right of every page jumps
straight to the source.

[Jekyll]: https://jekyllrb.com/
[just-the-docs]: https://just-the-docs.com/

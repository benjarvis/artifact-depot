#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
#
# SPDX-License-Identifier: Apache-2.0

# Shared helpers for network namespace management in test scripts.
# Source this file; do not execute it directly.

# make_netns_name <prefix>
# Returns a unique namespace name by appending the caller's PID.
# PID suffix enables orphan detection by sweep_leaked_namespaces.
make_netns_name() {
  echo "${1}-$$"
}

# sweep_leaked_namespaces
# Deletes any depot-*-<pid> namespaces whose owning PID is dead.
sweep_leaked_namespaces() {
  local ns pid
  while IFS= read -r ns; do
    # Extract trailing numeric PID suffix
    pid="${ns##*-}"
    # Skip if suffix isn't a number
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    # If the PID is dead, remove the leaked namespace
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "Sweeping leaked namespace: $ns (PID $pid dead)"
      ip netns del "$ns" 2>/dev/null || true
    fi
  done < <(ip netns list 2>/dev/null | awk '/^depot-/{print $1}')
}

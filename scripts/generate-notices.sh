#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
#
# SPDX-License-Identifier: Apache-2.0

#
# Combine Rust and npm license JSON into a single THIRD-PARTY-NOTICES file.
# Expects target/cargo-about.json and target/npm-licenses.json to exist
# (produced by generate-notices-cargo.sh and generate-notices-npm.sh).
# Output: target/THIRD-PARTY-NOTICES  (also copied to ui/frontend/public/)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TARGET="$ROOT/target"
PUBLIC="$ROOT/ui/frontend/public"

mkdir -p "$TARGET" "$PUBLIC"

# Skip if neither input changed since last run.
SENTINEL="$TARGET/.notices-sentinel"
if [ -f "$SENTINEL" ] && [ -f "$PUBLIC/THIRD-PARTY-NOTICES" ]; then
  up_to_date=true
  for f in "$TARGET/cargo-about.json" "$TARGET/npm-licenses.json"; do
    if [ "$f" -nt "$SENTINEL" ]; then
      up_to_date=false
      break
    fi
  done
  if $up_to_date; then
    exit 0
  fi
fi

python3 - "$TARGET/cargo-about.json" "$TARGET/npm-licenses.json" > "$TARGET/THIRD-PARTY-NOTICES" << 'PYTHON'
import json, sys, textwrap

SEP = "=" * 70
THIN = "-" * 70

rust_path, npm_path = sys.argv[1], sys.argv[2]

print("THIRD-PARTY SOFTWARE NOTICES AND INFORMATION")
print()
print("Artifact Depot incorporates third-party software components.")
print("The following notices are provided in compliance with the license")
print("terms of each component.")
print()
print(SEP)

# --- Rust crates ---
with open(rust_path) as f:
    data = json.load(f)

# Build a map from crate id (string) to license texts
license_map = {}
for lic in data["licenses"]:
    for crate_info in lic.get("used_by", []):
        crate_id = crate_info["crate"]["id"]
        if crate_id not in license_map:
            license_map[crate_id] = []
        license_map[crate_id].append({
            "name": lic["name"],
            "id": lic["id"],
            "text": lic.get("text", ""),
        })

# Emit per-crate notices
for crate in sorted(data["crates"], key=lambda c: c["package"]["name"].lower()):
    pkg = crate["package"]
    name = pkg["name"]
    version = pkg["version"]
    spdx = crate.get("license", "UNKNOWN")

    if name in ("depot", "depot-bench"):
        continue

    print()
    print(f"{name} {version}")
    print(f"License: {spdx}")
    print(THIN)

    texts = license_map.get(pkg["id"], [])
    if texts:
        print(texts[0]["text"].strip())
    else:
        print(f"Licensed under {spdx}")

    print()
    print(SEP)

# --- npm packages ---
with open(npm_path) as f:
    npm_data = json.load(f)

for pkg_key in sorted(npm_data.keys(), key=str.lower):
    info = npm_data[pkg_key]
    # Skip our own package
    if pkg_key.startswith("depot-ui@"):
        continue

    licenses = info.get("licenses", "UNKNOWN")
    repository = info.get("repository", "")
    publisher = info.get("publisher", "")
    license_file = info.get("licenseFile", "")

    print()
    print(pkg_key)
    print(f"License: {licenses}")
    if publisher:
        print(f"Author: {publisher}")
    if repository:
        print(f"Repository: {repository}")
    print(THIN)

    # Try to read the license file
    if license_file:
        try:
            with open(license_file) as lf:
                print(lf.read().strip())
        except (OSError, IOError):
            print(f"Licensed under {licenses}")
    else:
        print(f"Licensed under {licenses}")

    print()
    print(SEP)

PYTHON

# Copy to frontend public dir so Vite includes it in dist/
# Only copy when content changed to avoid triggering build.rs rebuild.
if ! cmp -s "$TARGET/THIRD-PARTY-NOTICES" "$PUBLIC/THIRD-PARTY-NOTICES"; then
  cp "$TARGET/THIRD-PARTY-NOTICES" "$PUBLIC/THIRD-PARTY-NOTICES"
fi

touch "$SENTINEL"
echo "Generated target/THIRD-PARTY-NOTICES ($(wc -l < "$TARGET/THIRD-PARTY-NOTICES") lines)"

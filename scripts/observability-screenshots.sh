#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
#
# SPDX-License-Identifier: Apache-2.0

# Bring up the docker/standalone compose with the `monitoring` profile,
# seed depot with `depot-bench demo`, spawn `depot-bench trickle` for
# live traffic, wait for dashboards to populate, then capture:
#
#   - Dashboard PNGs via Grafana's server-side Image Renderer (curl)
#   - Loki logs + Tempo traces via Playwright (Explore pages)
#
# Outputs land in docs/screenshots/observability/ ready to commit.
#
# Usage:
#   bash scripts/observability-screenshots.sh              # builds everything
#   bash scripts/observability-screenshots.sh --skip-build # if already built

set -euo pipefail

SKIP_BUILD=false
for arg in "$@"; do
  case "$arg" in
    --skip-build) SKIP_BUILD=true ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_DIR="$ROOT/docker/standalone"
FRONTEND_DIR="$ROOT/ui/frontend"
BUILD_DIR="$ROOT/build/observability-screenshots"
SHOTS_DIR="$BUILD_DIR/shots"
DOCS_OUT="$ROOT/docs/screenshots/observability"

DEPOT_URL="http://localhost:8080"
GRAFANA_URL="http://localhost:3000"

PROJECT="depot-obs-shots"
TRICKLE_PID=""

cleanup() {
  if [[ -n "${TRICKLE_PID:-}" ]]; then
    kill "$TRICKLE_PID" 2>/dev/null || true
    wait "$TRICKLE_PID" 2>/dev/null || true
  fi
  (cd "$COMPOSE_DIR" && docker-compose -p "$PROJECT" --profile monitoring down -v --remove-orphans >/dev/null 2>&1) || true
}
trap cleanup EXIT

# --- Build (unless caller already did it) ---
if [[ "$SKIP_BUILD" == "false" ]]; then
  echo "Building depot-bench..."
  cargo build -p depot-bench
fi

# --- Clean prior state ---
rm -rf "$BUILD_DIR"
mkdir -p "$SHOTS_DIR"

# --- Start the monitoring stack ---
echo "Starting docker/standalone with monitoring profile..."
(cd "$COMPOSE_DIR" && docker-compose -p "$PROJECT" --profile monitoring up -d --build)

# --- Wait for depot ---
echo "Waiting for depot..."
for i in $(seq 1 60); do
  curl -sf "${DEPOT_URL}/api/v1/health" >/dev/null 2>&1 && break
  if [ "$i" -eq 60 ]; then
    echo "ERROR: depot did not start in 120s" >&2
    (cd "$COMPOSE_DIR" && docker-compose -p "$PROJECT" logs depot | tail -40) >&2
    exit 1
  fi
  sleep 2
done
echo "  depot ready."

# --- Wait for Grafana ---
echo "Waiting for Grafana..."
for i in $(seq 1 60); do
  curl -sf "${GRAFANA_URL}/api/health" >/dev/null 2>&1 && break
  if [ "$i" -eq 60 ]; then
    echo "ERROR: grafana did not start in 120s" >&2
    exit 1
  fi
  sleep 2
done
echo "  grafana ready."

# --- Seed ---
echo "Seeding demo data..."
"$ROOT/target/debug/depot-bench" demo --url "$DEPOT_URL" > "$BUILD_DIR/demo.log" 2>&1
echo "Demo data seeded."

# --- Trickle ---
echo "Starting depot-bench trickle (background)..."
"$ROOT/target/debug/depot-bench" trickle --url "$DEPOT_URL" > "$BUILD_DIR/trickle.log" 2>&1 &
TRICKLE_PID=$!

echo "Waiting ~4 minutes for dashboards to populate..."
sleep 240

# =========================================================================
# Dashboard PNGs via Grafana Image Renderer (server-side curl)
# =========================================================================
echo "Rendering dashboard via Grafana Image Renderer..."
RENDER_BASE="${GRAFANA_URL}/render/d/depot-overview"
RENDER_ARGS="orgId=1&width=1600&height=1200&from=now-5m&to=now&timeout=60"

for theme in light dark; do
  OUT_FILE="$SHOTS_DIR/depot-overview-${theme}.png"
  HTTP_CODE=$(curl -sf -o "$OUT_FILE" -w "%{http_code}" \
    -u admin:admin \
    "${RENDER_BASE}?${RENDER_ARGS}&theme=${theme}")
  if [ "$HTTP_CODE" -ne 200 ]; then
    echo "ERROR: render returned HTTP ${HTTP_CODE} for theme=${theme}" >&2
    cat "$OUT_FILE" >&2
    exit 1
  fi
  SIZE=$(wc -c < "$OUT_FILE")
  echo "  depot-overview-${theme}.png: ${SIZE} bytes"
done

# =========================================================================
# Explore PNGs via Playwright (Loki logs + Tempo traces)
# =========================================================================
echo "Capturing Explore screenshots via Playwright..."
cd "$FRONTEND_DIR"
DEPOT_TEST_URL="${GRAFANA_URL}" \
DEPOT_UI_SKIP_SETUP=1 \
OBS_SCREENSHOTS_OUT="$SHOTS_DIR" \
npx playwright test --project=observability
cd "$ROOT"

# =========================================================================
# Publish into docs/
# =========================================================================
rm -rf "$DOCS_OUT"
mkdir -p "$DOCS_OUT"
shopt -s nullglob
PNGS=("$SHOTS_DIR"/*.png)
shopt -u nullglob
if (( ${#PNGS[@]} == 0 )); then
  echo "No screenshots produced!" >&2
  exit 1
fi
cp "${PNGS[@]}" "$DOCS_OUT/"

echo
echo "Wrote ${#PNGS[@]} screenshots to $DOCS_OUT/"
ls -la "$DOCS_OUT"

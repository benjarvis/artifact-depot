#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
#
# SPDX-License-Identifier: Apache-2.0

# Launch depot server, optionally seed demo data, then wait.
# Ensures all child processes are cleaned up on exit/interrupt.
set -euo pipefail

SEED="${1:-false}"
CONFIG="etc/depotd.toml"
URL="http://localhost:8080"

SERVER_PID=""
TRICKLE_PID=""

cleanup() {
    if [ -n "$TRICKLE_PID" ]; then
        echo "Stopping trickle (pid $TRICKLE_PID)..."
        kill "$TRICKLE_PID" 2>/dev/null || true
        wait "$TRICKLE_PID" 2>/dev/null || true
    fi
    if [ -n "$SERVER_PID" ]; then
        echo ""
        echo "Stopping depot server (pid $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

if [ "$SEED" = "true" ]; then
    rm -rf build/demo
fi
mkdir -p build/demo
echo "Starting depot server..."
./target/debug/depot -c "$CONFIG" &
SERVER_PID=$!

if [ "$SEED" = "true" ]; then
    echo "Waiting for depot to start..."
    for i in $(seq 1 10); do
        curl -sf "$URL/api/v1/health" > /dev/null 2>&1 && break
        sleep 1
    done

    echo "Seeding demo data..."
    ./target/debug/depot-bench demo --url "$URL" \
        --repos 3 --docker-repos 2 --artifacts 50 --images 5 --tags 3
    echo "Seed complete."

    echo "Starting background activity..."
    ./target/debug/depot-bench trickle --url "$URL" --username trickle --password trickle &
    TRICKLE_PID=$!
fi

echo "Server running at $URL (Ctrl-C to stop)..."
wait "$SERVER_PID"

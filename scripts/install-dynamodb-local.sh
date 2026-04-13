#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
#
# SPDX-License-Identifier: Apache-2.0

# Download and install DynamoDB Local JAR for integration testing.
# Installs to $DYNAMODB_LOCAL_DIR (default: ~/.local/lib/dynamodb-local).
set -euo pipefail

INSTALL_DIR="${DYNAMODB_LOCAL_DIR:-$HOME/.local/lib/dynamodb-local}"
URL="https://d1ni2b6xgvw0s0.cloudfront.net/v2.x/dynamodb_local_latest.tar.gz"

if [ -f "$INSTALL_DIR/DynamoDBLocal.jar" ]; then
  echo "DynamoDB Local already installed at $INSTALL_DIR"
  exit 0
fi

echo "Downloading DynamoDB Local..."
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

curl -fSL "$URL" -o "$TMP_DIR/dynamodb_local.tar.gz"

echo "Extracting to $INSTALL_DIR..."
mkdir -p "$INSTALL_DIR"
tar xzf "$TMP_DIR/dynamodb_local.tar.gz" -C "$INSTALL_DIR"

echo "DynamoDB Local installed at $INSTALL_DIR"
echo "JAR: $INSTALL_DIR/DynamoDBLocal.jar"

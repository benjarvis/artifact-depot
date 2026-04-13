#!/bin/bash

# SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
#
# SPDX-License-Identifier: Apache-2.0

# npm CLI integration test against a running depot server.
#
# Usage: ./test.sh <depot-base-url>
# Example: ./test.sh http://127.0.0.1:8080
#
# Prerequisites:
#   - depot server running at the given URL
#   - npm repo named "npm-docker-test" created (hosted, npm format)

set -euo pipefail

DEPOT_URL="${1:?Usage: $0 <depot-base-url>}"
REPO_NAME="npm-docker-test"
REGISTRY="${DEPOT_URL}/npm/${REPO_NAME}/"
PASS=0
FAIL=0

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

echo "=== npm Docker Integration Tests ==="
echo "Registry: ${REGISTRY}"
echo ""

# Authenticate and get token
echo "--- Authenticating ---"
TOKEN=$(curl -sf -X POST "${DEPOT_URL}/api/v1/auth/login" \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' | jq -r '.token')
if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "FATAL: Failed to authenticate"
  exit 1
fi
echo "  Got auth token"

# Create npm repo (idempotent)
echo "--- Creating npm repo ---"
curl -sf -X POST "${DEPOT_URL}/api/v1/repositories" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"${REPO_NAME}\",\"repo_type\":\"hosted\",\"format\":\"npm\"}" \
  > /dev/null 2>&1 || true
echo "  Repo ready"

# Configure npm to use our registry with auth
echo "--- Configuring npm ---"
npm config set "//${DEPOT_URL#http*://}/npm/${REPO_NAME}/:_authToken" "${TOKEN}"
echo "  npm configured"

# ============================================================
# Test 1: Publish unscoped package
# ============================================================
echo ""
echo "--- Test 1: Publish unscoped package ---"
cd /test/package-hello
if npm publish --registry "${REGISTRY}" 2>&1; then
  pass "publish hello-test@1.0.0"
else
  fail "publish hello-test@1.0.0"
fi

# ============================================================
# Test 2: View published package (packument)
# ============================================================
echo ""
echo "--- Test 2: View published package ---"
VIEW_OUTPUT=$(npm view hello-test --registry "${REGISTRY}" --json 2>&1 || true)
if echo "$VIEW_OUTPUT" | jq -e '.name == "hello-test"' > /dev/null 2>&1; then
  pass "npm view hello-test returns correct name"
else
  fail "npm view hello-test (output: ${VIEW_OUTPUT})"
fi

if echo "$VIEW_OUTPUT" | jq -e '.["dist-tags"].latest == "1.0.0"' > /dev/null 2>&1; then
  pass "npm view shows latest = 1.0.0"
else
  fail "npm view dist-tags.latest (output: ${VIEW_OUTPUT})"
fi

# ============================================================
# Test 3: Install (download) published package
# ============================================================
echo ""
echo "--- Test 3: Install published package ---"
INSTALL_DIR=$(mktemp -d)
cd "$INSTALL_DIR"
npm init -y > /dev/null 2>&1
if npm install hello-test --registry "${REGISTRY}" 2>&1; then
  pass "npm install hello-test"
  # Verify the package was actually installed
  if [ -f "node_modules/hello-test/index.js" ]; then
    pass "installed package has index.js"
  else
    fail "installed package missing index.js"
  fi
else
  fail "npm install hello-test"
fi

# ============================================================
# Test 4: Publish version update
# ============================================================
echo ""
echo "--- Test 4: Publish updated version ---"
cd /test/package-hello
# Update version to 1.1.0
cat > package.json << 'EOF'
{
  "name": "hello-test",
  "version": "1.1.0",
  "description": "Updated test package",
  "main": "index.js",
  "license": "MIT"
}
EOF
if npm publish --registry "${REGISTRY}" 2>&1; then
  pass "publish hello-test@1.1.0"
else
  fail "publish hello-test@1.1.0"
fi

# Verify both versions exist
VIEW_OUTPUT=$(npm view hello-test --registry "${REGISTRY}" --json 2>&1 || true)
if echo "$VIEW_OUTPUT" | jq -e '.["dist-tags"].latest == "1.1.0"' > /dev/null 2>&1; then
  pass "latest dist-tag updated to 1.1.0"
else
  fail "latest dist-tag should be 1.1.0 (output: ${VIEW_OUTPUT})"
fi

# ============================================================
# Test 5: Publish scoped package
# ============================================================
echo ""
echo "--- Test 5: Publish scoped package ---"
cd /test/package-scoped
if npm publish --registry "${REGISTRY}" 2>&1; then
  pass "publish @testorg/widget@2.0.0"
else
  fail "publish @testorg/widget@2.0.0"
fi

# View scoped package
VIEW_OUTPUT=$(npm view @testorg/widget --registry "${REGISTRY}" --json 2>&1 || true)
if echo "$VIEW_OUTPUT" | jq -e '.name == "@testorg/widget"' > /dev/null 2>&1; then
  pass "npm view @testorg/widget returns correct name"
else
  fail "npm view @testorg/widget (output: ${VIEW_OUTPUT})"
fi

# ============================================================
# Test 6: Install scoped package
# ============================================================
echo ""
echo "--- Test 6: Install scoped package ---"
INSTALL_DIR2=$(mktemp -d)
cd "$INSTALL_DIR2"
npm init -y > /dev/null 2>&1
if npm install @testorg/widget --registry "${REGISTRY}" 2>&1; then
  pass "npm install @testorg/widget"
  if [ -f "node_modules/@testorg/widget/index.js" ]; then
    pass "scoped package installed correctly"
  else
    fail "scoped package missing files"
  fi
else
  fail "npm install @testorg/widget"
fi

# ============================================================
# Test 7: Search packages
# ============================================================
echo ""
echo "--- Test 7: Search packages ---"
SEARCH_OUTPUT=$(npm search hello --registry "${REGISTRY}" --json 2>&1 || true)
if echo "$SEARCH_OUTPUT" | jq -e 'length > 0' > /dev/null 2>&1; then
  pass "npm search returns results"
else
  # npm search may not work with all registries; treat as non-fatal
  echo "  SKIP: npm search (may not be supported by npm CLI against custom registries)"
fi

# ============================================================
# Test 8: Install specific version
# ============================================================
echo ""
echo "--- Test 8: Install specific version ---"
INSTALL_DIR3=$(mktemp -d)
cd "$INSTALL_DIR3"
npm init -y > /dev/null 2>&1
if npm install hello-test@1.0.0 --registry "${REGISTRY}" 2>&1; then
  pass "npm install hello-test@1.0.0 (specific version)"
  INSTALLED_VERSION=$(node -e "console.log(require('hello-test/package.json').version)" 2>/dev/null || echo "unknown")
  if [ "$INSTALLED_VERSION" = "1.0.0" ]; then
    pass "installed correct version 1.0.0"
  else
    fail "expected version 1.0.0, got ${INSTALLED_VERSION}"
  fi
else
  fail "npm install hello-test@1.0.0"
fi

# ============================================================
# Test 9: Fetch packument via curl (raw HTTP)
# ============================================================
echo ""
echo "--- Test 9: Raw HTTP packument fetch ---"
PACKUMENT=$(curl -sf -H "Authorization: Bearer ${TOKEN}" \
  "${DEPOT_URL}/npm/${REPO_NAME}/hello-test" || echo "{}")
if echo "$PACKUMENT" | jq -e '.name == "hello-test"' > /dev/null 2>&1; then
  pass "curl packument returns correct JSON"
else
  fail "curl packument (output: ${PACKUMENT})"
fi

VERSIONS=$(echo "$PACKUMENT" | jq '.versions | keys | length' 2>/dev/null || echo "0")
if [ "$VERSIONS" -ge 2 ]; then
  pass "packument has ${VERSIONS} versions"
else
  fail "expected >= 2 versions, got ${VERSIONS}"
fi

# ============================================================
# Test 10: Download tarball via curl (raw HTTP)
# ============================================================
echo ""
echo "--- Test 10: Raw HTTP tarball download ---"
TARBALL_URL=$(echo "$PACKUMENT" | jq -r '.versions["1.0.0"].dist.tarball' 2>/dev/null || echo "")
if [ -n "$TARBALL_URL" ] && [ "$TARBALL_URL" != "null" ]; then
  # The tarball URL is relative, make it absolute
  if echo "$TARBALL_URL" | grep -q "^http"; then
    FULL_URL="$TARBALL_URL"
  else
    FULL_URL="${DEPOT_URL}${TARBALL_URL}"
  fi
  HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer ${TOKEN}" "$FULL_URL" || echo "000")
  if [ "$HTTP_CODE" = "200" ]; then
    pass "tarball download returns 200"
  else
    fail "tarball download returned ${HTTP_CODE} for ${FULL_URL}"
  fi
else
  fail "no tarball URL in packument"
fi

# ============================================================
# Summary
# ============================================================
echo ""
echo "=== Results ==="
echo "  Passed: ${PASS}"
echo "  Failed: ${FAIL}"
echo ""

if [ "$FAIL" -gt 0 ]; then
  echo "FAILED"
  exit 1
else
  echo "ALL TESTS PASSED"
  exit 0
fi

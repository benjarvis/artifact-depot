# Docker-based npm CLI integration test.
#
# This image contains Node.js and npm, and runs a series of npm commands
# against a depot server to validate end-to-end npm registry behavior.
#
# Usage:
#   # Start depot server first:
#   cargo run --bin depot -- -c etc/depotd.toml &
#
#   # Create the npm repo:
#   curl -u admin:admin -X POST http://host.docker.internal:8080/api/v1/repositories \
#     -H 'Content-Type: application/json' \
#     -d '{"name":"npm-docker-test","repo_type":"hosted","format":"npm"}'
#
#   # Build and run the test:
#   docker build -t depot-npm-test tests/npm-docker/
#   docker run --rm --network host depot-npm-test http://127.0.0.1:8080

FROM node:20-slim

RUN apt-get update && apt-get install -y curl jq && rm -rf /var/lib/apt/lists/*

WORKDIR /test

# Copy test scripts
COPY test.sh /test/test.sh
COPY package-hello/ /test/package-hello/
COPY package-scoped/ /test/package-scoped/

RUN chmod +x /test/test.sh

ENTRYPOINT ["/test/test.sh"]

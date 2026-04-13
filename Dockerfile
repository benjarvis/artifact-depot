# --- Build stage ---
FROM node:24.14.0-bookworm-slim AS node
FROM rust:1.94.0-bookworm AS builder

RUN rustup target add x86_64-unknown-linux-musl \
    && apt-get update && apt-get install -y --no-install-recommends musl-tools mold \
    && rm -rf /var/lib/apt/lists/*

COPY --from=node /usr/local/bin/node /usr/local/bin/
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm

WORKDIR /src
COPY . .

RUN cargo build --release --features dynamodb --bin depot --target x86_64-unknown-linux-musl

# --- Runtime stage ---
FROM scratch

LABEL org.opencontainers.image.title="Artifact Depot" \
      org.opencontainers.image.description="Scale-out artifact repository manager" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.vendor="Quantum"

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /src/target/x86_64-unknown-linux-musl/release/depot /depot

USER 65534:65534
EXPOSE 8080
VOLUME /data

ENTRYPOINT ["/depot"]
CMD ["-c", "/etc/depot/depotd.toml"]

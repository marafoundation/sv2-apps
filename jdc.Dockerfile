# ── Builder ──────────────────────────────────────────────────────────────────
FROM rust:1.85 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    capnproto libcapnp-dev curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY miner-apps/ miner-apps/
COPY bitcoin-core-sv2/ bitcoin-core-sv2/
COPY stratum-apps/ stratum-apps/

RUN cargo build --release --manifest-path miner-apps/jd-client/Cargo.toml --target-dir ./ 

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    gettext-base && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/release/jd_client_sv2 /app/jd_client_sv2
COPY config/jdc-config.toml.template /app/jdc-config.toml.template

ENTRYPOINT ["/bin/sh", "-c", "envsubst < /app/jdc-config.toml.template > /app/jdc-config.toml && exec /app/jd_client_sv2"]
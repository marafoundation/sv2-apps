# ── Builder ──────────────────────────────────────────────────────────────────
FROM rust:1.85 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    capnproto libcapnp-dev curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pool-apps/ pool-apps/
COPY bitcoin-core-sv2/ bitcoin-core-sv2/
COPY stratum-apps/ stratum-apps/

RUN cargo build --release --manifest-path pool-apps/pool/Cargo.toml --target-dir ./

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    gettext-base && \
    rm -rf /var/lib/apt/lists/*

ENV IPC_DIR=/root/.bitcoin/

WORKDIR /app

COPY --from=builder /app/release/pool_sv2 /app/pool_sv2
COPY config/pool-jds-config.toml.template /app/pool-jds-config.toml.template
# check if the IPC file exists on a loop and wait until it does before starting the pool_sv2
ENTRYPOINT ["/bin/sh", "-c", "envsubst < /app/pool-jds-config.toml.template > /app/pool-config.toml && while [ ! -S \"$IPC_DIR/node.sock\" ]; do sleep 1; echo waiting for IPC file...; done && exec /app/pool_sv2"]

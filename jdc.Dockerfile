# ── Builder ──────────────────────────────────────────────────────────────────
FROM rust:1.85 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    capnproto libcapnp-dev curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY miner-apps/ miner-apps/
COPY bitcoin-core-sv2/ bitcoin-core-sv2/
COPY stratum-apps/ stratum-apps/

# Cache mounts keep the cargo download caches and the compiled target/ dir warm
# across CI runs (persisted via buildkit-cache-dance), so a source change only
# recompiles what changed instead of every dependency. target/ lives in the
# mount and is not part of the image, so the binary is copied out in the same step.
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target,target=/app/target \
    cargo build --release --manifest-path miner-apps/jd-client/Cargo.toml --target-dir /app/target && \
    cp /app/target/release/jd_client_sv2 /app/jd_client_sv2

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM ubuntu:24.04

WORKDIR /app

COPY --from=builder /app/jd_client_sv2 /app/jd_client_sv2
# Baked default config. Values can be overridden at runtime with JDC__* env vars
# (the binary layers environment variables on top of this file).
COPY config/jdc-config.toml /app/jdc-config.toml

ENTRYPOINT ["/app/jd_client_sv2"]
# syntax=docker/dockerfile:1
###
# ---- Builder Stage ----
###
FROM rust:1.88-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    capnproto libcapnp-dev curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# to define what app will be built
ARG APP

### workspace manifests

# The workspace has to load before building, and integration-tests is the one
# member that is not copied below, so its manifest is copied here. The cargo
# registry and git dirs are cache mounts in the build step, which shadow
# anything downloaded in an earlier layer, so dependencies are not fetched here.
COPY Cargo.toml Cargo.lock ./
COPY integration-tests/Cargo.toml integration-tests/Cargo.toml


### copying source code

COPY pool-apps/ pool-apps/
COPY miner-apps/ miner-apps/
COPY bitcoin-core-sv2/ bitcoin-core-sv2/
COPY stratum-apps/ stratum-apps/

# build target app
# The cache mounts keep the cargo download caches and the compiled target dir
# warm across builds. The mounted target dir is not part of the image, so the
# binary is copied out in the same step.
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target,target=/app/target \
    echo "Building ${APP}" && \
    case "$APP" in \
        pool_sv2|jd_client_sv2|translator_sv2) cargo build --release --locked -p "$APP" --target-dir /app/target ;; \
        *) echo "Unknown APP: $APP" && exit 1 ;; \
    esac && \
    mkdir -p /app/release && \
    cp "/app/target/release/${APP}" "/app/release/${APP}"

###
# ---- Runtime Stage ----
###
FROM debian:bookworm-slim AS runtime

# CA certificates are needed for reqwest-based miner telemetry requests.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
ARG APP
ENV APP=${APP}

COPY --from=builder /app/release/${APP} /app/${APP}

# There is no config file: each app is configured entirely from the environment
# (<PREFIX>__<FIELD> variables supplied via docker_env). The optional config file
# the binary would otherwise read from its working directory is simply absent.
#
# `exec` replaces the shell so the app runs as PID 1 and receives the container's
# stop signal directly, allowing graceful shutdown.
ENTRYPOINT ["/bin/sh", "-c", "exec /app/${APP}"]

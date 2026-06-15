# ── Builder ──────────────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

ARG BITCOIND_VERSION=31.0
ARG CAPNPROTO_VERSION=1.4.0

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    cmake \
    git \
    pkgconf \
    python3 \
    libevent-dev \
    libboost-dev \
    libsqlite3-dev \
    libzmq3-dev \
    && rm -rf /var/lib/apt/lists/*
    

RUN curl -O https://capnproto.org/capnproto-c++-${CAPNPROTO_VERSION}.tar.gz \
    && tar zxf capnproto-c++-${CAPNPROTO_VERSION}.tar.gz \
    && cd capnproto-c++-${CAPNPROTO_VERSION} \
    && ./configure \
    && make -j6 check \
    && make install \
    && rm -rf ../*

RUN git clone --branch=v${BITCOIND_VERSION} --depth=1 https://github.com/bitcoin/bitcoin .

RUN cmake -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DENABLE_WALLET=ON \
        -DWITH_ZMQ=ON \
        -DBUILD_GUI=OFF \
        -DENABLE_IPC=ON \
        -DBUILD_TESTS=OFF \
        -DBUILD_BENCH=OFF \
    && cmake --build build \
    && cmake --install build

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    libevent-dev \
    libzmq3-dev \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/bin/*                /usr/local/bin/
COPY --from=builder /usr/local/bin/*                /usr/local/bin/
COPY --from=builder /usr/local/lib/*                /usr/local/lib/
COPY --from=builder /usr/local/include/*            /usr/local/include/

ENV BITCOIN_DATA=/root/.bitcoin
VOLUME ["${BITCOIN_DATA}"]
WORKDIR /root
RUN mkdir -p ${BITCOIN_DATA} && chown -R 1000:1000 ${BITCOIN_DATA}

# P2P (mainnet / testnet4 / regtest), RPC, ZMQ
EXPOSE 8333 18333 18444 8332 18332 18443 28332

ENTRYPOINT ["bitcoin"]

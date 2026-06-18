# Reproducible OCI image builds (experimental)

The Nix flake at the repo root (`flake.nix`) is an alternative, opt-in path
for building reproducible OCI images of the three release binaries
(`pool_sv2`, `jd_client_sv2`, `translator_sv2`). It runs **alongside** the
existing `docker/Dockerfile` + `.github/workflows/docker-release.yaml`
pipeline; neither is removed.

## Status

Experimental. The flake is wired and ready to build (toolchain pinned via
`rust-toolchain.toml`; `stratum-core` git dep resolved automatically by
Crane from the workspace `Cargo.lock`). Bit-identical-output reproducibility
across independent rebuilders has not yet been verified end-to-end. The
existing Buildx + QEMU pipeline remains the source of truth for releases.

## How to build locally

```sh
# Build a single binary as a Nix package
nix build .#pool_sv2

# Build an OCI image tarball (linux only)
nix build .#container.pool_sv2

# Load into your local Docker
docker load < ./result
```

## How to verify reproducibility

Two builders sharing the same `flake.lock` and source tree should produce
byte-identical OCI tarballs:

```sh
nix build .#container.pool_sv2 && sha256sum result
```

Compare the resulting hash against another builder's. If they match, the
image is reproducible.

## How CI verifies reproducibility

`.github/workflows/ci-nix.yml` runs **two independent build instances per
(arch, app) cell**: builders "a" and "b" on separate `ubuntu-latest` (amd64)
or `ubuntu-24.04-arm` (arm64) runners. The `verify-reproducible` job
downloads both tarballs and fails the workflow if the sha256 sums diverge.
On mismatch, `diffoscope` produces a structured diff uploaded as an
artifact named `diffoscope-<app>-<arch>` (30-day retention). Per-arch
push and multi-arch manifest stitching only run after verification
passes — meaning published images are always cross-builder reproducible
within their arch.

Cross-arch comparison is intentionally not done: amd64 and arm64
binaries contain different machine code, so different digests by
definition. Each arch is verified against itself.

## Context

Background, design rationale, and the Fedimint pattern this is modeled on
are documented in
`~/wiki/topics/nixos-reproducible-builds-bitcoin/wiki/topics/sv2-apps-oci-reproducibility-feasibility.md`.

# flake.nix — reproducible builds for sv2-apps release binaries.
#
# WHAT THIS PRODUCES
#   packages.<system>.{pool_sv2, jd_client_sv2, translator_sv2}            -- release binaries
#   packages.<system>.container.{pool_sv2, jd_client_sv2, translator_sv2}  -- OCI images (gzipped tarballs)
#   devShells.<system>.default                                             -- dev env mirroring docker/Dockerfile
#
# DESIGN
#   - `rustPlatform.buildRustPackage` from nixpkgs (NOT Crane) for Rust builds.
#     Crane's per-crate `cargo package --exclude-lockfile` vendoring path
#     fails for our case: stratum-mining/stratum is a transitive git
#     dependency that's itself a workspace, and cargo's source-replacement
#     consumer rejects vendored git checkouts whose `SourceId.precise`
#     isn't populated correctly. `buildRustPackage` invokes cargo's own
#     `cargo vendor`, which preserves lockfiles and emits config in cargo's
#     native format. See the deleted Crane block in git history (commit
#     4b0d7df5 and parents) for the failed approach + research notes.
#   - Fenix pins the Rust toolchain from `rust-toolchain.toml` and is
#     plumbed into `buildRustPackage` via `makeRustPlatform`.
#   - `cargoLock.allowBuiltinFetchGit = true` lets nix's eval-time
#     `builtins.fetchGit` resolve the rev-pinned stratum git source from
#     each workspace's Cargo.lock without manually maintaining
#     `outputHashes` entries (would be 18 entries for stratum's workspace
#     members). Trade-off: eval-time network access — fine on CI runners
#     and operator workstations, not for fully-pure offline builds.
#   - `dockerTools.buildLayeredImage` produces OCI tarballs (no Docker
#     daemon needed at build time). NO `runAsRoot` — sidesteps nixpkgs#416467.
#   - The repo has TWO cargo workspaces (pool-apps/, miner-apps/). Each
#     binary is built from its respective workspace lockfile.

{
  description = "Stratum V2 sv2-apps reproducible OCI image builds";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, fenix }:
    flake-utils.lib.eachSystem [
      "x86_64-linux"
      "aarch64-linux"
      # darwin systems are dev-shell only — OCI images target linux.
      "x86_64-darwin"
      "aarch64-darwin"
    ] (system:
      let
        pkgs = import nixpkgs { inherit system; };
        isLinux = pkgs.stdenv.hostPlatform.isLinux;

        # Rust toolchain pinned via rust-toolchain.toml. Hash regenerated
        # whenever rust-toolchain.toml changes (set to all-A's, run
        # `nix build`, paste the "got" value from the error).
        rustToolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-mvUGEOHYJpn3ikC5hckneuGixaC+yGrkMM/liDIDgoU=";
        };

        # Splice the fenix toolchain into a rustPlatform so
        # buildRustPackage uses our pinned cargo + rustc instead of
        # nixpkgs defaults.
        rustPlatform = pkgs.makeRustPlatform {
          cargo = rustToolchain;
          rustc = rustToolchain;
        };

        nativeBuildInputs = with pkgs; [
          pkg-config
          capnproto      # build-time, matches Dockerfile builder stage
          clang
        ];

        buildInputs = with pkgs; [
          # Add openssl/zlib here if a transitive dep complains on first build.
        ];

        # Each binary's `src` is the whole repo (path deps cross workspace
        # boundaries: pool-apps depends on bitcoin-core-sv2 and stratum-apps).
        src = ./.;

        mkSv2Bin = { pname, version, workspace, lockFile }:
          rustPlatform.buildRustPackage {
            inherit pname version src nativeBuildInputs buildInputs;

            # `cargoRoot` is the directory (relative to src) containing
            # the Cargo.lock for this binary's workspace. The cargo-setup
            # hook reads `<src>/<cargoRoot>/Cargo.lock` for the
            # consistency check against the vendored deps lockfile.
            cargoRoot = workspace;
            # `buildAndTestSubdir` tells the build/check phase where to
            # run cargo from. Aligned with cargoRoot for our two-workspace
            # layout (pool-apps/, miner-apps/).
            buildAndTestSubdir = workspace;

            cargoLock = {
              inherit lockFile;
              # Use builtins.fetchGit for git deps in Cargo.lock so we
              # don't have to maintain 18 outputHashes entries for
              # stratum-mining/stratum's workspace members. The rev is
              # pinned in the lockfile so reproducibility is preserved.
              allowBuiltinFetchGit = true;
            };

            # Limit cargo to the requested binary; matches the original
            # Dockerfile per-stage build.
            cargoBuildFlags = [ "--bin" pname ];
            cargoTestFlags = [ "--bin" pname ];

            # CI runs the test suite separately; this build only produces
            # the release binary for closure deployment.
            doCheck = false;
          };

        pool_sv2 = mkSv2Bin {
          pname = "pool_sv2";
          version = "0.4.0";
          workspace = "pool-apps";
          lockFile = ./pool-apps/Cargo.lock;
        };

        jd_client_sv2 = mkSv2Bin {
          pname = "jd_client_sv2";
          version = "0.3.0";
          workspace = "miner-apps";
          lockFile = ./miner-apps/Cargo.lock;
        };

        translator_sv2 = mkSv2Bin {
          pname = "translator_sv2";
          version = "0.3.0";
          workspace = "miner-apps";
          lockFile = ./miner-apps/Cargo.lock;
        };

        # Single-arch per system. CI stitches multi-arch with
        # `docker manifest create`.
        mkContainer = { name, pkg, binName }:
          pkgs.dockerTools.buildLayeredImage {
            inherit name;
            tag = "latest";  # CI retags with ${github.ref_name}-${arch}.
            # +1 sidesteps a historical dockerTools quirk that treats 0 as unset.
            created = "1970-01-01T00:00:01Z";

            architecture =
              if pkgs.stdenv.hostPlatform.system == "x86_64-linux" then "amd64"
              else if pkgs.stdenv.hostPlatform.system == "aarch64-linux" then "arm64"
              else throw "OCI images only supported on linux systems; got ${pkgs.stdenv.hostPlatform.system}";

            contents = [
              pkg
              pkgs.gettext       # envsubst — runtime parity with Dockerfile
              pkgs.bash
              pkgs.coreutils
              pkgs.cacert
            ];

            config = {
              Cmd = [ "${pkg}/bin/${binName}" ];
              Env = [
                "PATH=/bin:/usr/bin"
                "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
              ];
              WorkingDir = "/";
              Labels = {
                "org.opencontainers.image.source" = "https://github.com/stratum-mining/sv2-apps";
                "org.opencontainers.image.licenses" = "MIT OR Apache-2.0";
              };
            };
          };

      in {
        packages = {
          inherit pool_sv2 jd_client_sv2 translator_sv2;

          # Containers only build on linux; on darwin these attrs are absent.
          container = pkgs.lib.optionalAttrs isLinux {
            pool_sv2       = mkContainer { name = "stratumv2/pool_sv2";       pkg = pool_sv2;       binName = "pool_sv2"; };
            jd_client_sv2  = mkContainer { name = "stratumv2/jd_client_sv2";  pkg = jd_client_sv2;  binName = "jd_client_sv2"; };
            translator_sv2 = mkContainer { name = "stratumv2/translator_sv2"; pkg = translator_sv2; binName = "translator_sv2"; };
          };

          default = pool_sv2;
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = pkgs.lib.optionals isLinux [ pool_sv2 ];
          packages = with pkgs; [
            rustToolchain
            capnproto
            pkg-config
            clang
            gettext
            cargo-watch
            cargo-nextest
          ];
          RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/library";
        };

        # `nix flake check` runs this; keep cheap so CI's
        # `nix flake check --no-build` works.
        checks = pkgs.lib.optionalAttrs isLinux {
          inherit pool_sv2 jd_client_sv2 translator_sv2;
        };
      });
}

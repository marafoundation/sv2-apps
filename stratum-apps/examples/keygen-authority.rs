//! keygen-authority — mint a fresh SV2 noise-protocol authority keypair.
//!
//! Output is two `pool.toml`-ready strings: a base58check secp256k1
//! private key (encoded as `Display` of `Secp256k1SecretKey`, plain
//! 32-byte secret with bs58 checksum) and a base58check x-only public
//! key (`[u16 LE version=1, 32-byte x-only pubkey]` with bs58 checksum).
//!
//! Usage:
//!     cargo run --release --example keygen-authority -p stratum-apps
//!
//! Output format (single-line, no surrounding whitespace, both lines):
//!     authority_secret_key = "<bs58check ~50 chars>"
//!     authority_public_key = "<bs58check ~52 chars>"
//!
//! These deserialize directly into `Secp256k1SecretKey` /
//! `Secp256k1PublicKey` via `serde(try_from = "String")`. Pool, JDC, and
//! translator binaries all consume this exact encoding via the
//! `key_utils` module re-exported from this crate.
//!
//! For per-host secrets, run twice and store each pair as JSON (e.g.
//! `{"private_key": "...", "public_key": "..."}`) in a secret manager.

use stratum_apps::key_utils::{Secp256k1PublicKey, Secp256k1SecretKey};
use secp256k1::{rand::thread_rng, Secp256k1};

fn main() {
    let secp = Secp256k1::new();
    let (sk, _pk) = secp.generate_keypair(&mut thread_rng());
    let secret = Secp256k1SecretKey(sk);
    let public: Secp256k1PublicKey = secret.into();
    println!("authority_secret_key = \"{}\"", secret);
    println!("authority_public_key = \"{}\"", public);
}

//! Process-wide traffic counters, recorded where every byte already passes: SV2 frames in
//! `OutboundFrame::encode_into` (out) and `noise_stream`'s decoder (in), SV1 lines in
//! `sv1_connection`. Relaxed atomics in two statics; monitoring reads them when scraped.
//! Labels are bounded (direction x SV2 message type, direction x [`SV1_KINDS`]); nothing is
//! keyed by peer.
//!
//! ponytail: always on, no feature flag. A record is ~2 ns (see `record_cost`), far below the
//! I/O it counts. SV2 wire bytes aren't counted: Noise adds a fixed 32 B per frame under 64 KiB.

use core::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    In,
    Out,
}

impl Direction {
    pub const ALL: [Direction; 2] = [Direction::In, Direction::Out];

    pub fn label(self) -> &'static str {
        ["in", "out"][self as usize]
    }
}

/// SV1 methods counted on their own; responses are `response`, any other method `other`.
pub const SV1_KINDS: [&str; 10] = [
    "mining.notify",
    "mining.set_difficulty",
    "mining.set_extranonce",
    "mining.submit",
    "mining.subscribe",
    "mining.authorize",
    "mining.configure",
    "client.reconnect",
    "response",
    "other",
];

/// Index into [`SV1_KINDS`]: a request's or notification's method, or `None` for a response.
pub fn sv1_kind(method: Option<&str>) -> usize {
    match method {
        None => 8,
        Some(m) => SV1_KINDS[..8].iter().position(|k| *k == m).unwrap_or(9),
    }
}

struct Tally {
    count: AtomicU64,
    bytes: AtomicU64,
}

impl Tally {
    const fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }
}

static SV2: [[Tally; 256]; 2] = [const { [const { Tally::new() }; 256] }; 2];
static SV1: [[Tally; SV1_KINDS.len()]; 2] =
    [const { [const { Tally::new() }; SV1_KINDS.len()] }; 2];

fn add(t: &Tally, bytes: usize) {
    t.count.fetch_add(1, Relaxed);
    t.bytes.fetch_add(bytes as u64, Relaxed);
}

/// One SV2 frame of `bytes` (plaintext, header included) of message type `msg_type`.
pub fn record_sv2(direction: Direction, msg_type: u8, bytes: usize) {
    add(&SV2[direction as usize][msg_type as usize], bytes);
}

/// One SV1 line of `bytes` (newline included) of kind `kind` (see [`sv1_kind`]).
pub fn record_sv1(direction: Direction, kind: usize, bytes: usize) {
    add(&SV1[direction as usize][kind], bytes);
}

/// Non-zero SV2 totals: `(direction, msg_type, frames, bytes)`.
pub fn sv2_totals() -> impl Iterator<Item = (Direction, u8, u64, u64)> {
    Direction::ALL.into_iter().flat_map(|d| {
        (0..=255u8)
            .filter_map(move |t| read(&SV2[d as usize][t as usize]).map(|(c, b)| (d, t, c, b)))
    })
}

/// Non-zero SV1 totals: `(direction, kind label, lines, bytes)`.
pub fn sv1_totals() -> impl Iterator<Item = (Direction, &'static str, u64, u64)> {
    Direction::ALL.into_iter().flat_map(|d| {
        (0..SV1_KINDS.len())
            .filter_map(move |k| read(&SV1[d as usize][k]).map(|(c, b)| (d, SV1_KINDS[k], c, b)))
    })
}

fn read(t: &Tally) -> Option<(u64, u64)> {
    let count = t.count.load(Relaxed);
    (count > 0).then(|| (count, t.bytes.load(Relaxed)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // One process-wide static: use a message type nothing else records, and compare deltas.
    fn sv2(direction: Direction, msg_type: u8) -> (u64, u64) {
        sv2_totals()
            .find(|(d, t, ..)| *d == direction && *t == msg_type)
            .map_or((0, 0), |(.., c, b)| (c, b))
    }

    #[test]
    fn counts_per_direction_and_type() {
        let (c0, b0) = sv2(Direction::In, 0xF1);
        record_sv2(Direction::In, 0xF1, 100);
        record_sv2(Direction::In, 0xF1, 50);
        record_sv2(Direction::Out, 0xF1, 7);
        assert_eq!(sv2(Direction::In, 0xF1), (c0 + 2, b0 + 150));
    }

    #[test]
    fn sv1_kinds_are_a_fixed_set() {
        assert_eq!(
            SV1_KINDS[sv1_kind(Some("mining.set_difficulty"))],
            "mining.set_difficulty"
        );
        assert_eq!(
            SV1_KINDS[sv1_kind(Some("anything a peer invents"))],
            "other"
        );
        assert_eq!(SV1_KINDS[sv1_kind(None)], "response");
    }

    /// `cargo test --release -p stratum-apps --lib record_cost -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn record_cost() {
        const N: u64 = 10_000_000;
        let t = std::time::Instant::now();
        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    (0..N / 8).for_each(|i| record_sv1(Direction::Out, 0, i as usize & 0x7ff))
                });
            }
        });
        println!(
            "record_sv1: {:.1} ns/op, 8 threads on one counter",
            t.elapsed().as_nanos() as f64 / N as f64
        );
    }
}

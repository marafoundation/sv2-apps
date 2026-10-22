//! Order in which configured upstreams are tried, and the guard against fallback loops.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use crate::utils::UpstreamEntry;

/// The configured upstreams plus the state that decides which one to try next.
///
/// Every walk visits each entry at most once, starting with the entry after the one that was
/// last connected, so a fallback moves on to the next upstream first and only comes back to the
/// one that just failed after every other entry has been tried. An upstream that served miners
/// stays eligible: a pool restart, or a single configured upstream, must not leave the
/// translator with nothing to reconnect to.
///
/// Because a used upstream can be picked again, an upstream that accepts the connection and then
/// keeps failing (for example a payout mismatch) would make the translator fall back forever.
/// More than `max_fallbacks` fallbacks within `fallback_window` is therefore treated as fatal,
/// which hands the restart policy back to the process supervisor.
#[derive(Debug)]
pub(crate) struct UpstreamWalk {
    entries: Vec<UpstreamEntry>,
    last_connected: Option<usize>,
    recent_fallbacks: VecDeque<Instant>,
    max_fallbacks: usize,
    fallback_window: Duration,
}

impl UpstreamWalk {
    pub(crate) fn new(
        entries: Vec<UpstreamEntry>,
        max_fallbacks: usize,
        fallback_window: Duration,
    ) -> Self {
        Self {
            entries,
            last_connected: None,
            recent_fallbacks: VecDeque::new(),
            max_fallbacks,
            fallback_window,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn entry(&self, index: usize) -> &UpstreamEntry {
        &self.entries[index]
    }

    /// Entry indexes for the next walk, in the order they should be tried.
    pub(crate) fn order(&self) -> Vec<usize> {
        let len = self.entries.len();
        let start = self.last_connected.map_or(0, |index| index + 1);
        (0..len).map(|offset| (start + offset) % len).collect()
    }

    pub(crate) fn mark_connected(&mut self, index: usize) {
        self.last_connected = Some(index);
    }

    /// Records a fallback away from the connected upstream.
    ///
    /// Returns the number of fallbacks inside the window if it exceeds `max_fallbacks`.
    pub(crate) fn record_fallback(&mut self, now: Instant) -> Result<(), usize> {
        while self
            .recent_fallbacks
            .front()
            .is_some_and(|at| now.duration_since(*at) >= self.fallback_window)
        {
            self.recent_fallbacks.pop_front();
        }
        self.recent_fallbacks.push_back(now);

        if self.recent_fallbacks.len() > self.max_fallbacks {
            return Err(self.recent_fallbacks.len());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use stratum_apps::key_utils::Secp256k1PublicKey;

    fn walk(len: usize) -> UpstreamWalk {
        let pubkey =
            Secp256k1PublicKey::from_str("9bDuixKmZqAJnrmP746n8zU1wyAQRrus7th9dxnkPg6RzQvCnan")
                .unwrap();
        let entries = (0..len)
            .map(|i| UpstreamEntry {
                host: "127.0.0.1".into(),
                port: 4000 + i as u16,
                authority_pubkey: pubkey,
                user_identity: "test".into(),
            })
            .collect();
        UpstreamWalk::new(entries, 5, Duration::from_secs(600))
    }

    #[test]
    fn first_walk_follows_config_order() {
        assert_eq!(walk(3).order(), vec![0, 1, 2]);
    }

    #[test]
    fn walk_after_fallback_starts_at_the_next_entry() {
        let mut walk = walk(3);
        walk.mark_connected(0);
        assert_eq!(walk.order(), vec![1, 2, 0]);
        walk.mark_connected(2);
        assert_eq!(walk.order(), vec![0, 1, 2]);
    }

    #[test]
    fn single_upstream_stays_eligible_after_it_was_used() {
        let mut walk = walk(1);
        walk.mark_connected(0);
        walk.record_fallback(Instant::now()).unwrap();
        assert_eq!(walk.order(), vec![0]);
    }

    #[test]
    fn too_many_fallbacks_inside_the_window_are_rejected() {
        let mut walk = walk(2);
        let start = Instant::now();
        for i in 0..5 {
            walk.record_fallback(start + Duration::from_secs(i))
                .unwrap();
        }
        assert_eq!(walk.record_fallback(start + Duration::from_secs(5)), Err(6));
    }

    #[test]
    fn zero_max_fallbacks_rejects_the_first_fallback() {
        let mut walk = walk(2);
        walk.max_fallbacks = 0;
        assert_eq!(walk.record_fallback(Instant::now()), Err(1));
    }

    #[test]
    fn fallbacks_older_than_the_window_are_forgotten() {
        let mut walk = walk(2);
        let start = Instant::now();
        for i in 0..5 {
            walk.record_fallback(start + Duration::from_secs(i))
                .unwrap();
        }
        assert_eq!(
            walk.record_fallback(start + Duration::from_secs(600)),
            Ok(())
        );
    }
}

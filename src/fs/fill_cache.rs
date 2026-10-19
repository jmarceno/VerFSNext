use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};

use moka::sync::Cache;
use parking_lot::Mutex;

const STRIPES: usize = 64;

/// Bounded cache of committed metadata whose fills cannot bring back a value
/// that was invalidated while it was being read from the store.
///
/// A fill is `ticket` (before the store read), then `insert_if_current`.
/// Every invalidation bumps the epoch of the key's stripe under the stripe
/// lock, and `insert_if_current` inserts under that same lock only when the
/// epoch is still the one the ticket saw. A mutation that commits and then
/// invalidates therefore either removes the filled value or makes the fill
/// drop it.
pub(crate) struct FillCache<K, V> {
    cache: Cache<K, V>,
    stripes: Box<[Mutex<u64>]>,
    hasher: RandomState,
}

/// The stripe epoch a fill observed before reading the store.
pub(crate) struct FillTicket {
    stripe: usize,
    epoch: u64,
}

impl<K, V> FillCache<K, V>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub(crate) fn new(max_capacity: u64) -> Self {
        Self {
            cache: Cache::builder().max_capacity(max_capacity).build(),
            stripes: (0..STRIPES).map(|_| Mutex::new(0)).collect(),
            hasher: RandomState::new(),
        }
    }

    fn stripe(&self, key: &K) -> usize {
        (self.hasher.hash_one(key) as usize) % STRIPES
    }

    pub(crate) fn get(&self, key: &K) -> Option<V> {
        self.cache.get(key)
    }

    pub(crate) fn ticket(&self, key: &K) -> FillTicket {
        let stripe = self.stripe(key);
        FillTicket {
            stripe,
            epoch: *self.stripes[stripe].lock(),
        }
    }

    pub(crate) fn insert_if_current(&self, key: K, value: V, ticket: FillTicket) {
        let epoch = self.stripes[ticket.stripe].lock();
        if *epoch == ticket.epoch {
            self.cache.insert(key, value);
        }
    }

    pub(crate) fn invalidate(&self, key: &K) {
        let mut epoch = self.stripes[self.stripe(key)].lock();
        *epoch += 1;
        self.cache.invalidate(key);
    }

    pub(crate) fn invalidate_all(&self) {
        let mut epochs = self.stripes.iter().map(|stripe| stripe.lock()).collect::<Vec<_>>();
        for epoch in epochs.iter_mut() {
            **epoch += 1;
        }
        self.cache.invalidate_all();
    }

    pub(crate) fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    pub(crate) fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks();
    }
}

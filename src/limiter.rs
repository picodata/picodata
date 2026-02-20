use ahash::AHashMap;
use std::hash::Hash;
use tarantool::fiber::{self, Cond, Mutex};

struct Ticket {
    holders: usize,
}

struct TicketBook<K> {
    by_owner: AHashMap<K, Ticket>,
}

impl<K> Default for TicketBook<K> {
    fn default() -> Self {
        Self {
            by_owner: AHashMap::new(),
        }
    }
}

pub(crate) struct TicketPool<K> {
    state: Mutex<TicketBook<K>>,
    cond: Cond,
}

impl<K> Default for TicketPool<K> {
    fn default() -> Self {
        Self {
            state: Mutex::new(TicketBook::default()),
            cond: Cond::new(),
        }
    }
}

pub(crate) enum AcquireResult {
    Saturated,
    Cancelled,
    Reentrant,
    Acquired,
}

pub(crate) enum ReleaseResult {
    Unknown,
    StillAcquired,
    Released,
}

impl<K> TicketPool<K>
where
    K: Eq + Hash + Clone,
{
    fn try_acquire_locked(
        state: &mut TicketBook<K>,
        owner_key: K,
        max_tickets: u64,
    ) -> AcquireResult {
        if let Some(ticket) = state.by_owner.get_mut(&owner_key) {
            ticket.holders += 1;
            return AcquireResult::Reentrant;
        }

        if state.by_owner.len() as u64 >= max_tickets {
            return AcquireResult::Saturated;
        }

        state.by_owner.insert(owner_key, Ticket { holders: 1 });
        AcquireResult::Acquired
    }

    pub(crate) fn acquire_blocking(&self, owner_key: K, max_tickets: u64) -> AcquireResult {
        loop {
            if fiber::is_cancelled() {
                return AcquireResult::Cancelled;
            }

            let acquire = {
                let mut state = self.state.lock();
                Self::try_acquire_locked(&mut state, owner_key.clone(), max_tickets)
            };

            match acquire {
                AcquireResult::Saturated => {
                    if !self.cond.wait() {
                        return AcquireResult::Cancelled;
                    }
                }
                _ => return acquire,
            }
        }
    }

    pub(crate) fn release(&self, owner_key: &K) -> ReleaseResult {
        let mut state = self.state.lock();
        {
            let Some(ticket) = state.by_owner.get_mut(owner_key) else {
                return ReleaseResult::Unknown;
            };

            if ticket.holders == 0 {
                return ReleaseResult::Unknown;
            }

            if ticket.holders > 1 {
                ticket.holders -= 1;
                return ReleaseResult::StillAcquired;
            }
        };

        if state.by_owner.remove(owner_key).is_none() {
            return ReleaseResult::Unknown;
        }

        drop(state);
        self.cond.signal();
        ReleaseResult::Released
    }
}

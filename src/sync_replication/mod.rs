use crate::config::{ElectionMode, PicodataConfig, ReplicationMode, DEFAULT_REPLICATION_MODE};
use crate::has_states;
use crate::instance::Instance;
use crate::luamod::lua_function;
use crate::tarantool::{
    box_info_election, box_is_ro, box_owns_synchro_queue, box_promote, box_ro_reason, cfg_field,
    set_cfg_field,
};
use crate::traft::{error::Error, Result};
use std::borrow::Borrow;
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::{BoxError, Error as TarantoolError, TarantoolErrorCode};
use tarantool::fiber;
use tarantool::transaction::transaction_force_async;

pub mod synchro_election_watcher;

/// Returns `true` if this instance runs Tarantool elections.
pub fn elections_enabled() -> bool {
    cfg_field::<String>("election_mode").is_some_and(|mode| mode != ElectionMode::Off.as_str())
}

/// Returns the synchro quorum of a replicaset: the majority of its members.
/// In a `replication_mode = sync` tier it's not less than the majority of the
/// replication factor.
///
/// It's also the election quorum. The replication factor alone isn't enough
/// once the replicaset has more members than that: e.g. with 2 members in a
/// tier with `replication_factor = 1` each of them could be elected without
/// the other, and their histories would diverge.
///
/// In a sync tier a replicaset which hasn't gathered its replication factor
/// yet has the quorum of a full one, so that a write is confirmed by as many
/// replicas as the replication factor promises. An async tier promises no
/// such thing, and there the quorum of the replication factor would only make
/// a replicaset which lacks members unwritable: it couldn't get a master in
/// the vshard configuration or apply a schema change, and after an expel it
/// couldn't even take a new member instead of the expelled one.
///
/// `member_count` must count the offline instances too, see
/// [`count_replicaset_members`].
pub fn synchro_quorum(
    replication_mode: ReplicationMode,
    replication_factor: u8,
    member_count: usize,
) -> u8 {
    let size = if replication_mode.is_sync() {
        member_count.max(replication_factor as usize)
    } else {
        member_count
    };
    u8::try_from(size / 2 + 1).unwrap_or(u8::MAX)
}

/// Returns the synchro quorum of a replicaset only `live_count` members of
/// which may respond, see [`synchro_quorum`].
///
/// In an async tier the governor breaks the tie when exactly half of the
/// members are offline, as it has marked them in the raft log: the quorum is
/// the majority of the live half. Otherwise a replicaset of 2 members couldn't
/// fail over to the remaining one. This is safe as long as the offline half
/// still runs with the quorum of the whole replicaset: then it can't elect a
/// leader on its own, and a leader among it is fenced. It isn't safe if a
/// member of that half missed the last quorum change while it had a lowered
/// one, e.g. it was cut off right when the other half came back. Then both
/// halves may have a writable leader.
///
/// A sync tier doesn't break ties: there the quorum is what makes a write
/// durable.
pub fn synchro_quorum_breaking_tie(
    replication_mode: ReplicationMode,
    replication_factor: u8,
    member_count: usize,
    live_count: usize,
) -> u8 {
    if !replication_mode.is_sync() && member_count > 0 && live_count * 2 == member_count {
        return synchro_quorum(replication_mode, replication_factor, live_count);
    }
    synchro_quorum(replication_mode, replication_factor, member_count)
}

/// Counts the members of a replicaset for [`synchro_quorum`]: every instance
/// which has joined it, and isn't expelled yet. An offline instance is a
/// member too: it may still be running, e.g. cut off by a network partition,
/// and the rest of the replicaset must not be able to elect a leader without
/// it.
///
/// An instance which is still joining, that is it has never been activated
/// yet, isn't a member: it can't vote before it's replicating, and if it
/// counted, the master would be fenced as soon as it registers the instance,
/// so other instances couldn't join. Its activation changes the target state,
/// so the governor reconfigures the replication and raises the quorum then.
pub fn count_replicaset_members<I>(instances: impl IntoIterator<Item = I>) -> usize
where
    I: Borrow<Instance>,
{
    instances
        .into_iter()
        .filter(|instance| {
            let instance = instance.borrow();
            has_states!(instance, not Expelled -> *) && !is_joining(instance)
        })
        .count()
}

/// Returns `true` if the instance has never been activated yet, see
/// [`count_replicaset_members`].
fn is_joining(instance: &Instance) -> bool {
    instance.target_state.incarnation == 0
}

/// Returns `true` if this instance and the peers it receives replication from
/// make up the synchro quorum, so it may win an election.
///
/// This is the Tarantool view of the replicaset, which unlike the topology
/// from the raft log is never behind. But right after a replication
/// reconfiguration the peers are reconnecting for a moment.
pub fn enough_live_members_to_win_election() -> bool {
    // `pico._enough_live_members_to_win_election` is defined in `src/luamod.lua`
    let res =
        lua_function("_enough_live_members_to_win_election").and_then(|func| Ok(func.into_call()?));
    match res {
        Ok(enough) => enough,
        Err(e) => {
            crate::tlog!(Warning, "election: failed to count live members: {e}");
            false
        }
    }
}

thread_local! {
    /// Serializes the promotions of this instance, see [`promote`].
    static PROMOTION_LOCK: Rc<fiber::Mutex<()>> = Rc::new(fiber::Mutex::new(()));
}

/// Makes this instance the election leader of its replicaset which owns the
/// synchro queue, so it's writable unless it's fenced, see [`fence`]. An
/// election leader can't write to the synchronous spaces before it owns the
/// queue, see [`wait_promotion_written`].
///
/// This is the only place which calls `box_promote`. The promotion is
/// requested by:
/// - the governor, which transfers the mastership, see
///   [`proc_replication_promote`];
/// - the election watcher, which recovers the replicaset master after a
///   quorum loss, see [`synchro_election_watcher`].
///
/// These promotions are serialized. Otherwise tarantool would reject a
/// concurrent `box_promote` with ER_IN_ANOTHER_PROMOTE, or would return at once
/// while the election of the other one is still in progress. One which waited
/// for another returns at once if that one has made this instance the leader,
/// and runs a new election otherwise.
///
/// [`proc_replication_promote`]: crate::rpc::replication::proc_replication_promote
///
/// Timeline of box_promote:
/// 1. box_promote() is called.
/// 2. Raft election. The term bump is written to the WAL and votes are collected.
/// 3. Election won, so box.info.election.state becomes leader. From here box_promote only waits; the rest is done by the raft_worker fiber.
/// 4. raft_worker waits for the old master's transactions. Unconfirmed sync transactions left in the queue by the old owner must get a quorum of acks first (box_wait_limbo_acked). This is quick if the queue is empty, but it can take a while.
/// 5. raft_worker writes the PROMOTE to the WAL. Once written, it goes into pending_promotes and counts as its own first ack.
/// 6. box_promote returns. It was only waiting for that write attempt.
/// 7. Waiting for acks. Replicas receive the PROMOTE, write it, and ack.
/// 8. Quorum reached. The leader writes a CONFIRM for the PROMOTE to the WAL.
/// 9. The PROMOTE is applied: owner_id = self and the pending entry is removed. That's the queue claim, and the instance becomes writable.
pub fn promote() -> Result<(), TarantoolError> {
    let lock = PROMOTION_LOCK.with(Rc::clone);
    let _guard = lock.lock();
    box_promote()?;
    wait_promotion_written(PROMOTION_WRITE_TIMEOUT)
}

/// Keeps this instance from accepting writes, even while it's the election
/// leader. The governor fences the old master of an async tier before it
/// transfers the mastership, see [`proc_replication_demote`].
///
/// [`proc_replication_demote`]: crate::rpc::replication::proc_replication_demote
pub fn fence() -> Result<()> {
    set_cfg_field("read_only", true)?;
    Ok(())
}

/// Lifts the [`fence`]. The writability of an instance which runs elections
/// is then up to its election role.
pub fn unfence() -> Result<()> {
    set_cfg_field("read_only", false)?;
    Ok(())
}

/// How long [`wait_promotion_written`] waits.
pub const PROMOTION_WRITE_TIMEOUT: Duration = Duration::from_secs(1);

/// Waits until this instance, promoted by `box_promote`, can write to the
/// synchronous spaces.
///
/// `box_promote` may return before that. It returns at once if this instance
/// is the election leader already, or if a promotion is in progress, and
/// otherwise once the PROMOTE entry is written. But the instance becomes the
/// synchro queue owner only when the synchro quorum confirms that entry, see
/// the timeline in [`promote`]. Until then the instance is read-only, except
/// while the queue has no owner and the PROMOTE isn't written yet: then
/// `box.info.ro` is `false`, but a write to a synchronous space fails with
/// ER_SYNC_QUEUE_UNCLAIMED. So both are checked.
///
/// Fails with ER_INTERFERING_ELECTIONS as soon as this instance isn't the
/// election leader anymore, e.g. it lost the connection quorum or a peer won
/// a newer term: then it can't claim the queue in the term it has won. This
/// is what upstream tarantool's `box_wait_for_limbo_claim` does, which our
/// fork lacks.
fn wait_promotion_written(timeout: Duration) -> Result<(), TarantoolError> {
    const POLL_INTERVAL: Duration = Duration::from_millis(10);

    let deadline = fiber::clock().saturating_add(timeout);
    loop {
        if !box_is_ro() && box_owns_synchro_queue() {
            return Ok(());
        }

        if fiber::is_cancelled() {
            let e = BoxError::new(TarantoolErrorCode::ProcC, "fiber is cancelled");
            return Err(e.into());
        }

        // If `box.info.election` can't be read, the deadline still bounds the
        // wait.
        if let Ok(election) = box_info_election() {
            if election.state != "leader" {
                let e = BoxError::new(
                    TarantoolErrorCode::InterferingElections,
                    format!(
                        "lost election leadership before claiming the synchro queue: state is {}, leader is {}",
                        election.state, election.leader
                    ),
                );
                return Err(e.into());
            }
        }

        let now = fiber::clock();
        if now >= deadline {
            let reason = box_ro_reason().unwrap_or("the synchro queue is not owned yet");
            let e = BoxError::new(
                TarantoolErrorCode::Readonly,
                format!("instance is still in read only mode after promotion: {reason}"),
            );
            return Err(e.into());
        }
        fiber::sleep(POLL_INTERVAL.min(deadline.duration_since(now)));
    }
}

/// Make the tarantool system spaces synchronous. Masters apply alters
/// directly to the storage engine and replicas receive it over tarantool
/// replication.
///
/// `_cluster` is deliberately not in the list: there is a bug,
/// see <https://git.picodata.io/core/picodata/-/work_items/3009>.
pub fn make_system_spaces_sync() -> Result<()> {
    let lua = ::tarantool::lua_state();
    // The alters must not become synchronous transactions themselves: as soon
    // as `_space` is flipped to synchronous, every following alter writes into
    // synchronous `_space`, and needs a quorum.
    transaction_force_async(|| -> Result<(), TarantoolError> {
        lua.exec(
            r#"
              local sys = {'_space','_index','_user','_priv','_func',
                           '_schema','_collation','_sequence','_sequence_data',
                           '_truncate','_trigger'}
              for _, name in ipairs(sys) do
                  local s = box.space[name]
                  if s and not s.is_sync then s:alter({is_sync = true}) end
              end
          "#,
        )?;
        Ok(())
    })?;

    crate::tlog!(Info, "tarantool system spaces are now synchronous");
    Ok(())
}

/// Make the tarantool system spaces synchronous if needed
/// (current tier has `replication_mode = sync` param).
pub fn maybe_make_system_spaces_sync() -> Result<()> {
    let (replication_mode, _) = get_this_tier_replication_mode_and_factor()?;
    if replication_mode.is_sync() {
        return make_system_spaces_sync();
    }

    Ok(())
}

/// Get replication mode and factor for current instance's tier.
///
/// Get values from config. Use when cannot get values from
/// catalog_cache reliably (no synchronization via `wait_index`).
pub fn get_this_tier_replication_mode_and_factor() -> Result<(ReplicationMode, u8)> {
    get_tier_replication_mode_and_factor(PicodataConfig::get())
}

/// Same as [`get_this_tier_replication_mode_and_factor`], but reads from the provided
/// config instead of the global one.
pub fn get_tier_replication_mode_and_factor(
    config: &PicodataConfig,
) -> Result<(ReplicationMode, u8)> {
    let my_tier_name = config.effective_instance_tier();
    let Some(tiers) = &config.cluster.tier else {
        return Ok((
            DEFAULT_REPLICATION_MODE,
            config.cluster.default_replication_factor(),
        ));
    };
    let (_, tier) = tiers
        .iter()
        .find(|(tier_name, _)| my_tier_name == tier_name)
        .ok_or_else(|| {
            Error::other(format!(
                "failed to get tier info from config: tier name = {my_tier_name}"
            ))
        })?;

    Ok((
        tier.replication_mode,
        tier.replication_factor
            .unwrap_or_else(|| config.cluster.default_replication_factor()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synchro_quorum_of_sync_tier_is_majority_of_members_or_replication_factor() {
        let sync = ReplicationMode::Sync;

        // Not full yet: the quorum of a full replicaset.
        assert_eq!(synchro_quorum(sync, 3, 1), 2);
        assert_eq!(synchro_quorum(sync, 2, 1), 2);

        // Full.
        assert_eq!(synchro_quorum(sync, 1, 1), 1);
        assert_eq!(synchro_quorum(sync, 2, 2), 2);
        assert_eq!(synchro_quorum(sync, 3, 3), 2);

        // More members than the replication factor.
        assert_eq!(synchro_quorum(sync, 1, 2), 2);
        assert_eq!(synchro_quorum(sync, 1, 3), 2);
        assert_eq!(synchro_quorum(sync, 3, 4), 3);
        assert_eq!(synchro_quorum(sync, 3, 5), 3);
    }

    #[test]
    fn synchro_quorum_of_async_tier_is_majority_of_members() {
        let async_ = ReplicationMode::Async;

        // Not full yet, or some members expelled.
        assert_eq!(synchro_quorum(async_, 3, 1), 1);
        assert_eq!(synchro_quorum(async_, 2, 1), 1);
        assert_eq!(synchro_quorum(async_, 3, 2), 2);

        // Full.
        assert_eq!(synchro_quorum(async_, 1, 1), 1);
        assert_eq!(synchro_quorum(async_, 2, 2), 2);
        assert_eq!(synchro_quorum(async_, 3, 3), 2);

        // More members than the replication factor.
        assert_eq!(synchro_quorum(async_, 1, 2), 2);
        assert_eq!(synchro_quorum(async_, 1, 3), 2);
        assert_eq!(synchro_quorum(async_, 3, 4), 3);

        // The members are not known yet, see `Cfg::set_replication_parameters`.
        assert_eq!(synchro_quorum(async_, 3, 0), 1);
    }

    #[test]
    fn synchro_quorum_breaks_tie_in_async_tier() {
        let async_ = ReplicationMode::Async;
        let sync = ReplicationMode::Sync;

        // Exactly half of the members are offline.
        assert_eq!(synchro_quorum_breaking_tie(async_, 2, 2, 1), 1);
        assert_eq!(synchro_quorum_breaking_tie(async_, 1, 2, 1), 1);
        assert_eq!(synchro_quorum_breaking_tie(async_, 3, 4, 2), 2);
        assert_eq!(synchro_quorum_breaking_tie(sync, 2, 2, 1), 2);
        assert_eq!(synchro_quorum_breaking_tie(sync, 3, 4, 2), 3);

        // Not a tie.
        assert_eq!(synchro_quorum_breaking_tie(async_, 2, 2, 2), 2);
        assert_eq!(synchro_quorum_breaking_tie(async_, 2, 2, 0), 2);
        assert_eq!(synchro_quorum_breaking_tie(async_, 3, 3, 1), 2);
        assert_eq!(synchro_quorum_breaking_tie(async_, 3, 3, 2), 2);
        assert_eq!(synchro_quorum_breaking_tie(async_, 3, 4, 1), 3);
        assert_eq!(synchro_quorum_breaking_tie(async_, 1, 1, 0), 1);
        assert_eq!(synchro_quorum_breaking_tie(async_, 1, 0, 0), 1);
    }
}

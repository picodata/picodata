use crate::config::{ElectionMode, PicodataConfig, ReplicationMode, DEFAULT_REPLICATION_MODE};
use crate::tarantool::{
    box_info_election, box_is_ro, box_owns_synchro_queue, box_promote, box_ro_reason, cfg_field,
};
use crate::traft::{error::Error, Result};
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

thread_local! {
    /// Serializes the promotions of this instance, see [`promote`].
    static PROMOTION_LOCK: Rc<fiber::Mutex<()>> = Rc::new(fiber::Mutex::new(()));
}

/// Makes this instance the election leader of its replicaset which owns the
/// synchro queue, so it's writable. An election leader can't write to the
/// synchronous spaces before it owns the queue, see
/// [`wait_promotion_written`].
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

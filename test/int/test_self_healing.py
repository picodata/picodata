import pytest
import time
import random
import re

from conftest import Cluster, Instance, TarantoolError, Retriable, ErrorCode, log_crawler


@pytest.fixture
def cluster3(cluster: Cluster):
    cluster.deploy(instance_count=3)
    return cluster


def break_picodata_procs(instance: Instance):
    instance.eval(
        """
        local log = require 'log'
        for _, proc in box.space._func:pairs()
            :filter(function (func)
                return func.name:sub(1,1) == '.' and func.language == 'C'
            end)
        do
            box.schema.func.drop(proc.name)
            box.schema.func.create(proc.name, {
                language = 'lua',
                body = [[
                    function()
                        require 'fiber'.sleep(99999)
                    end
                ]]
            })
            log.info('\x1b[31m'.. proc.name .. ' is broken\x1b[0m')
        end
    """
    )


def fix_picodata_procs(instance: Instance):
    instance.eval(
        """
        local log = require 'log'
        for _, proc in box.space._func:pairs()
            :filter(function (func)
                return func.name:sub(1,1) == '.' and func.language ~= 'C'
            end)
        do
            box.schema.func.drop(proc.name)
            box.schema.func.create(proc.name, { language = 'C' })
            log.info('\x1b[32m'.. proc.name .. ' is fixed\x1b[0m')
        end
    """
    )


def test_log_rollback(cluster3: Cluster):
    # Scanario: the Leader can't propose without Followers
    #   Given a cluster
    #   When all Followers killed without graceful shutdown
    #   And the Leader proposing changes
    #   Then the proposition failed

    i1, i2, i3 = cluster3.instances
    i1.assert_raft_status("Leader")
    i2.assert_raft_status("Follower")
    i3.assert_raft_status("Follower")

    key = 0

    def propose_state_change(srv: Instance, value, timeout=10):
        deadline = time.time() + timeout
        nonlocal key
        while True:
            try:
                index, _ = cluster3.cas(
                    "insert",
                    "_pico_property",
                    (f"check{key}", value),
                    peer=srv,
                )
                srv.raft_wait_index(index)
                break
            except TarantoolError as e:
                print(f"\x1b[33m### CaS error: {e}", end="")
                if time.time() > deadline:
                    print(", timeout\x1b[0m")
                    raise

                if ErrorCode.is_retriable_for_cas(e.args[0]):
                    print(", retrying...\x1b[0m")
                    time.sleep(0.1)
                    continue

                print(", failure!\x1b[0m")
                raise
        key += 1

    propose_state_change(i1, "i1 is a leader")

    # Simulate the network partitioning: i1 can't reach i2 and i3.
    break_picodata_procs(i2)
    break_picodata_procs(i3)

    # No operations can be committed, i1 is alone.
    with pytest.raises(TarantoolError):
        propose_state_change(i1, "i1 lost the quorum")

    # And now i2 + i3 can't reach i1.
    break_picodata_procs(i1)
    fix_picodata_procs(i2)
    fix_picodata_procs(i3)

    # Help i2 to become a new leader
    i2.promote_or_fail()
    Retriable(timeout=10, rps=5).call(lambda: i3.assert_raft_status("Follower", i2.raft_id))

    print(i2.call("pico.raft_log", dict(return_string=True)))
    print(i2.call("box.space._raft_state:select"))
    propose_state_change(i2, "i2 takes the leadership")

    # Now i1 has an uncommitted, but persisted entry that should be rolled back.
    fix_picodata_procs(i1)
    Retriable(timeout=10, rps=5).call(lambda: i1.assert_raft_status("Follower", i2.raft_id))

    propose_state_change(i1, "i1 is alive again")


def test_leader_disruption(cluster3: Cluster):
    # Scenario: Follower reconnection on disconnect from the cluster
    #   Given a cluster
    #   When any Follower lost network connection with all other cluster nodes
    #   And this Follower starts new election
    #   And the network connection was established again
    #   Then the Follower became Follower as it was before

    i1, i2, i3 = cluster3.instances
    i1.assert_raft_status("Leader")
    i2.assert_raft_status("Follower")
    i3.assert_raft_status("Follower")

    # Simulate asymmetric network failure.
    # Node i3 doesn't receive any messages,
    # including the heartbeat from the leader.
    # Then it starts a new election.
    i3.call("box.schema.func.drop", ".proc_raft_interact")

    # Speed up election timeout
    i3.eval(
        """
        while pico.raft_status().raft_state == 'Follower' do
            pico.raft_tick(1)
        end
        """
    )
    i3.assert_raft_status("PreCandidate", None)

    # Advance the raft log. It makes i1 and i2 to reject the RequestPreVote.
    i1.raft_propose_nop()

    # Restore normal network operation
    i3.call(
        "box.schema.func.create",
        ".proc_raft_interact",
        {"language": "C", "if_not_exists": True},
    )

    # i3 should become the follower again without disrupting i1
    Retriable(timeout=10, rps=5).call(lambda: i3.assert_raft_status("Follower", i1.raft_id))


def target_state_reason(peer: Instance, target: Instance) -> str:
    [[reason]] = peer.sql("SELECT target_state_reason FROM _pico_instance WHERE name = ?", target.name)
    return reason


def test_instance_automatic_offline_detection(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3)
    rows = i1.sql("ALTER SYSTEM SET governor_auto_offline_timeout=0.5")
    assert rows["row_count"] == 1

    cluster.wait_has_states(i3, "Online", "Online")

    i3.kill()

    # Give the sentinel some time to detect the problem and act accordingly.
    time.sleep(2)

    cluster.wait_has_states(i3, "Offline", "Offline")
    reason = target_state_reason(i1, target=i3)
    assert reason.startswith("No successful RPC for ") or re.match(r"Applied index \d+ hasn't changed for ", reason)

    i3.start()

    cluster.wait_has_states(i3, "Online", "Online")


def test_instance_automatic_offline_after_leader_change(cluster: Cluster):
    i1, i2, i3, i4, i5 = cluster.deploy(instance_count=5)
    rows = i1.sql("ALTER SYSTEM SET governor_auto_offline_timeout=0.5")
    assert rows["row_count"] == 1

    # Make sure target instances are fully initialized
    i1.assert_raft_status("Leader")

    # Brutally kill 2 instances (one of them is the current leader)
    i1.kill()
    i3.kill()

    # A new leader is chosen, it detects that instances are offline and changes their states eventually
    cluster.wait_has_states(i1, "Offline", "Offline")
    cluster.wait_has_states(i3, "Offline", "Offline")

    leader = cluster.leader()
    reason = target_state_reason(leader, target=i1)
    assert reason.startswith("No successful RPC for ")
    reason = target_state_reason(leader, target=i3)
    assert reason.startswith("No successful RPC (at all) for ")

    # Just for clarity, the new leader is not the old one
    leader = cluster.leader()
    assert leader != i1

    runtime_info = leader.call(".proc_runtime_info")
    internal = runtime_info["internal"]
    assert internal["sentinel_last_action"] == "auto offline by leader"
    assert internal["sentinel_index_of_last_success"] < leader.raft_get_index()
    assert internal["sentinel_time_since_last_success"] > 0


def test_governor_timeout_when_proposing_raft_op(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3)

    i2.call("pico._inject_error", "BLOCK_WHEN_PERSISTING_DDL_COMMIT", True)
    i3.call("pico._inject_error", "BLOCK_WHEN_PERSISTING_DDL_COMMIT", True)

    with pytest.raises(TimeoutError):
        i1.sql(
            """
            CREATE TABLE dining_table (id INTEGER NOT NULL PRIMARY KEY) DISTRIBUTED BY (id)
            """
        )

    # Wait until governor starts applying the DDL.
    # This will block because both followers can't apply.
    i1.wait_governor_status("apply clusterwide schema change")

    # FIXME: this is going to be flaky, need some way to make this stable
    time.sleep(3)

    i2.call("pico._inject_error", "BLOCK_WHEN_PERSISTING_DDL_COMMIT", False)
    i3.call("pico._inject_error", "BLOCK_WHEN_PERSISTING_DDL_COMMIT", False)

    # Wait until governor finishes with all the needed changes.
    i1.wait_governor_status("idle")


def test_sentinel_backoff(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3)

    # Make it so the instance stops receiving raft log updates right after it
    # finds out that cluster thinks it's Offline
    raft_failure = "BLOCK_AFTER_APPLIED_ENTRY_IF_OWN_TARGET_STATE_OFFLINE"
    i3.call("pico._inject_error", raft_failure, True)

    # Also break the sentinel's ability to communicate to check how often it retries
    connection_failure = "SENTINEL_CONNECTION_POOL_CALL_FAILURE"
    i3.call("pico._inject_error", connection_failure, True)

    # Make it so everybody thinks `i3` if Offline
    i1.call(
        ".proc_update_instance_v2",
        [
            i3.name,
            i3.cluster_name,
            i3.cluster_uuid,
            None,
            "Offline",
            None,
            False,
            None,
        ],
        "injected offline",
    )

    def check_sentinel_failed_with_injection():
        info = i3.call(".proc_runtime_info")
        internal = info["internal"]
        # Sentinel is trying to change the instance's state to Online
        assert internal["sentinel_last_action"] == "auto online by self"
        assert internal["sentinel_fail_streak"]["count"] > 0
        assert internal["sentinel_fail_streak"]["error"]["code"] == ErrorCode.Other
        assert internal["sentinel_fail_streak"]["error"]["message"] == "injected error"

    Retriable().call(check_sentinel_failed_with_injection)

    old_counter = i1.wait_governor_status("idle")
    # Everybody thinks `i3` is Offline
    i1.wait_has_states("Offline", "Offline", target=i3)
    assert target_state_reason(i1, target=i3) == "injected offline"

    # Restore the sentinel's ability to send requests, but it will still not see
    # the result of it's requests because it's raft log update is broken, so it
    # will now block while waiting for the raft log to advance
    i3.call("pico._inject_error", connection_failure, False)

    def check_sentinel_succeeded_and_is_waiting():
        info = i3.call(".proc_runtime_info")
        internal = info["internal"]
        # Sentinel is trying to change the instance's state to Online
        assert internal["sentinel_last_action"] == "auto online by self"
        # No failures are registered, because technically the request was handled
        assert "sentinel_fail_streak" not in internal
        # This is important, this means that since last sentinel request was sent
        # no raft log entries have been applied. This is the criteria for the
        # sentinel backoff
        index_of_attempt = internal["sentinel_index_of_last_success"]
        assert index_of_attempt == info["raft"]["applied"]
        return index_of_attempt

    old_index_of_attempt = Retriable().call(check_sentinel_succeeded_and_is_waiting)

    counter = i1.wait_governor_status("update current sharding configuration")
    # Governor has performed 4 steps:
    # - change target_state=Online (also bump config versions)
    # - configure replication within replicaset (with `i3` isolated from others)
    # - synchornize replication on `i3`
    # - configure replication within replicaset (now with `i3` same as on others)
    #
    # This is important, because it shows that there weren't a bunch of
    # redundant state updates (regression test for the original bug report)
    assert counter - old_counter == 4
    old_counter = counter

    # Now `i3` is trying to go back Online, but cannot yet, because it's raft loop is broken
    i1.wait_has_states("Offline", "Online", target=i3)
    assert target_state_reason(i1, target=i3) == "auto-online"

    # Fix `i3`'s raft loop
    i3.call("pico._inject_error", raft_failure, False)

    # It's finally online
    i1.wait_has_states("Online", "Online", target=i3)
    assert target_state_reason(i1, target=i3) == "auto-online"

    counter = i1.wait_governor_status("idle")
    # Also just 2 steps, no spam
    assert counter - old_counter == 2
    old_counter = counter

    info = i3.call(".proc_runtime_info")
    internal = info["internal"]
    # Last action by sentinel is recorded for debugging purposes
    assert internal["sentinel_last_action"] == "auto online by self"
    # There's no fail streak, because the last action was a success
    assert "sentinel_fail_streak" not in internal
    index_of_attempt = internal["sentinel_index_of_last_success"]
    # Applied index at the moment of last request is less than the most recent
    # applied index, which means that the request was handled and instance is
    # aware of the results
    assert index_of_attempt < info["raft"]["applied"]
    # And this is important, because it means that there was only this one
    # request to update target state (no spam)
    assert old_index_of_attempt == index_of_attempt


def test_automatic_offline_due_to_global_dml_conflict(cluster: Cluster):
    leader, follower, victim = cluster.deploy(instance_count=3)

    # Create a global table
    leader.sql("CREATE TABLE foo (bar INTEGER PRIMARY KEY, baz TEXT) DISTRIBUTED GLOBALLY")

    # Reduce the timeout so that the test doesn't take too long
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 3")

    victims_applied = victim.raft_get_index()
    # Do an illegal operation which will break victim's raft_main_loop
    victim.eval("box.space.foo:put({1337, 'bad'})")

    # Now do a legal operation
    leader.sql("INSERT INTO foo VALUES (1337, 'good')")

    latest_index = leader.call(".proc_get_index")
    # This guy successfully receives the operation
    follower.call(".proc_wait_index", latest_index, 10)

    # But this guy is now broken
    try:
        victim.call(".proc_wait_index", latest_index, 1)
    except TarantoolError as e:
        assert e.args[0] == "ER_TIMEOUT"

    # And it's state is eventually turned Offline automatically
    cluster.wait_has_states(victim, "Offline", "Offline")
    reason = target_state_reason(leader, target=victim)
    assert reason.startswith(f"Applied index {victims_applied} hasn't changed for ")

    # Meanwhile the cluster keeps working fine
    leader.sql("INSERT INTO foo VALUES (69105, 'better')")
    latest_index = leader.call(".proc_get_index")
    follower.call(".proc_wait_index", latest_index, 10)

    # Now the admin comes along and fixes the error manually
    victim.eval("box.space.foo:delete({1337})")

    # And victim automatically comes back eventually
    victim.wait_online()

    # Sanity check
    victim.call(".proc_wait_index", latest_index, 10)
    rows = victim.call("box.space.foo:select")
    assert rows == [
        [1337, "good"],
        [69105, "better"],
    ]


def test_automatic_offline_by_applied_index_is_not_too_eager(cluster: Cluster):
    leader, follower = cluster.deploy(instance_count=2)

    leader.promote_or_fail()
    # Reduce the timeout so that the test doesn't take too long
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 10")
    leader.sql("CREATE TABLE global_table (id INT PRIMARY KEY) DISTRIBUTED GLOBALLY")

    # Inject a tiny delay but no longer that governor_auto_offline_timeout
    error_injection = "DELAY_BEFORE_APPLYING_RAFT_ENTRY"
    follower.call("pico._inject_error", error_injection, "5")

    # I wish we could speed up the time, but unfortunately I don't see any
    # better way to check what happens when we haven't applied any raft log
    # entries in a long time other then not applying any raft log entries in a
    # long time
    time.sleep(10)

    # Propose a raft log entry
    leader.sql("INSERT INTO global_table VALUES (1)")

    # Wait until the lagger catches up
    follower.raft_wait_index(leader.raft_get_index())

    # Make sure auto offline wasn't triggered
    runtime_info = leader.call(".proc_runtime_info")
    internal = runtime_info["internal"]
    assert "sentinel_last_action" not in internal


@pytest.mark.xfail(
    reason=(
        """The following issues should be fixed before this test is stable:
- https://git.picodata.io/core/picodata/-/issues/2349
- https://git.picodata.io/core/picodata/-/issues/2336
"""
    )
)
def test_bootstrap_fault_tolerance(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        default:
            replication_factor: 2
        """
    )
    cluster.add_instance(wait_online=False, tier="default")
    cluster.add_instance(wait_online=False, tier="default")
    cluster.add_instance(wait_online=False, tier="default")
    cluster.add_instance(wait_online=False, tier="default")
    cluster.add_instance(wait_online=False, tier="default")
    cluster.add_instance(wait_online=False, tier="default")

    for instance in cluster.instances:
        instance.start()

    time.sleep(random.random() * 3)

    # Don't wait_online on purpose

    for instance in cluster.instances:
        instance.terminate()

    for instance in cluster.instances:
        instance.start()

    cluster.wait_online()


def test_automatic_offline_due_to_sharded_dml_conflict(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        arbiter:
            can_vote: true
        storage:
            can_vote: false
            replication_factor: 2
        """
    )
    leader = cluster.add_instance(tier="arbiter", wait_online=False)
    storage_1_1 = cluster.add_instance(tier="storage", wait_online=False)
    storage_1_2 = cluster.add_instance(tier="storage", wait_online=False)
    cluster.wait_online()

    # Create a sharded table
    leader.sql("CREATE TABLE foo (bar INTEGER PRIMARY KEY, baz TEXT) DISTRIBUTED BY (bar) IN TIER storage")

    # Disable the replication error check so that we know what we're missing and start valueing it
    leader.sql("ALTER SYSTEM SET governor_check_replication_error = false")

    # Put one of the instance to sleep...
    storage_1_2.terminate()

    leader.sql("INSERT INTO foo VALUES (1337, 'v1')")

    storage_1_1.terminate()
    storage_1_2.start()
    storage_1_2.wait_online()

    leader.sql("INSERT INTO foo VALUES (1337, 'v2')")

    storage_1_1.start()
    # Governor is stuck waiting until instance synchronizes with replicaset,
    # which will never happen
    leader.wait_governor_status("replicaset sync")
    cluster.wait_has_states(storage_1_1, "Offline", "Online")

    # Now let's enable auto offline due due to replication error
    leader.sql("ALTER SYSTEM SET governor_check_replication_error = true")

    # After that auto offline by sentinel works rather quick
    counter = leader.wait_governor_status("idle")
    cluster.wait_has_states(storage_1_1, "Offline", "Offline")
    reason = target_state_reason(leader, target=storage_1_1)
    assert reason.startswith("Replication broken")
    assert "Duplicate key exists in unique index" in reason

    # Just a sanity check, that restarting the instance doens't change anything
    # while it's broken
    storage_1_1.terminate()
    storage_1_1.start()
    leader.wait_governor_status("idle", old_step_counter=counter)
    cluster.wait_has_states(storage_1_1, "Offline", "Offline")

    # Instance is still actually alive, and we can go fix it manually, or
    # recover the data if needed
    #
    # Note: `Retriable` is needed because wait_online doesn't work and there's
    # no other good way to syncrhonize
    Retriable(timeout=10).call(
        storage_1_1.eval,
        """
        box.cfg{read_only = false}
        box.space.foo:delete(1337)
        """,
    )

    # Now let's restart the instance so it tries becoming online again and now
    # succeeds
    storage_1_1.terminate()
    storage_1_1.start()
    storage_1_1.wait_online()
    leader.wait_governor_status("idle")

    # This guy is also still online obviously
    storage_1_2.wait_online()


def test_non_voter_doesnt_become_leader(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        arbiter:
            can_vote: true
        storage:
            can_vote: false
        """
    )

    arbiter = cluster.add_instance(wait_online=False, tier="arbiter")
    cluster.add_instance(wait_online=False, tier="storage")
    cluster.add_instance(wait_online=False, tier="storage")
    cluster.add_instance(wait_online=False, tier="storage")

    followers = [i for i in cluster.instances if i is not arbiter]

    lcs = [log_crawler(i, "leader not known for too long, trying to promote") for i in followers]

    cluster.wait_online()

    for i in followers:
        i.terminate()

    arbiter.terminate()

    error_injection = "LEADER_NOT_KNOWN_DURING_RESTART"
    for i in followers:
        i.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
        i.start()

    timeout = 3.0
    for i in followers:
        try:
            i.wait_online(timeout=timeout)
        except AssertionError as e:
            # Instance.wait_online raises AssertionError on timeout because the
            # failure message is better that way
            assert f"Timed out waiting for instance '{i.name}' state 'Online'" in str(e)

        # After first iteration everybody has waited for 3 seconds already so let's reset the timeout
        timeout = 0.1

    for lc in lcs:
        assert not lc.matched

    try:
        assert cluster.leader()
    except Exception as e:
        assert e.args[0] == "leader unknown"

    for i in followers:
        i.call("pico._inject_error", error_injection, False)

    arbiter.start()
    cluster.wait_online()

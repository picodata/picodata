import time

from conftest import Cluster, Retriable
from framework.util.build import picodata_executable_path, skip_unless_debug_tarantool


def test_vote_waits_for_the_wal_flush(cluster: Cluster):
    """A vote grant runs under TXN_SYNC_WAL, so the response must not leave
    the instance while its flush is stuck in fdatasync: with the flush
    blocked on one voter, the surviving candidate must not be able to win
    the election."""

    # The injection we stall the grant on lives under NDEBUG in the WAL
    # writer, so check the build before deploying anything.
    skip_unless_debug_tarantool(picodata_executable_path())

    cluster.deploy(instance_count=3)
    leader = cluster.wait_leader_elected()
    voter, candidate = [i for i in cluster.instances if i is not leader]

    # Every TXN_SYNC_WAL commit now stalls inside fdatasync on the voter.
    voter.eval("box.error.injection.set('ERRINJ_SYNC_WAL_DELAY', true)")

    leader.kill()

    # The election timeout is randomized 10-20s, so wait for the campaign
    # itself. The pre-vote round writes nothing - only the real vote grant
    # is stuck in the flush; a delivered grant would promote the candidate
    # within milliseconds, so check Leader explicitly.
    def campaign_started():
        state = candidate.call(".proc_raft_info")["state"]
        assert state != "Leader", "leader elected while the vote flush is blocked"
        assert state in ("PreCandidate", "Candidate"), "the candidate has not campaigned yet"

    Retriable().call(campaign_started)

    # With the response delivered at write(2) the candidate would become
    # the leader immediately; the response is stuck in the flush instead.
    time.sleep(5)
    assert candidate.call(".proc_raft_info")["state"] != "Leader", "leader elected while the vote flush is blocked"

    # Release the flush: the vote grant commits, the response goes out and
    # the candidate wins.
    voter.eval("box.error.injection.set('ERRINJ_SYNC_WAL_DELAY', false)")
    cluster.wait_leader_elected(instances=[candidate, voter])

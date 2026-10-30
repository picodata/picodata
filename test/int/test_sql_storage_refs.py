"""
SQL protects the data of a storage from bucket moves with a vshard storage
reference: a bucket move does not start while the storage is referenced.
"""

from conftest import Instance


def test_expired_ref_does_not_block_moves(instance: Instance):
    # A router took a ref on the first step of a two-step dispatch and never
    # came back. Once the ref expires, it must not block bucket moves.
    moved, ref_count = instance.eval(
        """
        local fiber = require('fiber')
        local sched = require('vshard.registry').storage_sched
        assert(pico.dispatch.lref.add(1, 'gone-router', 0.1))
        fiber.sleep(0.3)
        local ok = sched.move_start(0.5)
        if ok then
            sched.move_end(1)
        end
        return {ok ~= nil, sched.ref_count}
        """
    )
    assert moved
    assert ref_count == 0

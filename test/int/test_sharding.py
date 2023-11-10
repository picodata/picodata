from conftest import (
    Cluster,
)


def test_sharding_reinitializes_on_restart(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    assert i1.call("vshard.router.info") is not None

    incarnation = i1.eval("return pico.instance_info().current_grade.incarnation")

    # Instance silently dies without it's grade being updated
    i1.kill()

    # Instance restarts, it's incarnation is updated and governor reconfigures
    # all the subsystems
    i1.start()
    i1.wait_online(expected_incarnation=incarnation + 1)

    # Vshard is configured even though the configuration didn't change
    assert i1.call("vshard.router.info") is not None

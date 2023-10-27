from conftest import Cluster
from test_shutdown import log_crawler


def test_run_init_cfg_enoent(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.env.update({"PICODATA_INIT_CFG": "./unexisting_dir/trash.yaml"})
    err = """\
error: Invalid value \"./unexisting_dir/trash.yaml\" for '--init-cfg <PATH>': can't read \
from ./unexisting_dir/trash.yaml, error: No such file or directory (os error 2)\
"""
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched


def test_run_init_cfg_with_duplicated_tier_names(cluster: Cluster):
    cfg = {
        "tiers": [
            {"name": "storage", "replication_factor": 1},
            {"name": "storage", "replication_factor": 2},
        ]
    }

    cluster.set_init_cfg(cfg)
    i1 = cluster.add_instance(wait_online=False)
    err = 'found tiers with the same name - "storage" in config'
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched


def test_run_init_cfg_with_empty_tiers(cluster: Cluster):
    cfg: dict = {"tiers": []}
    cluster.set_init_cfg(cfg)
    i1 = cluster.add_instance(wait_online=False)
    err = "empty 'tiers' field in 'init-cfg'"
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched


def test_run_with_tier_which_is_not_in_tier_list(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.tier = "unexistent_tier"
    err = "tier 'unexistent_tier' for current instance is not found in 'init-cfg'"
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched


def test_run_init_cfg_with_garbage(cluster: Cluster):
    cfg = {"trash": [], "garbage": "tier"}
    cluster.set_init_cfg(cfg)
    i1 = cluster.add_instance(wait_online=False)
    err = "error: missing field `tiers`"
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched


def test_run_init_cfg_with_init_replication_factor(cluster: Cluster):
    cfg = {"no matter": "init-cfg doesn't allow to use with `init-replication-factor"}
    cluster.set_init_cfg(cfg)
    i1 = cluster.add_instance(wait_online=False)
    i1.init_replication_factor = 1
    err = """\
error: The argument '--init-replication-factor <INIT_REPLICATION_FACTOR>' \
cannot be used with '--init-cfg <PATH>'\
"""
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched

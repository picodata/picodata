from conftest import Cluster, log_crawler


def test_config_works(cluster: Cluster):
    instance = cluster.add_instance(instance_id=False, wait_online=False)
    config_path = cluster.data_dir + "/config.yaml"
    with open(config_path, "w") as f:
        f.write(
            """
instance:
    instance-id: from-config
    replicaset-id: with-love
    memtx-memory: 42069
            """
        )
    instance.env["PICODATA_CONFIG_FILE"] = config_path
    instance.start()
    instance.wait_online()

    info = instance.call(".proc_instance_info")
    assert info["instance_id"] == "from-config"
    assert info["replicaset_id"] == "with-love"

    assert instance.eval("return box.cfg.memtx_memory") == 42069


def test_run_init_cfg_enoent(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.env.update({"PICODATA_INIT_CFG": "./unexisting_dir/trash.yaml"})
    err = """\
can't read from ./unexisting_dir/trash.yaml, error: No such file or directory (os error 2)\
"""
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched


def test_run_init_cfg_with_duplicated_tier_names(cluster: Cluster):
    cluster.cfg_path = cluster.data_dir + "/init_cfg.yaml"
    cfg_content = """\
tier:
    storage:
        replication_factor: 1
    storage:
        replication_factor: 1
"""
    with open(cluster.cfg_path, "w") as yaml_file:
        yaml_file.write(cfg_content)

    i1 = cluster.add_instance(wait_online=False)
    err = f"""\
error while parsing {cluster.cfg_path}, \
reason: tier: duplicated tier name `storage` found at line 2\
"""
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched


def test_run_init_cfg_with_empty_tiers(cluster: Cluster):
    cfg: dict = {"tier": {}}
    cluster.set_init_cfg(cfg)
    i1 = cluster.add_instance(wait_online=False)
    err = f"empty `tier` field in init-cfg by path: {cluster.cfg_path}"
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched


def test_run_with_tier_which_is_not_in_tier_list(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.tier = "unexistent_tier"
    err = "tier 'unexistent_tier' for current instance is not found in init-cfg"
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched


def test_run_init_cfg_with_garbage(cluster: Cluster):
    cfg = {"trash": [], "garbage": "tier"}
    cluster.set_init_cfg(cfg)
    i1 = cluster.add_instance(wait_online=False)
    err = "reason: missing field `tier`"
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

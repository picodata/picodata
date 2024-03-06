from conftest import Cluster, Instance, log_crawler, color
import os


def test_config_works(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    tiers:
        default:
instance:
    cluster_id: test
    instance_id: from-config
    replicaset_id: with-love
    memtx_memory: 42069
"""
    )
    instance = cluster.add_instance(instance_id=False, wait_online=False)
    instance.start()
    instance.wait_online()

    info = instance.call(".proc_instance_info")
    assert info["instance_id"] == "from-config"
    assert info["replicaset_id"] == "with-love"

    assert instance.eval("return box.cfg.memtx_memory") == 42069


def test_pico_config(cluster: Cluster):
    host = cluster.base_host
    port = cluster.base_port + 1
    listen = f"{host}:{port}"
    data_dir = f"{cluster.data_dir}/my-instance"
    cluster.set_config_file(
        yaml=f"""
cluster:
    tiers:
        deluxe:
instance:
    data_dir: {data_dir}
    listen: {listen}
    peers:
        - {listen}
    cluster_id: my-cluster
    instance_id: my-instance
    replicaset_id: my-replicaset
    tier: deluxe
    audit: {data_dir}/audit.log
    log_level: verbose
"""
    )
    instance = Instance(
        binary_path=cluster.binary_path,
        cwd=cluster.data_dir,
        color=color.cyan,
        config_path=cluster.config_path,
        audit=False,
    )
    cluster.instances.append(instance)

    instance.start()

    # These fields should be set after the instance starts, so that the
    # parameters are extracted from the config file, but before we try doing any
    # rpc, because our test harness needs them to be set
    instance.host = host
    instance.port = port

    instance.wait_online()

    config = instance.call("pico.config")
    assert config == dict(
        cluster=dict(
            tiers=dict(
                deluxe=dict(
                    can_vote=True,
                )
            ),
            unknown_parameters=[],
        ),
        instance=dict(
            cluster_id="my-cluster",
            instance_id="my-instance",
            replicaset_id="my-replicaset",
            tier="deluxe",
            audit=f"{data_dir}/audit.log",
            config_file=instance.config_path,
            data_dir=data_dir,
            listen=dict(host=host, port=str(port)),
            log_level="verbose",
            peers=[dict(host=host, port=str(port))],
            unknown_parameters=[],
        ),
        unknown_sections=[],
    )


def test_default_path_to_config_file(cluster: Cluster):
    instance = cluster.add_instance(instance_id=False, wait_online=False)

    # By default ./config.yaml will be used in the instance's current working directory
    work_dir = cluster.data_dir + "/work-dir"
    os.mkdir(work_dir)
    with open(work_dir + "/config.yaml", "w") as f:
        f.write(
            """
cluster:
    cluster_id: test
    tiers:
        default:
instance:
    memtx_memory: 0xdeadbeef
            """
        )
    instance.start(cwd=work_dir)
    instance.wait_online()

    assert instance.eval("return box.cfg.memtx_memory") == 0xDEADBEEF
    instance.terminate()

    # But if a config is specified explicitly, it will be used instead
    config_path = cluster.data_dir + "/explicit-config.yaml"
    with open(config_path, "w") as f:
        f.write(
            """
cluster:
    cluster_id: test
    tiers:
        default:
instance:
    memtx_memory: 0xcafebabe
            """
        )
    instance.env["PICODATA_CONFIG_FILE"] = config_path

    msg = f"""\
A path to configuration file '{config_path}' was provided explicitly,
but a 'config.yaml' file in the current working directory '{work_dir}' also exists.
Using configuration file '{config_path}'.
"""  # noqa E501
    crawler = log_crawler(instance, msg)
    instance.start(cwd=work_dir)
    instance.wait_online()
    assert crawler.matched

    assert instance.eval("return box.cfg.memtx_memory") == 0xCAFEBABE
    instance.terminate()


def test_init_cfg_is_removed(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)

    i1.env["PICODATA_INIT_CFG"] = "any-path"
    err = """\
error: option `--init-cfg` is removed, use `--config` instead
"""
    crawler = log_crawler(i1, err)
    i1.fail_to_start()
    assert crawler.matched


def test_config_file_enoent(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.env.update({"PICODATA_CONFIG_FILE": "./unexisting_dir/trash.yaml"})
    err = """\
can't read from './unexisting_dir/trash.yaml': No such file or directory (os error 2)
"""
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched


def test_config_file_with_empty_tiers(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    tiers:
"""
    )
    i1 = cluster.add_instance(wait_online=False)
    err = """\
invalid configuration: empty `cluster.tiers` section which is required to define the initial tiers\
"""  # noqa: E501
    crawler = log_crawler(i1, err.strip())

    i1.fail_to_start()

    assert crawler.matched


def test_run_with_tier_which_is_not_in_tier_list(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.tier = "unexistent_tier"
    err = """\
current instance is assigned tier 'unexistent_tier' which is not defined in the configuration file\
"""
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched


def test_config_file_with_garbage(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    cluster_id: test
    tiers:
        default:
    replication_topology: mobius

instance:
    instance-id: i1

super-cluster:
    - foo
    - bar
"""
    )
    i1 = cluster.add_instance(wait_online=False)
    err = """\
invalid configuration: unknown parameters: `super-cluster` (did you mean `cluster`?), `cluster.replication_topology`, `instance.instance-id` (did you mean `instance_id`?)\
"""  # noqa: E501
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched


def test_config_file_with_init_replication_factor(cluster: Cluster):
    cfg = {"no matter": "init-cfg doesn't allow to use with `init-replication-factor"}
    cluster.set_config_file(cfg)
    i1 = cluster.add_instance(wait_online=False)
    i1.init_replication_factor = 1
    err = """\
error: option `--init-replication-factor` cannot be used with `--config` simultaneously\
"""
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched

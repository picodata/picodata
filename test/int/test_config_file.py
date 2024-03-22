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

    memtx:
        memory: 42069
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
    log:
        level: verbose
"""
    )
    instance = Instance(
        binary_path=cluster.binary_path,
        cwd=cluster.data_dir,
        color=color.cyan,
        config_path=cluster.config_path,
        audit=f"{data_dir}/audit.log",
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
                value=dict(deluxe=dict(can_vote=True)),
                source="config_file",
            ),
            default_replication_factor=dict(value=1, source="default"),
        ),
        instance=dict(
            admin_socket=dict(value=f"{data_dir}/admin.sock", source="default"),
            advertise_address=dict(value=f"{host}:{port}", source="default"),
            failure_domain=dict(value=dict(), source="default"),
            shredding=dict(value=False, source="default"),
            cluster_id=dict(value="my-cluster", source="config_file"),
            instance_id=dict(value="my-instance", source="config_file"),
            replicaset_id=dict(value="my-replicaset", source="config_file"),
            tier=dict(value="deluxe", source="config_file"),
            audit=dict(
                value=f"{data_dir}/audit.log", source="commandline_or_environment"
            ),
            config_file=dict(
                value=instance.config_path, source="commandline_or_environment"
            ),
            data_dir=dict(value=data_dir, source="config_file"),
            listen=dict(value=f"{host}:{port}", source="config_file"),
            log=dict(
                level=dict(value="verbose", source="commandline_or_environment"),
                format=dict(value="plain", source="default"),
            ),
            peers=dict(value=[f"{host}:{port}"], source="config_file"),
            memtx=dict(
                memory=dict(value=64 * 1024 * 1024, source="default"),
                checkpoint_count=dict(value=2, source="default"),
                checkpoint_interval=dict(value=3600, source="default"),
            ),
            vinyl=dict(
                memory=dict(value=128 * 1024 * 1024, source="default"),
                cache=dict(value=128 * 1024 * 1024, source="default"),
            ),
            iproto=dict(
                max_concurrent_messages=dict(value=768, source="default"),
            ),
        ),
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
    memtx:
        memory: 0xdeadbeef
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
    memtx:
        memory: 0xcafebabe
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


def test_config_file_box_cfg_parameters(cluster: Cluster):
    #
    # Check default values
    #
    cluster.set_config_file(
        yaml="""
# just the required part
cluster:
    cluster_id: test
    tiers:
        default:
"""
    )
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    box_cfg = i1.eval("return box.cfg")

    assert box_cfg.get("log") is None  # type: ignore
    assert box_cfg["log_level"] == 6  # means verbose -- set by our testing harness
    assert box_cfg["log_format"] == "plain"

    assert box_cfg["memtx_memory"] == 64 * 1024 * 1024
    assert box_cfg["slab_alloc_factor"] == 1.05
    assert box_cfg["checkpoint_count"] == 2
    assert box_cfg["checkpoint_interval"] == 3600

    assert box_cfg["vinyl_memory"] == 128 * 1024 * 1024
    assert box_cfg["vinyl_cache"] == 128 * 1024 * 1024
    assert box_cfg["vinyl_read_threads"] == 1
    assert box_cfg["vinyl_write_threads"] == 4
    assert box_cfg["vinyl_defer_deletes"] == False  # noqa: E712
    assert box_cfg["vinyl_page_size"] == 8 * 1024
    assert box_cfg["vinyl_run_count_per_level"] == 2
    assert box_cfg["vinyl_run_size_ratio"] == 3.5
    assert box_cfg["vinyl_bloom_fpr"] == 0.05

    assert box_cfg["net_msg_max"] == 0x300

    #
    # Check explicitly set values
    #
    cluster.config_path = None
    cluster.set_config_file(
        yaml="""
cluster:
    cluster_id: test
    tiers:
        default:

instance:
    log:
        destination: file:/proc/self/fd/2  # this is how you say `stderr` explicitly
        level: debug
        format: json

    memtx:
        memory: 0x7777777
        checkpoint_count: 8
        checkpoint_interval: 1800

    vinyl:
        memory: 0x8888888
        cache: 0x4444444

    iproto:
        max_concurrent_messages: 0x600
"""
    )

    # XXX: Just pretend this value comes from the config,
    # even though this will override any value from the config
    i1.env["PICODATA_LOG_LEVEL"] = "debug"

    i1.restart(remove_data=True)
    i1.wait_online()

    box_cfg = i1.eval("return box.cfg")

    assert box_cfg["log"] == "file:/proc/self/fd/2"
    assert box_cfg["log_level"] == 7  # means debug
    assert box_cfg["log_format"] == "json"

    assert box_cfg["memtx_memory"] == 0x777_7777
    assert box_cfg["checkpoint_count"] == 8
    assert box_cfg["checkpoint_interval"] == 1800

    assert box_cfg["vinyl_memory"] == 0x8888888
    assert box_cfg["vinyl_cache"] == 0x4444444

    assert box_cfg["net_msg_max"] == 0x600

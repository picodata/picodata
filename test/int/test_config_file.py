from conftest import Cluster, Instance, PortDistributor, log_crawler, ColorCode
from framework.util.build import Executable
import os
import pathlib
import subprocess
import json
import yaml


def test_config_works(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        default:
instance:
    name: from-config
    replicaset_name: with-love

    memtx:
        memory: 42069
        max_tuple_size: 10485760
"""
    )
    instance = cluster.add_instance(wait_online=False)
    instance.start()
    instance.wait_online()

    info = instance.call(".proc_instance_info")
    assert info["name"] == "from-config"
    assert info["replicaset_name"] == "with-love"

    assert instance.eval("return box.cfg.memtx_memory") == 42069
    assert instance.eval("return box.cfg.memtx_max_tuple_size") == 10485760


def test_wal_dir_differs_from_instance_dir(cluster: Cluster):
    wal_dir = f"{cluster.data_dir}/my-wal"

    cluster.set_config_file(
        yaml=f"""
cluster:
    name: test
    tier:
        default:
instance:
    name: from-config
    wal_dir: {wal_dir}
"""
    )
    [instance] = cluster.deploy(instance_count=1)
    instance_dir = str(instance.instance_dir)

    # By default, `wal_dir` is equal to `instance_dir`, but we've
    # just changed this, so let's be sure that it also changed.
    assert wal_dir != instance_dir

    # `box.cfg` must have the configured `wal_dir`, while engines must still use `instance_dir`.
    assert instance.eval("return box.cfg.wal_dir") == wal_dir
    assert instance.eval("return box.cfg.memtx_dir") == instance_dir
    assert instance.eval("return box.cfg.vinyl_dir") == instance_dir

    # Check that WAL files are actually written to `wal_dir`, not `instance_dir`.
    assert any(f.endswith(".xlog") for f in os.listdir(wal_dir))
    assert not any(f.endswith(".xlog") for f in os.listdir(instance_dir))


def test_memtx_and_vinyl_dir_differ_from_instance_dir(cluster: Cluster):
    memtx_dir = f"{cluster.data_dir}/my-memtx"
    vinyl_dir = f"{cluster.data_dir}/my-vinyl"

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    name: from-config
    memtx:
        dir: {memtx_dir}
    vinyl:
        dir: {vinyl_dir}
"""
    )
    [instance] = cluster.deploy(instance_count=1)
    instance_dir = str(instance.instance_dir)

    # By default, `memtx.dir`/`vinyl.dir` are equal to `instance_dir`, but
    # we've just changed this, so let's be sure that it also changed.
    assert memtx_dir != instance_dir
    assert vinyl_dir != instance_dir

    assert instance.eval("return box.cfg.memtx_dir") == memtx_dir
    assert instance.eval("return box.cfg.vinyl_dir") == vinyl_dir
    assert instance.eval("return box.cfg.wal_dir") == instance_dir


def test_pico_config(cluster: Cluster, port_distributor: PortDistributor):
    host = cluster.base_host
    port = port_distributor.get()
    listen = f"{host}:{port}"
    instance_dir = f"{cluster.data_dir}/my-instance"
    cluster.set_config_file(
        yaml=f"""
cluster:
    name: my-cluster
    tier:
        deluxe:
instance:
    instance_dir: {instance_dir}
    iproto:
        listen: {listen}
    peer:
        - {listen}
    name: my-instance
    replicaset_name: my-replicaset
    tier: deluxe
    log:
        level: verbose
"""
    )
    instance = Instance(
        executable=Executable.current(),
        cwd=cluster.data_dir,
        color_code=ColorCode.Cyan,
        config_path=cluster.config_path,
        audit=f"{instance_dir}/audit.log",
    )
    cluster.instances.append(instance)

    instance.start()

    # These fields should be set after the instance starts, so that the
    # parameters are extracted from the config file, but before we try doing any
    # rpc, because our test harness needs them to be set
    instance.host = host
    instance.port = port

    instance.wait_online()

    config = instance.call(".proc_get_config")
    assert config == dict(
        cluster=dict(
            name=dict(value="my-cluster", source="config_file"),
            tier=dict(
                value=dict(deluxe=dict(can_vote=True, replication_mode="async")),
                source="config_file",
            ),
            default_bucket_count=dict(value=3000, source="default"),
            default_replication_factor=dict(value=1, source="default"),
            shredding=dict(value=False, source="default"),
        ),
        instance=dict(
            admin_socket=dict(value=f"{instance_dir}/admin.sock", source="default"),
            failure_domain=dict(value=dict(), source="default"),
            name=dict(value="my-instance", source="config_file"),
            replicaset_name=dict(value="my-replicaset", source="config_file"),
            tier=dict(value="deluxe", source="config_file"),
            share_dir=dict(value="/usr/share/picodata/", source="default"),
            audit=dict(value=f"{instance_dir}/audit.log", source="commandline_or_environment"),
            config_file=dict(value=f"{instance.config_path}", source="commandline_or_environment"),
            instance_dir=dict(value=instance_dir, source="config_file"),
            backup_dir=dict(value=f"{instance_dir}/backup", source="default"),
            wal_dir=dict(value=instance_dir, source="default"),
            log=dict(
                level=dict(value="verbose", source="commandline_or_environment"),
                format=dict(value="plain", source="default"),
            ),
            peer=dict(value=[f"{host}:{port}"], source="config_file"),
            memtx=dict(
                memory=dict(value="64M", source="default"),
                system_memory=dict(value="256M", source="default"),
                max_tuple_size=dict(value="1M", source="default"),
                dir=dict(value=instance_dir, source="default"),
            ),
            vinyl=dict(
                memory=dict(value="128M", source="default"),
                cache=dict(value="128M", source="default"),
                bloom_fpr=dict(value=0.05000000074505806, source="default"),
                max_tuple_size=dict(value="1M", source="default"),
                page_size=dict(value="8K", source="default"),
                range_size=dict(value="1G", source="default"),
                run_count_per_level=dict(value=2, source="default"),
                run_size_ratio=dict(value=3.5, source="default"),
                read_threads=dict(value=1, source="default"),
                write_threads=dict(value=4, source="default"),
                timeout=dict(value=60.0, source="default"),
                dir=dict(value=instance_dir, source="default"),
            ),
            iproto=dict(
                enabled=dict(source="default", value=True),
                listen=dict(source="config_file", value=f"{host}:{port}"),
                advertise=dict(source="default", value=f"{host}:{port}"),
                tls=dict(enabled=dict(source="default", value=False)),
            ),
            pgproto=dict(
                # pg is enabled by default, so listen should be set
                enabled=dict(source="default", value=True),
                listen=dict(source="default", value="127.0.0.1:4327"),
                advertise=dict(source="default", value="127.0.0.1:4327"),
                tls=dict(enabled=dict(source="default", value=False)),
            ),
            http=dict(
                enabled=dict(source="default", value=True),
                kubernetes_probes=dict(source="default", value=True),
                listen=dict(source="default", value="127.0.0.1:5327"),
                advertise=dict(source="default", value="127.0.0.1:5327"),
                tls=dict(enabled=dict(source="default", value=False)),
            ),
            boot_timeout=dict(value=7200, source="default"),
            ldap=dict(
                enabled=dict(source="default", value=False),
                tls=dict(
                    enabled=dict(source="default", value=False),
                    method=dict(source="default", value="implicit"),
                ),
            ),
        ),
    )


def test_config_storage_conflicts_on_restart(cluster: Cluster):
    # Add `new-tier` so we can change persisted `default` tier.
    # Note: If we set nonexistent tier we will fail while attempting to
    # read `can_vote` parameter for that tier during discovery.
    cluster.set_config_file(
        yaml="""
cluster:
    name: default-cluster-name
    default_replication_factor: 2
    default_bucket_count: 3000
    tier:
        default:
        new-tier:
            can_vote: false
            replication_factor: 1
"""
    )
    instance = cluster.add_instance()

    instance.terminate()

    #
    # Change cluster_name
    #
    was = instance.cluster_name  # type: ignore
    instance.cluster_name = "new-cluster-name"
    assert instance.cluster_name != was
    err = f"""\
invalid configuration: instance restarted with a different `cluster_name`, which is not allowed, was: '{was}' became: 'new-cluster-name'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched
    instance.cluster_name = was

    #
    # Change instance name
    #
    was = instance.name  # type: ignore
    instance.name = "new-instance-name"
    assert instance.name != was
    err = f"""\
invalid configuration: instance restarted with a different `instance_name`, which is not allowed, was: '{was}' became: 'new-instance-name'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched
    instance.name = was

    #
    # Change tier
    #
    was = instance.tier  # type: ignore
    instance.tier = "new-tier"
    assert instance.tier != was
    err = """\
invalid configuration: instance restarted with a different `tier`, which is not allowed, was: 'default' became: 'new-tier'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched
    instance.tier = was

    #
    # Change replicaset_name
    #
    was = instance.replicaset_name  # type: ignore
    instance.replicaset_name = "new-replicaset-name"
    assert instance.replicaset_name != was
    err = f"""\
invalid configuration: instance restarted with a different `replicaset_name`, which is not allowed, was: '{was}' became: 'new-replicaset-name'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched
    instance.replicaset_name = was

    #
    # Change advertise address
    #
    was = instance.iproto_listen  # type: ignore
    instance.env["PICODATA_IPROTO_ADVERTISE"] = "example.com:1234"
    assert instance.env["PICODATA_IPROTO_ADVERTISE"] != was
    err = f"""\
invalid configuration: instance restarted with a different iproto advertise address, which is not allowed, was: '{was}' became: 'example.com:1234'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched
    del instance.env["PICODATA_IPROTO_ADVERTISE"]

    #
    # Change tier configuration: add new tier
    #
    cluster.config_path = None
    cluster.set_config_file(
        yaml="""
cluster:
    name: default-cluster-name
    default_replication_factor: 2
    default_bucket_count: 3000
    tier:
        default:
        new-tier:
            can_vote: false
            replication_factor: 1
        new-new-tier:
"""
    )
    err = """\
invalid configuration: instance restarted with a different set of tiers in configuration file, which is not allowed: tiers previously not seen: 'new-new-tier'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched

    #
    # Change tier configuration: remove old tier
    #
    cluster.config_path = None
    cluster.set_config_file(
        yaml="""
cluster:
    name: default-cluster-name
    default_replication_factor: 2
    default_bucket_count: 3000
    tier:
        default:
"""
    )
    err = """\
invalid configuration: instance restarted with a different set of tiers in configuration file, which is not allowed: tiers not found in configuration: 'new-tier'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched

    #
    # Change tier configuration: default_replication_factor
    #
    cluster.config_path = None
    cluster.set_config_file(
        yaml="""
cluster:
    name: default-cluster-name
    default_replication_factor: 69
    default_bucket_count: 3000
    tier:
        default:
        new-tier:
            can_vote: false
            replication_factor: 1
"""
    )
    err = """\
invalid configuration: instance restarted with a different tier configuration, which is not allowed: tier 'default' `replication_factor` was persisted as `2`, but became `69`
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched

    #
    # Change tier configuration: default_bucket_count
    #
    cluster.config_path = None
    cluster.set_config_file(
        yaml="""
cluster:
    name: default-cluster-name
    default_replication_factor: 2
    default_bucket_count: 6000
    tier:
        default:
        new-tier:
            can_vote: false
            replication_factor: 1
"""
    )
    err = """\
invalid configuration: instance restarted with a different tier configuration, which is not allowed: tier 'default' `bucket_count` was persisted as `3000`, but became `6000`
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched

    #
    # Change tier configuration: can_vote
    #
    cluster.config_path = None
    cluster.set_config_file(
        yaml="""
cluster:
    name: default-cluster-name
    default_replication_factor: 2
    default_bucket_count: 3000
    tier:
        default:
        new-tier:
            can_vote: true
            replication_factor: 1
"""
    )
    err = """\
invalid configuration: instance restarted with a different tier configuration, which is not allowed: tier 'new-tier' `can_vote` was persisted as `false`, but became `true`
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched

    #
    # Change tier configuration: replication_factor
    #
    cluster.config_path = None
    cluster.set_config_file(
        yaml="""
cluster:
    name: default-cluster-name
    default_replication_factor: 2
    default_bucket_count: 3000
    tier:
        default:
        new-tier:
            can_vote: true
"""
    )
    err = """\
invalid configuration: instance restarted with a different tier configuration, which is not allowed: tier 'new-tier' `replication_factor` was persisted as `1`, but became `2`
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched

    #
    # Change tier configuration: bucket_count
    #
    cluster.config_path = None
    cluster.set_config_file(
        yaml="""
cluster:
    name: default-cluster-name
    default_replication_factor: 2
    default_bucket_count: 3000
    tier:
        default:
            bucket_count: 0
        new-tier:
            can_vote: true
            replication_factor: 1
"""
    )
    err = """\
invalid configuration: instance restarted with a different tier configuration, which is not allowed: tier 'default' `bucket_count` was persisted as `3000`, but became `0`
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched

    #
    # Change tier configuration: experimental_sharding_implementation
    #
    cluster.config_path = None
    cluster.set_config_file(
        yaml="""
cluster:
    name: default-cluster-name
    default_replication_factor: 2
    default_bucket_count: 3000
    tier:
        default:
            bucket_count: 3000
            experimental_sharding_implementation: false
        new-tier:
            can_vote: false
            replication_factor: 1
            experimental_sharding_implementation: true
"""
    )
    err = """\
invalid configuration: instance restarted with a different tier configuration, which is not allowed: tier 'new-tier' `experimental_sharding_implementation` was persisted as `false`, but became `true`
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched

    #
    # Change tier configuration: wal_mode
    #
    cluster.config_path = None
    cluster.set_config_file(
        yaml="""
cluster:
    name: default-cluster-name
    default_replication_factor: 2
    default_bucket_count: 3000
    tier:
        default:
            bucket_count: 3000
            wal_mode: write
        new-tier:
            can_vote: false
            replication_factor: 1
            wal_mode: fsync
"""
    )
    err = """\
invalid configuration: instance restarted with a different tier configuration, which is not allowed: tier 'new-tier' `wal_mode` was persisted as `fsync`, but became `write`
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched


def test_default_path_to_config_file(cluster: Cluster):
    instance = cluster.add_instance(wait_online=False)

    # By default ./picodata.yaml will be used in the instance's current working directory
    work_dir = cluster.data_dir + "/work-dir"
    os.mkdir(work_dir)
    with open(work_dir + "/picodata.yaml", "w") as f:
        f.write(
            """
cluster:
    name: test
    tier:
        default:
instance:
    memtx:
        memory: 256K
            """
        )
    instance.start(cwd=work_dir)
    instance.wait_online()

    assert instance.eval("return box.cfg.memtx_memory") == 262144
    instance.terminate()

    # But if a config is specified explicitly, it will be used instead
    config_path = cluster.data_dir + "/explicit-picodata.yaml"
    with open(config_path, "w") as f:
        f.write(
            """
cluster:
    name: test
    tier:
        default:
instance:
    memtx:
        memory: 512M
            """
        )
    instance.env["PICODATA_CONFIG_FILE"] = config_path

    msg = f"""\
A path to configuration file '{config_path}' was provided explicitly,
but a 'picodata.yaml' file in the current working directory '{work_dir}' also exists.
Using configuration file '{config_path}'.
"""  # noqa E501
    crawler = log_crawler(instance, msg)
    instance.start(cwd=work_dir)
    instance.wait_online()
    assert crawler.matched

    assert instance.eval("return box.cfg.memtx_memory") == 536870912
    instance.terminate()


def test_config_file_enoent(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.env.update({"PICODATA_CONFIG_FILE": "./unexisting_dir/trash.yaml"})
    err = f"""\
can't read from '{cluster.data_dir}/./unexisting_dir/trash.yaml': No such file or directory (os error 2)
"""  # noqa: E501
    crawler = log_crawler(i1, err)

    i1.fail_to_start()

    assert crawler.matched


def test_config_file_with_empty_tiers(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    tier:
"""
    )
    i1 = cluster.add_instance(wait_online=False)
    err = """\
invalid configuration: empty `cluster.tier` section which is required to define the initial tiers\
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
    name: test
    tier:
        default:
    replication_topology: mobius

instance:
    instance_id: i1

super-cluster:
    - foo
    - bar
"""
    )
    i1 = cluster.add_instance(wait_online=False)
    err = """\
invalid configuration: unknown parameters: `super-cluster` (did you mean `cluster`?), `cluster.replication_topology`, `instance.instance_id` (did you mean `name`?)\
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
    name: test
    tier:
        default:
"""
    )
    i1 = cluster.add_instance(wait_online=False, log_to_file=False, log_to_console=True)
    i1.start()
    i1.wait_online()

    box_cfg = i1.eval("return box.cfg")

    assert box_cfg.get("log") is None  # type: ignore
    assert box_cfg["log_level"] == 6  # means verbose -- set by our testing harness
    assert box_cfg["log_format"] == "plain"
    assert box_cfg["memtx_memory"] == 67108864
    assert box_cfg["slab_alloc_factor"] == 1.05
    assert box_cfg["checkpoint_count"] == 1
    assert box_cfg["checkpoint_interval"] == 3600

    assert box_cfg["vinyl_memory"] == 134217728
    assert box_cfg["vinyl_cache"] == 134217728
    assert box_cfg["vinyl_bloom_fpr"] == 0.05000000074505806
    assert box_cfg["vinyl_max_tuple_size"] == 1024 * 1024
    assert box_cfg["vinyl_page_size"] == 8 * 1024
    assert box_cfg["vinyl_range_size"] == 1024 * 1024 * 1024
    assert box_cfg["vinyl_run_count_per_level"] == 2
    assert box_cfg["vinyl_run_size_ratio"] == 3.5
    assert box_cfg["vinyl_read_threads"] == 1
    assert box_cfg["vinyl_write_threads"] == 4
    assert box_cfg["vinyl_timeout"] == 60.0
    assert box_cfg["vinyl_defer_deletes"] == False  # noqa: E712

    assert box_cfg["net_msg_max"] == 0x300

    i1.terminate()
    i1.remove_data()

    #
    # Check explicitly set values
    #
    cluster.config_path = None
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        default:

instance:
    log:
        destination: file:/proc/self/fd/2  # this is how you say `stderr` explicitly
        level: debug
        format: json

    memtx:
        memory: 2G

    vinyl:
        memory: 600M
        cache: 300M
"""
    )

    # XXX: Just pretend this value comes from the config,
    # even though this will override any value from the config
    i1.env["PICODATA_LOG_LEVEL"] = "debug"

    # Check that json output format works
    json_line_count = 0
    non_json_lines = []

    def check_parses_as_json(line: bytes):
        nonlocal json_line_count
        nonlocal non_json_lines
        try:
            json.loads(line)
            json_line_count += 1
        except json.JSONDecodeError:
            non_json_lines.append(line)

    i1.on_output_line(check_parses_as_json)
    i1.start()
    i1.wait_online()

    assert json_line_count != 0
    for line in non_json_lines:
        assert line.startswith(b"[supervisor")

    box_cfg = i1.eval("return box.cfg")

    assert box_cfg["log"] == "file:/proc/self/fd/2"
    assert box_cfg["log_level"] == 7  # means debug
    assert box_cfg["log_format"] == "json"

    assert box_cfg["memtx_memory"] == 2147483648

    assert box_cfg["vinyl_memory"] == 629145600
    assert box_cfg["vinyl_cache"] == 314572800


def test_picodata_default_config(cluster: Cluster):
    picodata_executable = Executable.current()

    # Check generating the default config
    data = subprocess.check_output([picodata_executable.command, "config", "default"])
    default_config = data.decode()
    assert len(default_config) != 0

    default_config_dict = yaml.safe_load(default_config)
    assert "listen" not in default_config_dict["instance"]
    assert "advertise" not in default_config_dict["instance"]
    assert "plugin_dir" not in default_config_dict["instance"]

    # Explicit filename
    subprocess.call(
        [picodata_executable.command, "config", "default", "-o", "filename.yaml"],
        cwd=cluster.data_dir,
    )
    with open(f"{cluster.data_dir}/filename.yaml", "r") as f:
        default_config_2 = f.read()
    assert default_config.strip() == default_config_2.strip()

    # Explicit stdout
    data = subprocess.check_output([picodata_executable.command, "config", "default", "-o-"])
    default_config_3 = data.decode()
    assert default_config.strip() == default_config_3.strip()

    #
    # Check that running with the generated config file works
    #
    # Modify the default config to remove the new iproto section and use old settings
    # to avoid conflicts with test harness which uses --iproto-listen flag
    default_config_dict = yaml.safe_load(default_config)
    if "iproto" in default_config_dict.get("instance", {}):
        del default_config_dict["instance"]["iproto"]
    if "http" in default_config_dict.get("instance", {}):
        del default_config_dict["instance"]["http"]
    if "pgproto" in default_config_dict.get("instance", {}):
        del default_config_dict["instance"]["pgproto"]
    modified_config = yaml.dump(default_config_dict, default_flow_style=False)

    cluster.set_config_file(yaml=modified_config)
    i = cluster.add_instance(wait_online=False)

    # Old-style settings will be provided via command line arguments by test harness
    i.env["PICODATA_IPROTO_ADVERTISE"] = i.iproto_listen  # type: ignore

    i.start()
    i.wait_online()


def test_picodata_default_config_has_unified_socket_sections(cluster: Cluster):
    """Test that picodata config default generates unified socket configuration sections
    and does not contain deprecated socket options."""
    picodata_executable = Executable.current()

    data = subprocess.check_output([picodata_executable.command, "config", "default"])
    default_config = data.decode()
    config = yaml.safe_load(default_config)

    instance = config["instance"]

    # Check that new unified sections have expected values
    assert instance["iproto"] == {
        "advertise": "127.0.0.1:3301",
        "enabled": True,
        "listen": "127.0.0.1:3301",
        "tls": {"enabled": False},
    }, f"Unexpected iproto section: {instance.get('iproto')}"
    assert instance["pgproto"] == {
        "advertise": "127.0.0.1:4327",
        "enabled": True,
        "listen": "127.0.0.1:4327",
        "tls": {"enabled": False},
    }, f"Unexpected pgproto section: {instance.get('pgproto')}"
    assert instance["http"] == {
        "advertise": "127.0.0.1:5327",
        "enabled": True,
        "kubernetes_probes": True,
        "listen": "127.0.0.1:5327",
        "tls": {"enabled": False},
    }, f"Unexpected http section: {instance.get('http')}"

    # Check that deprecated socket options are not present
    deprecated_options = [
        "iproto_listen",
        "iproto_advertise",
        "iproto_tls",
        "http_listen",
        "https",
        "pg",
        "listen",
        "advertise_address",
    ]
    for option in deprecated_options:
        assert option not in instance, f"Deprecated option '{option}' should not be present in default config"


def test_default_tier_is_not_created_with_configuration_file(cluster: Cluster):
    # default tier wasn't created only in case of explicit configuration file
    cluster.set_config_file(
        yaml="""
cluster:
    default_replication_factor: 3
    name: test
    tier:
        not_default:
"""
    )

    instance = cluster.add_instance(tier="not_default")

    dql = instance.sql(
        'select "replication_factor" from "_pico_tier" where "name" = \'default\'',
        sudo=True,
    )
    assert dql == []


def test_output_config_parameters(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
    cluster:
        name: test
        tier:
            default:
    instance:
        name: from-config
        replicaset_name: with-love
        memtx:
            memory: 42069B
        boot_timeout: 3600
    """
    )

    output_params = """'cluster.name':
        'cluster.tier':
        'cluster.default_replication_factor':
        'cluster.shredding':
        'instance.instance_dir':
        'instance.config_file':
        'instance.name': "from-config"
        'instance.replicaset_name': "with-love"
        'instance.failure_domain': {}
        'instance.peer':
        'instance.iproto.listen':
        'instance.iproto.advertise':
        'instance.admin_socket':
        'instance.share_dir':
        'instance.audit':
        'instance.log.level': "verbose"
        'instance.log.format': "plain"
        'instance.memtx.memory': \"42069B\"
        'instance.boot_timeout': 3600"""

    params_list = [line.strip().encode("ASCII") for line in output_params.splitlines()]
    found_params = set()

    def check_output(line: bytes):
        nonlocal params_list
        nonlocal found_params

        for param in params_list:
            if line.find(param) != -1:
                found_params.add(param)

    i1 = cluster.add_instance(wait_online=False)
    i1.on_output_line(check_output)
    i1.start()
    i1.wait_online()
    assert len(found_params) == len(params_list)


def test_logger_configuration(cluster: Cluster):
    log_file = f"{cluster.data_dir}/i1.log"
    cluster.set_config_file(
        yaml=f"""
cluster:
    name: test
    tier:
        default:
instance:
    log:
        destination: {log_file}
"""
    )

    assert not os.path.exists(log_file)

    i1 = cluster.add_instance(
        wait_online=True,
        # Don't request logging to file via ENV, instead the setting from the config file will be used
        log_to_file=False,
        log_to_console=True,
    )
    assert os.path.exists(log_file)

    i1.terminate()
    os.remove(log_file)

    other_log_file = f"{cluster.data_dir}/other-i1.log"
    assert not os.path.exists(other_log_file)

    i1.env["PICODATA_LOG"] = other_log_file
    i1.start()
    i1.wait_online()

    assert os.path.exists(other_log_file)
    assert not os.path.exists(log_file)


def test_iproto_tls_parameters(cluster: Cluster):
    ssl_dir = pathlib.Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"
    cert_file = ssl_dir / "server-with-ext.crt"
    key_file = ssl_dir / "server.key"
    ca_file = ssl_dir / "combined-ca.crt"

    cluster.set_config_file(
        yaml=f"""
cluster:
    name: test
    tier:
        default:
instance:
    iproto:
        tls:
            enabled: true
            cert_file: {cert_file}
            key_file: {key_file}
            ca_file: {ca_file}
    """
    )

    instance = cluster.add_instance(wait_online=False)
    instance.iproto_tls = (cert_file, key_file, ca_file)
    instance.start_and_wait()

    source = "config_file"
    configuration = instance.call(".proc_get_config")
    assert configuration["instance"]["iproto"]["tls"] == dict(
        enabled=dict(value=True, source=source),
        cert_file=dict(value=str(cert_file), source=source),
        key_file=dict(value=str(key_file), source=source),
        ca_file=dict(value=str(ca_file), source=source),
    )

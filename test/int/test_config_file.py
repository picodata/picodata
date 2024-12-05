from conftest import Cluster, Instance, PortDistributor, log_crawler, color
import os
import subprocess
import json


def test_config_works(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    name: from-config
    replicaset_name: with-love

    memtx:
        memory: 42069
"""
    )
    instance = cluster.add_instance(name=False, wait_online=False)
    instance.start()
    instance.wait_online()

    info = instance.call(".proc_instance_info")
    assert info["name"] == "from-config"
    assert info["replicaset_name"] == "with-love"

    assert instance.eval("return box.cfg.memtx_memory") == 42069


def test_pico_config(cluster: Cluster, port_distributor: PortDistributor):
    host = cluster.base_host
    port = port_distributor.get()
    listen = f"{host}:{port}"
    data_dir = f"{cluster.data_dir}/my-instance"
    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        deluxe:
instance:
    data_dir: {data_dir}
    listen: {listen}
    peer:
        - {listen}
    cluster_name: my-cluster
    name: my-instance
    replicaset_name: my-replicaset
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

    config = instance.call(".proc_get_config")
    assert config == dict(
        cluster=dict(
            tier=dict(
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
            cluster_name=dict(value="my-cluster", source="config_file"),
            name=dict(value="my-instance", source="config_file"),
            replicaset_name=dict(value="my-replicaset", source="config_file"),
            tier=dict(value="deluxe", source="config_file"),
            audit=dict(
                value=f"{data_dir}/audit.log", source="commandline_or_environment"
            ),
            config_file=dict(
                value=f"{instance.config_path}", source="commandline_or_environment"
            ),
            data_dir=dict(value=data_dir, source="config_file"),
            listen=dict(value=f"{host}:{port}", source="config_file"),
            log=dict(
                level=dict(value="verbose", source="commandline_or_environment"),
                format=dict(value="plain", source="default"),
            ),
            peer=dict(value=[f"{host}:{port}"], source="config_file"),
            memtx=dict(
                memory=dict(value="64M", source="default"),
                checkpoint_count=dict(value=2, source="default"),
                checkpoint_interval=dict(value=3600.0, source="default"),
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
            ),
            iproto=dict(
                max_concurrent_messages=dict(value=768, source="default"),
            ),
            pg=dict(
                # by default listen is None, so pg is disabled
                ssl=dict(source="default", value=False),
            ),
        ),
    )


def test_default_path_to_config_file(cluster: Cluster):
    instance = cluster.add_instance(name=False, wait_online=False)

    # By default ./config.yaml will be used in the instance's current working directory
    work_dir = cluster.data_dir + "/work-dir"
    os.mkdir(work_dir)
    with open(work_dir + "/config.yaml", "w") as f:
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
    config_path = cluster.data_dir + "/explicit-config.yaml"
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
but a 'config.yaml' file in the current working directory '{work_dir}' also exists.
Using configuration file '{config_path}'.
"""  # noqa E501
    crawler = log_crawler(instance, msg)
    instance.start(cwd=work_dir)
    instance.wait_online()
    assert crawler.matched

    assert instance.eval("return box.cfg.memtx_memory") == 536870912
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
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    box_cfg = i1.eval("return box.cfg")

    assert box_cfg.get("log") is None  # type: ignore
    assert box_cfg["log_level"] == 6  # means verbose -- set by our testing harness
    assert box_cfg["log_format"] == "plain"
    assert box_cfg["memtx_memory"] == 67108864
    assert box_cfg["slab_alloc_factor"] == 1.05
    assert box_cfg["checkpoint_count"] == 2
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
        checkpoint_count: 8
        checkpoint_interval: 1800

    vinyl:
        memory: 600M
        cache: 300M

    iproto:
        max_concurrent_messages: 0x600
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
    assert box_cfg["checkpoint_count"] == 8
    assert box_cfg["checkpoint_interval"] == 1800

    assert box_cfg["vinyl_memory"] == 629145600
    assert box_cfg["vinyl_cache"] == 314572800

    assert box_cfg["net_msg_max"] == 0x600


def test_picodata_default_config(cluster: Cluster):
    # Check generating the default config
    data = subprocess.check_output([cluster.binary_path, "config", "default"])
    default_config = data.decode()
    assert len(default_config) != 0

    # Explicit filename
    subprocess.call(
        [cluster.binary_path, "config", "default", "-o", "filename.yaml"],
        cwd=cluster.data_dir,
    )
    with open(f"{cluster.data_dir}/filename.yaml", "r") as f:
        default_config_2 = f.read()
    assert default_config.strip() == default_config_2.strip()

    # Explicit stdout
    data = subprocess.check_output([cluster.binary_path, "config", "default", "-o-"])
    default_config_3 = data.decode()
    assert default_config.strip() == default_config_3.strip()

    # Check that running with the generated config file works
    cluster.set_config_file(yaml=default_config)
    i = cluster.add_instance(wait_online=False)

    # Default config contains default values for `listen`, `advertise` & `peers`,
    # but our testing harness overrides the `listen` & `peers` values so that
    # running tests in parallel doesn't result in binding to conflicting ports.
    # For this reason we must also specify `advertise` explictily.
    i.env["PICODATA_ADVERTISE"] = i.listen  # type: ignore

    i.start()
    i.wait_online()


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
        tier:
            default:
    instance:
        cluster_name: test
        name: from-config
        replicaset_name: with-love
        memtx:
            memory: 42069B
    """
    )

    output_params = """'cluster.name':
        'cluster.tier':
        'cluster.default_replication_factor':
        'instance.data_dir':
        'instance.config_file':
        'instance.cluster_name':
        'instance.name': "i1"
        'instance.replicaset_name': "with-love"
        'instance.tier': "default"
        'instance.failure_domain': {}
        'instance.peer':
        'instance.listen':
        'instance.advertise_address':
        'instance.admin_socket':
        'instance.plugin_dir':
        'instance.audit':
        'instance.shredding': false
        'instance.log.level': "verbose"
        'instance.log.format': "plain"
        'instance.memtx.memory': \"42069B\""""

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
    tier:
        default:
instance:
    cluster_name: test
    log:
        destination: {log_file}
"""
    )

    assert not os.path.exists(log_file)

    i1 = cluster.add_instance(wait_online=True)
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

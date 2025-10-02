import os
from pathlib import Path
import pytest
from conftest import (
    Cluster,
    CommandFailed,
    Retriable,
    Instance,
    TarantoolError,
    log_crawler,
)
from datetime import datetime, timezone


def backup_folder_name_from_timestamp(ts: int):
    # Timestamp is always saved in UTC.
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y%m%dT%H%M%S")


# Use in case `backup_folder_name_from_timestamp` doesn't work.
def backup_folder_name_from_dir(instance_backup_dir: str):
    for _, dirs, _ in os.walk(instance_backup_dir):
        for d in dirs:
            return d


def check_no_pending_schema_change(i: Instance):
    rows = i.sql("select count(*) from _pico_property where key = 'pending_schema_change'")
    assert rows == [[0]]


def check_last_backup_timestamp(i: Instance):
    rows = i.sql("select count(*) from _pico_property where key = 'last_backup_timestamp'")
    assert rows == [[1]]


def get_backup_timestamp_unfinished(i: Instance):
    dql = i.sql("select value from _pico_property where key = 'pending_schema_change'")
    res = dql[0][0][1]
    return res


def get_backup_timestamp_finished(i: Instance):
    ddl = i.sql("select value from _pico_property where key = 'last_backup_timestamp'")
    return ddl[0][0]


################### BACKUP TESTS ###################


def test_backup_basic_restore_sharded_table(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, *_ = cluster.wait_online()

    sharded_table_name = "t"
    ddl = i1.sql(f"create table {sharded_table_name}(a int primary key)")
    assert ddl["row_count"] == 1

    data_to_insert = [1, 2, 3]
    for data in data_to_insert:
        dml = i1.sql(f"insert into {sharded_table_name} values ({data})")
        assert dml["row_count"] == 1

    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    ddl = i1.sql(f"drop table {sharded_table_name}")
    assert ddl["row_count"] == 1

    # Check there is no data left
    with pytest.raises(TarantoolError, match=f'table with name "{sharded_table_name}" not found'):
        i1.sql(f"select * from {sharded_table_name}")

    # Restore from backups
    cluster.restore(new_backup_folder_name, 1)

    # Data is restored when backup is applied
    dql_sharded = i1.sql(f"select * from {sharded_table_name}")
    for data in data_to_insert:
        assert [data] in dql_sharded


def test_backup_basic_restore_global_table(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, *_ = cluster.wait_online()

    global_table_name = "gt"
    ddl = i1.sql(f"create table {global_table_name}(a int primary key) distributed globally")
    assert ddl["row_count"] == 1

    data_to_insert = [1, 2, 3]
    for data in data_to_insert:
        dml = i1.sql(f"insert into {global_table_name} values ({data})")
        assert dml["row_count"] == 1

    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    ddl = i1.sql(f"drop table {global_table_name}")
    assert ddl["row_count"] == 1

    # Check there is no data left
    with pytest.raises(TarantoolError, match=f'table with name "{global_table_name}" not found'):
        i1.sql(f"select * from {global_table_name}")

    # Restore from backups
    cluster.restore(new_backup_folder_name, 1)

    # Data is restored when backup is applied
    dql_sharded = i1.sql(f"select * from {global_table_name}")
    for data in data_to_insert:
        assert [data] in dql_sharded


def test_backup_restore_table_not_dropped(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, *_ = cluster.wait_online()

    table_name = "t"
    ddl = i1.sql(f"create table {table_name}(a int primary key)")
    assert ddl["row_count"] == 1

    data_to_reserve = [1, 2, 3]
    for data in data_to_reserve:
        dml = i1.sql(f"insert into {table_name} values ({data})")
        assert dml["row_count"] == 1

    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    data_to_forget = [4, 5, 6]
    for data in data_to_forget:
        dml = i1.sql(f"insert into {table_name} values ({data})")
        assert dml["row_count"] == 1

    # Restore from backups
    cluster.restore(new_backup_folder_name, 1)

    dql = i1.sql(f"select * from {table_name}")
    # Data before BACKUP is restored when backup is applied
    for data in data_to_reserve:
        assert [data] in dql
    # Data after BACKUP is not restored
    for data in data_to_forget:
        assert [data] not in dql


def _test_backup_restore_big_cluster(cluster: Cluster):
    cluster.deploy(instance_count=6, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=3)

    i1, *_ = cluster.wait_online()

    table_name = "t"
    ddl = i1.sql(f"create table {table_name}(a int primary key)")
    assert ddl["row_count"] == 1

    data_to_reserve = [1, 2, 3]
    for data in data_to_reserve:
        dml = i1.sql(f"insert into {table_name} values ({data})")
        assert dml["row_count"] == 1

    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    data_to_forget = [4, 5, 6]
    for data in data_to_forget:
        dml = i1.sql(f"insert into {table_name} values ({data})")
        assert dml["row_count"] == 1

    # Restore from backups
    cluster.restore(new_backup_folder_name, 2)

    dql = i1.sql(f"select * from {table_name}")
    # Data before BACKUP is restored when backup is applied
    for data in data_to_reserve:
        assert [data] in dql
    # Data after BACKUP is not restored
    for data in data_to_forget:
        assert [data] not in dql


@pytest.mark.xfail(reason="flaky, will be fixed ASAP")
def test_backup_double_round_trip_no_data(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, *_ = cluster.wait_online()

    # Made initial backup.
    ddl = i1.sql("BACKUP", timeout=40)
    backup_folder_name = ddl[0][0]

    # Restore from backup.
    cluster.restore(backup_folder_name, 1)

    # Made backup of restored data.
    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    assert new_backup_folder_name != backup_folder_name
    # Restore from backup of backup.
    cluster.restore(new_backup_folder_name, 1)


def test_backup_double_round_trip(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, *_ = cluster.wait_online()

    table_name = "t"
    ddl = i1.sql(f"create table {table_name}(a int primary key)")
    assert ddl["row_count"] == 1

    data_to_reserve = [1, 2, 3]
    for data in data_to_reserve:
        dml = i1.sql(f"insert into {table_name} values ({data})")
        assert dml["row_count"] == 1

    ddl = i1.sql("BACKUP", timeout=40)
    backup_folder_name = ddl[0][0]

    # Restore from backups
    cluster.restore(backup_folder_name, 1)

    dql = i1.sql(f"select * from {table_name}")
    # Data before BACKUP is restored when backup is applied
    for data in data_to_reserve:
        assert [data] in dql

    # Made backup of restored data.
    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]
    assert new_backup_folder_name != backup_folder_name

    # Restore from backup of backup.
    cluster.restore(new_backup_folder_name, 1)

    # Initial data is restored
    dql = i1.sql(f"select * from {table_name}")
    for data in data_to_reserve:
        assert [data] in dql


def test_backup_abort_is_called(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, *_ = cluster.wait_online()

    table_name = "t"
    ddl = i1.sql(f"create table {table_name}(a int primary key)")
    assert ddl["row_count"] == 1

    # Pause BACKUP application.
    error_injection = "BLOCK_BEFORE_BACKUP_START"
    injection_log = f"ERROR INJECTION '{error_injection}'"
    i1.call("pico._inject_error", error_injection, True)

    lc = log_crawler(i1, injection_log)
    with pytest.raises(TimeoutError):
        # Send BACKUP request.
        i1.sql("BACKUP")
    lc.wait_matched(timeout=30)

    # Manually call box.backup.start() to cause abort of backup.
    i1.call("box.backup.start")

    # Resume BACKUP execution.
    lc = log_crawler(i1, "Backup is already in progress")
    i1.call("pico._inject_error", error_injection, False)
    lc.wait_matched(timeout=30)


@pytest.mark.xfail(reason="flaky, will be fixed ASAP")
def test_backup_enables_read_only_on_prepare_sharded(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, *_ = cluster.wait_online()

    sharded_table_name = "t"
    ddl = i1.sql(f"create table {sharded_table_name}(a int primary key)")
    assert ddl["row_count"] == 1

    # Pause BACKUP application.
    error_injection = "BLOCK_BACKUP_TRANSACTION"
    injection_log = f"ERROR INJECTION '{error_injection}'"
    i1.call("pico._inject_error", error_injection, True)

    lc = log_crawler(i1, injection_log)
    with pytest.raises(TimeoutError):
        # Send BACKUP request.
        i1.sql("BACKUP")
    lc.wait_matched(timeout=30)

    with pytest.raises(TarantoolError, match="cannot be modified now as DDL operation is in progress"):
        i1.sql(f"insert into {sharded_table_name} values (1)")

    # Disable injection that leads to BACKUP completion.
    lc = log_crawler(i1, "finalizing schema change")
    i1.call("pico._inject_error", error_injection, False)
    lc.wait_matched(timeout=25)

    # Check that write read-only mode is disabled
    dml = i1.sql(f"insert into {sharded_table_name} values (1)")
    assert dml["row_count"] == 1


@pytest.mark.xfail(reason="flaky, will be fixed ASAP")
def test_backup_enables_read_only_on_prepare_global(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, *_ = cluster.wait_online()

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 3000)

    global_table_name = "gt"
    ddl = i1.sql(f"create table {global_table_name}(a int primary key) distributed globally")
    assert ddl["row_count"] == 1

    # Pause BACKUP application.
    error_injection = "BLOCK_BACKUP_TRANSACTION"
    injection_log = f"ERROR INJECTION '{error_injection}'"
    i1.call("pico._inject_error", error_injection, True)

    lc = log_crawler(i1, injection_log)
    with pytest.raises(TimeoutError):
        # Send BACKUP request.
        i1.sql("BACKUP")
    lc.wait_matched(timeout=30)

    # Write to global table is disabled because its `operable`
    # flag is set to `false`.
    with pytest.raises(TarantoolError, match="cannot be modified now as DDL operation is in progress"):
        i1.sql(f"insert into {global_table_name} values (1)")

    # Disable injection that leads to BACKUP completion.
    lc = log_crawler(i1, "finalizing schema change")
    i1.call("pico._inject_error", error_injection, False)
    lc.wait_matched(timeout=35)

    # Check that write to global table is enabled.
    dml = i1.sql(f"insert into {global_table_name} values (1)")
    assert dml["row_count"] == 1


@pytest.mark.xfail(reason="flaky, will be fixed ASAP")
def test_backup_disables_read_only_mode_on_abort(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, *_ = cluster.wait_online()

    sharded_table_name = "t"
    ddl = i1.sql(f"create table {sharded_table_name}(a int primary key)")
    assert ddl["row_count"] == 1

    # Pause BACKUP application.
    error_injection = "BLOCK_BEFORE_BACKUP_START"
    injection_log = f"ERROR INJECTION '{error_injection}'"
    i1.call("pico._inject_error", error_injection, True)

    lc = log_crawler(i1, injection_log)
    with pytest.raises(TimeoutError):
        # Send BACKUP request.
        i1.sql("BACKUP")
    lc.wait_matched(timeout=30)

    with pytest.raises(TarantoolError, match="cannot be modified now as DDL operation is in progress"):
        i1.sql(f"insert into {sharded_table_name} values (1)")

    # Manually call box.backup.start() to cause abort of backup.
    # TODO: This RPC may fail with timeout and next apply_backup RPC will succeed.
    i1.call("box.backup.start")

    # Resume BACKUP execution.
    lc = log_crawler(i1, "finalizing schema change")
    i1.call("pico._inject_error", error_injection, False)
    lc.wait_matched(timeout=25)

    # Check that write read-only mode is disabled.
    dml = i1.sql(f"insert into {sharded_table_name} values (1)")
    assert dml["row_count"] == 1


@pytest.mark.xfail(reason="flaky, will be fixed ASAP")
def test_backup_saves_cookie(cluster: Cluster):
    # Deploy cluster with custom password.
    password = "MyPassword"
    cluster.deploy(instance_count=2, wait_online=False, service_password=password)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, *_ = cluster.wait_online()

    # Execute BACKUP.
    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    # Check that cookie file was copied correctly.
    cookie_backup_path = os.path.join(i1.backup_dir, new_backup_folder_name, ".picodata-cookie")
    assert os.path.isfile(cookie_backup_path)
    with open(cookie_backup_path, "r", encoding="utf-8") as f:
        password_actual = f.read()
        assert password_actual == password


def test_backup_saves_share_dir(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=2, wait_online=False)

    share_dir_name = "share_dir"
    share_dir_path = Path(os.path.join(i1.instance_dir, share_dir_name))

    cluster.set_share_dir(str(share_dir_path))

    cluster.set_unique_configs_for_instances(init_replication_factor=2, share_dir_path=share_dir_path)

    i1, *_ = cluster.wait_online()

    # Create share dir.
    os.makedirs(share_dir_path)

    # Save some mock files into the share dir.
    plugin_name = "my_plugin"
    plugin_version = "0.1.0"
    manifest_expected = "manifest inner"
    plugin_dir = os.path.join(share_dir_path, plugin_name, plugin_version)
    manifest_path = os.path.join(plugin_dir, "manifest.yaml")
    os.makedirs(plugin_dir)
    with open(manifest_path, "w", encoding="utf-8") as f:
        f.write(manifest_expected)

    # Execute BACKUP.
    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    # Check that manifest file was copied correctly.
    new_manifest_path = os.path.join(
        i1.backup_dir, new_backup_folder_name, share_dir_name, plugin_name, plugin_version, "manifest.yaml"
    )
    assert os.path.isfile(new_manifest_path)
    with open(new_manifest_path, "r", encoding="utf-8") as f:
        manifest_actual = f.read()
        assert manifest_actual == manifest_expected


# TODO: For some reason _pico_property is not updated with LastBackupTimestamp
#       on i1 when it's online again (flaky?).
def _test_backup_executes_correctly_on_instance_termination(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=1)

    i1, i2 = cluster.wait_online()

    table_name = "t"
    ddl = i1.sql(f"create table {table_name}(a int primary key)")
    assert ddl["row_count"] == 1

    data_to_reserve = [1, 2, 3]
    for data in data_to_reserve:
        dml = i1.sql(f"insert into {table_name} values ({data})")
        assert dml["row_count"] == 1

    # Terminate i1.
    i1.terminate()

    # Backup is not executed because one instance is down.
    with pytest.raises(TimeoutError):
        i2.sql("BACKUP")

    # Restart i1.
    i1.start()
    i1.wait_online()

    # Wait until the schema change is finalized (BACKUP execution is finalized).
    Retriable(timeout=20, rps=2).call(check_last_backup_timestamp, i1)
    new_backup_timestamp = get_backup_timestamp_finished(i1)
    new_backup_folder_name = backup_folder_name_from_timestamp(new_backup_timestamp)

    # Restore from backups
    cluster.restore(new_backup_folder_name, 2)

    dql = i1.sql(f"select * from {table_name}")
    # Data before BACKUP is restored when backup is applied
    for data in data_to_reserve:
        assert [data] in dql


@pytest.mark.xfail(reason="flaky, will be fixed ASAP")
def test_backup_is_failing_with_timeout_when_replica_is_terminated(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_name="r1", wait_online=False)
    i2 = cluster.add_instance(replicaset_name="r2", wait_online=False)
    i3 = cluster.add_instance(replicaset_name="r2", wait_online=False)

    cluster.set_unique_configs_for_instances()

    for i in [i1, i2, i3]:
        i.start()

    for i in [i1, i2, i3]:
        i.wait_online()

    for i in [i1, i2]:
        cluster.wait_until_instance_has_this_many_active_buckets(i, 1500)

    i3.terminate()

    # Backup is not executed because replica is down.
    with pytest.raises(TimeoutError):
        i2.sql("BACKUP")

    # Restart replica i3.
    i3.start()

    i3.wait_online()

    # Wait until the schema change is finalized (BACKUP execution is finalized).
    Retriable(timeout=20, rps=2).call(check_last_backup_timestamp, i1)


# TODO: Doesn't pass.
#       Should we disable failover?
#       Seems like DdlPrepare is compacted for the moment i2 becames master?
def _test_backup_makes_replica_read_only_on_master_down(cluster: Cluster):
    # i1 -- master, i2 -- replica.
    i1, i2 = cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    for i in [i1, i2]:
        i.start()

    for i in [i1, i2]:
        i.wait_online()

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 3000)

    sharded_table_name = "t"
    ddl = i1.sql(f"create table {sharded_table_name}(a int primary key)")
    assert ddl["row_count"] == 1

    # Pause BACKUP application.
    error_injection = "BLOCK_BACKUP_TRANSACTION"
    injection_log = f"ERROR INJECTION '{error_injection}'"
    i1.call("pico._inject_error", error_injection, True)

    lc = log_crawler(i1, injection_log)
    with pytest.raises(TimeoutError):
        i1.sql("BACKUP")
    lc.wait_matched(timeout=30)

    # i1 is in read-only mode.
    with pytest.raises(TarantoolError, match="cannot be modified now as DDL operation is in progress"):
        i1.sql(f"insert into {sharded_table_name} values (1)")

    # Terminate i1 so that i2 becomes a master.
    i1.terminate()

    # Wait until DdlPrepare is handled on i2.
    def check_read_only(i: Instance):
        assert i.eval("return box.cfg.read_only")

    Retriable(timeout=20, rps=2).call(check_read_only, i2)

    # i2 is in read-only mode too.
    with pytest.raises(TarantoolError, match="a read-only instance"):
        i2.sql(f"insert into {sharded_table_name} values (1)")


@pytest.mark.xfail(reason="flaky, will be fixed ASAP")
def test_backup_is_using_hardlinks(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)
    i1, *_ = cluster.wait_online()

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 3000)

    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    # Retrieve .snap file from backup.
    backup_dir = os.path.join(i1.backup_dir, new_backup_folder_name)
    snap_path_backup = next(Path(backup_dir).glob("*.snap"), None)
    assert snap_path_backup is not None, "Backup .snap file not found"

    # Retrieve .snap file from instance_dir
    snap_name = snap_path_backup.name
    snap_path_initial = Path(i1.instance_dir) / snap_name
    assert snap_path_initial.exists(), "Initial .snap file not found"

    # Assert that hardlink was used for file in backup.
    stat1 = os.stat(snap_path_backup)
    stat2 = os.stat(snap_path_initial)
    assert (stat1.st_ino == stat2.st_ino) and (stat1.st_dev == stat2.st_dev)


def test_backup_is_failing_when_config_file_is_not_specified(cluster: Cluster):
    # Cluster is deployed without config file.
    cluster.deploy(instance_count=1, wait_online=False)
    i1, *_ = cluster.wait_online()

    # Backup is failing.
    with pytest.raises(TarantoolError, match="No config found for local backup"):
        i1.sql("BACKUP")


def test_backup_is_failing_when_config_file_is_missing(cluster: Cluster):
    cluster.deploy(instance_count=1, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=1)

    i1, *_ = cluster.wait_online()

    # Remove config file from instance dir.
    config_path = Path(i1.instance_dir) / "picodata.yaml"
    assert config_path.exists()
    config_path.unlink()
    assert not config_path.exists()

    # Backup is failing.
    with pytest.raises(TarantoolError, match="Config file which instance is referencing, does not exist"):
        i1.sql("BACKUP")


def test_backup_removes_partially_created_dir_on_abort(cluster: Cluster):
    # Deploy cluster.
    # i1 -- master, i2 -- replica.
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, i2 = cluster.wait_online()

    # Pause BACKUP application on replica.
    error_injection = "BLOCK_BEFORE_BACKUP_START"
    injection_log = f"ERROR INJECTION '{error_injection}'"
    i2.call("pico._inject_error", error_injection, True)

    lc = log_crawler(i2, injection_log)
    with pytest.raises(TimeoutError):
        # Send BACKUP request.
        # Backup directory would be created on i1, but not on i2.
        i1.sql("BACKUP")
    lc.wait_matched(timeout=30)

    new_backup_timestamp = get_backup_timestamp_unfinished(i1)
    new_backup_folder_name = backup_folder_name_from_timestamp(new_backup_timestamp)

    # Make sure backup directory was created on i1.
    backup_dir = os.path.join(i1.backup_dir, new_backup_folder_name)
    assert os.path.isdir(backup_dir), "Backup directory should be created on i1"
    # Make sure backup directory was not created on i2.
    backup_dir_i2 = os.path.join(i2.backup_dir, new_backup_folder_name)
    assert not os.path.isdir(backup_dir_i2), "Backup directory should not be created on i2"

    # Remove i2 config file to cause abort of backup.
    config_path = Path(os.path.join(i2.instance_dir, "picodata.yaml"))
    assert config_path.exists()
    config_path.unlink()
    assert not config_path.exists()

    # Resume BACKUP execution.
    lc = log_crawler(i2, "Config file which instance is referencing, does not exist")
    i2.call("pico._inject_error", error_injection, False)
    lc.wait_matched(timeout=30)

    # Wait until backup execution is finished.
    Retriable(timeout=20, rps=2).call(check_no_pending_schema_change, i1)

    # Check that backup dir was removed on i1.
    assert not os.path.isdir(backup_dir), "Backup directory should be removed on i1"


@pytest.mark.xfail(reason="flaky, will be fixed ASAP")
def test_backup_does_not_fail_when_backup_retries(cluster: Cluster):
    # Deploy cluster.
    # i1 -- master, i2 -- replica.
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, i2 = cluster.wait_online()

    # Pause BACKUP application on replica.
    # It's stopped right after data files (like .snap files are moved).
    error_injection = "BLOCK_AFTER_DATA_IS_BACKUPED"
    injection_log = f"ERROR INJECTION '{error_injection}'"
    i1.call("pico._inject_error", error_injection, True)

    lc = log_crawler(i1, injection_log)
    with pytest.raises(TimeoutError):
        # Send BACKUP request.
        # Backup directory would be created on i1, but not on i2.
        i1.sql("BACKUP")
    lc.wait_matched(timeout=30)

    new_backup_timestamp = get_backup_timestamp_unfinished(i1)
    new_backup_folder_name = backup_folder_name_from_timestamp(new_backup_timestamp)

    # Make sure backup directory was created on i1.
    backup_dir_i1 = os.path.join(i1.backup_dir, new_backup_folder_name)
    assert os.path.isdir(backup_dir_i1), "Backup directory should be created on i1"

    # Make sure backup directory doesn't contain config file (because of error_injection).
    config_path_i1 = os.path.join(backup_dir_i1, "picodata.yaml")
    assert not os.path.isfile(config_path_i1)

    # Make sure backup directory was not created on i2.
    backup_dir_i2 = os.path.join(i2.backup_dir, new_backup_folder_name)
    assert not os.path.isdir(backup_dir_i2), "Backup directory should not be created on i2"

    # Resume BACKUP execution.
    i1.call("pico._inject_error", error_injection, False)

    # Wait until backup execution is finished.
    Retriable(timeout=30, rps=2).call(check_no_pending_schema_change, i2)

    # Check that backup dir was created on i2.
    assert os.path.isdir(backup_dir_i2), "Backup directory should be created on i2"

    # Check that config file was created on i1.
    assert os.path.isfile(config_path_i1)


@pytest.mark.xfail(reason="flaky, will be fixed ASAP")
def test_backup_works_with_vinyl(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)
    i1, i2 = cluster.wait_online()

    # Create vinyl table.
    table_name = "t"
    ddl = i1.sql(f"create table {table_name}(a int primary key) using vinyl")
    assert ddl["row_count"] == 1

    # Fill table with data so that folder for .run and .index
    # files is created
    data_to_insert = [1, 2, 3]
    for data in data_to_insert:
        dml = i1.sql(f"insert into {table_name} values ({data})")
        assert dml["row_count"] == 1

    ddl = i1.sql("BACKUP", timeout=35)
    new_backup_folder_name = ddl[0][0]

    # Restore from backup.
    cluster.restore(new_backup_folder_name, 1)

    # Data is restored when backup is applied
    dql = i1.sql(f"select * from {table_name}")
    for data in data_to_insert:
        assert [data] in dql


def test_backup_raises_schema_version(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)
    i1, i2 = cluster.wait_online()

    # At cluster boot schema version is 3.
    assert i1.call("box.space._pico_property:get", "global_schema_version")[1] == 3
    assert i2.call("box.space._pico_property:get", "global_schema_version")[1] == 3
    assert i1.call("box.space._schema:get", "local_schema_version")[1] == 3
    assert i2.call("box.space._schema:get", "local_schema_version")[1] == 3

    # And next schema version will be 4.
    assert i1.next_schema_version() == 4
    assert i2.next_schema_version() == 4

    ddl = i1.sql("BACKUP", timeout=40)
    assert ddl[0][0] is not None

    # After BACKUP execution both of them are updated.
    assert i1.call("box.space._pico_property:get", "global_schema_version")[1] == 4
    assert i2.call("box.space._pico_property:get", "global_schema_version")[1] == 4
    assert i1.call("box.space._schema:get", "local_schema_version")[1] == 4
    assert i2.call("box.space._schema:get", "local_schema_version")[1] == 4
    assert i1.next_schema_version() == 5
    assert i2.next_schema_version() == 5


@pytest.mark.xfail(reason="flaky, will be fixed ASAP")
def test_backup_consecutive_ddls_are_not_broken_after_restore(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, *_ = cluster.wait_online()

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 3000)

    # Execute backup.
    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    # Restore from backup.
    cluster.restore(new_backup_folder_name, 1)

    # Execute DDL operation after restore.
    ddl = i1.sql("create table t2(a int primary key)")
    assert ddl["row_count"] == 1

    # Check DDL operation work after new replicaset is added.
    ddl = i1.sql("create table t3(a int primary key)")
    assert ddl["row_count"] == 1


@pytest.mark.xfail(reason="flaky, will be fixed ASAP")
def test_backup_does_not_break_new_replicaset_ddl_catching_up_with_restore(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, *_ = cluster.wait_online()

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 3000)

    # Tinker raft log options so that DDL operations are not compacted.
    i1.sql("ALTER SYSTEM SET raft_wal_size_max  = 200000000")
    i1.sql("ALTER SYSTEM SET raft_wal_count_max = 10000000")

    # Execute backup.
    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    # Execute DDL operation before restore
    # (table wouldn't exist after restore as table was created after BACKUP).
    ddl = i1.sql("create table t1(a int primary key)")
    assert ddl["row_count"] == 1

    # Restore from backup.
    cluster.restore(new_backup_folder_name, 1)

    # Execute DDL operation after restore.
    ddl = i1.sql("create table t2(a int primary key)")
    assert ddl["row_count"] == 1

    # Fill table with data
    data_to_insert = [1, 2, 3]
    for data in data_to_insert:
        dml = i1.sql(f"insert into t2 values ({data})")
        assert dml["row_count"] == 1

    # Add new replicaset to the cluster.
    i3 = cluster.add_instance(wait_online=False, init_replication_factor=2)
    i4 = cluster.add_instance(wait_online=False, init_replication_factor=2)
    # Wait until new instances are online.
    i3.start()
    i4.start()
    i3.wait_online()
    i4.wait_online()

    # Wait until new instances catch up with DDL operations.
    Retriable(timeout=20, rps=2).call(check_no_pending_schema_change, i3)
    Retriable(timeout=20, rps=2).call(check_no_pending_schema_change, i4)

    for i in [i1, i3]:
        cluster.wait_until_instance_has_this_many_active_buckets(i, 1500)

    # Check select from tables works.
    for data in data_to_insert:
        # TODO: Fails in case we add a WHERE condition.
        # dql = i1.sql(f"select * from t2 where a = {data}")
        dql = i1.sql("select * from t2")
        assert [data] in dql

    # Check DDL operation work after new replicaset is added.
    ddl = i1.sql("create table t3(a int primary key)")
    assert ddl["row_count"] == 1


@pytest.mark.xfail(reason="flaky, will be fixed ASAP")
def test_backup_does_not_break_new_replicaset_ddl_catching_up_no_restore(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    i1, *_ = cluster.wait_online()

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 3000)

    # Tinker raft log options so that DDL operations are not compacted.
    i1.sql("ALTER SYSTEM SET raft_wal_size_max  = 200000000")
    i1.sql("ALTER SYSTEM SET raft_wal_count_max = 10000000")

    # Execute backup.
    ddl = i1.sql("BACKUP", timeout=40)

    # Execute DDL operation.
    ddl = i1.sql("create table t1(a int primary key)")
    assert ddl["row_count"] == 1

    # Fill table with data
    data_to_insert = [1, 2, 3]
    for data in data_to_insert:
        dml = i1.sql(f"insert into t1 values ({data})")
        assert dml["row_count"] == 1

    # Add new replicaset to the cluster.
    i3 = cluster.add_instance(wait_online=False)
    i4 = cluster.add_instance(wait_online=False)
    # Wait until new instances are online.
    i3.start()
    i4.start()
    i3.wait_online()
    i4.wait_online()

    # Wait until new instances catch up with DDL operations.
    Retriable(timeout=30, rps=2).call(check_no_pending_schema_change, i3)
    Retriable(timeout=30, rps=2).call(check_no_pending_schema_change, i4)

    # Check t1 is created on new replicaset instances.
    assert i3.call("box.space._space.index.name:get", "t1") is not None
    assert i4.call("box.space._space.index.name:get", "t1") is not None

    # Wait for rebalance
    cluster.wait_until_instance_has_this_many_active_buckets(i1, 1500)
    cluster.wait_until_instance_has_this_many_active_buckets(i3, 1500)

    # Check select from tables works.
    for data in data_to_insert:
        dql = i4.sql("select * from t1")
        assert [data] in dql

    # Check DDL operation work after new replicaset is added.
    ddl = i1.sql("create table t2(a int primary key)")
    assert ddl["row_count"] == 1


################### RESTORE TESTS ###################


def test_restore_dir_equal_to_instance_dir_cause_error(cluster: Cluster):
    cluster.deploy(instance_count=1, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=1)
    i1, *_ = cluster.wait_online()

    # Check that query fails.
    with pytest.raises(CommandFailed, match="Chosen backup directory is currently used instance dir"):
        cluster.restore(i1.instance_dir, is_absolute=True)


@pytest.mark.xfail(reason="flaky, will be fixed ASAP")
def test_restore_is_working_with_custom_config_name(cluster: Cluster):
    cluster.deploy(instance_count=2, wait_online=False)

    # Generate default configs named "picodata.yaml"
    cluster.set_unique_configs_for_instances(init_replication_factor=2)

    # Rename instances configs
    custom_config_name = "picodata_custom.my_ext"
    for i in cluster.instances:
        config_path = Path(str(i.config_path))
        new_config_path = config_path.parent / custom_config_name
        config_path.rename(new_config_path)
        i.config_path = str(new_config_path)
        print("Custom config path is", new_config_path)

    i1, *_ = cluster.wait_online()

    # Create backup.
    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    # Restore with default config name fails.
    with pytest.raises(CommandFailed, match="No config file to execute restore"):
        cluster.restore(new_backup_folder_name)

    # Restore with custom config name successes.
    cluster.restore(new_backup_folder_name, config_name=custom_config_name)


def test_restore_is_saving_the_single_snap_file(cluster: Cluster):
    cluster.deploy(instance_count=1, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=1)

    i1, *_ = cluster.wait_online()

    # Create backup.
    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    # Check that backup_dir contains only one .snap file.
    backup_dir = os.path.join(i1.backup_dir, new_backup_folder_name)
    snap_files = list(Path(backup_dir).glob("*.snap"))
    assert len(snap_files) == 1, "backup dir should contain only one .snap file"

    # Restore from backup.
    cluster.restore(new_backup_folder_name)

    # Check that instance_dir contains only one .snap file after `restore` is executed.
    snap_files_after_restore = list(Path(i1.instance_dir).glob("*.snap"))
    assert len(snap_files_after_restore) == 1, "instance_dir should contain only one .snap file after restore"


def test_restore_is_failing_when_config_file_is_missing(cluster: Cluster):
    cluster.deploy(instance_count=1, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=1)

    i1, *_ = cluster.wait_online()

    # Create backup.
    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    # Remove config file from backup.
    backup_dir = os.path.join(i1.backup_dir, new_backup_folder_name)
    config_path = Path(backup_dir) / "picodata.yaml"
    assert config_path.exists()
    config_path.unlink()
    assert not config_path.exists()

    # Check that restore fails.
    with pytest.raises(CommandFailed, match="No config file to execute restore"):
        cluster.restore(new_backup_folder_name)


def test_restore_is_failing_after_new_instance_is_added(cluster: Cluster):
    cluster.deploy(instance_count=1, wait_online=False)
    cluster.set_unique_configs_for_instances(init_replication_factor=1)
    i1, *_ = cluster.wait_online()

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 3000)

    # Create backup.
    ddl = i1.sql("BACKUP", timeout=40)
    new_backup_folder_name = ddl[0][0]

    # Manually shutdown cluster.
    cluster.terminate()

    # Add new instance.
    cluster.add_instance(wait_online=False)

    # Redeploy cluster.
    cluster.wait_online()

    # Check that restore fails (because new instance doesn't contain a backup dir).
    with pytest.raises(AssertionError, match="Backup does not exist"):
        cluster.restore(new_backup_folder_name)


################### TODO TESTS ###################

# * BACKUP is failing when there are no memory left
# * Partially made backups are removed in case some instances (e.g. replicas are terminated)
# * BACKUP works for vinyl (files are saved under specific enumerated directories which we don't handle now)
# * BACKUP/restore is possible only for admin
# * Restore is failing when topology is changed
# * BACKUP is executed on picodata version1 and restore is executed on picodata version2
#   without errors
# * Restore correctly handles share_dir which is stored not under instance_dir
# * BACKUP is executed correctly in case backup directory is partially created and then second backup
#   request (e.g. second RPC because of timeout) gets to instance
# * last_backup_timestamp is updated when two consecutive BACKUPs are executed
# * BACKUP/RESTORE are executed correctly when backup_dir contains "." and ".." when specified
#   via ENV, config, cli argument

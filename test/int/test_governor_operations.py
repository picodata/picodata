import pytest
from conftest import (
    Cluster,
    Compatibility,
    copy_dir,
    Instance,
    TarantoolError,
)
from typing import Any


def make_operation_tuple(id: int, op: str) -> list[Any]:
    return [
        id,
        "batch_id",
        op,
        "sql",
        "pending",
        "",
        "custom",
        "my description",
    ]


def insert_operations(cluster: Cluster, ops: list[str]) -> int:
    index, _, _ = cluster.batch_cas(
        [
            dict(
                table="_pico_governor_queue",
                kind="insert",
                tuple=make_operation_tuple(i + 1, op),
            )
            for i, op in enumerate(ops)
        ],
    )
    return index


def test_catalog_upgrade(compat_instance: Instance):
    compat = Compatibility()
    copy_dir(compat.previous_minor_path, compat_instance.instance_dir)
    compat_instance.start()
    compat_instance.wait_online()

    compat_instance.wait_governor_status("idle")
    res = compat_instance.sql("SELECT * FROM _pico_governor_queue")
    tt_procs = ["proc_instance_uuid", "proc_before_online", "proc_cas_v2"]
    # NOTE: will uncomment these lines when we can test with
    # specific version of Picodata
    # assert res == [
    #     [
    #         index + 1,
    #         "25.3.1",
    #         proc_name,
    #         "proc_name",
    #         "done",
    #         "upgrade",
    #         "upgrade to catalog version 25.3.1",
    #     ]
    #     for index, proc_name in enumerate(tt_procs)
    # ]

    res_all = compat_instance.sql("SELECT COUNT(*) FROM _pico_governor_queue")
    res_done = compat_instance.sql("SELECT COUNT(*) FROM _pico_governor_queue WHERE status = 'done'")
    assert res_all == res_done

    res = compat_instance.sql("SELECT id FROM _pico_table WHERE name = '_pico_governor_queue'")
    assert res == [[522]]

    res = compat_instance.sql("SELECT id, name FROM _pico_index WHERE table_id = 522")
    assert res == [[0, "_pico_governor_queue_pkey"]]

    res = compat_instance.call("box.space._space:select", [522])
    assert res[0][2] == "_pico_governor_queue"

    res = compat_instance.call("box.space._index:select", [522])
    assert res[0][2] == "_pico_governor_queue_pkey"

    for proc_name in tt_procs:
        res = compat_instance.call("box.space._func.index.name:select", [f".{proc_name}"])
        assert res[0][2] == f".{proc_name}"

    res = compat_instance.sql("SELECT value FROM _pico_property WHERE key = 'system_catalog_version'")
    assert res == [["25.3.1"]]


def test_ddl_ok(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)
    index, _ = i1.cas(
        "insert",
        "_pico_governor_queue",
        [
            1,
            "batch_id",
            "CREATE TABLE my_table (id UNSIGNED NOT NULL, PRIMARY KEY (id)) DISTRIBUTED GLOBALLY",
            "sql",
            "pending",
            "",
            "custom",
            "my description",
        ],
    )
    cluster.raft_wait_index(index + 2)

    for i in cluster.instances:
        res = i.sql(
            """
            SELECT id, batch_id, op, op_format, status, status_description, kind, description
            FROM _pico_governor_queue WHERE id = 1
            """,
        )
        assert res == [
            [
                1,
                "batch_id",
                "CREATE TABLE my_table (id UNSIGNED NOT NULL, PRIMARY KEY (id)) DISTRIBUTED GLOBALLY",
                "sql",
                "done",
                "",
                "custom",
                "my description",
            ],
        ]

        res = i.sql("SELECT * FROM my_table")
        assert res == []


def test_batch_ddl_ok(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)
    ops = [
        "CREATE TABLE my_table (id UNSIGNED NOT NULL, PRIMARY KEY (id)) DISTRIBUTED GLOBALLY",
        "ALTER TABLE my_table ADD COLUMN name TEXT NOT NULL",
        "DROP TABLE my_table",
        "CREATE TABLE my_table2 (id UNSIGNED NOT NULL, PRIMARY KEY (id)) DISTRIBUTED GLOBALLY",
        "ALTER TABLE my_table2 ADD COLUMN description TEXT NOT NULL",
    ]
    index = insert_operations(cluster, ops)
    cluster.raft_wait_index(index + len(ops) * 2)

    for i in cluster.instances:
        with pytest.raises(TarantoolError, match='table with name "my_table" not found'):
            res = i.sql("SELECT id FROM my_table")

        res = i.sql("SELECT id, description FROM my_table2")
        assert res == []

        res = i.sql("SELECT COUNT(*) FROM _pico_governor_queue WHERE status = 'done'")
        assert res == [[len(ops)]]


def test_ddl_error(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)
    # sql with typo
    op = "REATE TABLE my_table (id UNSIGNED NOT NULL, PRIMARY KEY (id)) DISTRIBUTED GLOBALLY"
    index, _ = i1.cas(
        "insert",
        "_pico_governor_queue",
        make_operation_tuple(1, op),
    )
    cluster.raft_wait_index(index + 1)

    for i in cluster.instances:
        res = i.sql("SELECT status, status_description FROM _pico_governor_queue WHERE id = 1")
        assert res[0][0] == "failed"
        assert "rule parsing error" in res[0][1]


def test_batch_ddl_error(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)
    ops = [
        "CREATE TABLE my_table (id UNSIGNED NOT NULL, PRIMARY KEY (id)) DISTRIBUTED GLOBALLY",
        "ALTER TABLE my_table ADD COLUMN name TEXT NOT NULL",
        "CREATE TABLE my_table (id UNSIGNED NOT NULL, PRIMARY KEY (id)) DISTRIBUTED GLOBALLY",
        "ALTER TABLE my_table ADD COLUMN description TEXT NOT NULL",
    ]
    index = insert_operations(cluster, ops)
    cluster.raft_wait_index(index + 5)

    for i in cluster.instances:
        res = i.sql("SELECT id FROM my_table")
        assert res == []

        res = i.sql("SELECT COUNT(*) FROM _pico_governor_queue WHERE status = 'done'")
        assert res == [[2]]

        res = i.sql("SELECT COUNT(*) FROM _pico_governor_queue WHERE status = 'failed'")
        assert res == [[1]]

        res = i.sql("SELECT status_description FROM _pico_governor_queue WHERE status = 'failed'")
        assert res == [["table my_table already exists"]]


def test_dml_ok(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)
    ops = [
        "CREATE TABLE my_table (id UNSIGNED NOT NULL, PRIMARY KEY (id)) DISTRIBUTED GLOBALLY",
        "INSERT INTO my_table VALUES (42)",
        "INSERT INTO _pico_property(key,value) SELECT key || '42', value FROM _pico_property WHERE key = 'system_catalog_version'",
    ]
    index = insert_operations(cluster, ops)
    cluster.raft_wait_index(index + 4)

    for i in cluster.instances:
        res = i.sql("SELECT id FROM my_table")
        assert res == [[42]]

        res = i.sql("SELECT COUNT(*) FROM _pico_governor_queue WHERE status = 'done'")
        assert res == [[len(ops)]]

        res = i.sql("SELECT key FROM _pico_property WHERE key = 'system_catalog_version42'")
        assert res == [["system_catalog_version42"]]


def test_ddl_if_not_exists_on_system_table(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)
    # no-op operation (table already exists)
    ops = [
        "CREATE TABLE IF NOT EXISTS _pico_property(id INT PRIMARY KEY)",
    ]
    index = insert_operations(cluster, ops)
    cluster.raft_wait_index(index + 1)

    for i in cluster.instances:
        res = i.sql("SELECT COUNT(*) FROM _pico_governor_queue WHERE status = 'done'")
        assert res == [[len(ops)]]


def test_tt_proc_creation_ok(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)
    index, _ = i1.cas(
        "insert",
        "_pico_governor_queue",
        [
            1,
            "batch_id",
            "proc_instance_uuid",
            "proc_name",
            "pending",
            "",
            "custom",
            "my description",
        ],
    )
    cluster.raft_wait_index(index + 1)

    for i in cluster.instances:
        res = i.sql(
            """
            SELECT id, batch_id, op, op_format, status, status_description, kind, description
            FROM _pico_governor_queue WHERE id = 1
            """,
        )
        assert res == [
            [
                1,
                "batch_id",
                "proc_instance_uuid",
                "proc_name",
                "done",
                "",
                "custom",
                "my description",
            ],
        ]

        res = i.call("box.space._func.index.name:select", [".proc_instance_uuid"])
        assert res[0][2] == ".proc_instance_uuid"


def test_tt_proc_creation_error(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)
    index, _ = i1.cas(
        "insert",
        "_pico_governor_queue",
        [
            1,
            "batch_id",
            "proc_nonexistent_function",
            "proc_name",
            "pending",
            "",
            "custom",
            "my description",
        ],
    )
    cluster.raft_wait_index(index + 1)

    for i in cluster.instances:
        res = i.sql(
            """
            SELECT id, batch_id, op, op_format, status, status_description, kind, description
            FROM _pico_governor_queue WHERE id = 1
            """,
        )
        assert res == [
            [
                1,
                "batch_id",
                "proc_nonexistent_function",
                "proc_name",
                "failed",
                "server responded with error: box error #10000: cannot find procedure proc_nonexistent_function in `proc::all_procs` for schema creation",
                "custom",
                "my description",
            ],
        ]

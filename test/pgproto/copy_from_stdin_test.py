import threading

import psycopg
import pytest

from conftest import (
    KeyDef,
    KeyPart,
    Postgres,
    Retriable,
    TarantoolError,
    find_routed_pk,
)
from pgproto.copy_test_utils import (
    connect_admin,
    count_rows,
    create_test_table,
    set_admin_password,
)

TEST_USER = "copy_no_write"
TEST_PASSWORD = "P@ssw0rd"  # noqa: S105


def _local_row_counts(postgres: Postgres, table_name: str, instance_count: int) -> list[int]:
    return [
        instance.eval(f'return box.space["{table_name}"]:count()')
        for instance in postgres.cluster.instances[:instance_count]
    ]


def _connect_admin_instance(postgres: Postgres, instance):
    set_admin_password(postgres, TEST_PASSWORD)
    conn = psycopg.connect(
        f"user=admin password={TEST_PASSWORD} host={instance.pg_host} port={instance.pg_port} sslmode=disable"
    )
    conn.autocommit = True
    return conn


def _create_indexed_copy_test_table(conn: psycopg.Connection, table_name: str) -> None:
    conn.execute(
        f'''
        CREATE TABLE "{table_name}" (
            "id" integer not null,
            "tenant_id" integer not null,
            "stream_id" integer not null,
            "value" string,
            primary key ("id")
        )
        USING memtx DISTRIBUTED BY ("id")
        OPTION (TIMEOUT = 3)
        '''
    )
    conn.execute(
        f'CREATE INDEX "{table_name}_tenant_id_idx" ON "{table_name}" ("tenant_id") OPTION (TIMEOUT = 10)'
    )
    conn.execute(
        f'CREATE INDEX "{table_name}_stream_id_idx" ON "{table_name}" ("stream_id") OPTION (TIMEOUT = 10)'
    )
    conn.execute(
        f'CREATE INDEX "{table_name}_tenant_stream_idx" ON "{table_name}" ("tenant_id", "stream_id") OPTION (TIMEOUT = 10)'
    )


def _bump_default_tier_bucket_state_version(postgres: Postgres) -> None:
    postgres.instance.sql(
        """
        UPDATE _pico_tier
        SET target_bucket_state_version = current_bucket_state_version + 1
        WHERE name = 'default'
        """
    )


def _default_tier_bucket_state(instance) -> tuple[int, int]:
    current, target = instance.eval(
        """
        local tier = box.space._pico_tier:get('default')
        return {tier.current_bucket_state_version, tier.target_bucket_state_version}
        """
    )
    assert current is not None
    assert target is not None
    return current, target


def _wait_until_default_tier_bucket_state_version_changes(postgres: Postgres) -> None:
    def bucket_state_changed() -> tuple[int, int]:
        current, target = _default_tier_bucket_state(postgres.instance)
        assert current != target
        return current, target

    Retriable().call(bucket_state_changed)


def _wait_until_three_way_bucket_distribution(postgres: Postgres) -> None:
    for instance in postgres.cluster.instances[:3]:
        postgres.cluster.wait_until_instance_has_this_many_active_buckets(
            instance, 1000, max_retries=20
        )


def _install_local_lref_probe(postgres: Postgres, *, pause_use_seconds: float = 0.0) -> None:
    postgres.instance.eval(
        """
        local lref = pico.dispatch.lref
        local fiber = require('fiber')
        _G.copy_lref_probe = {{
            add = 0,
            use = 0,
            del = 0,
            pause_use_seconds = {pause_use_seconds},
            old_add = lref.add,
            old_use = lref.use,
            old_del = lref.del,
        }}

        lref.add = function(...)
            _G.copy_lref_probe.add = _G.copy_lref_probe.add + 1
            return _G.copy_lref_probe.old_add(...)
        end
        lref.use = function(...)
            _G.copy_lref_probe.use = _G.copy_lref_probe.use + 1
            if _G.copy_lref_probe.pause_use_seconds > 0 then
                fiber.sleep(_G.copy_lref_probe.pause_use_seconds)
            end
            return _G.copy_lref_probe.old_use(...)
        end
        lref.del = function(...)
            _G.copy_lref_probe.del = _G.copy_lref_probe.del + 1
            return _G.copy_lref_probe.old_del(...)
        end
        """
        .format(pause_use_seconds=pause_use_seconds)
    )


def _restore_local_lref_probe(postgres: Postgres) -> None:
    postgres.instance.eval(
        """
        local probe = rawget(_G, 'copy_lref_probe')
        if probe ~= nil then
            local lref = pico.dispatch.lref
            lref.add = probe.old_add
            lref.use = probe.old_use
            lref.del = probe.old_del
            _G.copy_lref_probe = nil
        end
        """
    )


def _local_lref_probe_counts(postgres: Postgres) -> list[int]:
    return postgres.instance.eval(
        """
        local probe = rawget(_G, 'copy_lref_probe')
        return {probe.add, probe.use, probe.del}
        """
    )


def _copy_rows(
    conn: psycopg.Connection,
    table_name: str,
    rows,
    *,
    columns: str | None = '"id", "value"',
    suffix: str = "",
) -> str:
    def write_rows(copy) -> None:
        for row in rows:
            copy.write(row)

    column_list = "" if columns is None else f" ({columns})"
    return _run_copy(conn, f'COPY "{table_name}"{column_list} FROM STDIN{suffix}', write_rows)


def _run_copy(conn: psycopg.Connection, statement: str, write_rows) -> str:
    with conn.cursor() as cur:
        with cur.copy(statement) as copy:
            write_rows(copy)
        return cur.statusmessage


def _abort_copy(conn: psycopg.Connection, statement: str, rows) -> None:
    def write_rows(copy) -> None:
        for row in rows:
            copy.write(row)
        raise RuntimeError("client aborted copy")

    _run_copy(conn, statement, write_rows)


def _assert_id_value_rows(
    conn: psycopg.Connection,
    table_name: str,
    expected: list[tuple[int, str | None]],
) -> None:
    rows = conn.execute(f'SELECT "id", "value" FROM "{table_name}" ORDER BY "id"').fetchall()
    assert rows == expected


def test_copy_text_basic(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_text_basic")

        status = _copy_rows(conn, "copy_text_basic", ["1\tal", "pha\n2\tbe", "ta\n"])
        assert status == "COPY 2"
        _assert_id_value_rows(conn, "copy_text_basic", [(1, "alpha"), (2, "beta")])


def test_copy_on_conflict_do_nothing_skips_conflicting_rows(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_conflict_do_nothing")
        conn.execute(
            'INSERT INTO "copy_conflict_do_nothing" ("id", "value") VALUES (%s, %s)',
            (1, "existing"),
        )

        status = _copy_rows(
            conn,
            "copy_conflict_do_nothing",
            ["1\tskipped\n", "2\tinserted\n"],
            suffix=" ON CONFLICT DO NOTHING",
        )
        assert status == "COPY 1"

        _assert_id_value_rows(conn, "copy_conflict_do_nothing", [(1, "existing"), (2, "inserted")])


def test_copy_on_conflict_do_replace_replaces_conflicting_rows(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_conflict_do_replace")
        conn.execute(
            'INSERT INTO "copy_conflict_do_replace" ("id", "value") VALUES (%s, %s)',
            (1, "existing"),
        )

        status = _copy_rows(
            conn,
            "copy_conflict_do_replace",
            ["1\treplaced\n", "2\tinserted\n"],
            suffix=" ON CONFLICT DO REPLACE",
        )
        assert status == "COPY 2"

        _assert_id_value_rows(conn, "copy_conflict_do_replace", [(1, "replaced"), (2, "inserted")])


def test_copy_text_with_null_marker(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_text_null")

        status = _copy_rows(conn, "copy_text_null", ["1\t\\N\n2\tvalue\n"])
        assert status == "COPY 2"

        _assert_id_value_rows(conn, "copy_text_null", [(1, None), (2, "value")])


def test_copy_text_without_explicit_column_list(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_text_default_columns")

        status = _copy_rows(
            conn,
            "copy_text_default_columns",
            ["1\talpha\n", "2\tbeta\n"],
            columns=None,
        )
        assert status == "COPY 2"

        _assert_id_value_rows(conn, "copy_text_default_columns", [(1, "alpha"), (2, "beta")])


def test_copy_bucket_id_primary_key_computes_system_bucket_id(postgres: Postgres):
    with connect_admin(postgres) as conn:
        conn.execute(
            """
            CREATE TABLE "copy_bucket_id_pk" (
                "id" integer not null,
                "tenant_id" integer not null,
                "value" string,
                primary key ("bucket_id", "id")
            )
            USING memtx DISTRIBUTED BY ("tenant_id")
            """
        )

        status = _copy_rows(
            conn,
            "copy_bucket_id_pk",
            ["1\t101\talpha\n", "2\t202\tbeta\n"],
            columns='"id", "tenant_id", "value"',
        )
        assert status == "COPY 2"

        rows = conn.execute(
            'SELECT "bucket_id", "id", "tenant_id", "value" '
            'FROM "copy_bucket_id_pk" ORDER BY "id"'
        ).fetchall()

        bucket_count = conn.execute(
            "SELECT bucket_count FROM _pico_tier WHERE name = 'default'"
        ).fetchone()[0]
        key_def = KeyDef([KeyPart(1, "integer", True)])
        expected = [
            (postgres.instance.hash((101,), key_def) % bucket_count + 1, 1, 101, "alpha"),
            (postgres.instance.hash((202,), key_def) % bucket_count + 1, 2, 202, "beta"),
        ]
        assert rows == expected

        for bucket_id, row_id, tenant_id, value in rows:
            stored_rows = postgres.instance.eval(
                f'return box.space["copy_bucket_id_pk"]:select({{{bucket_id}, {row_id}}})'
            )
            assert stored_rows == [[bucket_id, row_id, tenant_id, value]]


def test_copy_with_explicit_delimiter_option(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_text_delim")

        status = _copy_rows(
            conn,
            "copy_text_delim",
            ["1|one\n2|two\n"],
            suffix=" WITH (DELIMITER '|')",
        )
        assert status == "COPY 2"

        _assert_id_value_rows(conn, "copy_text_delim", [(1, "one"), (2, "two")])


def test_copy_text_decodes_supported_escapes(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_text_escapes")

        status = _copy_rows(
            conn,
            "copy_text_escapes",
            [
                "1\thello\\\\world\n",
                "2\tline\\nfeed\n",
                "3\ttab\\tsep\n",
                "4\tascii\\141\\x42\n",
                "5\tvert\\vform\\fback\\b\n",
            ],
        )
        assert status == "COPY 5"

        _assert_id_value_rows(
            conn,
            "copy_text_escapes",
            [
                (1, "hello\\world"),
                (2, "line\nfeed"),
                (3, "tab\tsep"),
                (4, "asciiaB"),
                (5, "vert\x0bform\x0cback\x08"),
            ],
        )


def test_copy_with_header_option(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_text_header")

        status = _copy_rows(
            conn,
            "copy_text_header",
            ["id\tvalue\n", "1\tx\n"],
            suffix=" WITH (HEADER TRUE)",
        )
        assert status == "COPY 1"

        _assert_id_value_rows(conn, "copy_text_header", [(1, "x")])


def test_copy_streaming_default_commits_flushed_prefix_on_late_error(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_streaming_late_error")

        prefix_rows = [f"{idx}\trow-{idx}\n" for idx in range(1, 2049)]
        with pytest.raises(psycopg.Error, match=r"(not a valid int|invalid input syntax)"):
            _copy_rows(conn, "copy_streaming_late_error", [*prefix_rows, "bad\tboom\n"])

        persisted = count_rows(conn, "copy_streaming_late_error")
        assert 0 < persisted <= len(prefix_rows)
        persisted_rows = conn.execute(
            'SELECT "id", "value" FROM "copy_streaming_late_error" ORDER BY "id"'
        ).fetchall()
        assert persisted_rows == [
            (idx, f"row-{idx}") for idx in range(1, persisted + 1)
        ]
        first_row = conn.execute(
            'SELECT "id", "value" FROM "copy_streaming_late_error" WHERE "id" = 1'
        ).fetchone()
        assert first_row == (1, "row-1")


def test_copy_session_flush_rows_controls_streaming_flush_granularity(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_session_flush_rows_late_error")

        with pytest.raises(psycopg.Error, match=r"(not a valid int|invalid input syntax)"):
            _copy_rows(
                conn,
                "copy_session_flush_rows_late_error",
                ["1\tone\n", "2\ttwo\n", "3\tthree\n", "4\tfour\n", "bad\tboom\n"],
                suffix=" WITH (SESSION_FLUSH_ROWS = 2, DESTINATION_FLUSH_ROWS = 2)",
            )

        _assert_id_value_rows(
            conn,
            "copy_session_flush_rows_late_error",
            [(1, "one"), (2, "two"), (3, "three"), (4, "four")],
        )


def test_copy_destination_flush_rows_controls_flush_granularity(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_destination_flush_rows_late_error")

        with pytest.raises(psycopg.Error, match=r"(not a valid int|invalid input syntax)"):
            _copy_rows(
                conn,
                "copy_destination_flush_rows_late_error",
                ["1\tone\n", "2\ttwo\n", "3\tthree\n", "bad\tboom\n"],
                suffix=" WITH (SESSION_FLUSH_ROWS = 10, DESTINATION_FLUSH_ROWS = 2)",
            )

        _assert_id_value_rows(
            conn,
            "copy_destination_flush_rows_late_error",
            [(1, "one"), (2, "two")],
        )


def test_copy_fallback_flush_byte_threshold_flushes_without_user_option(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_fallback_flush_byte_threshold")

        large_value = "x" * (700 * 1024)
        with pytest.raises(psycopg.Error, match=r"(not a valid int|invalid input syntax)"):
            _copy_rows(
                conn,
                "copy_fallback_flush_byte_threshold",
                [f"1\t{large_value}\n", f"2\t{large_value}\n", "bad\tboom\n"],
                suffix=" WITH (SESSION_FLUSH_ROWS = 10, DESTINATION_FLUSH_ROWS = 10)",
            )

        assert count_rows(conn, "copy_fallback_flush_byte_threshold") == 1


def test_copy_session_flush_bytes_controls_flush_granularity(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_session_flush_byte_threshold")

        large_value = "x" * (700 * 1024)
        with pytest.raises(psycopg.Error, match=r"(not a valid int|invalid input syntax)"):
            _copy_rows(
                conn,
                "copy_session_flush_byte_threshold",
                [f"1\t{large_value}\n", f"2\t{large_value}\n", "bad\tboom\n"],
                suffix=(
                    " WITH ("
                    "SESSION_FLUSH_ROWS = 10, DESTINATION_FLUSH_ROWS = 10, "
                    "SESSION_FLUSH_BYTES = 2097152, DESTINATION_FLUSH_BYTES = 2097152"
                    ")"
                ),
            )

        assert count_rows(conn, "copy_session_flush_byte_threshold") == 0


@pytest.mark.parametrize(
    ("table_name", "suffix", "expected_error"),
    [
        (
            "copy_zero_flush_byte_option",
            " WITH (SESSION_FLUSH_BYTES = 0)",
            "session_flush_bytes must be greater than zero",
        ),
        (
            "copy_zero_row_bytes_option",
            " WITH (ROW_BYTES = 0)",
            "row_bytes must be greater than zero",
        ),
    ],
)
def test_copy_rejects_zero_limit_options(
    postgres: Postgres,
    table_name: str,
    suffix: str,
    expected_error: str,
):
    with connect_admin(postgres) as conn:
        create_test_table(conn, table_name)

        with pytest.raises(psycopg.Error, match=expected_error):
            _copy_rows(conn, table_name, [], suffix=suffix)


def test_copy_row_bytes_controls_pending_row_limit(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_row_bytes_limit")

        with pytest.raises(psycopg.Error, match="COPY row exceeds maximum size of 8 bytes"):
            _copy_rows(
                conn,
                "copy_row_bytes_limit",
                ["1\ttoo-long\n"],
                suffix=" WITH (ROW_BYTES = 8)",
            )

        assert count_rows(conn, "copy_row_bytes_limit") == 0


def test_copy_session_flush_rows_persists_flushed_prefix_on_late_bad_copy_file_format(
    postgres: Postgres,
):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_session_flush_rows_late_format_error")

        with pytest.raises(psycopg.Error, match="COPY row has 1 columns but expected 2") as exc_info:
            _copy_rows(
                conn,
                "copy_session_flush_rows_late_format_error",
                ["1\tone\n", "2\ttwo\n", "3\tthree\n", "4\tfour\n", "5\n"],
                suffix=" WITH (SESSION_FLUSH_ROWS = 2, DESTINATION_FLUSH_ROWS = 2)",
            )

        assert exc_info.value.sqlstate == "22P04"
        _assert_id_value_rows(
            conn,
            "copy_session_flush_rows_late_format_error",
            [(1, "one"), (2, "two"), (3, "three"), (4, "four")],
        )
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_copy_client_fail_aborts_and_connection_recovers(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_fail_recovery")

        with pytest.raises(psycopg.Error, match=r"client aborted copy"):
            _abort_copy(
                conn,
                'COPY "copy_fail_recovery" ("id", "value") FROM STDIN',
                ["1\twill_be_aborted\n"],
            )

        assert count_rows(conn, "copy_fail_recovery") == 0
        ping = conn.execute("SELECT 1").fetchone()
        assert ping == (1,)


def test_copy_session_flush_rows_preserves_flushed_prefix_on_client_abort(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_fail_after_flush")

        with pytest.raises(psycopg.Error, match=r"client aborted copy"):
            _abort_copy(
                conn,
                'COPY "copy_fail_after_flush" ("id", "value") '
                "FROM STDIN WITH (SESSION_FLUSH_ROWS = 2, DESTINATION_FLUSH_ROWS = 2)",
                ["1\tone\n", "2\ttwo\n", "3\tthree\n", "4\tfour\n"],
            )

        _assert_id_value_rows(
            conn,
            "copy_fail_after_flush",
            [(1, "one"), (2, "two"), (3, "three"), (4, "four")],
        )
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_copy_sharded_indexed_table_routes_across_all_replicasets(postgres: Postgres):
    postgres.cluster.add_instance(wait_online=True, replicaset_name="copy_indexed_rs2")
    postgres.cluster.add_instance(wait_online=True, replicaset_name="copy_indexed_rs3")
    _wait_until_three_way_bucket_distribution(postgres)

    with connect_admin(postgres) as conn:
        _create_indexed_copy_test_table(conn, "copy_multi_node_indexed")

        status = _copy_rows(
            conn,
            "copy_multi_node_indexed",
            [
                f"{row_id}\t{row_id % 17}\t{row_id % 9}\tvalue-{row_id}\n"
                for row_id in range(1, 385)
            ],
            columns='"id", "tenant_id", "stream_id", "value"',
        )
        assert status == "COPY 384"

        rows = conn.execute(
            'SELECT "id", "tenant_id", "stream_id", "value" '
            'FROM "copy_multi_node_indexed" ORDER BY "id"'
        ).fetchall()
        assert rows == [
            (row_id, row_id % 17, row_id % 9, f"value-{row_id}") for row_id in range(1, 385)
        ]

    local_counts = _local_row_counts(postgres, "copy_multi_node_indexed", 3)
    assert sum(local_counts) == 384
    assert all(count > 0 for count in local_counts)


def test_copy_sharded_all_local_batch_uses_local_fast_path(postgres: Postgres):
    postgres.cluster.add_instance(wait_online=True, replicaset_name="copy_local_fast_rs2")
    postgres.cluster.add_instance(wait_online=True, replicaset_name="copy_local_fast_rs3")
    _wait_until_three_way_bucket_distribution(postgres)

    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_all_local_fast_path")

        local_pks = []
        next_start = 1
        for _ in range(3):
            local_pk = find_routed_pk(postgres.instance, is_local=True, start=next_start)
            local_pks.append(local_pk)
            next_start = local_pk + 1

        _install_local_lref_probe(postgres)
        try:
            status = _copy_rows(
                conn,
                "copy_all_local_fast_path",
                [f"{row_id}\tvalue-{row_id}\n" for row_id in local_pks],
                suffix=" WITH (SESSION_FLUSH_ROWS = 2, DESTINATION_FLUSH_ROWS = 2)",
            )
            assert status == f"COPY {len(local_pks)}"

            lref_add, lref_use, lref_del = _local_lref_probe_counts(postgres)
            assert lref_add > 0
            assert lref_add == lref_use == lref_del
        finally:
            _restore_local_lref_probe(postgres)

        _assert_id_value_rows(
            conn,
            "copy_all_local_fast_path",
            [(row_id, f"value-{row_id}") for row_id in sorted(local_pks)],
        )

    local_counts = _local_row_counts(postgres, "copy_all_local_fast_path", 3)
    assert local_counts[0] == len(local_pks)
    assert local_counts[1:] == [0, 0]


def test_copy_sharded_prefix_persists_when_bucket_state_version_changes_during_execution(
    postgres: Postgres,
):
    postgres.cluster.add_instance(wait_online=True, replicaset_name="copy_routing_rs2")
    for instance in postgres.cluster.instances[:2]:
        postgres.cluster.wait_until_instance_has_this_many_active_buckets(
            instance, 1500, max_retries=20
        )

    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_bucket_state_changed")

        local_pk = find_routed_pk(postgres.instance, is_local=True)
        remote_pk = find_routed_pk(postgres.instance, is_local=False, start=local_pk + 1)

        def write_rows(copy) -> None:
            copy.write(f"{local_pk}\tone\n")
            copy.write(f"{remote_pk}\ttwo\n")
            _bump_default_tier_bucket_state_version(postgres)
            _wait_until_default_tier_bucket_state_version_changes(postgres)
            copy.write(f"{remote_pk + 1}\tthree\n")
            copy.write(f"{remote_pk + 2}\tfour\n")

        with pytest.raises(psycopg.Error, match="bucket routing changed during execution") as exc_info:
            _run_copy(
                conn,
                'COPY "copy_bucket_state_changed" ("id", "value") '
                "FROM STDIN WITH (SESSION_FLUSH_ROWS = 2, DESTINATION_FLUSH_ROWS = 2)",
                write_rows,
            )

        assert exc_info.value.sqlstate == "55000"
        rows = conn.execute(
            'SELECT "id", "value" FROM "copy_bucket_state_changed" ORDER BY "id"'
        ).fetchall()
        assert rows == [(local_pk, "one"), (remote_pk, "two")]
        assert _local_row_counts(postgres, "copy_bucket_state_changed", 2) == [1, 1]
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_copy_sharded_mixed_batch_rechecks_routing_before_remote_dispatch(
    postgres: Postgres,
):
    postgres.cluster.add_instance(wait_online=True, replicaset_name="copy_routing_rs2")
    for instance in postgres.cluster.instances[:2]:
        postgres.cluster.wait_until_instance_has_this_many_active_buckets(
            instance, 1500, max_retries=20
        )

    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_mixed_batch_routing_recheck")

    local_pk = find_routed_pk(postgres.instance, is_local=True)
    remote_pk = find_routed_pk(postgres.instance, is_local=False, start=local_pk + 1)

    _install_local_lref_probe(postgres, pause_use_seconds=1.0)
    copy_error = None

    def run_copy() -> None:
        nonlocal copy_error
        try:
            with connect_admin(postgres) as conn:
                _copy_rows(
                    conn,
                    "copy_mixed_batch_routing_recheck",
                    [f"{local_pk}\tlocal\n", f"{remote_pk}\tremote\n"],
                    suffix=" WITH (SESSION_FLUSH_ROWS = 2, DESTINATION_FLUSH_ROWS = 2)",
                )
        except psycopg.Error as error:
            copy_error = error

    def wait_until_local_lref_is_used() -> None:
        assert _local_lref_probe_counts(postgres)[1] >= 1

    copy_thread = threading.Thread(target=run_copy)
    copy_thread.start()

    try:
        Retriable().call(wait_until_local_lref_is_used)
        _bump_default_tier_bucket_state_version(postgres)
        _wait_until_default_tier_bucket_state_version_changes(postgres)

        copy_thread.join(timeout=10)
        assert not copy_thread.is_alive()
        assert copy_error is not None
        assert "bucket routing changed during execution" in str(copy_error)
        assert copy_error.sqlstate == "55000"

        with connect_admin(postgres) as conn:
            _assert_id_value_rows(
                conn,
                "copy_mixed_batch_routing_recheck",
                [(local_pk, "local")],
            )
            assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        _restore_local_lref_probe(postgres)


def test_copy_sharded_mixed_local_and_remote_batch_persists_local_prefix_on_remote_conflict(
    postgres: Postgres,
):
    postgres.cluster.add_instance(wait_online=True, replicaset_name="copy_conflict_rs2")
    for instance in postgres.cluster.instances[:2]:
        postgres.cluster.wait_until_instance_has_this_many_active_buckets(
            instance, 1500, max_retries=20
        )

    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_mixed_local_remote_conflict")

        local_pk = find_routed_pk(postgres.instance, is_local=True)
        remote_pk = find_routed_pk(postgres.instance, is_local=False, start=local_pk + 1)

        conn.execute(
            'INSERT INTO "copy_mixed_local_remote_conflict" ("id", "value") VALUES (%s, %s)',
            (remote_pk, "existing-remote"),
        )

        with pytest.raises(psycopg.Error, match="Duplicate key exists") as exc_info:
            _copy_rows(
                conn,
                "copy_mixed_local_remote_conflict",
                [f"{local_pk}\tlocal-prefix\n", f"{remote_pk}\tremote-conflict\n"],
                suffix=" WITH (SESSION_FLUSH_ROWS = 2, DESTINATION_FLUSH_ROWS = 2)",
            )

        assert exc_info.value.sqlstate == "XX000"
        _assert_id_value_rows(
            conn,
            "copy_mixed_local_remote_conflict",
            [(local_pk, "local-prefix"), (remote_pk, "existing-remote")],
        )
        assert sorted(
            _local_row_counts(
                postgres, "copy_mixed_local_remote_conflict", len(postgres.cluster.instances)
            )
        ) == [1, 1]
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_copy_sharded_on_conflict_do_nothing_skips_remote_conflict(
    postgres: Postgres,
):
    postgres.cluster.add_instance(wait_online=True, replicaset_name="copy_do_nothing_rs2")
    for instance in postgres.cluster.instances[:2]:
        postgres.cluster.wait_until_instance_has_this_many_active_buckets(
            instance, 1500, max_retries=20
        )

    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_sharded_do_nothing")

        local_pk = find_routed_pk(postgres.instance, is_local=True)
        remote_pk = find_routed_pk(postgres.instance, is_local=False, start=local_pk + 1)

        conn.execute(
            'INSERT INTO "copy_sharded_do_nothing" ("id", "value") VALUES (%s, %s)',
            (remote_pk, "existing-remote"),
        )

        status = _copy_rows(
            conn,
            "copy_sharded_do_nothing",
            [f"{local_pk}\tlocal\n", f"{remote_pk}\tremote-skipped\n"],
            suffix=(
                " WITH (SESSION_FLUSH_ROWS = 2, DESTINATION_FLUSH_ROWS = 2) "
                "ON CONFLICT DO NOTHING"
            ),
        )
        assert status == "COPY 1"

        _assert_id_value_rows(
            conn,
            "copy_sharded_do_nothing",
            [(local_pk, "local"), (remote_pk, "existing-remote")],
        )
        assert sorted(
            _local_row_counts(postgres, "copy_sharded_do_nothing", len(postgres.cluster.instances))
        ) == [1, 1]


def test_copy_sharded_single_replicaset_from_read_only_replica_dispatches_to_leader(
    postgres: Postgres,
):
    follower = postgres.cluster.add_instance(
        wait_online=True, replicaset_name=postgres.instance.replicaset_name
    )
    assert follower.eval("return box.info.ro")

    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_single_rs_from_replica")

    with _connect_admin_instance(postgres, follower) as conn:
        status = _copy_rows(conn, "copy_single_rs_from_replica", ["1\tone\n", "2\ttwo\n"])
        assert status == "COPY 2"

        _assert_id_value_rows(conn, "copy_single_rs_from_replica", [(1, "one"), (2, "two")])


def test_copy_global_table_from_read_only_replica_uses_global_write_path(postgres: Postgres):
    follower = postgres.cluster.add_instance(
        wait_online=True, replicaset_name=postgres.instance.replicaset_name
    )
    assert follower.eval("return box.info.ro")

    with connect_admin(postgres) as conn:
        conn.execute(
            """
            CREATE TABLE "copy_global_from_replica" (
                "id" integer not null,
                "value" string,
                primary key ("id")
            )
            USING memtx DISTRIBUTED GLOBALLY
            """
        )

    with _connect_admin_instance(postgres, follower) as conn:
        status = _copy_rows(conn, "copy_global_from_replica", ["1\tone\n", "2\ttwo\n"])
        assert status == "COPY 2"

        _assert_id_value_rows(conn, "copy_global_from_replica", [(1, "one"), (2, "two")])


def test_copy_text_with_crlf_and_final_line_without_newline(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_crlf")

        status = _copy_rows(conn, "copy_crlf", ["1\talpha\r\n2\tbeta"])
        assert status == "COPY 2"

        _assert_id_value_rows(conn, "copy_crlf", [(1, "alpha"), (2, "beta")])


def test_copy_text_escaped_physical_newline(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_escaped_newline")

        status = _copy_rows(conn, "copy_escaped_newline", ["1\thello\\\nworld\n"])
        assert status == "COPY 1"

        _assert_id_value_rows(conn, "copy_escaped_newline", [(1, "hello\nworld")])


def test_copy_text_treats_backslash_dot_as_data(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_backslash_dot")

        status = _copy_rows(conn, "copy_backslash_dot", ["1\t\\.\n"])
        assert status == "COPY 1"

        _assert_id_value_rows(conn, "copy_backslash_dot", [(1, ".")])


def test_copy_text_rejects_mixed_line_endings(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_mixed_line_endings")

        with pytest.raises(psycopg.Error, match="mixed line endings"):
            _copy_rows(conn, "copy_mixed_line_endings", ["1\talpha\n2\tbeta\r\n"])

        assert count_rows(conn, "copy_mixed_line_endings") == 0


def test_copy_empty_string_is_not_null(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_empty_string")

        status = _copy_rows(conn, "copy_empty_string", ["1\t\n", "2\t\\N\n"])
        assert status == "COPY 2"

        _assert_id_value_rows(conn, "copy_empty_string", [(1, ""), (2, None)])


@pytest.mark.parametrize(
    ("table_name", "row", "expected_error"),
    [
        ("copy_too_few", "1\n", "COPY row has 1 columns but expected 2"),
        ("copy_too_many", "1\talpha\textra\n", "COPY row has 3 columns but expected 2"),
    ],
)
def test_copy_rejects_wrong_column_count_without_partial_rows(
    postgres: Postgres,
    table_name: str,
    row: str,
    expected_error: str,
):
    with connect_admin(postgres) as conn:
        create_test_table(conn, table_name)

        # psycopg still sends CopyDone while unwinding a failed COPY context manager,
        # so same-connection recovery is asserted in the raw protocol suite instead.
        with pytest.raises(psycopg.Error, match=expected_error):
            _copy_rows(conn, table_name, [row])

    with connect_admin(postgres) as check_conn:
        assert check_conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(check_conn, table_name) == 0


def test_copy_rejects_duplicate_columns_in_column_list(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_duplicate_columns")

        with pytest.raises(psycopg.Error, match='column "id" specified more than once') as exc_info:
            _copy_rows(conn, "copy_duplicate_columns", [], columns='"id", "id"')

        assert exc_info.value.sqlstate == "42701"
        assert conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(conn, "copy_duplicate_columns") == 0


def test_copy_rejects_unknown_column_in_column_list(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_unknown_column")

        with pytest.raises(psycopg.Error, match='column does not exist: missing') as exc_info:
            _copy_rows(conn, "copy_unknown_column", [], columns='"id", "missing"')

        assert exc_info.value.sqlstate == "42703"
        assert conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(conn, "copy_unknown_column") == 0


def test_copy_rejects_system_bucket_id_column_in_column_list(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_system_bucket_id")

        with pytest.raises(
            psycopg.Error, match='system column "bucket_id" cannot be inserted'
        ) as exc_info:
            _copy_rows(
                conn,
                "copy_system_bucket_id",
                [],
                columns='"bucket_id", "id", "value"',
            )

        assert exc_info.value.sqlstate == "42P10"
        assert conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(conn, "copy_system_bucket_id") == 0


def test_copy_rejects_missing_required_columns_in_column_list(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_missing_required_column")

        with pytest.raises(psycopg.Error, match='NonNull column "id" must be specified') as exc_info:
            _copy_rows(conn, "copy_missing_required_column", [], columns='"value"')

        assert exc_info.value.sqlstate == "23502"
        assert conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(conn, "copy_missing_required_column") == 0


def test_copy_rejects_missing_required_global_bucket_id_column(postgres: Postgres):
    with connect_admin(postgres) as conn:
        conn.execute(
            """
            CREATE TABLE "copy_missing_global_bucket_id" (
                "id" INT PRIMARY KEY,
                "bucket_id" INT NOT NULL,
                "value" TEXT
            ) DISTRIBUTED GLOBALLY
            """
        )

        with pytest.raises(
            psycopg.Error, match='NonNull column "bucket_id" must be specified'
        ) as exc_info:
            _copy_rows(
                conn,
                "copy_missing_global_bucket_id",
                [],
                columns='"id", "value"',
            )

        assert exc_info.value.sqlstate == "23502"
        assert conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(conn, "copy_missing_global_bucket_id") == 0


def test_copy_requires_write_privilege(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_acl_denied")

    postgres.instance.sql(f'CREATE USER "{TEST_USER}" WITH PASSWORD \'{TEST_PASSWORD}\' USING md5')

    with psycopg.connect(
        f"user={TEST_USER} password={TEST_PASSWORD} host={postgres.host} port={postgres.port} sslmode=disable"
    ) as conn:
        conn.autocommit = True

        with pytest.raises(psycopg.Error) as exc_info:
            _copy_rows(conn, "copy_acl_denied", [])

        assert exc_info.value.sqlstate == "42501"
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_copy_rejects_unsupported_copy_format(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_unsupported_format")

        with pytest.raises(
            psycopg.Error, match="COPY format csv is not supported"
        ) as exc_info:
            _copy_rows(conn, "copy_unsupported_format", [], suffix=" WITH (FORMAT CSV)")

        assert exc_info.value.sqlstate == "0A000"
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_copy_rejects_non_operable_table_at_start(postgres: Postgres):
    set_admin_password(postgres, TEST_PASSWORD)

    error_injection = "BLOCK_GOVERNOR_BEFORE_DDL_COMMIT"
    postgres.instance.call("pico._inject_error", error_injection, True)
    try:
        with pytest.raises((TimeoutError, TarantoolError), match="timeout"):
            postgres.instance.sql(
                """
                CREATE TABLE "copy_not_operable" (
                    "id" integer not null,
                    "value" string,
                    primary key ("id")
                )
                using memtx distributed by ("id")
                option (timeout = 1)
                """
            )

        with psycopg.connect(
            f"user=admin password={TEST_PASSWORD} host={postgres.host} port={postgres.port} sslmode=disable"
        ) as conn:
            conn.autocommit = True

            with pytest.raises(
                psycopg.Error, match="cannot be modified now as DDL operation is in progress"
            ) as exc_info:
                _copy_rows(conn, "copy_not_operable", [])

            assert exc_info.value.sqlstate == "55000"
            assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        postgres.instance.call("pico._inject_error", error_injection, False)


def test_copy_rejects_trailing_escape_and_connection_recovers(postgres: Postgres):
    with connect_admin(postgres) as conn:
        create_test_table(conn, "copy_trailing_escape")

        with pytest.raises(psycopg.Error, match="COPY data ended inside an escape sequence"):
            _copy_rows(conn, "copy_trailing_escape", ["1\tbroken\\"])

        assert conn.execute("SELECT 1").fetchone() == (1,)
        assert count_rows(conn, "copy_trailing_escape") == 0

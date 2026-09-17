import subprocess
from enum import StrEnum
from pathlib import Path
from typing import Any

from conftest import Cluster, Instance

# It's copy from `SPACE_ID_INTERNAL_MAX` in `src/storage.rs`.
SPACE_ID_INTERNAL_MAX = 1024

ADMIN_PASSWORD = "T0psecret"

TIERS = """
cluster:
    name: {cluster_name}
    tier:
        default:
            replication_mode: async
        storage:
            replication_mode: sync
"""

EXPECTED_REPLICATION_MODES = {"default": "async", "storage": "sync"}

PRIMARY_KEY = "<primary key>"


class Table(StrEnum):
    SHARDED = "t_sharded"
    IN_STORAGE = "t_in_storage"
    GLOBAL = "t_global"
    UNLOGGED = "t_unlogged"
    PK_BUCKET = "t_pk_bucket"
    DESC_PK = "t_desc_pk"
    MULTI_KEY = "t_multi_key"
    VINYL = "t_vinyl"
    TYPES = "t_types"
    QUOTED = "Quoted Name"


SCHEMA = [
    f"CREATE TABLE {Table.SHARDED} (id INT NOT NULL, payload TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id)",
    f"CREATE TABLE {Table.IN_STORAGE} (id INT NOT NULL, v TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER storage",
    f"""
    CREATE TABLE {Table.GLOBAL} (id INT NOT NULL, bucket_id UNSIGNED, name TEXT NOT NULL, PRIMARY KEY (id))
    DISTRIBUTED GLOBALLY
    """,
    f"CREATE UNLOGGED TABLE {Table.UNLOGGED} (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED BY (id)",
    f"CREATE TABLE {Table.PK_BUCKET} (a INT NOT NULL, b TEXT, PRIMARY KEY (bucket_id, a)) DISTRIBUTED BY (a)",
    f"CREATE TABLE {Table.DESC_PK} (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a DESC, b)) DISTRIBUTED BY (a)",
    f"""
    CREATE TABLE {Table.MULTI_KEY} (a INT NOT NULL, b TEXT NOT NULL, c INT, PRIMARY KEY (a, b))
    DISTRIBUTED BY (b, a)
    """,
    f"""
    CREATE TABLE {Table.VINYL} (id INT NOT NULL, v TEXT, PRIMARY KEY (id))
    USING vinyl WITH (page_size = 1024, run_size_ratio = 3.5, bloom_fpr = 0.125)
    DISTRIBUTED BY (id)
    """,
    f"""
    CREATE TABLE {Table.TYPES} (
        c_int INT NOT NULL, c_unsigned UNSIGNED, c_double DOUBLE, c_decimal DECIMAL,
        c_bool BOOL, c_text TEXT, c_uuid UUID, c_datetime DATETIME, c_json JSON,
        c_text_arr TEXT[], c_int_arr INT[], c_uuid_arr UUID[], c_double_arr DOUBLE[],
        c_bool_arr BOOL[], c_datetime_arr DATETIME[], c_decimal_arr DECIMAL[], c_json_arr JSON[],
        PRIMARY KEY (c_int)
    )
    DISTRIBUTED GLOBALLY
    """,
    f"""
    CREATE TABLE "{Table.QUOTED}" ("select" INT NOT NULL, "it's" TEXT, "MixedCase" INT, PRIMARY KEY ("select"))
    DISTRIBUTED GLOBALLY
    """,
    f"CREATE INDEX idx_payload ON {Table.SHARDED} (payload)",
    f"CREATE UNIQUE INDEX idx_global_name ON {Table.GLOBAL} (name DESC)",
    f"CREATE INDEX idx_types_multi ON {Table.TYPES} (c_text, c_int DESC)",
    f"CREATE UNIQUE INDEX idx_global_hash ON {Table.GLOBAL} USING hash (id)",
    # The grammar requires decimals with a point and without an exponent
    f"CREATE INDEX idx_vinyl_v ON {Table.VINYL} (v) WITH (page_size = 2048, run_size_ratio = 2.0, bloom_fpr = 0.0000001)",
    f'CREATE INDEX "Idx Quoted" ON "{Table.QUOTED}" ("MixedCase")',
]


def deploy(cluster: Cluster) -> Instance:
    cluster.set_config_file(yaml=TIERS.format(cluster_name=cluster.name))
    router = cluster.add_instance(tier="default")
    cluster.add_instance(tier="storage")
    router.sql(f"ALTER USER \"admin\" WITH PASSWORD '{ADMIN_PASSWORD}'")
    return router


def obtain_schema_from(instance: Instance) -> dict[str, dict[str, Any]]:
    """Leaves out what the receiving cluster assigns itself: ids, the primary key
    name (`<table_id>_pkey`) and the owner (there is no `ALTER TABLE ... OWNER TO`).
    """
    tables = instance.sql(
        "SELECT id, name, distribution, format, engine, description, opts "
        f"FROM _pico_table WHERE id > {SPACE_ID_INTERNAL_MAX}",
        sudo=True,
    )
    indexes = instance.sql(
        f"SELECT table_id, id, name, type, opts, parts FROM _pico_index WHERE table_id > {SPACE_ID_INTERNAL_MAX}",
        sudo=True,
    )

    name_by_id = {table_id: name for table_id, name, *_ in tables}
    schema: dict[str, dict[str, Any]] = {
        name: {
            "distribution": distribution,
            "format": table_format,
            "engine": engine,
            "description": description,
            "opts": opts,
            "indexes": {},
        }
        for _, name, distribution, table_format, engine, description, opts in tables
    }
    for table_id, index_id, index_name, index_type, opts, parts in indexes:
        key = PRIMARY_KEY if index_id == 0 else index_name
        schema[name_by_id[table_id]]["indexes"][key] = {
            "type": index_type,
            "opts": opts,
            "parts": parts,
        }

    return schema


def export(instance: Instance, dump: Path) -> None:
    dsn = f"host={instance.pg_host} port={instance.pg_port} user=admin password={ADMIN_PASSWORD}"
    result = subprocess.run(
        [instance.executable.command, "export", "--schema-only", dsn, "-f", str(dump)],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr


def obtain_replication_modes_from(dump: str) -> dict[str, str]:
    modes: dict[str, str] = {}
    for line in dump.splitlines():
        if not line.startswith("--   ") or "replication_mode" not in line:
            continue
        name, parameters = line.removeprefix("--   ").split(":", maxsplit=1)
        for parameter in parameters.split(","):
            key, _, value = parameter.partition("=")
            if key.strip() == "replication_mode":
                modes[name] = value.strip()

    return modes


def obtain_statements_from(dump: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    for line in dump.splitlines():
        stripped = line.strip()
        if not buffer and (not stripped or stripped.startswith("--")):
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(buffer))
            buffer = []

    assert not buffer, f"the dump ends in the middle of a statement: {buffer}"
    return statements


def test_export_round_trip(cluster: Cluster, second_cluster: Cluster, tmp_path: Path):
    source = deploy(cluster)
    for statement in SCHEMA:
        source.sql(statement, sudo=True)

    expected_schema = obtain_schema_from(source)
    assert set(expected_schema) == {table.value for table in Table}

    assert expected_schema[Table.IN_STORAGE]["opts"] == [{"synchronous": [True]}]

    dump = tmp_path / "dump.sql"
    export(source, dump)
    dumped = dump.read_text()

    assert obtain_replication_modes_from(dumped) == EXPECTED_REPLICATION_MODES

    target = deploy(second_cluster)
    statements = obtain_statements_from(dumped)
    assert len(statements) == len(SCHEMA)

    in_storage = next(statement for statement in statements if Table.IN_STORAGE in statement)
    assert "synchronous" not in in_storage.lower(), in_storage

    with target.connect_via_pgproto(user="admin", password=ADMIN_PASSWORD) as connection:
        connection.autocommit = True
        for statement in statements:
            connection.execute(statement)

    assert obtain_schema_from(target) == expected_schema

    round_trip = tmp_path / "dump_b.sql"
    export(target, round_trip)
    assert obtain_statements_from(round_trip.read_text()) == statements

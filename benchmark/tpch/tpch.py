import argparse
import csv
from dataclasses import dataclass, field
from pathlib import Path
import sys
from typing import Iterable, List

import psycopg
from tqdm import tqdm

TPCH_DIR = Path(__file__).parent

BATCH = 1000
VALUES_PER_QUERY = 64

INSERT_INTO_NATION = """
INSERT INTO nation(
    n_nationkey,
    n_name,
    n_regionkey,
    n_comment
) VALUES
"""


INSERT_INTO_REGION = """
INSERT INTO region(
    r_regionkey,
    r_name,
    r_comment
) VALUES
"""


INSERT_INTO_PART = """
INSERT INTO part(
    p_partkey,
    p_name,
    p_mfgr,
    p_brand,
    p_type,
    p_size,
    p_container,
    p_retailprice,
    p_comment
) VALUES
"""


INSERT_INTO_SUPPLIER = """
INSERT INTO supplier(
    s_suppkey,
    s_name,
    s_address,
    s_nationkey,
    s_phone,
    s_acctbal,
    s_comment
) VALUES
"""


INSERT_INTO_PARTSUPP = """
INSERT INTO partsupp(
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    ps_comment
) VALUES
"""


INSERT_INTO_CUSTOMER = """
INSERT INTO customer(
    c_custkey,
    c_name,
    c_address,
    c_nationkey,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment
) VALUES
"""

INSERT_INTO_ORDERS = """
INSERT INTO orders(
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment
) VALUES
"""

INSERT_INTO_LINEITEM = """
INSERT INTO lineitem(
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode,
    l_comment
) VALUES
"""


def table_csv_path(table_name: str) -> Path:
    r = TPCH_DIR / "data" / f"{table_name}.csv"
    assert r.exists(), f"Data file not found at {r}. Did you run tpchgen-cli?"
    return r


def workaround_date(rows: List[str], dt_idx: List[int]) -> List[str]:
    for idx in dt_idx:
        rows[idx] = f"{rows[idx]} 00:00:00Z"
    return rows


@dataclass
class Table:
    name: str
    insert_into: str
    num_columns: int
    date_columns: List[int] = field(default_factory=list)


def do_insert(table: Table, rows: List[List[str]], conn: psycopg.Connection):
    placeholders_for_one_row = ",".join(["%s"] * table.num_columns)
    placeholders_for_insert = ", ".join([f"({placeholders_for_one_row})"] * len(rows))
    params = [x for row in rows for x in row]

    conn.execute(
        f"{table.insert_into} {placeholders_for_insert}",
        params,
    )
    rows.clear()


@dataclass
class CountBytes:
    """
    We cant use f.tell() during iteration over the file:
    "OSError: telling position disabled by next() call"
    so use the wrapper to keep track how many bytes we've read so far
    """

    inner: Iterable[str]
    total: int

    def __next__(self):
        ret = self.inner.__next__()
        self.total += len(ret)
        return ret

    def __iter__(self):
        return self


def populate_table(table: Table, conn: psycopg.Connection):
    print(f"Populating {table.name}...")

    table_file = table_csv_path(table.name)
    with table_file.open() as f:
        counter = CountBytes(inner=f, total=0)
        reader = csv.reader(counter)
        # disgard header
        _ = next(reader)
        with conn.pipeline() as pipeline:
            # batch rows so we can execute less queries
            rows = []

            # manually setup progress bar. we measure progress in bytes,
            # because we dont want to read file twice. Though it's pure perfectionism,
            # because file is not that big and counting lines in it is nothing
            # compared to actual loading time.
            with tqdm(unit="bytes", total=table_file.stat().st_size, unit_scale=True) as progress:
                counter_prev = 0
                for row in reader:
                    # we manually update progress bar with a difference from previous value
                    if counter_prev != counter.total:
                        progress.update(counter.total - counter_prev)
                        counter_prev = counter.total

                    rows.append(workaround_date(row, table.date_columns))

                    if len(rows) == VALUES_PER_QUERY:
                        do_insert(table, rows, conn)

                    if len(rows) % BATCH == 0:
                        pipeline.sync()

                if rows:
                    do_insert(table, rows, conn)

                pipeline.sync()


def init(args):
    # Connect to the database
    conn = psycopg.connect(args.connection, autocommit=True)

    tables = [
        Table(
            name="nation",
            insert_into=INSERT_INTO_NATION,
            num_columns=4,
        ),
        Table(
            name="region",
            insert_into=INSERT_INTO_REGION,
            num_columns=3,
        ),
        Table(
            name="part",
            insert_into=INSERT_INTO_PART,
            num_columns=9,
        ),
        Table(
            name="supplier",
            insert_into=INSERT_INTO_SUPPLIER,
            num_columns=7,
        ),
        Table(
            name="partsupp",
            insert_into=INSERT_INTO_PARTSUPP,
            num_columns=5,
        ),
        Table(
            name="customer",
            insert_into=INSERT_INTO_CUSTOMER,
            num_columns=8,
        ),
        Table(
            name="orders",
            insert_into=INSERT_INTO_ORDERS,
            date_columns=[4],
            num_columns=9,
        ),
        Table(
            name="lineitem",
            insert_into=INSERT_INTO_LINEITEM,
            date_columns=[10, 11, 12],
            num_columns=16,
        ),
    ]

    # Create tables
    print("Creating tables...")

    # Drop existing tables if they exist
    for table in tables:
        conn.execute(f"DROP TABLE IF EXISTS {table.name};")

    # Create new tables
    schema = (TPCH_DIR / "schema.sql").read_text()
    # sbroad doesnt support comments, strip them manually
    # https://github.com/dimitri/tpch-citus/blob/master/schema/tpch-schema.sql
    # https://github.com/dimitri/tpch-citus/blob/master/schema/tpch-pkeys.sql
    queries = list(filter(lambda q: not q.startswith("--"), (query for query in schema.split("\n\n"))))

    for query in queries:
        conn.execute(query)

    # Populate tables from csv
    for table in tables:
        populate_table(table, conn)

    print("Initialization complete!")


def main():
    parser = argparse.ArgumentParser("Initialize tables for tpch benchmark with Picodata.")
    subparsers = parser.add_subparsers(dest="command")

    init_parser = subparsers.add_parser(
        "init",
        help="Create tables and load generated data into the database",
    )
    init_parser.add_argument("connection", type=str, help="Connection string for pgproto server")

    commands = {
        "init": init,
    }

    args = parser.parse_args()
    command = commands.get(args.command)
    if command is None:
        print(f"Unknown command: {command}", file=sys.stderr)
        sys.exit(1)

    command(args)


if __name__ == "__main__":
    main()

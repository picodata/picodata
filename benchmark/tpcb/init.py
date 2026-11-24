#!/usr/bin/env python

"""
This script imitates default initialization performed by `pgbench -i` command.
It is based on RunInitSteps* function from pgbench sources.
* https://github.com/postgres/postgres/blob/db22b900244d3c51918acce44cbe5bb6f6507d32/src/bin/pgbench/pgbench.c#L5224
"""

import argparse
import psycopg
from tqdm import tqdm


# Set up command-line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Initialize tables for pgbench benchmarking with Picodata.")
    parser.add_argument("connection", type=str, help="Connection string for the pgproto server")
    parser.add_argument("-s", "--scale", type=int, default=1, help="Scaling factor for the benchmark (default: 1)")
    parser.add_argument(
        "--engine",
        type=str.lower,
        choices=["memtx", "vinyl"],
        default="memtx",
        help="Storage engine for the benchmark tables (default: memtx)",
    )
    parser.add_argument(
        "--global",
        dest="global_",
        action="store_true",
        help="Create the benchmark tables as global (DISTRIBUTED GLOBALLY) instead of the default tables sharded by the primary key",
    )
    parser.add_argument(
        "--unlogged",
        action="store_true",
        help="Create the benchmark tables as unlogged (CREATE UNLOGGED TABLE)",
    )
    args = parser.parse_args()

    # Combinations rejected by picodata. Checked here, so that an invalid one
    # does not leave the instance without the tables dropped below.
    if args.global_ and args.engine != "memtx":
        parser.error("global tables can use only the memtx engine, so --global cannot be combined with --engine vinyl")
    if args.unlogged and args.engine != "memtx":
        parser.error(
            "unlogged tables can use only the memtx engine, so --unlogged cannot be combined with --engine vinyl"
        )
    if args.global_ and args.unlogged:
        parser.error("global tables can't be unlogged, so --global cannot be combined with --unlogged")

    return args


def main():
    # Parse command-line arguments
    args = parse_args()

    # Connect to the database
    conn = psycopg.connect(args.connection, autocommit=True)

    # Create tables
    create = "CREATE UNLOGGED TABLE" if args.unlogged else "CREATE TABLE"
    table_options = f"USING {args.engine} DISTRIBUTED GLOBALLY" if args.global_ else f"USING {args.engine}"
    print(f"Creating tables: {create} ... {table_options}")

    # Drop existing tables if they exist
    conn.execute("DROP TABLE IF EXISTS pgbench_branches;")
    conn.execute("DROP TABLE IF EXISTS pgbench_tellers;")
    conn.execute("DROP TABLE IF EXISTS pgbench_accounts;")
    conn.execute("DROP TABLE IF EXISTS pgbench_history;")

    # Create new tables following the structure from initCreateTables*.
    # * https://github.com/postgres/postgres/blob/db22b900244d3c51918acce44cbe5bb6f6507d32/src/bin/pgbench/pgbench.c#L4809
    conn.execute(f"{create} pgbench_branches (bid int PRIMARY KEY, bbalance int, filler varchar(88)) {table_options};")
    conn.execute(
        f"{create} pgbench_tellers (tid int PRIMARY KEY, bid int, tbalance int, filler varchar(84)) {table_options};"
    )
    conn.execute(
        f"{create} pgbench_accounts (aid int PRIMARY KEY, bid int, abalance int, filler varchar(84)) {table_options};"
    )
    conn.execute(
        f"{create} pgbench_history (tid int, bid int, aid int, delta int, mtime datetime, filler varchar(22), "
        f"PRIMARY KEY(tid, bid, aid, delta)) {table_options};"
    )

    # Set scale and table sizes
    scale = args.scale
    nbranches = 1
    ntellers = 10
    naccounts = 100000

    # Populate tables with data according to initGenerateDataServerSide*.
    # * https://github.com/postgres/postgres/blob/db22b900244d3c51918acce44cbe5bb6f6507d32/src/bin/pgbench/pgbench.c#L5082

    # Populate pgbench_branches table
    print("Populating pgbench_branches...")
    for i in tqdm(range(nbranches * scale)):
        conn.execute("INSERT INTO pgbench_branches(bid, bbalance) VALUES (%s, 0)", (i,))

    # Populate pgbench_tellers table
    print("Populating pgbench_tellers...")
    for tid in tqdm(range(ntellers * scale)):
        conn.execute(
            "INSERT INTO pgbench_tellers(tid, bid, tbalance) VALUES (%(tid)s::int, (%(tid)s::int - 1) / %(ntellers)s::int + 1, 0)",
            {"tid": tid, "ntellers": ntellers},
        )

    # Populate pgbench_accounts table
    print("Populating pgbench_accounts...")
    batch = 1000
    values_per_query = 64

    with conn.pipeline() as pipeline:
        # batch rows so we can execute less queries
        rows = []
        for aid in tqdm(range(naccounts * scale)):
            bid = (aid - 1) // naccounts + 1
            rows.append((aid, bid))

            if len(rows) == values_per_query:
                placeholders = ", ".join(["(%s::int, %s::int, 0, '')"] * len(rows))
                params = [x for row in rows for x in row]
                conn.execute(
                    f"INSERT INTO pgbench_accounts(aid, bid, abalance, filler) VALUES {placeholders}",
                    params,
                )
                rows.clear()

            if aid % batch == 0:
                pipeline.sync()

        if rows:
            placeholders = ", ".join(["(%s::int, %s::int, 0, '')"] * len(rows))
            params = [x for row in rows for x in row]
            conn.execute(
                f"INSERT INTO pgbench_accounts(aid, bid, abalance, filler) VALUES {placeholders}",
                params,
            )
            rows.clear()

        pipeline.sync()

    print("Initialization complete!")


if __name__ == "__main__":
    main()

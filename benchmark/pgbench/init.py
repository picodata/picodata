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
    parser.add_argument(
        "connection", type=str, help="Connection string for the pgproto server"
    )
    parser.add_argument(
        "-s", "--scale", type=int, default=1, help="Scaling factor for the benchmark (default: 1)"
    )
    return parser.parse_args()

def main():
    # Parse command-line arguments
    args = parse_args()

    # Connect to the database
    conn = psycopg.connect(args.connection, autocommit=True)

    # Create tables
    print("Creating tables...")

    # Drop existing tables if they exist
    conn.execute("DROP TABLE IF EXISTS pgbench_branches;")
    conn.execute("DROP TABLE IF EXISTS pgbench_tellers;")
    conn.execute("DROP TABLE IF EXISTS pgbench_accounts;")
    conn.execute("DROP TABLE IF EXISTS pgbench_history;")

    # Create new tables following the structure from initCreateTables*.
    # * https://github.com/postgres/postgres/blob/db22b900244d3c51918acce44cbe5bb6f6507d32/src/bin/pgbench/pgbench.c#L4809
    conn.execute("CREATE TABLE pgbench_branches (bid int PRIMARY KEY, bbalance int, filler varchar(88));")
    conn.execute("CREATE TABLE pgbench_tellers (tid int PRIMARY KEY, bid int, tbalance int, filler varchar(84));")
    conn.execute("CREATE TABLE pgbench_accounts (aid int PRIMARY KEY, bid int, abalance int, filler varchar(84));")
    conn.execute("CREATE TABLE pgbench_history (tid int, bid int, aid int, delta int, mtime int, filler varchar(22), PRIMARY KEY(tid, bid, aid, delta));")

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
        conn.execute("INSERT INTO pgbench_tellers(tid, bid, tbalance) VALUES (%(tid)s::int, (%(tid)s::int - 1) / %(ntellers)s::int + 1, 0)",
                     {"tid": tid, "ntellers": ntellers})

    # Populate pgbench_accounts table
    print("Populating pgbench_accounts...")
    batch = 1000
    with conn.pipeline() as pipeline:
        for aid in tqdm(range(naccounts * scale)):
            conn.execute("INSERT INTO pgbench_accounts(aid, bid, abalance, filler) VALUES (%(aid)s::int, (%(aid)s::int - 1) / %(naccounts)s::int + 1, 0, '')",
                         {"aid": aid, "naccounts": naccounts})
            if aid % batch == 0:
                pipeline.sync()

    print("Initialization complete!")

if __name__ == "__main__":
    main()


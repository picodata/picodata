#!/usr/bin/env python

"""
Initialize the tables for every block benchmark in this directory — a single,
self-contained init.

Usage:
    python init.py "postgres://postgres:Passw0rd@127.0.0.1:4327?sslmode=disable" -s 10
"""

import argparse

import psycopg
from tqdm import tqdm

NACCOUNTS = 100000
CHECKING_BALANCE = 1000000
SAVINGS_BALANCE = 0


def parse_args():
    parser = argparse.ArgumentParser(
        description="Initialize tables for the transactional-block benchmarks with Picodata."
    )
    parser.add_argument("connection", type=str, help="Connection string for the pgproto server")
    parser.add_argument("-s", "--scale", type=int, default=1, help="Scaling factor (default: 1)")
    return parser.parse_args()


def populate(conn, table, balance, nusers):
    """Batch-insert `nusers` rows into a (user_id, balance) table."""
    print(f"Populating {table}...")
    values_per_query = 64
    sync_every = 1000

    with conn.pipeline() as pipeline:
        user_ids = []
        for user_id in tqdm(range(1, nusers + 1)):
            user_ids.append(user_id)

            if len(user_ids) == values_per_query:
                placeholders = ", ".join([f"(%s::int, {balance})"] * len(user_ids))
                conn.execute(f"INSERT INTO {table}(user_id, balance) VALUES {placeholders}", user_ids)
                user_ids.clear()

            if user_id % sync_every == 0:
                pipeline.sync()

        if user_ids:
            placeholders = ", ".join([f"(%s::int, {balance})"] * len(user_ids))
            conn.execute(f"INSERT INTO {table}(user_id, balance) VALUES {placeholders}", user_ids)

        pipeline.sync()


def main():
    args = parse_args()
    conn = psycopg.connect(args.connection, autocommit=True)
    nusers = NACCOUNTS * args.scale

    print("Creating tables...")
    conn.execute("DROP TABLE IF EXISTS checking;")
    conn.execute("DROP TABLE IF EXISTS savings;")
    # Default distribution shards by the primary key (user_id).
    conn.execute("CREATE TABLE checking (user_id int PRIMARY KEY, balance int);")
    conn.execute("CREATE TABLE savings (user_id int PRIMARY KEY, balance int);")

    populate(conn, "checking", CHECKING_BALANCE, nusers)
    populate(conn, "savings", SAVINGS_BALANCE, nusers)

    # ledger: append target for the insert benchmark (starts empty).
    # counter: single hot row for the upsert benchmark.
    conn.execute("DROP TABLE IF EXISTS ledger;")
    conn.execute("DROP TABLE IF EXISTS counter;")
    conn.execute("CREATE TABLE ledger (id int PRIMARY KEY, user_id int, amount int);")
    conn.execute("CREATE TABLE counter (id int PRIMARY KEY, value int);")
    conn.execute("INSERT INTO counter VALUES (1, 0);")

    print("Initialization complete!")


if __name__ == "__main__":
    main()

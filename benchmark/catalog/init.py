#!/usr/bin/env python

"""
Create the tables for the catalog size benchmark: `t0` with one row and empty
filler tables up to `--tables` in total. Tables that already exist are kept,
so running it again with a larger `--tables` only adds the missing ones.

Usage:
    python init.py "postgres://postgres:Passw0rd@127.0.0.1:4327?sslmode=disable" --tables 10000
"""

import argparse

import psycopg
from tqdm import tqdm


def parse_args():
    parser = argparse.ArgumentParser(description="Initialize tables for the catalog size benchmark with Picodata.")
    parser.add_argument("connection", type=str, help="Connection string for the pgproto server")
    parser.add_argument("-t", "--tables", type=int, default=100, help="Total number of tables (default: 100)")
    return parser.parse_args()


def main():
    args = parse_args()
    conn = psycopg.connect(args.connection, autocommit=True)

    conn.execute("CREATE TABLE IF NOT EXISTS t0 (id int PRIMARY KEY, v int);")
    conn.execute("INSERT INTO t0 VALUES (1, 1) ON CONFLICT DO NOTHING;")

    print(f"Creating filler tables up to {args.tables} in total...")
    for i in tqdm(range(1, args.tables)):
        conn.execute(f"CREATE TABLE IF NOT EXISTS filler_{i} (id int PRIMARY KEY);")

    print("Initialization complete!")


if __name__ == "__main__":
    main()

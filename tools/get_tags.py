#!/usr/bin/env python3

import argparse
import subprocess
import pathlib
import os
import time

import logging

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)

GET_SOURCES_ATTEMPTS = int(os.environ.get("GET_SOURCES_ATTEMPTS", 3))
PROJECT_DIR = pathlib.Path(__file__).parent.parent

DEFAULT_FETCH_DIRS = ["tarantool-sys", "tarantool-sys/third_party/luajit"]

DEEPEN_LIMIT = 1000


def get_fetch_dirs():
    fetch_dirs = os.environ.get("GIT_FETCH_TAGS_DIRS")
    if fetch_dirs is not None:
        return fetch_dirs.split(",")

    return DEFAULT_FETCH_DIRS


def run(cmd: str, cwd_in_project_dir: pathlib.Path):
    logging.info(f"Running: '{cmd}' in '{cwd_in_project_dir}'")
    completed = subprocess.run(cmd, shell=True, executable="/bin/bash", text=True, cwd=PROJECT_DIR / cwd_in_project_dir)
    logging.info(f"Exitcode: {completed.returncode}")
    return completed


def deepen(path: pathlib.Path):
    for i in range(GET_SOURCES_ATTEMPTS):
        completed = run("git fetch --deepen 50", path)
        if completed.returncode == 0:
            return

        logging.info(f"Sleeping for {2**i}s")
        time.sleep(2**i)

    raise Exception(f"out of attempts for git fetch on {path}")


def ensure_describe(path: pathlib.Path):
    for _ in range(DEEPEN_LIMIT):
        completed = run("git describe", path)
        if completed.returncode == 0:
            return

        logging.info("Describe failed, fetching more")

        deepen(path)

    raise Exception(f"cant fetch tag on {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="GetGitTags", description="Fetch enough commits for specified paths for git describe to return proper tag"
    )
    parser.add_argument("dirs", nargs="*", default=get_fetch_dirs(), type=str)
    args = parser.parse_args()

    for path in args.dirs:
        t0 = time.time()
        ensure_describe(path)
        logging.info(f"Completed tag fetching for '{path}'. Took: {time.time() - t0:.2f}s")

#!/usr/bin/env python3

import argparse
import subprocess
import pathlib
import os
from time import sleep


GET_SOURCES_ATTEMPTS = int(os.environ.get('GET_SOURCES_ATTEMPTS', 3))
PROJECT_DIR = pathlib.Path(__file__).parent.parent


def run_shell(path, shell=True, executable='/bin/bash', text=True):
    retry = GET_SOURCES_ATTEMPTS
    timeout = 3
    while retry > 0:
        try:
            while True:
                result = ""
                proc = subprocess.run("git describe",
                                      shell=shell, executable=executable, text=text,
                                      cwd="{}/{}".format(PROJECT_DIR, path))
                result = proc.stdout
                code = proc.returncode
                if not code:
                    return

                print("fetching tag for", path)
                subprocess.run(
                    "git fetch --deepen 50",
                    shell=shell,
                    executable=executable,
                    text=text,
                    cwd="{}/{}".format(PROJECT_DIR, path),
                )
        except Exception as e:
            print("can't run: " + str(e))
            retry -= 1
            sleep(timeout)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="GetGitTags", description="Get project tags")
    parser.add_argument("dirs", nargs="*", default=".", type=str)
    args = parser.parse_args()

    for path in args.dirs:
        run_shell(path)

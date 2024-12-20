from conftest import Cluster, Compatibility, PortDistributor
from pathlib import Path

import conftest
import os
import shutil
import sys
import time


def check_start_dir():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    current_dir = os.getcwd()
    not_in_test_directory = script_dir != current_dir

    if not_in_test_directory:
        hint_path = os.path.relpath(script_dir, current_dir)
        print(
            f"Run this script from a directory where it is located.\nHint: `cd {hint_path}/`"
        )
        sys.exit(-1)


if __name__ == "__main__":
    check_start_dir()

    conftest.cargo_build()
    port_distributor = PortDistributor(1333, 1344)
    tmpdir = Path("./tmp_generate_snapshot").resolve()
    tmpdir.mkdir(exist_ok=True)

    try:
        cluster = Cluster(
            binary_path=conftest.binary_path(),
            id="cluster_to_gen_snap",
            instance_dir=str(tmpdir),
            base_host="localhost",
            port_distributor=port_distributor,
        )
        inst = cluster.add_instance()
        inst.fill_with_data()

        parent_dir = Path(os.path.dirname(os.getcwd()))
        compat = Compatibility(root_path=parent_dir)
        version, path = compat.current_tag()

        snapshot = inst.latest_snapshot(path)
        assert (
            snapshot
        ), f'Should\'ve found the snapshot at "{path}". Current version is {version}. Something unexpected happened!'  # noqa: E501

        cluster.terminate()
        print("A short wait to ensure the instance has completed successfully...")
        time.sleep(3)

        copy_path = f"{os.getcwd()}/compat/{str(version)}"
        shutil.copy2(snapshot, copy_path)
        print(
            f'Successfully generated snapshot of version {version}, copied into "{copy_path}".'
        )
    finally:
        shutil.rmtree(tmpdir)

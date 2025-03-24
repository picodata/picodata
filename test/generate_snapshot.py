from conftest import Cluster, Compatibility, PortDistributor
from pathlib import Path

import conftest
import os
import shutil
import time


def check_start_dir():
    script_dir = Path(os.path.abspath(__file__)).parent.parent.absolute()
    if script_dir != Path(os.getcwd()):
        raise ValueError("run this script from a root of a project")


if __name__ == "__main__":
    check_start_dir()

    conftest.cargo_build()
    port_distributor = PortDistributor(3301, 3303)  # default pgproto port
    tmpdir = Path("./tmp_generate_snapshot").resolve()
    tmpdir.mkdir(exist_ok=True)

    try:
        cluster = Cluster(
            binary_path=conftest.binary_path(),
            id="demo",  # default cluster name
            data_dir=str(tmpdir),
            base_host="127.0.0.1",  # default advertise
            port_distributor=port_distributor,
        )
        inst = cluster.add_instance()
        inst.fill_with_data()

        compat = Compatibility()
        version = compat.current_tag

        snapshot = inst.latest_snapshot()
        if not snapshot:
            raise ValueError(f'should have found the snapshot at "{inst.instance_dir}"')

        cluster.terminate()
        print("A short wait to ensure the instance had finished successfully...")
        time.sleep(2)

        copy_path = compat.version_to_dir_path(version)
        os.makedirs(copy_path, exist_ok=True)
        shutil.copy2(snapshot, copy_path)
        print(f'Successfully generated snapshot of version {version}, copied into "{copy_path}".')
    finally:
        shutil.rmtree(tmpdir)

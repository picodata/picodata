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
            base_host=conftest.BASE_HOST,
            port_distributor=port_distributor,
        )
        inst = cluster.add_instance()
        inst.fill_with_data()

        cluster.terminate()
        print("A short wait to ensure the instance had finished successfully...")
        time.sleep(2)

        compat = Compatibility()
        version = compat.current_tag

        src_dir = Path(inst.instance_dir)
        dest_dir = compat.version_to_dir_path(version)
        conftest.copy_dir(src_dir, dest_dir)
        print(f'Success! Auto-generated snapshot of {version} copied into "{dest_dir}".')
    finally:
        shutil.rmtree(tmpdir)

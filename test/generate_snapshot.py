from conftest import Cluster, Compatibility, PortDistributor
from pathlib import Path

import conftest
import shutil


if __name__ == "__main__":
    conftest.cargo_build()
    port_distributor = PortDistributor(1333, 1344)
    tmpdir = Path("./tmp_generate_snapshot").resolve()
    tmpdir.mkdir(exist_ok=True)
    try:
        cluster = Cluster(
            binary_path=conftest.binary_path(),
            id="cluster_to_gen_snap",
            data_dir=str(tmpdir),
            base_host="localhost",
            port_distributor=port_distributor,
        )
        inst = cluster.add_instance()

        compat = Compatibility()
        compat.fetch_current_tag()
        compat.fill_snapshot_with_data(inst)
        compat.copy_latest_snapshot(inst)

        print(
            f"Succesfully generated snapshot of {compat.tag} and copied into {compat.path}"
        )
        cluster.kill()
    finally:
        shutil.rmtree(tmpdir)

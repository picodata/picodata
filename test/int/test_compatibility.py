import pytest
import os
import shutil


from conftest import Cluster, Compatibility


@pytest.mark.xfail
def test_upgrade(cluster: Cluster):
    inst = cluster.add_instance(wait_online=False)
    compat = Compatibility()

    os.makedirs(inst.data_dir, exist_ok=True)
    compat.fetch_previous_tag()

    print(f"snapshot tag: {compat._tag}")
    snap = compat.get_snapshot_path()
    err = "Snapshot of the previous MAJOR version was not found. Generate one using `make generate`."  # noqa: E501
    assert snap, err
    shutil.copy(snap, f"{inst.data_dir}/")

    inst.start()
    inst.wait_online()

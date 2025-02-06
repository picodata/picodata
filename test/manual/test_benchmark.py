import os
import pytest
import subprocess

from conftest import Connection, Instance, MalformedAPI, pgrep_tree, pid_alive
from dataclasses import field
from datetime import datetime
from functools import wraps
from multiprocessing.pool import ThreadPool
from prettytable import PrettyTable


USER_NAME = "kelthuzad"
USER_PASS = "g$$dP4ss"
TABLE_NAME = "warehouse"


def flame(f, duration, instance, tmpdir):
    @wraps(f)
    def wrapper():
        flamegraph = Flamegraph(
            name=f"{f.__name__}",
            watchpid=child_pid(instance),
            duration=duration,
            tmp_dir=tmpdir,
            out_dir="tmp/flamegraph",
        )

        flamegraph.start()
        f()
        flamegraph.wait()
        flamegraph.gather()

    return wrapper


def bench(f, duration, instance, capsys):
    @wraps(f)
    def wrapper():
        before = datetime.now()
        f()
        after = datetime.now()
        delta = after - before
        return delta

    return wrapper


def test_benchmark_iproto_execute(instance: Instance, tmpdir, capsys, with_flamegraph):
    # "f" means only on flamegraph creation, "b" only on benchmarking
    # 1) create table
    # 2) fill the table with the data
    # 3) create user
    # 4) grant rights for the table to the user
    # 5) connect as a created user to an instance
    # b) instantiate an output
    # 6) run select only tests:
    #      f) create flamegraph for each
    #      b) calculate execution times for each
    #      b) append calculated times to output
    # b) print output to the user

    dcl = instance.sql(
        f"""
        CREATE USER {USER_NAME} WITH PASSWORD '{USER_PASS}' USING chap-sha1
    """
    )
    assert dcl["row_count"] == 1
    conn = Connection(
        instance.host,
        instance.port,
        user=USER_NAME,
        password=USER_PASS,
        connect_now=True,
        reconnect_max_attempts=0,
    )
    assert conn

    row_count = 5000

    # https://docs.picodata.io/picodata/stable/reference/legend/#create_test_tables
    ddl = instance.sql(
        """
        CREATE TABLE warehouse (
            id INTEGER NOT NULL,
            item TEXT NOT NULL,
            type TEXT NOT NULL,
            PRIMARY KEY (id))
        USING memtx DISTRIBUTED BY (id)
        OPTION (TIMEOUT = 3.0)
    """
    )
    assert ddl["row_count"] == 1
    acl = instance.sql(f"GRANT READ ON TABLE warehouse TO {USER_NAME}")
    assert acl["row_count"] == 1
    acl = instance.sql(f"GRANT WRITE ON TABLE warehouse TO {USER_NAME}")
    assert acl["row_count"] == 1
    for i in range(row_count):
        instance.sql(f"INSERT INTO warehouse VALUES ({i}, 'Sargeras', 'Deathwing')")

    # https://docs.picodata.io/picodata/stable/reference/legend/#create_test_tables
    ddl = instance.sql(
        """
        CREATE TABLE items (
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            PRIMARY KEY (id))
        USING memtx DISTRIBUTED BY (id)
        OPTION (TIMEOUT = 3.0)
    """
    )
    assert ddl["row_count"] == 1
    acl = instance.sql(f"GRANT READ ON TABLE items TO {USER_NAME}")
    assert acl["row_count"] == 1
    acl = instance.sql(f"GRANT WRITE ON TABLE items TO {USER_NAME}")
    assert acl["row_count"] == 1
    for i in range(row_count):
        instance.sql(f"INSERT INTO items VALUES ({i}, 'Kiljaeden')")

    if not with_flamegraph:
        table = PrettyTable()
        table.field_names = ["case", ".proc_sql_dispatch", "IPROTO_EXECUTE", "diff"]
        table.align = "c"

    select_amount = 500  # we have got a lot of rows already
    select_query = "SELECT * FROM warehouse"
    select_duration = 20 * 2  # two tests by 20 seconds

    def sql_select():
        for i in range(select_amount):
            conn.call(".proc_sql_dispatch", select_query, [])

    def execute_select():
        for i in range(select_amount):
            with pytest.raises(MalformedAPI) as _:
                conn.execute(select_query)

    if with_flamegraph:
        flame(sql_select, select_duration, instance, tmpdir)()
        flame(execute_select, select_duration, instance, tmpdir)()
    else:
        sql_select_time = bench(sql_select, select_duration, instance, capsys)()
        execute_select_time = bench(execute_select, select_duration, instance, capsys)()

        diff_select_time = execute_select_time - sql_select_time
        table.add_row(["select", sql_select_time, execute_select_time, diff_select_time])

    update_amount = 500  # we have got a lot of rows already
    update_query = """
        UPDATE warehouse SET item = name
        FROM (SELECT id AS i, name FROM items)
        WHERE id = i
    """
    update_duration = 20 * 2  # two tests by 20 seconds

    def sql_update():
        for i in range(update_amount):
            conn.call(".proc_sql_dispatch", update_query, [])

    def execute_update():
        for i in range(update_amount):
            # with pytest.raises(MalformedAPI) as _:
            conn.execute(update_query)

    if with_flamegraph:
        flame(sql_update, update_duration, instance, tmpdir)()
        flame(execute_update, update_duration, instance, tmpdir)()
    else:
        sql_update_time = bench(sql_update, update_duration, instance, capsys)()
        execute_update_time = bench(execute_update, update_duration, instance, capsys)()

        diff_update_time = execute_update_time - sql_update_time
        table.add_row(["update", sql_update_time, execute_update_time, diff_update_time])

    if not with_flamegraph:
        with capsys.disabled():
            print(f"\n{table}")


class Flamegraph:
    flamegraph_dir: str
    tmp_dir: str
    out_dir: str
    name: str
    watchpid: int
    duration: int
    process: subprocess.Popen | None = None
    env: dict[str, str] = field(default_factory=dict)

    def __init__(
        self,
        tmp_dir: str,
        out_dir: str,
        name: str,
        watchpid: int,
        duration: int,
    ):
        self.flamegraph_dir = os.environ.get("FLAMEGRAPH_DIR", "../FlameGraph")
        self.tmp_dir = tmp_dir
        self.out_dir = out_dir
        self.name = name
        self.watchpid = watchpid
        self.duration = duration

    @property
    def perf_data(self):
        return os.path.join(self.tmp_dir, f"{self.name}-perf.data")

    @property
    def perf_out(self):
        return os.path.join(self.tmp_dir, f"{self.name}-perf.out")

    @property
    def perf_folded(self):
        return os.path.join(self.tmp_dir, f"{self.name}-perf.folded")

    @property
    def perf_svg(self):
        return os.path.join(self.out_dir, f"{self.name}-perf.svg")

    @property
    def stackcollapse_perf_pl(self):
        return os.path.join(self.flamegraph_dir, "stackcollapse-perf.pl")

    @property
    def flamegraph_pl(self):
        return os.path.join(self.flamegraph_dir, "flamegraph.pl")

    @property
    def record_command(self):
        # fmt: off
        return [
            "perf", "record",
            "-p", f"{self.watchpid}",
            "-F", "99",
            "-a",
            "--call-graph", "dwarf",
            "-o", self.perf_data,
            "--",
            "sleep", f"{self.duration}",
        ]
        # fmt: on

    def start(self):
        os.makedirs(self.tmp_dir, exist_ok=True)
        os.makedirs(self.out_dir, exist_ok=True)
        self.process = subprocess.Popen(self.record_command)

    def wait(self):
        self.process.wait()

    def gather(self):
        subprocess.run(["perf", "script", "-i", self.perf_data], stdout=open(self.perf_out, "wb"))
        subprocess.run(
            [self.stackcollapse_perf_pl, self.perf_out],
            stdout=open(self.perf_folded, "wb"),
        )
        subprocess.run([self.flamegraph_pl, self.perf_folded], stdout=open(self.perf_svg, "wb"))

    def gather_multiple(flamegraphs):
        with ThreadPool() as pool:
            pool.map(lambda flamegraph: flamegraph.gather(), flamegraphs)


def child_pid(i: Instance):
    assert i.process
    assert i.process.pid
    assert pid_alive(i.process.pid)
    pids = pgrep_tree(i.process.pid)
    assert pid_alive(pids[1])
    return pids[1]

from conftest import Cluster, Instance
from perf import K6
import funcy  # type: ignore
import os


@funcy.retry(tries=30, timeout=0.2)
def apply_migration(i: Instance, n: int):
    assert i.call("pico.migrate", n) == n


def test_projection(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    # Create a sharded space and populate it with data.
    for n, sql in {
        1: """create table t(a int, "bucket_id" unsigned, primary key (a));""",
        2: """create index "bucket_id" on t ("bucket_id");""",
        3: """create table "_pico_space"("id" int, "distribution" text, primary key("id"));""",
    }.items():
        i1.call("pico.add_migration", n, sql)
    apply_migration(i1, 3)

    space_id = i1.eval("return box.space.T.id")
    sql = """insert into "_pico_space" values({id}, 'A');""".format(id=space_id)
    i1.call("pico.add_migration", 4, sql)
    apply_migration(i1, 4)

    row_number = 100
    for n in range(row_number):
        i1.sql("""insert into t values(?);""", n)

    assert i1.sql("""select count(a) from t;""")["rows"] == [[row_number]]
    assert i2.sql("""select count(a) from t;""")["rows"] == [[row_number]]

    # Init k6 script and run it.
    path = os.path.dirname(os.path.abspath(__file__))
    k6_script = """
        import tarantool from "k6/x/tarantool";
        import {{randomItem}} from "https://jslib.k6.io/k6-utils/1.1.0/index.js";
        import {{callTarantool}} from "{metrics}";

        const clients = [
            tarantool.connect(["{i1_host}:{i1_port}"], {{ "user": "{user}" }}),
            tarantool.connect(["{i2_host}:{i2_port}"], {{ "user": "{user}" }}),
        ]

        export let current_server = 0

        function get_client() {{
            let c = clients[current_server]
            current_server += 1
            if (current_server >= clients.length) {{
                current_server = 0
            }}
            return c
        }}

        let ids = Array.from(
            {{
                length: 1000000
            }},
            (_, id) => id
        )

        let pattern = `SELECT *
            FROM (
                SELECT a FROM t
                ) as "t1"
            WHERE a = ?`

        export default () => {{
            callTarantool(get_client(), "pico.sql", [pattern, [randomItem(ids)]]);
        }}
    """.format(
        metrics=os.path.join(path, "metrics.js"),
        user="guest",
        i1_host=i1.host,
        i1_port=i1.port,
        i2_host=i2.host,
        i2_port=i2.port,
    )
    k6 = K6(
        concurrency=10, duration="10s", name="projection", path=path, program=k6_script
    )
    k6.run()
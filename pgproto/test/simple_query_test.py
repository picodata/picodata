import pytest
from conftest import Cluster
import pg8000.dbapi as pg
import os
import time

def start_pg_server(instance, host, service):
    start_pg_server_lua_code = f"""
        package.cpath="{os.environ['LUA_CPATH']}"

        box.schema.func.create('tcpserver.server_start', {{ language = 'C' }})
        box.schema.user.grant('guest', 'execute', 'function', 'tcpserver.server_start')

        box.func['tcpserver.server_start']:call({{ '{host}', '{service}' }})
    """
    instance.eval(start_pg_server_lua_code)

def stop_pg_server(instance):
    stop_pg_server_lua_code = f"""
        box.schema.func.create('tcpserver.server_stop', {{language = 'C'}})
        box.schema.user.grant('guest', 'execute', 'function', 'tcpserver.server_stop')

        box.func['tcpserver.server_stop']:call()

        box.schema.func.drop('tcpserver.server_start')
        box.schema.func.drop('tcpserver.server_stop')
    """
    instance.eval(stop_pg_server_lua_code)


def test_simple_query_flow_errors(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    host = '127.0.0.1'
    service = '35776'
    start_pg_server(i1, host, service)

    user = 'admin'
    password = 'fANPIOUWEh79p12hdunqwADI'
    i1.eval("box.cfg{auth_type='md5'}")
    i1.eval(f"box.schema.user.passwd('{user}', '{password}')")

    with pytest.raises(pg.InterfaceError, match="Server refuses SSL"):
        pg.Connection(user, password=password, host=host, port=int(service), ssl_context=True)

    os.environ['PGSSLMODE'] = 'disable'
    conn = pg.Connection(user, password=password, host=host, port=int(service))
    conn.autocommit = True
    cur = conn.cursor()

    with pytest.raises(pg.DatabaseError, match="expected CreateTable, DropTable, Explain, or Query"):
        cur.execute("""
            CREATE TEMPORARY TABLE book (id SERIAL, title TEXT);
        """)

    with pytest.raises(pg.DatabaseError, match="space BOOK not found"):
        cur.execute("""
            INSERT INTO book VALUES (1, 2);
        """)

def test_simple_flow_session(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    host = '127.0.0.1'
    service = '5432'
    start_pg_server(i1, host, service)

    user = 'admin'
    password = 'password'
    i1.eval("box.cfg{auth_type='md5', log_level=7}")
    i1.eval(f"box.schema.user.passwd('{user}', '{password}')")

    os.environ['PGSSLMODE'] = 'disable'
    conn = pg.Connection(user, password=password, host=host, port=int(service))
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        create table "tall" (
            "id" integer not null,
            "str" string,
            "bool" boolean,
            "real" double,
            primary key ("id")
        )
        using memtx distributed by ("id")
        option (timeout = 3);
    """)

    cur.execute("""
        INSERT INTO "tall" VALUES
            (1, 'one', true, CAST(0.1 AS DOUBLE)),
            (2, 'to', false, CAST(0.2 AS DOUBLE)),
            (4, 'for', true, CAST(0.4 AS DOUBLE));
    """)

    cur.execute("""
        SELECT * FROM "tall";
    """)

    tuples = cur.fetchall()
    assert [1, 'one', True, 0.1] in tuples
    assert [2, 'to', False, 0.2] in tuples
    assert [4, 'for', True, 0.4] in tuples

    stop_pg_server(i1)

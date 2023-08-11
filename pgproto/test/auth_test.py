import pytest
from conftest import Cluster
import pg8000.dbapi as pg
import os

def start_pg_server(instance, host, service):
    start_pg_server_lua_code = f"""
        package.cpath="{os.environ['LUA_CPATH']}"
        net_box = require('net.box')
        box.schema.func.create('pgproto.server_start', {{language = 'C'}})
        box.schema.user.grant('guest', 'execute', 'function', 'pgproto.server_start')

        box.cfg{{listen=3301}}
        caller = net_box:new(3301)
        caller:call('pgproto.server_start', {{ '{host}', '{service}' }})
    """
    instance.eval(start_pg_server_lua_code)

def stop_pg_server(instance):
    stop_pg_server_lua_code = f"""
        local net_box = require('net.box')
        box.schema.func.create('pgproto.server_stop', {{language = 'C'}})
        box.schema.user.grant('guest', 'execute', 'function', 'pgproto.server_stop')

        box.cfg{{listen=3301}}
        local caller = net_box:new(3301)
        caller:call('pgproto.server_stop')

        box.schema.func.drop('pgproto.server_start')
        box.schema.func.drop('pgproto.server_stop')
    """
    instance.eval(stop_pg_server_lua_code)

def test_auth(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    host = '127.0.0.1'
    service = '36118'
    start_pg_server(i1, host, service)

    user = 'user'
    password = 'fANPIOUWEh79p12hdunqwADI'
    i1.eval("box.cfg{auth_type='md5'}")
    i1.call("pico.create_user", user, password, dict(timeout=3))

    # test successful authentication
    conn = pg.Connection(user, password=password, host=host, port=int(service))
    conn.close()

    # test authentication with a wrong password
    with pytest.raises(pg.DatabaseError, match=f"md5 authentication failed for user '{user}'"):
        pg.Connection(user, password='wrong password', host=host, port=int(service))

    # test authentication with an unknown user
    with pytest.raises(pg.DatabaseError, match=f"authentication failed for user 'unknown-user'"):
        pg.Connection("unknown-user", password='aaa', host=host, port=int(service))

    sha_user = 'chap-sha-enjoyer'
    sha_password = '231321fnijphui217h08'
    i1.eval("box.cfg{auth_type='chap-sha1'}")
    i1.call("pico.create_user", sha_user, sha_password, dict(timeout=3))

    # test authentication with an unsupported method
    with pytest.raises(pg.DatabaseError, match=f"authentication failed for user '{sha_user}'"):
        pg.Connection(sha_user, password='aaa', host=host, port=int(service))

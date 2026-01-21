import os
import subprocess
import sys

import pexpect  # type: ignore
import pytest
from conftest import (
    CLI_TIMEOUT,
    Cluster,
    Instance,
    assert_starts_with,
    log_crawler,
)
from tarantool.error import (  # type: ignore
    NetworkError,
)
from test_plugin import _PLUGIN, _PLUGIN_VERSION_1, PluginReflection
from typing import Any
from typing import List
from typing import Optional


def test_connect_ux(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()
    i1.create_user(with_name="andy", with_password="Testpa55")
    i1.sql('GRANT CREATE TABLE TO "andy"', sudo=True)

    cli = pexpect.spawn(
        command=i1.runtime.command,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "andy"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for andy: ")
    cli.sendline("Testpa55")

    cli.expect_exact(f'Connected to interactive console by address "{i1.host}:{i1.port}" under "andy" user')
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("sql> ")

    # sql console doesn't know about language switching
    cli.sendline("\\lua")
    cli.expect_exact("Language cannot be changed in this console")

    cli.sendline("\\sql")
    cli.expect_exact("Language cannot be changed in this console")

    # for not registried command nothing can happen
    cli.sendline("\\lya")
    cli.expect_exact("Unknown special sequence")
    cli.sendline("\\scl")
    cli.expect_exact("Unknown special sequence")
    cli.sendline("\\set language lua")
    cli.expect_exact("Language cannot be changed in this console")

    # nothing happens for completion
    cli.sendline("\t\t")
    cli.expect_exact("sql> ")

    # ensure that server responds on correct query
    cli.sendline("CREATE TABLE ids (id INTEGER NOT NULL, PRIMARY KEY(id)) USING MEMTX DISTRIBUTED BY (id);")
    cli.expect_exact("1")
    cli.expect_exact("sql> ")

    # ensure that server responds on invalid query
    cli.sendline("invalid query;")
    cli.expect_exact("rule parsing error")
    cli.expect_exact("sql> ")

    # ensure that server responds after processing invalid query
    cli.sendline("INSERT INTO ids VALUES(1);")
    cli.expect_exact("1")
    cli.expect_exact("sql> ")

    cli.sendline("SELECT * FROM ids;")
    cli.expect_exact("+----+")
    cli.expect_exact("| id |")
    cli.expect_exact("+====+")
    cli.expect_exact("| 1  |")
    cli.expect_exact("+----+")
    cli.expect_exact("(1 rows)")
    cli.expect_exact("sql> ")

    cli.sendline("EXPLAIN SELECT * FROM ids;")
    cli.expect_exact('projection ("ids"."id"::int -> "id")')
    cli.expect_exact('scan "ids"')
    cli.expect_exact("execution options:")
    cli.expect_exact("sql_vdbe_opcode_max = 45000")
    cli.expect_exact("sql_motion_row_max = 5000")
    cli.expect_exact("buckets = [1-3000]")

    # hitting enter sends query to the server
    cli.sendline("")
    cli.expect_exact("0")


def test_admin_ux(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    cli = pexpect.spawn(
        cwd=i1.instance_dir,
        command=i1.runtime.command,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )

    cli.logfile = sys.stdout

    cli.expect_exact('Connected to admin console by socket path "./admin.sock"')
    cli.expect_exact("type '\\help' for interactive help")

    # in admin console language switching is availiable
    cli.sendline("\\lua")
    cli.expect_exact("Language switched to lua")
    cli.expect_exact("(admin) lua> ")

    # Lua does not require delimiter
    cli.sendline("box.session.user()")
    cli.expect_exact("admin")

    cli.expect_exact("(admin) lua> ")
    # Enter a command with the default delimiter
    cli.sendline("box.session.user();")
    cli.expect_exact("admin")

    cli.expect_exact("(admin) lua> ")
    # Press the up arrow key to access the command history
    cli.sendline("\033[A")
    # Command is retrieved from the history with the delimiter
    cli.expect_exact("box.session.user();")

    cli.expect_exact("(admin) lua> ")
    cli.sendline("\\sql")
    cli.expect_exact("Language switched to sql")
    cli.expect_exact("(admin) sql> ")

    # for not registried command nothing happend
    cli.sendline("\\lya")
    cli.expect_exact("Unknown special sequence")
    cli.sendline("\\scl")
    cli.expect_exact("Unknown special sequence")

    cli.expect_exact("(admin) sql> ")
    # variations of `\s l sql/lua` is registred, but not in help
    cli.sendline("\\set language lua")
    cli.expect_exact("Language switched to lua")
    cli.expect_exact("(admin) lua> ")

    cli.sendline("\\s lang sql")
    cli.expect_exact("Language switched to sql")
    cli.expect_exact("(admin) sql> ")

    # nothing happens on completion in SQL mode
    cli.sendline("\\sql")
    cli.expect_exact("Language switched to sql")
    cli.sendline("\t\t")
    cli.expect_exact("(admin) sql> ")

    cli.sendline("box.c\t\t;")
    cli.expect_exact("rule parsing error:")

    cli.expect_exact("(admin) sql> ")
    # something happens on completion in Lua mode
    cli.sendline("\\lua")
    cli.expect_exact("Language switched to lua")

    cli.expect_exact("(admin) lua> ")
    cli.sendline("hel\t")
    cli.expect_exact("(admin) lua> help")


def check_plugin_schema(
    instance: Instance,
    username: str,
    expected_output: str,
    service_names: str,
    plugin_name: str = _PLUGIN,
    plugin_version: str = _PLUGIN_VERSION_1,
    plugin_config: Optional[str] = None,
    expected_values: Optional[List[Any]] = None,
    must_error: bool = False,
):
    config_file = instance.instance_dir / "config.yml"
    if plugin_config is not None:
        config_file.write_text(plugin_config)

    credentials = f"{username}@{instance.iproto_listen}"
    # fmt: off
    command = [
        instance.runtime.command, "plugin", "configure",
        "--peer", credentials,
        plugin_name, plugin_version, f"{config_file}",
        "--service-password-file", f"{instance.service_password_file}",
        "--service-names", service_names,
    ]
    # fmt: on
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, stderr = process.communicate()

    actual_code = process.returncode
    expected_code = int(must_error)
    if (expected_code == 0) != (actual_code == 0):
        error = f"{command} return code mismatch"
        hint = f"expected: {expected_code}, got: {actual_code}"
        message = f"{error}, {hint}"
        raise ValueError(message)

    output_stream = stderr if must_error else stdout
    actual_output = output_stream.strip()
    if expected_output not in actual_output:
        error = f"{command} output message mismatch"
        hint = f"expected: '{expected_output}', got: '{actual_output}'"
        message = f"{error}, {hint}"
        raise ValueError(message)

    if not must_error and expected_values is not None:
        actual_values = instance.sql("SELECT * FROM _pico_plugin_config")
        if actual_values != expected_values:
            error = f"{command} output values mismatch"
            hint = f"expected: {expected_values}, got: {actual_values}"
            message = f"{error}, {hint}"
            raise ValueError(message)


def test_plugin_ux(cluster: Cluster):
    inst = cluster.add_instance(wait_online=False)

    service_password = "T0psecret"
    inst.env["PICODATA_ADMIN_PASSWORD"] = service_password
    inst.set_service_password(service_password)
    assert inst.service_password_file

    inst.start_and_wait()

    plugin_ref = PluginReflection.default(inst)
    inst.call("pico.install_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    plugin_ref.install(True).enable(False)

    # test as `pico_service` system user on
    # list of services with multiple elements:
    # testservice_1.bar = 101 -> 42
    # testservice_1.foo = true -> true, shouldn't be even altered initially
    # testservice_1.baz = ["one", "two", "three"] -> ["cool", "nice"], multiple elements
    # testservice_2.foo = 0 -> 13, check more than a one service
    check_plugin_schema(
        instance=inst,
        username="pico_service",
        service_names="testservice_1,testservice_2",
        plugin_config="""\
testservice_1:
    bar: 42
    foo: true
    baz: ["cool", "nice"]

testservice_2:
    foo: 13

unknown_service:
    unknown_field: "unknown_value"
""",
        expected_values=[
            ["testplug", "0.1.0", "testservice_1", "bar", 42],
            [
                "testplug",
                "0.1.0",
                "testservice_1",
                "baz",
                ["cool", "nice"],
            ],
            ["testplug", "0.1.0", "testservice_1", "foo", True],
            ["testplug", "0.1.0", "testservice_2", "foo", 13],
        ],
        expected_output=f"new configuration for plugin '{_PLUGIN}' successfully applied",
    )

    # test as `admin` priviliged user on
    # list of services with a single element:
    # testservice_1.baz = ["cool", "baz"] -> ["sindragosa"]
    check_plugin_schema(
        instance=inst,
        username="admin",
        service_names="testservice_1",
        plugin_config="""\
testservice_1:
    baz: ["sindragosa"]
""",
        expected_values=[
            ["testplug", "0.1.0", "testservice_1", "bar", 42],
            ["testplug", "0.1.0", "testservice_1", "baz", ["sindragosa"]],
            ["testplug", "0.1.0", "testservice_1", "foo", True],
            ["testplug", "0.1.0", "testservice_2", "foo", 13],
        ],
        expected_output=f"new configuration for plugin '{_PLUGIN}' successfully applied",
    )

    # test as `admin` priviliged user on
    # list of services with a single element:
    # testservice_1.baz = ["sindragosa"] -> ["sindragosa"]
    check_plugin_schema(
        instance=inst,
        username="admin",
        service_names="testservice_1",
        plugin_config="""\
testservice_1:
    baz: ["sindragosa"]
""",
        expected_values=[
            ["testplug", "0.1.0", "testservice_1", "bar", 42],
            ["testplug", "0.1.0", "testservice_1", "baz", ["sindragosa"]],
            ["testplug", "0.1.0", "testservice_1", "foo", True],
            ["testplug", "0.1.0", "testservice_2", "foo", 13],
        ],
        expected_output="no values to update",
    )

    # test as `admin` priviliged user
    # on non-existing plugin:
    check_plugin_schema(
        instance=inst,
        username="admin",
        plugin_name="unknown_plugin",
        service_names="unknown_service",
        expected_output="no such plugin unknown_plugin:0.1.0",
        must_error=True,
    )

    # test as `admin` priviliged user
    # on non-existing service:
    check_plugin_schema(
        instance=inst,
        username="admin",
        service_names="unknown_service",
        plugin_config="""\
unknown_service:
    unknown_key: "unknown_value"
""",
        expected_output=f"no such service {_PLUGIN}.unknown_service",
        must_error=True,
    )

    username = "somebody"

    res = inst.sql(f"CREATE USER {username} WITH PASSWORD '{service_password}' USING MD5", sudo=True)
    assert isinstance(res, dict)
    assert res["row_count"] == 1

    # test as `somebody` unpriviliged user on
    # list of services with multiple elements
    # which should error:
    # testservice_1.bar = 42 -> 101
    # testservice_1.baz = ["sindragosa"] -> ["one", "two", "three"]
    # testservice_2.foo = 13 -> 0
    check_plugin_schema(
        instance=inst,
        username=username,
        expected_values=[
            ["testplug", "0.1.0", "testservice_1", "bar", 101],
            ["testplug", "0.1.0", "testservice_1", "baz", ["one", "two", "three"]],
            ["testplug", "0.1.0", "testservice_1", "foo", True],
            ["testplug", "0.1.0", "testservice_2", "foo", 0],
        ],
        service_names="testservice_1,testservice_2",
        plugin_config="""\
testservice_1:
    bar: 101
    foo: true
    baz: ["one", "two", "three"]

testservice_2:
    foo: 0
""",
        expected_output=f"Read access to space '_pico_plugin' is denied for user '{username}'",
        must_error=True,
    )


def test_lua_completion(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    cli = pexpect.spawn(
        cwd=i1.instance_dir,
        command=i1.runtime.command,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.sendline("\\lua")
    cli.expect_exact("(admin) lua> ")

    # With several possible variants they are shown as list
    cli.send("to")
    cli.send("\t\t")
    cli.expect_exact("tostring(    tonumber(    tonumber64(")
    cli.sendcontrol("c")

    cli.send("box.c")
    cli.send("\t\t")
    cli.expect_exact("box.ctl      box.cfg      box.commit(")
    cli.sendcontrol("c")

    cli.send("tonumber(to")
    cli.send("\t\t")
    cli.expect_exact("tostring(    tonumber(    tonumber64(")
    cli.sendcontrol("c")

    # With one possible variant it automaticaly completes current word
    # so we can check that is completed by result of completing this command
    cli.send("hel")
    cli.send("\t")
    cli.expect_exact("help")
    cli.sendcontrol("c")

    # do not crash on failed completion request to tnt
    i1.terminate()
    cli.send("\t\t")
    cli.expect_exact("Getting completions failed: Broken pipe (os error 32)")


def test_sql_explain_ok(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    cli = pexpect.spawn(
        cwd=i1.instance_dir,
        command=i1.runtime.command,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("sql> ")

    i1.sql(
        """
        CREATE TABLE "assets"
             ("id" INTEGER NOT NULL,
              "name" TEXT,
              "stock" INTEGER,
              PRIMARY KEY("id")
            )
        DISTRIBUTED BY("id")
        OPTION (TIMEOUT = 3.0);""",
        sudo=True,
    )

    i1.sql(
        """
        CREATE TABLE "characters"
           ("id" INTEGER NOT NULL,
            "name" TEXT NOT NULL,
            "year" INTEGER,
            PRIMARY KEY ("id")
            )
        USING MEMTX DISTRIBUTED BY ("id")
        OPTION (TIMEOUT = 3.0);""",
        sudo=True,
    )

    cli.sendline("""EXPLAIN INSERT INTO "assets" VALUES (1, 'Woody', 2561);""")

    cli.expect_exact('insert "assets" on conflict: fail')
    cli.expect_exact('motion [policy: segment([ref("COLUMN_1")]), program: ReshardIfNeeded]')
    cli.expect_exact("values")
    cli.expect_exact("value row (data=ROW(1::int, 'Woody'::string, 2561::int))")
    cli.expect_exact("execution options:")
    cli.expect_exact("sql_vdbe_opcode_max = 45000")
    cli.expect_exact("sql_motion_row_max = 5000")
    cli.expect_exact("buckets = unknown")

    cli.sendline("""EXPLAIN UPDATE "characters" SET "year" = 2010;""")

    cli.expect_exact('update "characters')
    cli.expect_exact('"year" = "col_0"')
    cli.expect_exact("motion [policy: local, program: ReshardIfNeeded]")
    cli.expect_exact('projection (2010::int -> "col_0", "characters"."id"::int -> "col_1")')
    cli.expect_exact('scan "characters"')
    cli.expect_exact("execution options:")
    cli.expect_exact("sql_vdbe_opcode_max = 45000")
    cli.expect_exact("sql_motion_row_max = 5000")
    cli.expect_exact("buckets = [1-3000]")

    cli.sendline("""EXPLAIN UPDATE "characters" SET "name" = 'Etch', "year" = 2010 WHERE "id" = 2;""")

    cli.expect_exact('update "characters"')
    cli.expect_exact('"name" = "col_0"')
    cli.expect_exact('"year" = "col_1"')
    cli.expect_exact("motion [policy: local, program: ReshardIfNeeded]")
    cli.expect_exact(
        'projection (\'Etch\'::string -> "col_0", 2010::int -> "col_1", "characters"."id"::int -> "col_2")'
    )
    cli.expect_exact('selection "characters"."id"::int = 2::int')
    cli.expect_exact('scan "characters"')
    cli.expect_exact("execution options:")
    cli.expect_exact("sql_vdbe_opcode_max = 45000")
    cli.expect_exact("sql_motion_row_max = 5000")
    cli.expect_exact("buckets = [1410]")


def test_lua_console_sql_error_messages(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True)

    result = i1.eval(
        """
        console = require 'console'
        return console.eval ' pico.sql [[ create table foo ]] '
        """
    )

    assert (
        result
        == """---
- null
- |+
  rule parsing error:  --> 1:15
    |
  1 |  create table foo
    |               ^---
    |
    = expected IfNotExists

...
"""
    )


def test_connect_pretty_message_on_server_crash(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    i2 = cluster.add_instance(wait_online=False)
    i2.start()
    i2.wait_online()

    # test crash error when run with `picodata connect`
    cli = pexpect.spawn(
        command=i1.runtime.command,
        args=["connect", f"{i1.host}:{i1.port}"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout
    cli.expect_exact(f'Connected to interactive console by address "{i1.host}:{i1.port}" under "guest" user')
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("sql> ")

    i1.terminate()
    cli.sendline("ping;")
    cli.expect("lost connection to the server: io error: unexpected end of file")
    cli.terminate()

    # test crash error when run with `picodata admin`
    cli = pexpect.spawn(
        cwd=i2.instance_dir,
        command=i2.runtime.command,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout
    cli.expect_exact("sql> ")

    i2.terminate()
    cli.sendline("ping;")
    cli.expect_exact("lost connection to the server: Broken pipe (os error 32)")


def test_input_with_delimiter(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()
    i1.create_user(with_name="andy", with_password="Testpa55")
    i1.sql('GRANT CREATE TABLE TO "andy"', sudo=True)

    cli = pexpect.spawn(
        command=i1.runtime.command,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "andy"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for andy: ")
    cli.sendline("Testpa55")

    cli.expect_exact(f'Connected to interactive console by address "{i1.host}:{i1.port}" under "andy" user')
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("sql> ")

    # several commands in one line
    cli.sendline(
        "\t\tCREATE TABLE ids"
        " (id INTEGER NOT NULL, PRIMARY KEY(id))"
        " USING MEMTX DISTRIBUTED BY (id);"
        " INSERT INTO ids VALUES(1);"
    )

    cli.expect_exact("1")
    cli.expect_exact("1")

    cli.sendline("SELECT * FROM ids;")

    cli.expect_exact("+----+")
    cli.expect_exact("| id |")
    cli.expect_exact("+====+")
    cli.expect_exact("| 1  |")
    cli.expect_exact("+----+")
    cli.expect_exact("(1 rows)")

    cli.sendline("DROP    TAB LE\tids;")
    cli.expect_exact("SqlUnrecognizedSyntax: rule parsing error:  --> 1:1")

    # client doesn't send query until delimiter
    cli.sendline("invalid query")
    cli.expect_exact("sql> ")
    cli.sendline("waiting until delimiter")
    cli.expect_exact("   > ")
    cli.sendline(";")
    cli.expect_exact("rule parsing error:  --> 1:1")

    # treating ';;;' with delimiter ';' as 3 empty queries
    cli.sendline(";;;")
    cli.expect_exact("0")
    cli.expect_exact("0")
    cli.expect_exact("0")

    cli.expect_exact("sql>")

    # formatting remains correct when copying and pasting a large piece of code
    # empty symbols after delimiter should be skipped
    cli.sendline("SELECT 1 AS id;     \t\t       SELECT 2   AS id  ;   \t  ")
    cli.expect_exact("+----+")
    cli.expect_exact("| id |")
    cli.expect_exact("+====+")
    cli.expect_exact("| 1  |")
    cli.expect_exact("+----+")
    cli.expect_exact("(1 rows)")
    cli.expect_exact("+----+")
    cli.expect_exact("| id |")
    cli.expect_exact("+====+")
    cli.expect_exact("| 2  |")
    cli.expect_exact("+----+")
    cli.expect_exact("(1 rows)")
    cli.expect_exact("sql>")

    # test enter delimiter
    cli.sendline("\\set delimiter enter")
    cli.expect_exact("Delimiter changed to 'enter'")

    cli.sendline("CREATE TABLE warehouse (id INTEGER PRIMARY KEY, item TEXT NOT NULL")
    cli.expect_exact("1")

    cli.sendline("\\set delimiter default")
    cli.expect_exact("Delimiter changed to ';'")


def test_cat_file_to_picodata_admin_stdin(cluster: Cluster):
    instance = cluster.add_instance()
    data = subprocess.check_output(
        [cluster.runtime.command, "admin", "--prompts", f"{instance.instance_dir}/admin.sock"],
        input=b"""\
CREATE TABLE ids (id INTEGER NOT NULL, PRIMARY KEY(id))
        USING MEMTX
        DISTRIBUTED BY (id);

INSERT INTO ids
        VALUES(1);

SELECT * FROM ids
""",
    )

    assert (
        data
        == f"""\
Connected to admin console by socket path "{instance.instance_dir}/admin.sock"
type '\\help' for interactive help
1
1
+----+
| id |
+====+
| 1  |
+----+
(1 rows)
Bye
""".encode()
    )


def test_admin_output_format_json(cluster: Cluster):
    instance = cluster.add_instance()
    data = subprocess.check_output(
        [cluster.runtime.command, "admin", "--json", f"{instance.instance_dir}/admin.sock"],
        input=b"""\
CREATE TABLE warehouse (id INTEGER NOT NULL, item TEXT NOT NULL, PRIMARY KEY(id))
        USING MEMTX DISTRIBUTED BY (id);
INSERT INTO warehouse VALUES (1, 'bricks');
INSERT INTO warehouse VALUES (2, 'hooves');
SELECT * FROM warehouse;
""",
    )

    assert (
        data
        == b"""\
1
1
1
[
  {
    "id": 1,
    "item": "bricks"
  },
  {
    "id": 2,
    "item": "hooves"
  }
]
"""
    )


def test_admin_output_format_csv(cluster: Cluster):
    instance = cluster.add_instance()
    data = subprocess.check_output(
        [cluster.runtime.command, "admin", "--csv", f"{instance.instance_dir}/admin.sock"],
        input=b"""\
CREATE TABLE warehouse (id INTEGER NOT NULL, item TEXT NOT NULL, PRIMARY KEY(id))
        USING MEMTX DISTRIBUTED BY (id);
INSERT INTO warehouse VALUES (1, 'bricks');
INSERT INTO warehouse VALUES (2, 'hooves');
SELECT * FROM warehouse;
""",
    )

    assert (
        data
        == b"""\
1
1
1
id,item
1,bricks
2,hooves
"""
    )


def test_admin_output_format_csv_custom_separator(cluster: Cluster):
    instance = cluster.add_instance()
    data = subprocess.check_output(
        [
            cluster.runtime.command,
            "admin",
            "--csv",
            "--field-separator",
            ";",
            f"{instance.instance_dir}/admin.sock",
        ],
        input=b"""\
CREATE TABLE warehouse (id INTEGER NOT NULL, item TEXT NOT NULL, PRIMARY KEY(id))
        USING MEMTX DISTRIBUTED BY (id);
INSERT INTO warehouse VALUES (1, 'bricks');
INSERT INTO warehouse VALUES (2, 'hooves');
SELECT * FROM warehouse;
""",
    )

    assert (
        data
        == b"""\
1
1
1
id;item
1;bricks
2;hooves
"""
    )


def test_admin_output_format_tuples_only(cluster: Cluster):
    instance = cluster.add_instance()
    data = subprocess.check_output(
        [cluster.runtime.command, "admin", "--tuples-only", f"{instance.instance_dir}/admin.sock"],
        input=b"""\
CREATE TABLE warehouse (id INTEGER NOT NULL, item TEXT NOT NULL, PRIMARY KEY(id))
        USING MEMTX DISTRIBUTED BY (id);
INSERT INTO warehouse VALUES (1, 'bricks');
INSERT INTO warehouse VALUES (2, 'hooves');
SELECT * FROM warehouse;
""",
    )

    assert (
        data
        == b"""\
1
1
1
1\tbricks
2\thooves
"""
    )


def test_admin_output_format_tuples_only_custom_separator(cluster: Cluster):
    instance = cluster.add_instance()
    data = subprocess.check_output(
        [
            cluster.runtime.command,
            "admin",
            "--tuples-only",
            "--field-separator",
            "|",
            f"{instance.instance_dir}/admin.sock",
        ],
        input=b"""\
CREATE TABLE warehouse (id INTEGER NOT NULL, item TEXT NOT NULL, PRIMARY KEY(id))
        USING MEMTX DISTRIBUTED BY (id);
INSERT INTO warehouse VALUES (1, 'bricks');
INSERT INTO warehouse VALUES (2, 'hooves');
SELECT * FROM warehouse;
""",
    )

    assert (
        data
        == b"""\
1
1
1
1|bricks
2|hooves
"""
    )


def test_cat_file_to_picodata_connect_stdin(cluster: Cluster):
    i1 = cluster.add_instance()

    data = subprocess.check_output(
        [cluster.runtime.command, "admin", "--prompts", f"{i1.instance_dir}/admin.sock"],
        input=b"""\
CREATE USER "alice" WITH PASSWORD 'T0psecret';
GRANT CREATE TABLE TO "alice"
""",
    )

    assert (
        data
        == f"""\
Connected to admin console by socket path "{i1.instance_dir}/admin.sock"
type '\\help' for interactive help
1
1
Bye
""".encode()
    )

    cli = pexpect.spawn(
        command=i1.runtime.command,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "alice"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for alice: ")
    cli.sendline("T0psecret")
    cli.expect_exact(f'Connected to interactive console by address "{i1.host}:{i1.port}" under "alice" user')
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("sql> ")

    cli.sendline(
        "CREATE TABLE ids (id INTEGER NOT NULL, PRIMARY KEY(id)) USING MEMTX DISTRIBUTED BY (id);"
        "INSERT INTO ids VALUES(1);"
        "INSERT INTO ids VALUES(11);"
        "INSERT INTO ids VALUES(111);"
        "INSERT INTO ids VALUES(1111);"
        "DELETE FROM ids where id = 1;"
        "DELETE FROM ids where id = 11;"
        "DELETE FROM ids where id = 111;"
        "DELETE FROM ids where id = 1111;"
        "SELECT * FROM ids;"
    )

    cli.expect_exact("1")
    cli.expect_exact("1")
    cli.expect_exact("1")
    cli.expect_exact("1")
    cli.expect_exact("1")
    cli.expect_exact("1")
    cli.expect_exact("1")
    cli.expect_exact("1")
    cli.expect_exact("1")
    cli.expect_exact("+----+")
    cli.expect_exact("| id |")
    cli.expect_exact("+====+")
    cli.expect_exact("+----+")
    cli.expect_exact("(0 rows)")

    cli.sendline("DROP")
    cli.expect_exact("   > ")

    cli.sendline("TABLE")
    cli.expect_exact("   > ")

    cli.sendline("ids")
    cli.expect_exact("   > ")

    cli.sendline("OPTION (TIMEOUT = 3.0)")
    cli.expect_exact("   > ")
    cli.sendline(";")
    cli.expect_exact("1")


def test_do_not_ban_admin_via_unix_socket(cluster: Cluster):
    password = "secret"
    cluster.set_service_password(password)

    i1 = cluster.add_instance(wait_online=False)

    admin_banned_lc = log_crawler(i1, "Maximum number of login attempts exceeded; user blocked")
    i1.start()
    i1.wait_online()

    # auth via pico_service many times
    for _ in range(6):
        with pytest.raises(NetworkError):
            i1.sql("try to auth", user="pico_service", password="wrong_password")

    # pico_service is not banned
    data = i1.sql("SELECT name FROM _pico_tier ", user="pico_service", password=password)
    assert data[0][0] == "default"

    # auth via admin until ban
    for _ in range(5):
        with pytest.raises(NetworkError):
            i1.sql("try to auth", user="admin", password="wrong_password")

    admin_banned_lc.wait_matched()

    cli = pexpect.spawn(
        cwd=i1.instance_dir,
        command=i1.runtime.command,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )

    cli.logfile = sys.stdout
    cli.expect_exact('Connected to admin console by socket path "./admin.sock"')
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("(admin) sql> ")


def test_picodata_tarantool(cluster: Cluster):
    test_lua = os.path.join(cluster.data_dir, "test.lua")
    with open(test_lua, "w") as f:
        print(
            """
            print('stdout check')
            file, err = io.open('output.txt', 'w')
            assert(err == nil)
            assert(file:write('it worked!'))
            assert(file:close())
        """,
            file=f,
        )

    stdout = subprocess.check_output(
        [cluster.runtime.command, "tarantool", "--", test_lua],
        cwd=cluster.data_dir,
    )
    assert stdout == b"stdout check\n"

    output_txt = os.path.join(cluster.data_dir, "output.txt")
    with open(output_txt, "r") as f:
        result = f.read()

    assert result == "it worked!"


def test_command_history_with_delimiter(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()
    i1.create_user(with_name="andy", with_password="Testpa55")
    i1.sql('GRANT CREATE TABLE TO "andy"', sudo=True)

    cli = pexpect.spawn(
        command=i1.runtime.command,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "andy"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for andy: ")
    cli.sendline("Testpa55")

    cli.expect_exact(f'Connected to interactive console by address "{i1.host}:{i1.port}" under "andy" user')
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("sql> ")

    # Set custom delimiter
    cli.sendline("\\set delimiter ?123")
    cli.expect_exact("Delimiter changed to '?123'")

    # Enter a command with the custom delimiter
    cli.sendline("CREATE TABLE test_table (id INTEGER PRIMARY KEY)?123")
    cli.expect_exact("1")

    # Press the up arrow key to access the command history
    cli.sendline("\033[A")  # \033[A is the escape sequence for the up arrow key
    cli.expect_exact("CREATE TABLE test_table (id INTEGER PRIMARY KEY)?123")

    # Press the down arrow key to clean the input
    cli.sendline("\033[B")  # \033[B is the escape sequence for the down arrow key
    cli.expect_exact("sql> ")

    # Set delimiter back to ;
    cli.sendline("\\set delimiter ;")
    cli.expect_exact("Delimiter changed to ';'")

    # Set delimiter back to default
    cli.sendline("\\set delimiter default")
    cli.expect_exact("Delimiter changed to ';'")

    # Enter a command with the default delimiter
    cli.sendline("DROP TABLE test_table;")
    cli.expect_exact("1")

    # Press the up arrow key to access the command history
    cli.sendline("\033[A")  # \033[A is the escape sequence for the up arrow key
    cli.expect_exact("DROP TABLE test_table;")

    # Press the down arrow key to clean the input
    cli.sendline("\033[B")  # \033[B is the escape sequence for the down arrow key
    cli.expect_exact("sql> ")


def test_picodata_version(cluster: Cluster):
    stdout = subprocess.check_output([cluster.runtime.command, "--version"])
    lines = iter(stdout.splitlines())
    assert_starts_with(next(lines), b"picodata ")
    assert_starts_with(next(lines), b"tarantool (fork) version")
    assert_starts_with(next(lines), b"Linux")


def test_admin_cli_exit_code(cluster: Cluster):
    # Test the exit code for SQL statements with syntax errors
    setup_sql = f"{cluster.data_dir}/setup.sql"
    with open(setup_sql, "w") as f:
        f.write(
            """
        CREATE USER "alice" WITH PASSWORD 'T0psecret';
        GRANT CREATE TABLE TO "alice";
        GRANT_SYNTAX_ERROR READ TABLE TO "alice";
        GRANT WRITE TABLE TO "alice";
        """
        )

    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    process = subprocess.run(
        [i1.runtime.command, "admin", f"{i1.instance_dir}/admin.sock"],
        stdin=open(setup_sql, "r"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )

    assert process.stderr.find("- null\n") != -1
    assert process.stderr.find('GRANT_SYNTAX_ERROR READ TABLE TO "alice"') != -1
    assert process.returncode != 0, f"Process failed with exit code {process.returncode}\n"

    # Test the exit code when a duplicate values error occurs
    insert_sql = f"{cluster.data_dir}/insert.sql"
    with open(insert_sql, "w") as f:
        f.write(
            """
        CREATE TABLE ad_warehouse (id INTEGER PRIMARY KEY);
        INSERT INTO ad_warehouse VALUES(1);
        INSERT INTO ad_warehouse VALUES(1);
        """
        )

    i2 = cluster.add_instance(wait_online=False)
    i2.start()
    i2.wait_online()

    process = subprocess.run(
        [i2.runtime.command, "admin", f"{i2.instance_dir}/admin.sock"],
        stdin=open(insert_sql, "r"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )

    assert process.stderr.find("- null\n") != -1
    assert process.stderr.find("transaction rolled-back: failed to insert space") != -1
    assert process.returncode != 0, f"Process failed with exit code {process.returncode}\n"

    # Test the exit code when attempting to drop non-existent plugins
    plugin_sql = f"{cluster.data_dir}/plugin.sql"
    with open(plugin_sql, "w") as f:
        f.write("DROP PLUGIN weather_cache 0.1.0;")

    i3 = cluster.add_instance(wait_online=False)
    i3.start()
    i3.wait_online()

    process = subprocess.run(
        [i3.runtime.command, "admin", f"{i3.instance_dir}/admin.sock"],
        stdin=open(plugin_sql, "r"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )

    assert process.stderr.find("- null\n") != -1
    assert process.stderr.find("no such plugin") != -1
    assert process.returncode != 0, f"Process failed with exit code {process.returncode}\n"


def test_connect_cli_exit_code(cluster: Cluster):
    connect_sql = f"{cluster.data_dir}/connect.sql"
    with open(connect_sql, "w") as f:
        f.write(
            """
        CREATE TABLE index (id INTEGER PRIMARY KEY, item TEXT NOT NULL);
        SELECT_WITH_SYNTAX_ERROR * FROM index;
        SELECT id from index;
        """
        )

    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()
    i1.create_user(with_name="andy", with_password="Testpa55")
    i1.sql('GRANT CREATE TABLE TO "andy"', sudo=True)

    process = subprocess.run(
        [i1.runtime.command, "admin", f"{i1.instance_dir}/admin.sock"],
        stdin=open(connect_sql, "r"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )

    assert process.stderr.find("rule parsing error") != -1
    assert process.stderr.find("SELECT_WITH_SYNTAX_ERROR * FROM index") != -1
    assert process.returncode != 0, f"Process failed with exit code {process.returncode}\n"


def test_admin_cli_with_ignore_errors(cluster: Cluster):
    setup_sql = f"{cluster.data_dir}/setup.sql"
    with open(setup_sql, "w") as f:
        f.write(
            """
        CREATE USER "alice" WITH PASSWORD 'T0psecret';
        GRANT_SYNTAX_ERROR READ TABLE TO "alice";
        GRANT WRITE TABLE TO "alice";
        """
        )

    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    process = subprocess.run(
        [i1.runtime.command, "admin", f"{i1.instance_dir}/admin.sock", "--ignore-errors"],
        stdin=open(setup_sql, "r"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )

    assert process.stdout.find("rule parsing error") != -1
    assert process.stdout.find('GRANT_SYNTAX_ERROR READ TABLE TO "alice"') != -1
    assert process.returncode == 0, f"Process failed with exit code {process.returncode}\n"


def strip(s: str) -> str:
    acc = ""
    for line in s.splitlines():
        acc += line.rstrip()

    return acc


def test_picodata_status_basic(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
    cluster:
        name: test
        tier:
            storage:
            router:
    """
    )

    service_password = "T3stP4ssword"
    cluster.set_service_password(service_password)

    _ = cluster.add_instance(failure_domain=dict(DC="MSK"), tier="router")
    _ = cluster.add_instance(failure_domain=dict(DC="SPB"), tier="storage")
    _ = cluster.add_instance(failure_domain=dict(DC="SPB"), tier="router")
    _ = cluster.add_instance(failure_domain=dict(DC="SPB"), tier="router")

    cluster.wait_online()
    i1, i2, i3, i4 = sorted(cluster.instances, key=lambda i: i.name or "")

    info = i1.instance_info()
    cluster_uuid = info["cluster_uuid"]
    cluster_name = info["cluster_name"]

    i1_address = f"{i1.host}:{i1.port}"
    i2_address = f"{i2.host}:{i2.port}"
    i3_address = f"{i3.host}:{i3.port}"
    i4_address = f"{i4.host}:{i4.port}"

    i1_uuid = i1.uuid()
    i2_uuid = i2.uuid()
    i3_uuid = i3.uuid()
    i4_uuid = i4.uuid()

    assert i1.service_password_file

    data = subprocess.check_output(
        [
            cluster.runtime.command,
            "status",
            "--peer",
            f"{i1_address}",
            "--service-password-file",
            i1.service_password_file,
        ],
    )

    output = f"""\
 CLUSTER NAME: {cluster_name}
 CLUSTER UUID: {cluster_uuid}
 TIER/DOMAIN: router/MSK

 name         state    uuid                                   uri
{i1.name}    Online   {i1_uuid}   {i1_address}

 TIER/DOMAIN: router/SPB

 name         state    uuid                                   uri
{i2.name}    Online   {i2_uuid}   {i2_address}
{i3.name}    Online   {i3_uuid}   {i3_address}

 TIER/DOMAIN: storage/SPB

 name         state    uuid                                   uri
{i4.name}   Online   {i4_uuid}   {i4_address}

"""

    assert strip(data.decode()) == strip(output)

    # let's kill i2, so after that it should be on last place in corresponding block
    i2.terminate()

    data = subprocess.check_output(
        [
            cluster.runtime.command,
            "status",
            "--peer",
            i1_address,
            "--service-password-file",
            i1.service_password_file,
        ],
    )

    output = f"""\
 CLUSTER NAME: {cluster_name}
 CLUSTER UUID: {cluster_uuid}
 TIER/DOMAIN: router/MSK

 name         state     uuid                                   uri
{i1.name}    Online    {i1_uuid}   {i1_address}

 TIER/DOMAIN: router/SPB

 name         state     uuid                                   uri
{i3.name}    Online    {i3_uuid}   {i3_address}
{i2.name}    Offline   {i2_uuid}   {i2_address}

 TIER/DOMAIN: storage/SPB

 name         state     uuid                                   uri
{i4.name}   Online    {i4_uuid}   {i4_address}

"""

    assert strip(data.decode()) == strip(output)


def assert_status_info(inst: Instance, cluster: Cluster, username: str, password_file: str, err: bool = False):
    info = inst.instance_info()

    cluster_name = info["cluster_name"]
    cluster_uuid = info["cluster_uuid"]
    inst_name = inst.name
    inst_uuid = inst.uuid()
    base_addr = inst.iproto_listen

    inst_addr = f"{username}@{base_addr}"
    if err:
        proc = subprocess.Popen(
            [
                cluster.runtime.command,
                "status",
                "--peer",
                inst_addr,
                "--service-password-file",
                password_file,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        _, stderr = proc.communicate()
        assert proc.returncode != 0
        assert f"AccessDenied: Read access to space '_raft_state' is denied for user '{username}'" in stderr
    else:
        data = subprocess.check_output(
            [
                cluster.runtime.command,
                "status",
                "--peer",
                inst_addr,
                "--service-password-file",
                password_file,
            ],
        )
        output = f"""\
 CLUSTER NAME: {cluster_name}
 CLUSTER UUID: {cluster_uuid}
 TIER/DOMAIN: default

 name         state    uuid                                   uri
{inst_name}   Online   {inst_uuid}   {base_addr}
"""
        assert strip(data.decode()) == strip(output)


def test_picodata_status_custom_user(cluster: Cluster):
    # setup cluster
    general_password = "T0psecret"
    inst = cluster.add_instance(wait_online=False)

    inst.env["PICODATA_ADMIN_PASSWORD"] = general_password
    inst.set_service_password(general_password)
    assert inst.service_password_file

    password_file = f"{inst.instance_dir}/password.txt"
    with open(password_file, "w") as f:
        f.write(general_password)

    inst.start_and_wait()

    # test as `pico_service` system user
    assert_status_info(inst, cluster, "pico_service", password_file)

    # test as `admin` priviliged user
    assert_status_info(inst, cluster, "admin", password_file)

    # test as `somebody` un-/priviliged user
    username = "somebody"

    res = inst.sql(f"CREATE USER {username} WITH PASSWORD '{general_password}' USING MD5", sudo=True)
    assert isinstance(res, dict)
    assert res["row_count"] == 1
    assert_status_info(inst, cluster, username, password_file, err=True)

    res = inst.sql(f"GRANT READ TABLE TO {username}", sudo=True)
    assert isinstance(res, dict)
    assert res["row_count"] == 1
    assert_status_info(inst, cluster, username, password_file, err=False)


def test_picodata_status_short_instance_name(cluster: Cluster):
    short_name = "a"
    service_password = "T3stP4ssword"
    cluster.set_service_password(service_password)
    # name with one symbol
    instance = cluster.add_instance(name=short_name)

    info = instance.instance_info()
    cluster_uuid = info["cluster_uuid"]
    cluster_name = info["cluster_name"]
    i1_address = f"{instance.host}:{instance.port}"
    i1_uuid = instance.uuid()

    assert instance.service_password_file

    data = subprocess.check_output(
        [
            cluster.runtime.command,
            "status",
            "--peer",
            f"{i1_address}",
            "--service-password-file",
            instance.service_password_file,
        ],
    )

    output = f"""\
 CLUSTER NAME: {cluster_name}
 CLUSTER UUID: {cluster_uuid}
 TIER/DOMAIN: default

 name   state    uuid                                   uri
{instance.name}       Online   {i1_uuid}   {i1_address}

"""

    assert strip(data.decode()) == strip(output)


def test_picodata_status_exit_code(cluster: Cluster):
    service_password = "T3stP4ssword"
    cluster.set_service_password(service_password)

    password_file = cluster.service_password_file
    assert password_file

    i1 = cluster.add_instance(wait_online=False)
    i1_address = f"{i1.host}:{i1.port}"
    assert i1.service_password_file

    process = subprocess.run(
        [
            cluster.runtime.command,
            "status",
            "--peer",
            f"{i1_address}",
            "--service-password-file",
            i1.service_password_file,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )

    # instance not started yet
    assert process.returncode != 0

    i1.start()
    i1.wait_online()

    process = subprocess.run(
        [
            cluster.runtime.command,
            "status",
            "--peer",
            f"{i1_address}",
            "--service-password-file",
            i1.service_password_file,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )

    assert process.returncode == 0

    # STEP: verify that broken pipe for both stdout and stderr will not panic.
    # NOTE: see <https://git.picodata.io/core/picodata/-/issues/2520>.

    broken_pipe_reader, broken_pipe_writer = os.pipe()
    os.close(broken_pipe_reader)

    process = subprocess.run(
        [
            cluster.runtime.command,
            "status",
            "--peer",
            f"{i1_address}",
            "--service-password-file",
            i1.service_password_file,
        ],
        stdout=broken_pipe_writer,
        stderr=broken_pipe_writer,
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    assert process.returncode == 1


def test_picodata_status_doesnt_show_expelled_instances(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
    cluster:
        name: test
        tier:
            storage:
            router:
    """
    )

    service_password = "T3stP4ssword"
    cluster.set_service_password(service_password)

    _ = cluster.add_instance(failure_domain=dict(DC="MSK"), tier="router")
    _ = cluster.add_instance(failure_domain=dict(DC="SPB"), tier="storage")
    _ = cluster.add_instance(failure_domain=dict(DC="SPB"), tier="router")
    _ = cluster.add_instance(failure_domain=dict(DC="SPB"), tier="router")

    cluster.wait_online()
    i1, i2, i3, i4 = sorted(cluster.instances, key=lambda i: i.name or "")

    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1000)
    cluster.wait_until_instance_has_this_many_active_buckets(i3, 1000)

    info = i1.instance_info()
    cluster_uuid = info["cluster_uuid"]
    cluster_name = info["cluster_name"]

    i1_address = f"{i1.host}:{i1.port}"
    i2_address = f"{i2.host}:{i2.port}"
    i3_address = f"{i3.host}:{i3.port}"
    i4_address = f"{i4.host}:{i4.port}"

    i1_uuid = i1.uuid()
    i2_uuid = i2.uuid()
    i3_uuid = i3.uuid()
    i4_uuid = i4.uuid()

    assert i1.service_password_file

    data = subprocess.check_output(
        [
            cluster.runtime.command,
            "status",
            "--peer",
            f"{i1_address}",
            "--service-password-file",
            i1.service_password_file,
        ],
    )

    output = f"""\
 CLUSTER NAME: {cluster_name}
 CLUSTER UUID: {cluster_uuid}
 TIER/DOMAIN: router/MSK

 name         state    uuid                                   uri
{i1.name}    Online   {i1_uuid}   {i1_address}

 TIER/DOMAIN: router/SPB

 name         state    uuid                                   uri
{i2.name}    Online   {i2_uuid}   {i2_address}
{i3.name}    Online   {i3_uuid}   {i3_address}

 TIER/DOMAIN: storage/SPB

 name         state    uuid                                   uri
{i4.name}   Online   {i4_uuid}   {i4_address}

"""

    assert strip(data.decode()) == strip(output)

    cluster.expel(i3, timeout=20, force=True)

    data = subprocess.check_output(
        [
            cluster.runtime.command,
            "status",
            "--peer",
            i1_address,
            "--service-password-file",
            i1.service_password_file,
        ],
    )

    output = f"""\
 CLUSTER NAME: {cluster_name}
 CLUSTER UUID: {cluster_uuid}
 TIER/DOMAIN: router/MSK

 name         state    uuid                                   uri
{i1.name}    Online   {i1_uuid}   {i1_address}

 TIER/DOMAIN: router/SPB

 name         state    uuid                                   uri
{i2.name}    Online   {i2_uuid}   {i2_address}

 TIER/DOMAIN: storage/SPB

 name         state    uuid                                   uri
{i4.name}   Online   {i4_uuid}   {i4_address}

"""

    assert strip(data.decode()) == strip(output)

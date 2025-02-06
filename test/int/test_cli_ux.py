import pexpect  # type: ignore
import os
import pytest
import sys
import subprocess
from conftest import (
    CLI_TIMEOUT,
    Cluster,
    log_crawler,
    assert_starts_with,
)
from tarantool.error import (  # type: ignore
    NetworkError,
)
from test_plugin import _PLUGIN, _PLUGIN_VERSION_1, PluginReflection
from time import sleep


def test_connect_ux(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()
    i1.create_user(with_name="andy", with_password="Testpa55")
    i1.sql('GRANT CREATE TABLE TO "andy"', sudo=True)

    cli = pexpect.spawn(
        command=i1.binary_path,
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
    cli.sendline("\\lua;")
    cli.expect_exact("Language cannot be changed in this console")

    cli.sendline("\\sql")
    cli.expect_exact("Language cannot be changed in this console")

    # for not registried command nothing can happen
    cli.sendline("\\lya;")
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
    cli.expect_exact('projection ("ids"."id"::integer -> "id")')
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
        command=i1.binary_path,
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

    cli.sendline("\\sql;")
    cli.expect_exact("Language switched to sql")
    cli.expect_exact("(admin) sql> ")

    # for not registried command nothing happend
    cli.sendline("\\lya")
    cli.expect_exact("Unknown special sequence")
    cli.sendline("\\scl")
    cli.expect_exact("Unknown special sequence")

    # variations of `\s l sql/lua` is registred, but not in help
    cli.sendline("\\set language lua")
    cli.expect_exact("Language switched to lua")
    cli.expect_exact("(admin) lua> ")

    cli.sendline("\\s lang sql")
    cli.expect_exact("Language switched to sql")
    cli.expect_exact("(admin) sql> ")

    # nothing happens on completion in SQL mode
    cli.sendline("\\sql;")
    cli.expect_exact("Language switched to sql")
    cli.sendline("\t\t")
    cli.expect_exact("(admin) sql> ")

    cli.sendline("box.c\t\t;")
    cli.expect_exact("rule parsing error:")

    # something happens on completion in Lua mode
    cli.sendline("\\lua")
    cli.expect_exact("Language switched to lua")
    cli.sendline("hel\t")
    cli.expect_exact("(admin) lua> help")


def test_plugin_ux(cluster: Cluster):
    service_password = "T3stP4ssword"
    cluster.set_service_password(service_password)
    assert cluster.service_password_file

    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    plugin_ref = PluginReflection.default(i1)
    i1.call("pico.install_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    plugin_ref.install(True).enable(False)

    new_config = f"{i1.instance_dir}/new_conf.yaml"

    # test list of services with multiple elements

    with open(new_config, "w") as f:
        # testservice_1.bar = 101 -> 42
        # testservice_1.foo = true -> true, shouldn't be even altered initially
        # testservice_1.baz = ["one", "two", "three"] -> ["cool", "nice"], multiple elements
        # testservice_2.foo = 0 -> 13, check more than a one service
        f.write(
            """\
testservice_1:
    bar: 42
    foo: true
    baz: ["cool", "nice"]

testservice_2:
    foo: 13

i_dont_care_about_this_service:
    fuck_off: "bullshit"
"""
        )

    subprocess.run(
        [
            i1.binary_path,
            "plugin",
            "configure",
            i1.iproto_listen,
            _PLUGIN,
            _PLUGIN_VERSION_1,
            new_config,
            "--service-password-file",
            cluster.service_password_file,
            "--service-names",
            "testservice_1,testservice_2",
        ],
        encoding="utf-8",
    )
    sleep(2)  # wait to finish updating configs, not the subprocess

    assert i1.sql("SELECT * FROM _pico_plugin_config;") == [
        ["testplug", "0.1.0", "testservice_1", "bar", 42],  # <- `101`
        [
            "testplug",
            "0.1.0",
            "testservice_1",
            "baz",
            ["cool", "nice"],
        ],  # <- ["one", "two", "three"]
        ["testplug", "0.1.0", "testservice_1", "foo", True],
        ["testplug", "0.1.0", "testservice_2", "foo", 13],  # <- `0`
    ]

    # test list of services with a single element

    with open(new_config, "w") as f:
        # testservice_1.baz = ["one", "two", "three"] -> ["sindragosa"], single element
        f.write(
            """\
testservice_1:
    baz: ["sindragosa"]
"""
        )

    subprocess.run(
        [
            i1.binary_path,
            "plugin",
            "configure",
            i1.iproto_listen,
            _PLUGIN,
            _PLUGIN_VERSION_1,
            new_config,
            "--service-password-file",
            cluster.service_password_file,
            "--service-names",
            "testservice_1",
        ],
        encoding="utf-8",
    )
    sleep(2)  # wait to finish updating configs, not the subprocess

    assert i1.sql("SELECT * FROM _pico_plugin_config;") == [
        ["testplug", "0.1.0", "testservice_1", "bar", 42],
        [
            "testplug",
            "0.1.0",
            "testservice_1",
            "baz",
            ["sindragosa"],
        ],  # <- ["one", "two", "three"]
        ["testplug", "0.1.0", "testservice_1", "foo", True],
        ["testplug", "0.1.0", "testservice_2", "foo", 13],
    ]


def test_lua_completion(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    cli = pexpect.spawn(
        cwd=i1.instance_dir,
        command=i1.binary_path,
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

    cli.send("bred bo")
    cli.send("\t")
    cli.expect_exact("bred box")

    # do not crash on failed completion request to tnt
    i1.terminate()
    cli.send("\t\t")
    cli.expect_exact("Server probably is closed, try to reconnect")


def test_sql_explain_ok(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    cli = pexpect.spawn(
        cwd=i1.instance_dir,
        command=i1.binary_path,
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
    cli.expect_exact('motion [policy: segment([ref("COLUMN_1")])]')
    cli.expect_exact("values")
    cli.expect_exact("value row (data=ROW(1::unsigned, 'Woody'::string, 2561::unsigned))")
    cli.expect_exact("execution options:")
    cli.expect_exact("sql_vdbe_opcode_max = 45000")
    cli.expect_exact("sql_motion_row_max = 5000")
    cli.expect_exact("buckets = unknown")

    cli.sendline("""EXPLAIN UPDATE "characters" SET "year" = 2010;""")

    cli.expect_exact('update "characters')
    cli.expect_exact('"year" = "col_0"')
    cli.expect_exact("motion [policy: local]")
    cli.expect_exact('projection (2010::unsigned -> "col_0", "characters"."id"::integer -> "col_1")')
    cli.expect_exact('scan "characters"')
    cli.expect_exact("execution options:")
    cli.expect_exact("sql_vdbe_opcode_max = 45000")
    cli.expect_exact("sql_motion_row_max = 5000")
    cli.expect_exact("buckets = [1-3000]")

    cli.sendline("""EXPLAIN UPDATE "characters" SET "name" = 'Etch', "year" = 2010 WHERE "id" = 2;""")

    cli.expect_exact('update "characters"')
    cli.expect_exact('"name" = "col_0"')
    cli.expect_exact('"year" = "col_1"')
    cli.expect_exact("motion [policy: local]")
    cli.expect_exact(
        'projection (\'Etch\'::string -> "col_0", 2010::unsigned -> "col_1", "characters"."id"::integer -> "col_2")'
    )
    cli.expect_exact('selection ROW("characters"."id"::integer) = ROW(2::unsigned)')
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
        command=i1.binary_path,
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
    cli.expect("Connection Error. Try to reconnect: io error: unexpected end of file")
    cli.terminate()

    # test crash error when run with `picodata admin`
    cli = pexpect.spawn(
        cwd=i2.instance_dir,
        command=i2.binary_path,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout
    cli.expect_exact("sql> ")

    i2.terminate()
    cli.sendline("ping;")
    cli.expect_exact("Server probably is closed, try to reconnect")


def test_input_with_delimiter(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()
    i1.create_user(with_name="andy", with_password="Testpa55")
    i1.sql('GRANT CREATE TABLE TO "andy"', sudo=True)

    cli = pexpect.spawn(
        command=i1.binary_path,
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
        [cluster.binary_path, "admin", f"{instance.instance_dir}/admin.sock"],
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


def test_cat_file_to_picodata_connect_stdin(cluster: Cluster):
    i1 = cluster.add_instance()

    data = subprocess.check_output(
        [cluster.binary_path, "admin", f"{i1.instance_dir}/admin.sock"],
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
        command=i1.binary_path,
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
    password_file = f"{cluster.instance_dir}/service-password.txt"
    with open(password_file, "w") as f:
        print("secret", file=f)

    os.chmod(password_file, 0o600)

    i1 = cluster.add_instance(wait_online=False)
    i1.service_password_file = password_file

    admin_banned_lc = log_crawler(i1, "Maximum number of login attempts exceeded; user blocked")
    i1.start()
    i1.wait_online()

    # auth via pico_service many times
    for _ in range(100):
        with pytest.raises(NetworkError):
            i1.sql("try to auth", user="pico_service", password="wrong_password")

    # pico_service is not banned
    data = i1.sql("SELECT name FROM _pico_tier ", user="pico_service", password="secret")

    assert data[0][0] == "default"

    # auth via admin until ban
    for _ in range(5):
        with pytest.raises(NetworkError):
            i1.sql("try to auth", user="admin", password="wrong_password")

    admin_banned_lc.wait_matched()

    cli = pexpect.spawn(
        cwd=i1.instance_dir,
        command=i1.binary_path,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )

    cli.logfile = sys.stdout
    cli.expect_exact('Connected to admin console by socket path "./admin.sock"')
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("(admin) sql> ")


def test_picodata_tarantool(cluster: Cluster):
    test_lua = os.path.join(cluster.instance_dir, "test.lua")
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
        [cluster.binary_path, "tarantool", "--", test_lua],
        cwd=cluster.instance_dir,
    )
    assert stdout == b"stdout check\n"

    output_txt = os.path.join(cluster.instance_dir, "output.txt")
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
        command=i1.binary_path,
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
    stdout = subprocess.check_output([cluster.binary_path, "--version"])
    lines = iter(stdout.splitlines())
    assert_starts_with(next(lines), b"picodata ")
    assert_starts_with(next(lines), b"tarantool (fork) version")
    assert_starts_with(next(lines), b"target: Linux")


def test_admin_cli_exit_code(cluster: Cluster):
    # Test the exit code for SQL statements with syntax errors
    setup_sql = f"{cluster.instance_dir}/setup.sql"
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
        [i1.binary_path, "admin", f"{i1.instance_dir}/admin.sock"],
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
    insert_sql = f"{cluster.instance_dir}/insert.sql"
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
        [i2.binary_path, "admin", f"{i2.instance_dir}/admin.sock"],
        stdin=open(insert_sql, "r"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )

    assert process.stderr.find("- null\n") != -1
    assert process.stderr.find("transaction: RolledBack") != -1
    assert process.returncode != 0, f"Process failed with exit code {process.returncode}\n"

    # Test the exit code when attempting to drop non-existent plugins
    plugin_sql = f"{cluster.instance_dir}/plugin.sql"
    with open(plugin_sql, "w") as f:
        f.write("DROP PLUGIN weather_cache 0.1.0;")

    i3 = cluster.add_instance(wait_online=False)
    i3.start()
    i3.wait_online()

    process = subprocess.run(
        [i3.binary_path, "admin", f"{i3.instance_dir}/admin.sock"],
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
    connect_sql = f"{cluster.instance_dir}/connect.sql"
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
        [i1.binary_path, "admin", f"{i1.instance_dir}/admin.sock"],
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
    setup_sql = f"{cluster.instance_dir}/setup.sql"
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
        [i1.binary_path, "admin", f"{i1.instance_dir}/admin.sock", "--ignore-errors"],
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
    """
    Remove table decorations so variable length data wont be affected
    by the table width calculations. An example would be port number
    which is sometimes 4 symbols long and sometimes 5.
    Additionaly replace | delimiter of columns with a space so even this
    stripped down form is readable.
    """
    to_remove = [" ", "+", "=", "-"]

    for c in to_remove:
        s = s.replace(c, "")

    return s.replace("|", " ")


def test_picodata_status_basic(cluster: Cluster):
    service_password = "T3stP4ssword"
    cluster.set_service_password(service_password)

    password_file = cluster.service_password_file
    assert password_file

    cluster.deploy(instance_count=3)
    cluster.wait_online()
    i1, i2, i3 = sorted(cluster.instances, key=lambda i: i.name or "")

    i1_address = f"{i1.host}:{i1.port}"
    i2_address = f"{i2.host}:{i2.port}"
    i3_address = f"{i3.host}:{i3.port}"

    i1_uuid = i1.uuid()
    i2_uuid = i2.uuid()
    i3_uuid = i3.uuid()

    i1_replicaset_uuid = i1.replicaset_uuid()
    i2_replicaset_uuid = i2.replicaset_uuid()
    i3_replicaset_uuid = i3.replicaset_uuid()

    data = subprocess.check_output(
        [
            cluster.binary_path,
            "status",
            "--peer",
            f"{i1_address}",
            "--service-password-file",
            password_file,
        ],
    )

    assert strip(data.decode()) == strip(
        f"""\


CLUSTER NAME: {i1.cluster_name}


+---------------+---------------+---------------+--------------------------------------+--------------------------------------+---------+----------------+
| instance_name | current_state | target_state  | instance_uuid                        | replicaset_uuid                      | tier    | uri            |
+========================================================================================================================================================+
| {i1.name}   | ["Online", 1] | ["Online", 1] | {i1_uuid} | {i1_replicaset_uuid} | default | {i1_address} |
|---------------+---------------+---------------+--------------------------------------+--------------------------------------+---------+----------------|
| {i2.name}   | ["Online", 1] | ["Online", 1] | {i2_uuid} | {i2_replicaset_uuid} | default | {i2_address} |
|---------------+---------------+---------------+--------------------------------------+--------------------------------------+---------+----------------|
| {i3.name}   | ["Online", 1] | ["Online", 1] | {i3_uuid} | {i3_replicaset_uuid} | default | {i3_address} |
+---------------+---------------+---------------+--------------------------------------+--------------------------------------+---------+----------------+
(3 rows)
"""  # noqa: E501
    )

    # let's kill i2, so after that it should be on last place in status table
    i2.terminate()

    data = subprocess.check_output(
        [
            cluster.binary_path,
            "status",
            "--peer",
            i1_address,
            "--service-password-file",
            password_file,
        ],
    )

    assert strip(data.decode()) == strip(
        f"""\


CLUSTER NAME: {i1.cluster_name}


+---------------+----------------+----------------+--------------------------------------+--------------------------------------+---------+----------------+
| instance_name | current_state  | target_state   | instance_uuid                        | replicaset_uuid                      | tier    | uri            |
+==========================================================================================================================================================+
| {i1.name}   | ["Online", 1]  | ["Online", 1]  | {i1_uuid} | {i1_replicaset_uuid} | default | {i1_address} |
|---------------+----------------+----------------+--------------------------------------+--------------------------------------+---------+----------------|
| {i3.name}   | ["Online", 1]  | ["Online", 1]  | {i3_uuid} | {i3_replicaset_uuid} | default | {i3_address} |
|---------------+----------------+----------------+--------------------------------------+--------------------------------------+---------+----------------|
| {i2.name}   | ["Offline", 1] | ["Offline", 1] | {i2_uuid} | {i2_replicaset_uuid} | default | {i2_address} |
+---------------+----------------+----------------+--------------------------------------+--------------------------------------+---------+----------------+
(3 rows)
"""  # noqa: E501
    )

    # let's kill i3. Equal by state instances should be sorted by instance_name
    i3.terminate()

    data = subprocess.check_output(
        [
            cluster.binary_path,
            "status",
            "--peer",
            i1_address,
            "--service-password-file",
            password_file,
        ],
    )

    assert strip(data.decode()) == strip(
        f"""\


CLUSTER NAME: {i1.cluster_name}


+---------------+----------------+----------------+--------------------------------------+--------------------------------------+---------+----------------+
| instance_name | current_state  | target_state   | instance_uuid                        | replicaset_uuid                      | tier    | uri            |
+==========================================================================================================================================================+
| {i1.name}   | ["Online", 1]  | ["Online", 1]  | {i1_uuid} | {i1_replicaset_uuid} | default | {i1_address} |
|---------------+----------------+----------------+--------------------------------------+--------------------------------------+---------+----------------|
| {i2.name}   | ["Offline", 1] | ["Offline", 1] | {i2_uuid} | {i2_replicaset_uuid} | default | {i2_address} |
|---------------+----------------+----------------+--------------------------------------+--------------------------------------+---------+----------------|
| {i3.name}   | ["Offline", 1] | ["Offline", 1] | {i3_uuid} | {i3_replicaset_uuid} | default | {i3_address} |
+---------------+----------------+----------------+--------------------------------------+--------------------------------------+---------+----------------+
(3 rows)
"""  # noqa: E501
    )


def test_picodata_status_exit_code(cluster: Cluster):
    service_password = "T3stP4ssword"
    cluster.set_service_password(service_password)

    password_file = cluster.service_password_file
    assert password_file

    i1 = cluster.add_instance(wait_online=False)
    i1_address = f"{i1.host}:{i1.port}"

    process = subprocess.run(
        [
            cluster.binary_path,
            "status",
            "--peer",
            f"{i1_address}",
            "--service-password-file",
            password_file,
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
            cluster.binary_path,
            "status",
            "--peer",
            f"{i1_address}",
            "--service-password-file",
            password_file,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )

    assert process.returncode == 0

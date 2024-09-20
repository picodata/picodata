import pexpect  # type: ignore
import os
import pytest
import sys
import subprocess
from conftest import Cluster, log_crawler
from tarantool.error import (  # type: ignore
    NetworkError,
)


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
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for andy: ")
    cli.sendline("Testpa55")

    cli.expect_exact(
        f'Connected to interactive console by address "{i1.host}:{i1.port}" under "andy" user'
    )
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("picodata> ")

    # sql console doesn't know about language switching
    cli.sendline("\\lua")
    cli.expect_exact("Unknown special sequence")

    cli.sendline("\\sql")
    cli.expect_exact("Unknown special sequence")

    # for not registried command nothing can happen
    cli.sendline("\\lya")
    cli.expect_exact("Unknown special sequence")
    cli.sendline("\\scl")
    cli.expect_exact("Unknown special sequence")
    cli.sendline("\\set language lua")
    cli.expect_exact("Unknown special sequence")

    # nothing happens for completion
    cli.sendline("\t\t")
    cli.expect_exact("picodata> ")

    # ensure that server responds on correct query
    cli.sendline(
        "CREATE TABLE ids (id INTEGER NOT NULL, PRIMARY KEY(id)) USING MEMTX DISTRIBUTED BY (id)"
    )
    cli.expect_exact("1")
    cli.expect_exact("picodata> ")

    # ensure that server responds on invalid query
    cli.sendline("invalid query")
    cli.expect_exact("rule parsing error")
    cli.expect_exact("picodata> ")

    # ensure that server responds after processing invalid query
    cli.sendline("INSERT INTO ids VALUES(1)")
    cli.expect_exact("1")
    cli.expect_exact("picodata> ")

    cli.sendline("SELECT * FROM ids")
    cli.expect_exact("+----+")
    cli.expect_exact("| id |")
    cli.expect_exact("+====+")
    cli.expect_exact("| 1  |")
    cli.expect_exact("+----+")
    cli.expect_exact("(1 rows)")
    cli.expect_exact("picodata> ")

    cli.sendline("EXPLAIN SELECT * FROM ids")
    cli.expect_exact('projection ("ids"."id"::integer -> "id")')
    cli.expect_exact('scan "ids"')
    cli.expect_exact("execution options:")
    cli.expect_exact("sql_vdbe_max_steps = 45000")
    cli.expect_exact("vtable_max_rows = 5000")

    # hitting enter sends query to the server
    cli.sendline("")
    cli.expect_exact("rule parsing error")


def test_admin_ux(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    cli = pexpect.spawn(
        cwd=i1.data_dir,
        command=i1.binary_path,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=1,
    )

    cli.logfile = sys.stdout

    cli.expect_exact('Connected to admin console by socket path "./admin.sock"')
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("picodata> ")

    # in admin console language switching is availiable
    cli.sendline("\\lua")
    cli.expect_exact("Language switched to Lua")

    cli.sendline("\\sql")
    cli.expect_exact("Language switched to SQL")

    # for not registried command nothing happend
    cli.sendline("\\lya")
    cli.expect_exact("Unknown special sequence")
    cli.sendline("\\scl")
    cli.expect_exact("Unknown special sequence")

    # variations of `\s l sql/lua` is registred, but not in help
    cli.expect_exact("picodata> ")
    cli.sendline("\\set language lua")
    cli.expect_exact("Language switched to Lua")

    cli.expect_exact("picodata> ")
    cli.sendline("\\s lang sql")
    cli.expect_exact("Language switched to SQL")

    # nothing happens on completion in SQL mode
    cli.sendline("\\sql")
    cli.expect_exact("Language switched to SQL")
    cli.sendline("\t\t")
    cli.expect_exact("picodata> ")

    cli.sendline("box.c\t\t")
    cli.expect_exact("rule parsing error:")
    cli.expect_exact("picodata> ")

    # something happens on completion in Lua mode
    cli.sendline("\\lua")
    cli.expect_exact("Language switched to Lua")
    cli.sendline("hel\t")
    cli.expect_exact("picodata> help")


def test_lua_completion(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    cli = pexpect.spawn(
        cwd=i1.data_dir,
        command=i1.binary_path,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("picodata> ")
    cli.sendline("\\lua")

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
        cwd=i1.data_dir,
        command=i1.binary_path,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("picodata> ")

    i1.sql(
        """
        CREATE TABLE "assets"
             ("id" INTEGER NOT NULL,
              "name" TEXT,
              "stock" INTEGER,
              PRIMARY KEY("id")
            )
        DISTRIBUTED BY("id")
        OPTION (TIMEOUT = 3.0)""",
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
        OPTION (TIMEOUT = 3.0)""",
        sudo=True,
    )

    cli.sendline("""EXPLAIN INSERT INTO "assets" VALUES (1, 'Woody', 2561)""")

    cli.expect_exact('insert "assets" on conflict: fail')
    cli.expect_exact('motion [policy: segment([ref("COLUMN_1")])]')
    cli.expect_exact("values")
    cli.expect_exact(
        "value row (data=ROW(1::unsigned, 'Woody'::string, 2561::unsigned))"
    )
    cli.expect_exact("execution options:")
    cli.expect_exact("sql_vdbe_max_steps = 45000")
    cli.expect_exact("vtable_max_rows = 5000")

    cli.sendline("""EXPLAIN UPDATE "characters" SET "year" = 2010""")

    cli.expect_exact('update "characters')
    cli.expect_exact('"year" = "col_0"')
    cli.expect_exact("motion [policy: local]")
    cli.expect_exact(
        'projection (2010::unsigned -> "col_0", "characters"."id"::integer -> "col_1")'
    )
    cli.expect_exact('scan "characters"')
    cli.expect_exact("execution options:")
    cli.expect_exact("sql_vdbe_max_steps = 45000")
    cli.expect_exact("vtable_max_rows = 5000")

    cli.sendline(
        """EXPLAIN UPDATE "characters" SET "name" = 'Etch', "year" = 2010 WHERE "id" = 2;"""
    )

    cli.expect_exact('update "characters"')
    cli.expect_exact('"name" = "col_0"')
    cli.expect_exact('"year" = "col_1"')
    cli.expect_exact("motion [policy: local]")
    cli.expect_exact(
        'projection (\'Etch\'::string -> "col_0", 2010::unsigned -> "col_1", '
        '"characters"."id"::integer -> "col_2")'
    )
    cli.expect_exact('selection ROW("characters"."id"::integer) = ROW(2::unsigned)')
    cli.expect_exact('scan "characters"')
    cli.expect_exact("execution options:")
    cli.expect_exact("sql_vdbe_max_steps = 45000")
    cli.expect_exact("vtable_max_rows = 5000")


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
  rule parsing error:  --> 1:9
    |
  1 |  create table foo
    |         ^---
    |
    = expected Unique

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
        timeout=1,
    )
    cli.logfile = sys.stdout
    cli.expect_exact(
        f'Connected to interactive console by address "{i1.host}:{i1.port}" under "guest" user'
    )
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("picodata> ")

    i1.terminate()
    cli.sendline("ping")
    cli.expect("Connection Error. Try to reconnect: io error: unexpected end of file")
    cli.terminate()

    # test crash error when run with `picodata admin`
    cli = pexpect.spawn(
        cwd=i2.data_dir,
        command=i2.binary_path,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout
    cli.expect_exact("picodata> ")

    i2.terminate()
    cli.sendline("ping")
    cli.expect_exact("Server probably is closed, try to reconnect")


def test_input_with_custom_delimiter(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()
    i1.create_user(with_name="andy", with_password="Testpa55")
    i1.sql('GRANT CREATE TABLE TO "andy"', sudo=True)

    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "andy"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for andy: ")
    cli.sendline("Testpa55")

    cli.expect_exact(
        f'Connected to interactive console by address "{i1.host}:{i1.port}" under "andy" user'
    )
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("picodata> ")

    cli.sendline("\\set delimiter ;")
    cli.expect_exact("Delimiter changed to ';'")

    # several commands in one line
    cli.sendline(
        "CREATE TABLE ids (id INTEGER NOT NULL, PRIMARY KEY(id)) USING MEMTX DISTRIBUTED BY (id);"
        "INSERT INTO ids VALUES(1);"
        "SELECT * FROM ids;"
    )

    cli.expect_exact("1")
    cli.expect_exact("1")
    cli.expect_exact("+----+")
    cli.expect_exact("| id |")
    cli.expect_exact("+====+")
    cli.expect_exact("| 1  |")
    cli.expect_exact("+----+")
    cli.expect_exact("(1 rows)")

    # client doesn't send query until delimiter
    cli.sendline("invalid query")
    cli.expect_exact("picodata> ")
    cli.sendline("waiting until delimiter")
    cli.expect_exact("picodata> ")
    cli.sendline(";")
    cli.expect_exact("rule parsing error:  --> 1:1")

    # treating ';;;' with delimiter ';' as 3 empty queries
    cli.sendline(";;;")
    cli.expect_exact("rule parsing error:  --> 1:1")
    cli.expect_exact("rule parsing error:  --> 1:1")
    cli.expect_exact("rule parsing error:  --> 1:1")

    # reset delimiter works --
    cli.sendline("\\set delimiter default;")
    cli.expect_exact("Delimiter changed to default")

    # ensure that delimiter resets to default - parsed only first statement `create table`
    # which is created
    cli.sendline(
        "CREATE TABLE ids (id INTEGER NOT NULL, PRIMARY KEY(id)) USING MEMTX DISTRIBUTED BY (id);"
        "INSERT INTO ids VALUES(1);"
        "SELECT * FROM ids;"
    )

    cli.expect_exact("0")
    cli.expect_exact("picodata>")


def test_cat_file_to_picodata_admin_stdin(cluster: Cluster):
    instance = cluster.add_instance()
    data = subprocess.check_output(
        [cluster.binary_path, "admin", f"{instance.data_dir}/admin.sock"],
        input=b"""\
\\set delimiter ;

CREATE TABLE ids (id INTEGER NOT NULL, PRIMARY KEY(id))
        USING MEMTX
        DISTRIBUTED BY (id);

INSERT INTO ids
        VALUES(1);

SELECT * FROM ids;

""",
    )

    assert (
        data
        == f"""\
Connected to admin console by socket path "{instance.data_dir}/admin.sock"
type '\\help' for interactive help
Delimiter changed to ';'
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


def test_do_not_ban_admin_via_unix_socket(cluster: Cluster):
    password_file = f"{cluster.data_dir}/service-password.txt"
    with open(password_file, "w") as f:
        print("secret", file=f)

    os.chmod(password_file, 0o600)

    i1 = cluster.add_instance(wait_online=False)
    i1.service_password_file = password_file

    admin_banned_lc = log_crawler(
        i1, "Maximum number of login attempts exceeded; user blocked"
    )
    i1.start()
    i1.wait_online()

    # auth via pico_service many times
    for _ in range(100):
        with pytest.raises(NetworkError):
            i1.sql("try to auth", user="pico_service", password="wrong_password")

    # pico_service is not banned
    data = i1.sql(
        "SELECT name FROM _pico_tier ", user="pico_service", password="secret"
    )

    assert data[0][0] == "default"

    # auth via admin until ban
    for _ in range(5):
        with pytest.raises(NetworkError):
            i1.sql("try to auth", user="admin", password="wrong_password")

    admin_banned_lc.wait_matched()

    cli = pexpect.spawn(
        cwd=i1.data_dir,
        command=i1.binary_path,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=1,
    )

    cli.logfile = sys.stdout
    cli.expect_exact('Connected to admin console by socket path "./admin.sock"')
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("picodata> ")

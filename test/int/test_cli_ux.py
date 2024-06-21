import pexpect  # type: ignore
import sys
from conftest import Cluster


def test_connect_ux(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

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
    cli.expect_exact(
        "getting completions failed: error during IO: Broken pipe (os error 32)"
    )


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

    i1.sudo_sql(
        """
        CREATE TABLE "assets"
             ("id" INTEGER NOT NULL,
              "name" TEXT,
              "stock" INTEGER,
              PRIMARY KEY("id")
            )
        DISTRIBUTED BY("id")
        OPTION (TIMEOUT = 3.0)"""
    )

    i1.sudo_sql(
        """
        CREATE TABLE "characters"
           ("id" INTEGER NOT NULL,
            "name" TEXT NOT NULL,
            "year" INTEGER,
            PRIMARY KEY ("id")
            )
        USING MEMTX DISTRIBUTED BY ("id")
        OPTION (TIMEOUT = 3.0)"""
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
    cli.expect_exact('"year" = COL_0')
    cli.expect_exact("motion [policy: local]")
    cli.expect_exact(
        'projection (2010::unsigned -> COL_0, "characters"."id"::integer -> COL_1)'
    )
    cli.expect_exact('scan "characters"')
    cli.expect_exact("execution options:")
    cli.expect_exact("sql_vdbe_max_steps = 45000")
    cli.expect_exact("vtable_max_rows = 5000")

    cli.sendline(
        """EXPLAIN UPDATE "characters" SET "name" = 'Etch', "year" = 2010 WHERE "id" = 2;"""
    )

    cli.expect_exact('update "characters"')
    cli.expect_exact('"name" = COL_0')
    cli.expect_exact('"year" = COL_1')
    cli.expect_exact("motion [policy: local]")
    cli.expect_exact(
        "projection ('Etch'::string -> COL_0, 2010::unsigned -> COL_1, "
        '"characters"."id"::integer -> COL_2)'
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
    cli.expect_exact(
        "CRITICAL: Server closed the connection unexpectedly. Try to reconnect."
    )

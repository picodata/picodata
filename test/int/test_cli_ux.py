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
    cli.expect_exact("sbroad: rule parsing error:")
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

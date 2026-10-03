from conftest import Postgres
from pg8000.dbapi import Connection, connect
from collections import deque


def setup_connection(postgres: Postgres, connection_options: str = "") -> Connection:
    """
    Function for quick creation of connection with wanted connection options.\n
    Connection is created with user *postgres* (password is **Password1**)
    on default *host* and *port*
    """

    password = "Password1"
    postgres.instance.sql(f"CREATE USER IF NOT EXISTS postgres WITH PASSWORD '{password}'")
    # if no options were passed create a connection without options
    if connection_options.isspace() or connection_options == "":
        return connect(
            user="postgres",
            password=password,
            host=postgres.host,
            port=postgres.port,
        )

    return connect(
        user="postgres",
        password=password,
        host=postgres.host,
        port=postgres.port,
        startup_params={"options": connection_options},
    )


def check_notice_contents(notices: deque, contents: list[str]):
    """
    Function to inspect the messages contents and verify that they
    correspond to certain string.
    """
    # check that we received only one notice response message during connection startup
    assert len(notices) == 1
    msg = notices.popleft()
    # assert that we received a warning message
    assert msg[b"S"].decode("UTF-8") == "WARNING"
    assert msg[b"C"].decode("UTF-8") == "01000"
    # Inspect message content, which can vary because of hash map used for options output.
    # Possible options of the message content are passed through contents parameter
    assert any(msg[b"M"].decode("UTF-8") == "Parsed the following unknown options:\n" + c for c in contents)


class TestNoUnknownOptions:
    """
    Test group for cases that should not send any notice messages related to unknown options
    warning to the client.
    """

    def test_no_options(self, postgres: Postgres):
        conn = setup_connection(postgres)
        assert len(conn.notices) == 0
        conn.close()

    def test_one_known_option(self, postgres: Postgres):
        conn = setup_connection(postgres, "sql_vdbe_opcode_max=0")
        assert len(conn.notices) == 0
        conn.close()

        # test duplicated known option
        conn = setup_connection(postgres, "sql_motion_row_max=10,sql_motion_row_max=0")
        assert len(conn.notices) == 0
        conn.close()

    def test_two_known_options(self, postgres: Postgres):
        conn = setup_connection(postgres, "sql_vdbe_opcode_max=0,sql_motion_row_max=0")
        assert len(conn.notices) == 0
        conn.close()

        conn = setup_connection(
            postgres, "sql_motion_row_max=10,sql_vdbe_opcode_max=0,sql_motion_row_max=60,sql_vdbe_opcode_max=10"
        )
        assert len(conn.notices) == 0
        conn.close()


class TestHandleUnknownOptions:
    def test_empty_unknown_option(self, postgres: Postgres):
        conn = setup_connection(postgres, "=")
        check_notice_contents(conn.notices, ["'empty name' = 'empty value'\n"])
        conn.close()

        # check only empty name
        conn = setup_connection(postgres, "=7")
        check_notice_contents(conn.notices, ["'empty name' = 7\n"])
        conn.close()

        # check only empty value
        conn = setup_connection(postgres, "foo=")
        check_notice_contents(conn.notices, ["foo = 'empty value'\n"])
        conn.close()

    def test_one_unknown_option(self, postgres: Postgres):
        conn = setup_connection(postgres, "foo=foo")
        check_notice_contents(conn.notices, ["foo = foo\n"])
        conn.close()

        # check one duplicated option, expecting the value be overwritten by last entry
        conn = setup_connection(postgres, "foo=foo,foo=10")
        check_notice_contents(conn.notices, ["foo = 10\n"])
        conn.close()

    def test_multiple_unknown_options(self, postgres: Postgres):
        conn = setup_connection(postgres, "foo=foo,bar=bar")
        check_notice_contents(conn.notices, ["foo = foo\nbar = bar\n", "bar = bar\nfoo = foo\n"])
        conn.close()

        # check that every unknown option is overwritten
        conn = setup_connection(postgres, "foo=foo,foo=5,bar=10,bar=foo")
        check_notice_contents(conn.notices, ["foo = 5\nbar = foo\n", "bar = foo\nfoo = 5\n"])
        conn.close()

        # check the the order of definition of option entries doesn't matter
        conn = setup_connection(postgres, "foo=foo,bar=10,foo=5,bar=foo")
        check_notice_contents(conn.notices, ["foo = 5\nbar = foo\n", "bar = foo\nfoo = 5\n"])
        conn.close()

    def test_mixed_options(self, postgres: Postgres):
        conn = setup_connection(postgres, "sql_vdbe_opcode_max=0,foo=foo")
        check_notice_contents(conn.notices, ["foo = foo\n"])
        conn.close()

        # check that definition of unknown option before or between known one doesn't matter
        conn = setup_connection(postgres, "foo=foo,sql_vdbe_opcode_max=0")
        check_notice_contents(conn.notices, ["foo = foo\n"])
        conn.close()

        conn = setup_connection(postgres, "sql_motion_row_max=0,foo=foo,sql_vdbe_opcode_max=0")
        check_notice_contents(conn.notices, ["foo = foo\n"])
        conn.close()

        # check redefinition of unknown option among known ones
        conn = setup_connection(postgres, "sql_motion_row_max=0,foo=foo,sql_vdbe_opcode_max=0,foo=10")
        check_notice_contents(conn.notices, ["foo = 10\n"])
        conn.close()

        # check redefinition of multiple unknown options among known ones
        conn = setup_connection(
            postgres, "sql_motion_row_max=0,foo=foo,sql_vdbe_opcode_max=0,foo=10,bar=5,forward=off,bar=10"
        )
        check_notice_contents(conn.notices, ["foo = 10\nbar = 10\n", "bar = 10\nfoo = 10\n"])
        conn.close()

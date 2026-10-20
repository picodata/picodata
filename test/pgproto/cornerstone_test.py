import pytest
from framework.pgwire import PgClient, PgError, PgStorage
from inline_snapshot import snapshot


def test_extended_ddl(pg_client: PgClient):
    storage = pg_client.storage
    assert storage.counts() == (0, 0)
    ddl = """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
    """
    pg_client.parse("", ddl)
    assert storage.counts() == (1, 0)

    desc = pg_client.describe_stmt("")
    assert desc["param_oids"] == []
    assert desc["columns"] is None

    pg_client.bind("", "portal", [])
    assert storage.counts() == (1, 1)
    data = pg_client.execute("portal")
    assert data["tag"] == "CREATE TABLE"
    assert storage.counts() == (1, 1)

    # close_stmt also closes statement's portals
    pg_client.close_stmt("")
    assert storage.counts() == (0, 0)


def test_extended_dml(pg_client: PgClient):
    storage = pg_client.storage
    instance = pg_client.instance
    instance.sql(
        """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
    """
    )

    assert storage.counts() == (0, 0)
    dml = """ insert into "t" values ($1::int, $2::text) """
    pg_client.parse("", dml)
    assert storage.counts() == (1, 0)

    pg_client.bind("", "", [1, "a"])
    assert storage.counts() == (1, 1)

    desc = pg_client.describe_stmt("")
    # params were not specified, so they are treated as text
    assert desc["param_oids"] == [20, 25]
    assert desc["columns"] is None

    data = pg_client.execute("")
    pg_client.close_portal("")
    assert data["tag"] == "INSERT 0 1"
    assert storage.counts() == (1, 0)

    pg_client.close_stmt("")
    assert storage.counts() == (0, 0)


def test_extended_dql(pg_client: PgClient):
    storage = pg_client.storage
    instance = pg_client.instance
    instance.sql(
        """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
    """
    )

    # First query
    assert storage.counts() == (0, 0)
    dql = """ select * from "t" """
    id1 = "1"
    pg_client.parse(id1, dql)
    assert storage.counts() == (1, 0)

    pg_client.bind(id1, id1, [])
    assert storage.counts() == (1, 1)

    desc = pg_client.describe_stmt(id1)
    assert desc["param_oids"] == []
    assert desc["columns"] == [("key", 20), ("value", 25)]

    data = pg_client.execute(id1)
    pg_client.close_portal(id1)
    assert storage.counts() == (1, 0)
    assert data["rows"] == []
    assert data["tag"] == "SELECT 0"

    # Second query
    dql = """ select "value" as "value_alias" from "t" """
    id2 = "2"
    pg_client.parse(id2, dql)
    assert storage.counts() == (2, 0)

    pg_client.bind(id2, id2, [])
    assert storage.counts() == (2, 1)

    desc = pg_client.describe_stmt(id2)
    assert desc["param_oids"] == []
    assert desc["columns"] == [("value_alias", 25)]

    # Third query
    dql = """ select * from (values (1)) """
    id3 = "3"
    pg_client.parse(id3, dql)
    assert storage.counts() == (3, 1)

    pg_client.bind(id3, id3, [])
    assert storage.counts() == (3, 2)

    desc = pg_client.describe_stmt(id3)
    assert desc["param_oids"] == []
    assert desc["columns"] == [("COLUMN_1", 20)]


def test_extended_errors(pg_client: PgClient):
    sql = """ invalid syntax """
    with pytest.raises(PgError, match="rule parsing error"):
        pg_client.parse("", sql)

    sql = """ select * from "t" """
    with pytest.raises(PgError, match="""table with name "t" not found"""):
        pg_client.parse("", sql)


def test_updates_with_unnamed(pg_client: PgClient):
    storage = pg_client.storage
    sql = """
        create table "t" ("val" int not null, primary key ("val"))
        using vinyl
        distributed by ("val")
    """
    pg_client.parse("", sql)
    pg_client.bind("", "", [])
    pg_client.execute("")
    pg_client.close_portal("")
    assert storage.counts() == (1, 0)

    sql = """
        insert into "t" values (1), (2), (3)
    """
    # update statement
    pg_client.parse("", sql)
    pg_client.bind("", "", [])
    pg_client.execute("")
    pg_client.close_portal("")
    assert storage.counts() == (1, 0)

    sql = """
        select * from "t" where "val" = 1
    """
    # update statement
    pg_client.parse("", sql)
    pg_client.bind("", "", [])
    row_with_one = pg_client.execute("")["rows"][0]
    pg_client.close_portal("")
    assert row_with_one == [1]
    assert storage.counts() == (1, 0)

    # bind portal
    pg_client.bind("", "", [])
    assert storage.counts() == (1, 1)
    sql = """
        select * from "t" where "val" = 2
    """
    pg_client.parse("", sql)
    # update portal
    pg_client.bind("", "", [])
    row_with_two = pg_client.execute("")["rows"][0]
    pg_client.close_portal("")
    assert row_with_two == [2]
    assert storage.counts() == (1, 0)


def test_updates_with_named(pg_client: PgClient):
    name = "named"
    sql = """
        create table "t" ("val" int not null, primary key ("val"))
        using vinyl
        distributed by ("val")
    """
    pg_client.parse(name, sql)
    with pytest.raises(PgError, match=f"Statement '{name}' already exists"):
        pg_client.parse(name, sql)

    pg_client.bind(name, name, [])
    with pytest.raises(PgError, match=f"Portal '{name}' already exists"):
        pg_client.bind(name, name, [])


def test_close_nonexistent(pg_client: PgClient):
    assert pg_client.storage.counts() == (0, 0)

    pg_client.close_portal("")
    pg_client.close_stmt("")
    pg_client.close_portal("nonexistent")
    pg_client.close_stmt("nonexistent")


def test_statement_close(pg_client: PgClient):
    storage = pg_client.storage
    sql = """
        create table "t" ("val" int not null, primary key ("val"))
        using vinyl
        distributed by ("val")
    """
    pg_client.parse("", sql)
    assert storage.counts() == (1, 0)

    pg_client.bind("", "1", [])
    pg_client.bind("", "2", [])
    pg_client.bind("", "3", [])
    assert storage.counts() == (1, 3)

    # close_stmt also closes statement's portals
    pg_client.close_stmt("")
    assert storage.counts() == (0, 0)
    assert not pg_client.has_statement("")

    pg_client.parse("", sql)
    assert storage.counts() == (1, 0)

    pg_client.bind("", "1", [])
    pg_client.bind("", "2", [])
    pg_client.bind("", "3", [])
    assert storage.counts() == (1, 3)

    # update for unnamed == close_stmt("") + parse("", sql)
    pg_client.parse("", sql)
    assert storage.counts() == (1, 0)
    assert pg_client.has_statement("")


def test_visibility(pg_storage: PgStorage):
    instance = pg_storage.instance
    client1 = pg_storage.new_client()
    client2 = pg_storage.new_client()

    ddl = """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
    """
    instance.sql(ddl)

    sql = """ select * from "t" """
    client1.parse("", sql)
    assert pg_storage.counts() == (1, 0)

    client1.bind("", "", [])
    assert pg_storage.counts() == (1, 1)

    # Client 2 has no names of client 1, so its Close does nothing.
    client2.close_portal("")
    client2.close_stmt("")
    assert pg_storage.counts() == (1, 1)
    assert client1.describe_portal("") == [("key", 20), ("value", 25)]
    assert not client2.has_statement("")
    with pytest.raises(PgError, match="Couldn't find portal"):
        client2.describe_portal("")

    client1.close_portal("")
    assert pg_storage.counts() == (1, 0)
    assert client1.has_statement("")

    client1.close_stmt("")
    assert pg_storage.counts() == (0, 0)
    assert not client1.has_statement("")


def test_param_oids(pg_client: PgClient):
    instance = pg_client.instance
    instance.sql(
        """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
    """
    )

    sql = """ insert into "t" values ($1, $2) """
    pg_client.parse("", sql, [20, 25])
    pg_client.bind("", "", [1, "a"])
    desc = pg_client.describe_stmt("")
    assert desc["param_oids"] == [20, 25]
    assert desc["columns"] is None

    sql = """ insert into "t" values ($1, $2) """
    pg_client.parse("", sql, [20, 25])
    pg_client.bind("", "", [1, "a"])
    desc = pg_client.describe_stmt("")
    assert desc["param_oids"] == [20, 25]
    assert desc["columns"] is None

    sql = """ insert into "t" values (1, $1) """
    pg_client.parse("", sql, [25])
    pg_client.bind("", "", ["a"])
    desc = pg_client.describe_stmt("")
    assert desc["param_oids"] == [25]
    assert desc["columns"] is None

    sql = """ insert into "t" values ($1::int, $2::text) """
    pg_client.parse("", sql)
    pg_client.bind("", "", [1, "a"])
    desc = pg_client.describe_stmt("")
    assert desc["param_oids"] == [20, 25]
    assert desc["columns"] is None


def test_interactive_portals(pg_client: PgClient):
    instance = pg_client.instance
    instance.sql(
        """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
    """
    )

    sql = """ select * from "t" """
    pg_client.parse("", sql)
    pg_client.bind("", "", [])
    # 0 - fetch all
    data = pg_client.execute("", 0)
    assert data["rows"] == []
    assert data["is_finished"] is True

    sql = """ insert into "t" values (1, 'kek'), (2, 'lol') """
    instance.sql(sql)

    sql = """ select * from "t" """
    pg_client.parse("", sql)
    pg_client.bind("", "", [])
    data = pg_client.execute("", 0)
    assert len(data["rows"]) == 2
    assert [1, "kek"] in data["rows"]
    assert [2, "lol"] in data["rows"]
    assert data["is_finished"] is True

    sql = """ select * from "t" """
    pg_client.parse("", sql)
    pg_client.bind("", "", [])
    data = pg_client.execute("", 1)
    assert len(data["rows"]) == 1
    assert [1, "kek"] in data["rows"] or [2, "lol"] in data["rows"]
    assert data["is_finished"] is False
    data = pg_client.execute("", 1)
    assert len(data["rows"]) == 1
    assert [1, "kek"] in data["rows"] or [2, "lol"] in data["rows"]
    assert data["is_finished"] is True

    with pytest.raises(PgError, match="Can't execute portal in state Done"):
        data = pg_client.execute("", 1)

    sql = """ explain select * from "t" """
    pg_client.parse("", sql)
    pg_client.bind("", "", [])

    data = pg_client.execute("", 1)
    assert "\n".join(row[0] for row in data["rows"]) == snapshot(
        "──────────────────────────────────────────────────────────────────────"
    )
    assert data["is_finished"] is False

    data = pg_client.execute("", 0)
    assert "\n".join(row[0] for row in data["rows"]) == snapshot("""\
 # Logical plan                                                       \n\
──────────────────────────────────────────────────────────────────────

projection (t.key::int -> key, t.value::text -> value)
  scan t

──────────────────────────────────────────────────────────────────────
 # Buckets                                                            \n\
──────────────────────────────────────────────────────────────────────

buckets <= [1-3000]\
""")
    assert data["is_finished"] is True

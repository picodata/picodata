from conftest import PgStorage, PgClient, ReturnError
import pytest


def test_extended_ddl(pg_client: PgClient):
    assert len(pg_client.statements["available"]) == 0
    assert len(pg_client.portals["available"]) == 0
    ddl = """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
        option (timeout = 3)
    """
    pg_client.parse("", ddl)
    assert len(pg_client.statements["available"]) == 1
    assert len(pg_client.portals["available"]) == 0

    desc = pg_client.describe_stmt("")
    assert desc["param_oids"] == []
    assert desc["query_type"] == 1
    assert desc["command_tag"] == 2
    assert desc["metadata"] == []

    pg_client.bind("", "portal", [], [])
    assert len(pg_client.portals["available"]) == 1
    data = pg_client.execute("portal")
    assert data["row_count"] is None
    assert len(pg_client.statements["available"]) == 1

    pg_client.close_stmt("")
    assert len(pg_client.statements["available"]) == 0


def test_extended_dml(pg_client: PgClient):
    instance = pg_client.instance
    instance.sql(
        """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
        option (timeout = 3)
    """
    )

    assert len(pg_client.statements["available"]) == 0
    assert len(pg_client.portals["available"]) == 0
    dml = """ insert into "t" values (?, ?) """
    pg_client.parse("", dml)
    assert len(pg_client.statements["available"]) == 1
    assert len(pg_client.portals["available"]) == 0

    pg_client.bind("", "", [1, "a"], [])
    assert len(pg_client.portals["available"]) == 1

    desc = pg_client.describe_stmt("")
    # params were not specified, so they are treated as text
    assert desc["param_oids"] == [25, 25]
    assert desc["query_type"] == 2
    assert desc["command_tag"] == 9
    assert desc["metadata"] == []

    data = pg_client.execute("")
    pg_client.close_portal("")
    assert data["row_count"] == 1
    assert len(pg_client.statements["available"]) == 1
    assert len(pg_client.portals["available"]) == 0

    pg_client.close_stmt("")
    assert len(pg_client.statements["available"]) == 0


def test_extended_dql(pg_client: PgClient):
    instance = pg_client.instance
    instance.sql(
        """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
        option (timeout = 3)
    """
    )

    # First query
    assert len(pg_client.statements["available"]) == 0
    assert len(pg_client.portals["available"]) == 0
    dql = """ select * from "t" """
    id1 = "1"
    pg_client.parse(id1, dql)
    assert len(pg_client.statements["available"]) == 1
    assert len(pg_client.portals["available"]) == 0

    pg_client.bind(id1, id1, [], [])
    assert len(pg_client.statements["available"]) == 1
    assert len(pg_client.portals["available"]) == 1

    desc = pg_client.describe_stmt(id1)
    assert desc["param_oids"] == []
    assert desc["query_type"] == 3
    assert desc["command_tag"] == 12
    assert desc["metadata"] == [
        {"name": '"key"', "type": "integer"},
        {"name": '"value"', "type": "string"},
    ]

    data = pg_client.execute(id1)
    pg_client.close_portal(id1)
    assert len(pg_client.statements["available"]) == 1
    assert len(pg_client.portals["available"]) == 0
    assert data["rows"] == []

    # Second query
    dql = """ select "value" as "value_alias" from "t" """
    id2 = "2"
    pg_client.parse(id2, dql)
    assert len(pg_client.statements["available"]) == 2
    assert len(pg_client.portals["available"]) == 0

    pg_client.bind(id2, id2, [], [])
    assert len(pg_client.statements["available"]) == 2
    assert len(pg_client.portals["available"]) == 1

    desc = pg_client.describe_stmt(id2)
    assert desc["param_oids"] == []
    assert desc["query_type"] == 3
    assert desc["command_tag"] == 12
    assert desc["metadata"] == [{"name": '"value_alias"', "type": "string"}]

    # Third query
    dql = """ select * from (values (1)) """
    id3 = "3"
    pg_client.parse(id3, dql)
    assert len(pg_client.statements["available"]) == 3
    assert len(pg_client.portals["available"]) == 1

    pg_client.bind(id3, id3, [], [])
    assert len(pg_client.statements["available"]) == 3
    assert len(pg_client.portals["available"]) == 2

    desc = pg_client.describe_stmt(id3)
    assert desc["param_oids"] == []
    assert desc["query_type"] == 3
    assert desc["command_tag"] == 12
    assert desc["metadata"] == [{"name": '"COLUMN_1"', "type": "unsigned"}]


def test_extended_errors(pg_client: PgClient):
    sql = """ invalid syntax """
    with pytest.raises(ReturnError, match="rule parsing error"):
        pg_client.parse("", sql)

    sql = """ select * from "t" """
    with pytest.raises(ReturnError, match="""table with name "t" not found"""):
        pg_client.parse("", sql)


def test_updates_with_unnamed(pg_client: PgClient):
    sql = """
        create table "t" ("val" int not null, primary key ("val"))
        using vinyl
        distributed by ("val")
        option (timeout = 3)
    """
    pg_client.parse("", sql)
    pg_client.bind("", "", [], [])
    pg_client.execute("")
    pg_client.close_portal("")
    assert len(pg_client.statements["available"]) == 1
    assert len(pg_client.portals["available"]) == 0

    sql = """
        insert into "t" values (1), (2), (3)
    """
    # update statement
    pg_client.parse("", sql)
    pg_client.bind("", "", [], [])
    pg_client.execute("")
    pg_client.close_portal("")
    assert len(pg_client.statements["available"]) == 1
    assert len(pg_client.portals["available"]) == 0

    sql = """
        select * from "t" where "val" = 1
    """
    # update statement
    pg_client.parse("", sql)
    pg_client.bind("", "", [], [])
    row_with_one = pg_client.execute("")["rows"][0]
    pg_client.close_portal("")
    assert row_with_one == [1]
    assert len(pg_client.statements["available"]) == 1
    assert len(pg_client.portals["available"]) == 0

    # bind portal
    pg_client.bind("", "", [], [])
    assert len(pg_client.portals["available"]) == 1
    sql = """
        select * from "t" where "val" = 2
    """
    pg_client.parse("", sql)
    # update portal
    pg_client.bind("", "", [], [])
    row_with_two = pg_client.execute("")["rows"][0]
    pg_client.close_portal("")
    assert row_with_two == [2]
    assert len(pg_client.statements["available"]) == 1
    assert len(pg_client.portals["available"]) == 0


def test_updates_with_named(pg_client: PgClient):
    name = "named"
    sql = """
        create table "t" ("val" int not null, primary key ("val"))
        using vinyl
        distributed by ("val")
        option (timeout = 3)
    """
    pg_client.parse(name, sql)
    with pytest.raises(ReturnError, match=f"Duplicated name '{name}'"):
        pg_client.parse(name, sql)

    pg_client.bind(name, name, [], [])
    with pytest.raises(ReturnError, match=f"Duplicated name '{name}'"):
        pg_client.bind(name, name, [], [])


def test_close_nonexistent(pg_client: PgClient):
    try:
        assert len(pg_client.statements["available"]) == 0
        assert len(pg_client.portals["available"]) == 0

        pg_client.close_portal("")
        pg_client.close_stmt("")
        pg_client.close_portal("nonexistent")
        pg_client.close_stmt("nonexistent")

    except Exception:
        pytest.fail("close causes errors")


def test_statement_close(pg_client: PgClient):
    sql = """
        create table "t" ("val" int not null, primary key ("val"))
        using vinyl
        distributed by ("val")
        option (timeout = 3)
    """
    pg_client.parse("", sql)
    assert len(pg_client.statements["available"]) == 1
    assert len(pg_client.portals["available"]) == 0

    pg_client.bind("", "1", [], [])
    pg_client.bind("", "2", [], [])
    pg_client.bind("", "3", [], [])
    assert len(pg_client.statements["available"]) == 1
    assert len(pg_client.portals["available"]) == 3

    # close_stmt also closes statement's portals
    pg_client.close_stmt("")
    assert pg_client.statements == {"available": [], "total": 0}
    assert pg_client.portals == {"available": [], "total": 0}

    pg_client.parse("", sql)
    assert len(pg_client.statements["available"]) == 1
    assert len(pg_client.portals["available"]) == 0

    pg_client.bind("", "1", [], [])
    pg_client.bind("", "2", [], [])
    pg_client.bind("", "3", [], [])
    assert len(pg_client.portals["available"]) == 3

    # update for unnamed == close_stmt("") + parse("", sql)
    pg_client.parse("", sql)
    assert pg_client.statements == {"available": [""], "total": 1}
    assert pg_client.portals == {"available": [], "total": 0}


def test_visibility(pg_storage: PgStorage):
    instance = pg_storage.instance
    client1 = pg_storage.new_client(1)
    client2 = pg_storage.new_client(2)

    ddl = """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
        option (timeout = 3)
    """
    instance.sql(ddl)

    sql = """ select * from "t" """
    client1.parse("", sql)
    assert client1.statements == {"available": [""], "total": 1}
    assert client2.statements == {"available": [], "total": 1}
    assert client1.portals == {"available": [], "total": 0}
    assert client2.portals == {"available": [], "total": 0}

    client1.bind("", "", [], [])
    assert client1.statements == {"available": [""], "total": 1}
    assert client2.statements == {"available": [], "total": 1}
    assert client1.portals == {"available": [""], "total": 1}
    assert client2.portals == {"available": [], "total": 1}

    client2.close_portal("")
    client2.close_stmt("")
    assert client1.statements == {"available": [""], "total": 1}
    assert client2.statements == {"available": [], "total": 1}
    assert client1.portals == {"available": [""], "total": 1}
    assert client2.portals == {"available": [], "total": 1}

    client1.close_portal("")
    assert client1.statements == {"available": [""], "total": 1}
    assert client2.statements == {"available": [], "total": 1}
    assert client1.portals == {"available": [], "total": 0}
    assert client2.portals == {"available": [], "total": 0}

    client1.close_stmt("")
    assert client1.statements == {"available": [], "total": 0}
    assert client2.statements == {"available": [], "total": 0}
    assert client1.portals == {"available": [], "total": 0}
    assert client2.portals == {"available": [], "total": 0}


def test_param_oids(pg_client: PgClient):
    instance = pg_client.instance
    instance.sql(
        """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
        option (timeout = 3)
    """
    )

    sql = """ insert into "t" values (?, ?) """
    pg_client.parse("", sql, [1, 2])
    pg_client.bind("", "", [1, "a"], [])
    desc = pg_client.describe_stmt("")
    assert desc["param_oids"] == [1, 2]
    assert desc["query_type"] == 2
    assert desc["command_tag"] == 9
    assert desc["metadata"] == []

    sql = """ insert into "t" values (?, ?) """
    pg_client.parse("", sql, [42, 42])
    pg_client.bind("", "", [1, "a"], [])
    desc = pg_client.describe_stmt("")
    assert desc["param_oids"] == [42, 42]
    assert desc["query_type"] == 2
    assert desc["command_tag"] == 9
    assert desc["metadata"] == []

    sql = """ insert into "t" values (1, ?) """
    pg_client.parse("", sql, [1])
    pg_client.bind("", "", [1, "a"], [])
    desc = pg_client.describe_stmt("")
    assert desc["param_oids"] == [1]
    assert desc["query_type"] == 2
    assert desc["command_tag"] == 9
    assert desc["metadata"] == []

    sql = """ insert into "t" values (?, ?) """
    pg_client.parse("", sql)
    pg_client.bind("", "", [1, "a"], [])
    desc = pg_client.describe_stmt("")
    assert desc["param_oids"] == [25, 25]
    assert desc["query_type"] == 2
    assert desc["command_tag"] == 9
    assert desc["metadata"] == []


def test_interactive_portals(pg_client: PgClient):
    instance = pg_client.instance
    instance.sql(
        """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
        option (timeout = 3)
    """
    )

    sql = """ select * from "t" """
    pg_client.parse("", sql)
    pg_client.bind("", "", [], [])
    data = pg_client.execute("", -1)
    assert data["rows"] == []
    assert data["is_finished"] is True

    sql = """ insert into "t" values (1, 'kek'), (2, 'lol') """
    instance.sql(sql)

    sql = """ select * from "t" """
    pg_client.parse("", sql)
    pg_client.bind("", "", [], [])
    # -1 - fetch all
    data = pg_client.execute("", -1)
    assert len(data["rows"]) == 2
    assert [1, "kek"] in data["rows"]
    assert [2, "lol"] in data["rows"]
    assert data["is_finished"] is True

    sql = """ select * from "t" """
    pg_client.parse("", sql)
    pg_client.bind("", "", [], [])
    data = pg_client.execute("", 1)
    assert len(data["rows"]) == 1
    assert [1, "kek"] in data["rows"] or [2, "lol"] in data["rows"]
    assert data["is_finished"] is False
    data = pg_client.execute("", 1)
    assert len(data["rows"]) == 1
    assert [1, "kek"] in data["rows"] or [2, "lol"] in data["rows"]
    assert data["is_finished"] is True

    with pytest.raises(ReturnError, match="Can't execute portal in state Finished"):
        data = pg_client.execute("", 1)

    sql = """ explain select * from "t" """
    pg_client.parse("", sql)
    pg_client.bind("", "", [], [])
    data = pg_client.execute("", 1)
    assert len(data["rows"]) == 1
    assert [
        """projection ("t"."key"::integer -> "key", "t"."value"::string -> "value")"""
    ] == data["rows"][0]
    assert data["is_finished"] is False
    data = pg_client.execute("", -1)
    assert len(data["rows"]) == 4
    assert ["""    scan "t\""""] == data["rows"][0]
    assert ["""execution options:"""] == data["rows"][1]
    assert ["""sql_vdbe_max_steps = 45000"""] == data["rows"][2]
    assert ["""vtable_max_rows = 5000"""] == data["rows"][3]
    assert data["is_finished"] is True

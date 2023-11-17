from conftest import Instance, PortalStorage, ReturnError
import pytest


def test_extended_ddl(pg_portals: PortalStorage):
    assert len(pg_portals.descriptors["available"]) == 0
    ddl = """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
        option (timeout = 3)
    """
    id = pg_portals.parse(ddl)
    assert len(pg_portals.descriptors["available"]) == 1

    desc = pg_portals.describe(id)
    assert desc["query_type"] == 1
    assert desc["command_tag"] == 2
    assert desc["metadata"] == []

    data = pg_portals.execute(id)
    assert data["row_count"] == 1
    assert len(pg_portals.descriptors["available"]) == 1

    pg_portals.close(id)
    assert len(pg_portals.descriptors["available"]) == 0


def test_extended_dml(pg_portals: PortalStorage):
    instance = pg_portals.instance
    instance.sql(
        """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
        option (timeout = 3)
    """
    )

    assert len(pg_portals.descriptors["available"]) == 0
    dml = """ insert into "t" values (?, ?) """
    id = pg_portals.parse(dml)
    assert len(pg_portals.descriptors["available"]) == 1

    pg_portals.bind(id, [1, "a"])

    desc = pg_portals.describe(id)
    assert desc["query_type"] == 2
    assert desc["command_tag"] == 9
    assert desc["metadata"] == []

    data = pg_portals.execute(id)
    assert data["row_count"] == 1
    assert len(pg_portals.descriptors["available"]) == 1

    pg_portals.close(id)
    assert len(pg_portals.descriptors["available"]) == 0


def test_extended_dql(pg_portals: PortalStorage):
    instance = pg_portals.instance
    instance.sql(
        """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
        option (timeout = 3)
    """
    )

    # First query
    assert len(pg_portals.descriptors["available"]) == 0
    dql = """ select * from "t" """
    id1 = pg_portals.parse(dql)
    assert len(pg_portals.descriptors["available"]) == 1

    pg_portals.bind(id1, [])

    desc = pg_portals.describe(id1)
    assert desc["query_type"] == 3
    assert desc["command_tag"] == 12
    assert desc["metadata"] == [
        {"name": '"key"', "type": "integer"},
        {"name": '"value"', "type": "string"},
    ]

    data = pg_portals.execute(id1)
    assert data["rows"] == []

    # Second query
    dql = """ select "value" as "value_alias" from "t" """
    id2 = pg_portals.parse(dql)
    assert len(pg_portals.descriptors["available"]) == 2

    pg_portals.bind(id2, [])

    desc = pg_portals.describe(id2)
    assert desc["query_type"] == 3
    assert desc["command_tag"] == 12
    assert desc["metadata"] == [{"name": '"value_alias"', "type": "string"}]

    # Third query
    dql = """ select * from (values (1)) """
    id3 = pg_portals.parse(dql)
    assert len(pg_portals.descriptors["available"]) == 3

    pg_portals.bind(id3, [])

    desc = pg_portals.describe(id3)
    assert desc["query_type"] == 3
    assert desc["command_tag"] == 12
    assert desc["metadata"] == [{"name": '"COLUMN_1"', "type": "unsigned"}]

    # Flush the cache
    pg_portals.flush()
    assert len(pg_portals.descriptors["available"]) == 0


def test_extended_errors(pg_portals: PortalStorage):
    sql = """ invalid syntax """
    with pytest.raises(ReturnError, match="rule parsing error"):
        pg_portals.parse(sql)

    sql = """ select * from "t" """
    with pytest.raises(ReturnError, match="space t not found"):
        pg_portals.parse(sql)


def test_portal_visibility(instance: Instance):
    ddl = """
        create table "t" ("key" int not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
        option (timeout = 3)
    """
    instance.sql(ddl)

    sql = """ select * from "t" """
    id = instance.eval(
        f"""
        box.session.su("admin")
        local res = pico.pg_parse([[{sql}]])
        box.session.su("guest")
        return res
        """
    )
    admin_portals = instance.eval(
        """ return box.session.su("admin", pico.pg_portals) """
    )
    assert admin_portals == {"available": [id], "total": 1}
    guest_portals = instance.eval(
        """ return box.session.su("guest", pico.pg_portals) """
    )
    assert guest_portals == {"available": [], "total": 1}
    with pytest.raises(ReturnError, match="No such descriptor"):
        instance.eval(f""" return box.session.su("guest", pico.pg_close, {id}) """)
    assert instance.eval(f""" return box.session.su("admin", pico.pg_close, {id}) """)

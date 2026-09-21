from typing import TypedDict

import psycopg
import pytest
from conftest import Cluster
from inline_snapshot import snapshot


class AsUser(TypedDict):
    user: str
    password: str


as_alice: AsUser = {
    "user": "alice",
    "password": "T0psecret",
}

as_bob: AsUser = {
    "user": "bob",
    "password": "T0tallysecret",
}


def test_current_user_is_resolved_per_session(cluster: Cluster):
    i1, _ = cluster.deploy(instance_count=2)
    i1.sql("""create user "alice" with password 'T0psecret' using chap-sha1""")
    i1.sql("""create user "bob" with password 'T0tallysecret' using chap-sha1""")

    # The query plan is cached by the query text and shared between
    # the users, so the user name must not be frozen in the cached plan.
    query = "SELECT current_user"
    assert i1.sql(query, **as_alice) == [["alice"]]
    assert i1.sql(query, **as_bob) == [["bob"]]
    assert i1.sql(query, **as_alice) == [["alice"]]
    assert i1.sql(query, sudo=True) == [["admin"]]

    # Check the metadata: CURRENT_USER is a string.
    result = i1.sql(query, strip_metadata=False, **as_alice)
    assert result["metadata"] == [{"name": "col_1", "type": "string"}]

    # EXPLAIN shows the user name substituted on execute.
    explain = "EXPLAIN SELECT current_user"
    assert "\n".join(i1.sql(explain, **as_alice)) == snapshot("""\
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       \n\
──────────────────────────────────────────────────────────────────────

projection ('alice'::string -> col_1)

──────────────────────────────────────────────────────────────────────
 # Buckets                                                            \n\
──────────────────────────────────────────────────────────────────────

buckets = any\
""")
    assert "\n".join(i1.sql(explain, **as_bob)) == snapshot("""\
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       \n\
──────────────────────────────────────────────────────────────────────

projection ('bob'::string -> col_1)

──────────────────────────────────────────────────────────────────────
 # Buckets                                                            \n\
──────────────────────────────────────────────────────────────────────

buckets = any\
""")


def test_current_user_via_pgproto(cluster: Cluster):
    (i1,) = cluster.deploy(instance_count=1)
    # pgproto doesn't support chap-sha1, so the default auth method is used.
    i1.sql("""create user "alice" with password 'T0psecret'""")
    i1.sql("""create user "bob" with password 'T0tallysecret'""")

    for creds in (as_alice, as_bob):
        user = creds["user"]
        with i1.connect_via_pgproto(**creds) as conn:
            conn.autocommit = True

            # Simple query protocol.
            assert conn.execute("SELECT CURRENT_USER").fetchall() == [(user,)]

            # Extended query protocol: the statement is prepared once,
            # but the user name is resolved on each bind.
            for _ in range(2):
                assert conn.execute("SELECT current_user, %s::int", (1,), prepare=True).fetchall() == [(user, 1)]

            # CURRENT_USER is described as a text column.
            cur = conn.execute("SELECT current_user")
            assert cur.description is not None
            assert cur.description[0].type_code == psycopg.adapters.types["text"].oid


def test_current_user_in_raw_explain(cluster: Cluster):
    i1, _ = cluster.deploy(instance_count=2)
    i1.sql("""create user "alice" with password 'T0psecret' using chap-sha1""")
    i1.sql("""create user "bob" with password 'T0tallysecret' using chap-sha1""")
    i1.sql("""create table notes (owner string primary key, note string) distributed by (owner)""")
    i1.sql("""grant read on table notes to "alice" """, sudo=True)
    i1.sql("""grant read on table notes to "bob" """, sudo=True)

    # The local SQL gets the user name as a constant.
    assert "\n".join(i1.sql("EXPLAIN (RAW) SELECT current_user", **as_alice)) == snapshot("""\
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯

SELECT CAST('alice' AS string) as "col_1"

plan:
    [0] TRIVIAL\
""")

    # As the user name is known on bind, the filter by the sharding key
    # is dispatched to a single replicaset only.
    query = "EXPLAIN (RAW) SELECT note FROM notes WHERE owner = current_user"
    assert "\n".join(i1.sql(query, **as_alice)) == snapshot("""\
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/2) │
╰────────────────────────────────────────╯

SELECT "notes"."note" FROM "notes" WHERE "notes"."owner" = CAST('alice' AS string)

plan:
    [0] SEARCH TABLE notes USING PRIMARY KEY (owner=?) (~1 row)\
""")
    assert "\n".join(i1.sql(query, **as_bob)) == snapshot("""\
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/2) │
╰────────────────────────────────────────╯

SELECT "notes"."note" FROM "notes" WHERE "notes"."owner" = CAST('bob' AS string)

plan:
    [0] SEARCH TABLE notes USING PRIMARY KEY (owner=?) (~1 row)\
""")


def test_current_user_requires_no_privileges(cluster: Cluster):
    (i1,) = cluster.deploy(instance_count=1)
    # pgproto doesn't support chap-sha1, so the default auth method is used.
    i1.sql("""create user "alice" with password 'T0psecret'""")

    with i1.connect_via_pgproto(**as_alice) as conn:
        conn.autocommit = True

        # A new user has no privileges on tables, the system ones included.
        with pytest.raises(psycopg.Error, match="Read access to space '_pico_user' is denied for user 'alice'"):
            conn.execute("SELECT name FROM _pico_user")

        # Still, the user can get its own name.
        assert conn.execute("SELECT current_user").fetchall() == [("alice",)]


def test_current_user_preserves_name_case(cluster: Cluster):
    (i1,) = cluster.deploy(instance_count=1)
    # Like in PostgreSQL, quoted names keep their case,
    # while unquoted ones are folded to lower case.
    i1.sql("""create user "Alice" with password 'T0psecret' using chap-sha1""")
    i1.sql("""create user Bob with password 'T0tallysecret' using chap-sha1""")

    query = "SELECT current_user, current_user = 'alice', current_user = 'Alice'"
    assert i1.sql(query, user="Alice", password="T0psecret") == [["Alice", False, True]]
    assert i1.sql(query, user="bob", password="T0tallysecret") == [["bob", False, False]]


def test_current_user_in_procedure_body(cluster: Cluster):
    i1, _ = cluster.deploy(instance_count=2)
    i1.sql("""create user "alice" with password 'T0psecret' using chap-sha1""")
    i1.sql("""create user "bob" with password 'T0tallysecret' using chap-sha1""")
    i1.sql("""grant create table to "alice" """, sudo=True)
    i1.sql("""grant create procedure to "alice" """, sudo=True)

    i1.sql("""create table log (id int primary key, who string)""", **as_alice)
    i1.sql(
        """create procedure log_me(int) language sql
        as $$ insert into log values ($1, current_user) $$""",
        **as_alice,
    )
    i1.sql("""grant execute on procedure "log_me" to "bob" """, **as_alice)
    i1.sql("""grant write on table log to "bob" """, **as_alice)

    # CURRENT_USER in the procedure body is resolved on each call,
    # so it is the caller, not the owner of the procedure.
    i1.sql("CALL log_me(1)", **as_alice)
    i1.sql("CALL log_me(2)", **as_bob)
    assert i1.sql("SELECT * FROM log ORDER BY id", **as_alice) == [[1, "alice"], [2, "bob"]]


def test_current_user_filters_rows(cluster: Cluster):
    i1, _ = cluster.deploy(instance_count=2)
    i1.sql("""create user "alice" with password 'T0psecret' using chap-sha1""")
    i1.sql("""create user "bob" with password 'T0tallysecret' using chap-sha1""")

    i1.sql("""create table notes (id int primary key, owner string, note string)""")
    i1.sql("""grant read on table notes to "alice" """, sudo=True)
    i1.sql("""grant read on table notes to "bob" """, sudo=True)
    i1.sql("""grant write on table notes to "alice" """, sudo=True)
    i1.sql("""grant write on table notes to "bob" """, sudo=True)

    insert = "INSERT INTO notes VALUES ($1, current_user, $2)"
    i1.sql(insert, 1, "alice's first", **as_alice)
    i1.sql(insert, 2, "bob's first", **as_bob)
    i1.sql(insert, 3, "alice's second", **as_alice)

    select = "SELECT id, note FROM notes WHERE owner = current_user ORDER BY id"
    assert i1.sql(select, **as_alice) == [[1, "alice's first"], [3, "alice's second"]]
    assert i1.sql(select, **as_bob) == [[2, "bob's first"]]

    group_by = "SELECT owner = current_user, count(*) FROM notes GROUP BY owner = current_user ORDER BY 1"
    assert i1.sql(group_by, **as_alice) == [[False, 1], [True, 2]]
    assert i1.sql(group_by, **as_bob) == [[False, 2], [True, 1]]

import signal
import pytest
import json

from typing import Generator

from tarantool.error import (  # type: ignore
    NetworkError,
)

from conftest import (
    MAX_LOGIN_ATTEMPTS,
    Instance,
    Cluster,
    Retriable,
    retrying,
)


class AuditFile:
    def __init__(self, path):
        self._f = open(path)

    def events(self) -> Generator[dict, None, None]:
        for line in self._f:
            yield json.loads(line)


def take_until_title(events, title: str) -> dict | None:
    for event in events:
        if event["title"] == title:
            return event
    return None


def test_startup(instance: Instance):
    instance.start()
    instance.terminate()

    events = list(AuditFile(instance.audit_flag_value).events())
    assert len(events) > 0

    # Check identifiers
    i = 1
    event: dict | None
    for event in events:
        assert event["id"] == f"1.0.{i}"
        i += 1

    # These should be the first two events
    assert events[0]["title"] == "init_audit"
    assert events[0]["message"] == "audit log is ready"
    assert events[0]["severity"] == "low"
    assert events[1]["title"] == "local_startup"
    assert events[1]["message"] == "instance is starting"
    assert events[1]["severity"] == "low"

    event = take_until_title(iter(events), "join_instance")
    assert event is not None
    assert event["instance_id"] == "i1"
    assert event["raft_id"] == "1"
    assert event["initiator"] == "admin"

    event = take_until_title(iter(events), "change_target_grade")
    assert event is not None
    assert event["new_grade"] == "Offline(0)"
    assert event["instance_id"] == "i1"
    assert event["raft_id"] == "1"
    assert (
        event["message"]
        == f"target grade of instance `{event['instance_id']}` changed to {event['new_grade']}"
    )
    assert event["severity"] == "low"
    assert event["initiator"] == "admin"

    event = take_until_title(iter(events), "change_current_grade")
    assert event is not None
    assert event["new_grade"] == "Offline(0)"
    assert event["instance_id"] == "i1"
    assert event["raft_id"] == "1"
    assert (
        event["message"]
        == f"current grade of instance `{event['instance_id']}` changed to {event['new_grade']}"
    )
    assert event["severity"] == "medium"
    assert event["initiator"] == "admin"

    event = take_until_title(iter(events), "change_config")
    assert event is not None
    assert event["initiator"] == "admin"


def test_recover_database(instance: Instance):
    instance.start()
    instance.wait_online()

    instance.restart()
    instance.wait_online()
    instance.terminate()

    events = AuditFile(instance.audit_flag_value).events()

    # At first local db (data storage) is initialized
    create_db = take_until_title(events, "create_local_db")
    assert create_db is not None
    assert create_db["initiator"] == "admin"
    assert create_db["instance_id"] == "i1"
    assert create_db["raft_id"] == "1"

    # On restart instance recovers it's local data
    event = take_until_title(iter(events), "recover_local_db")
    assert event is not None
    assert event["initiator"] == "admin"
    assert event["instance_id"] == "i1"
    assert event["raft_id"] == "1"


def test_create_drop_table(instance: Instance):
    instance.start()
    instance.sql(
        """
        create table "foo" ("val" int not null, primary key ("val"))
        distributed by ("val")
        """
    )
    instance.sql(
        """
        drop table "foo"
        """
    )
    instance.terminate()

    events = AuditFile(instance.audit_flag_value).events()

    create_table = take_until_title(events, "create_table")
    assert create_table is not None
    assert create_table["name"] == "foo"
    assert create_table["message"] == "created table `foo`"
    assert create_table["severity"] == "medium"
    assert create_table["initiator"] == "pico_service"

    drop_table = take_until_title(events, "drop_table")
    assert drop_table is not None
    assert drop_table["name"] == "foo"
    assert drop_table["message"] == "dropped table `foo`"
    assert drop_table["severity"] == "medium"
    assert drop_table["initiator"] == "pico_service"


def test_user(instance: Instance):
    instance.start()
    instance.sql(
        """
        create user "ymir" with password 'T0psecret' using chap-sha1
        """
    )
    # TODO user cant change password without access to _pico_property
    # https://git.picodata.io/picodata/picodata/picodata/-/issues/449
    instance.sudo_sql(
        """
        alter user "ymir" password 'Topsecre1'
        """,
    )
    instance.sql(
        """
        drop user "ymir"
        """
    )
    instance.terminate()

    events = AuditFile(instance.audit_flag_value).events()

    create_user = take_until_title(events, "create_user")
    assert create_user is not None
    assert create_user["user"] == "ymir"
    assert create_user["auth_type"] == "chap-sha1"
    assert create_user["message"] == f"created user `{create_user['user']}`"
    assert create_user["severity"] == "high"
    assert create_user["initiator"] == "pico_service"

    change_password = take_until_title(events, "change_password")
    assert change_password is not None
    assert change_password["user"] == "ymir"
    assert change_password["auth_type"] == "chap-sha1"
    assert (
        change_password["message"]
        == f"password of user `{change_password['user']}` was changed"
    )
    assert change_password["severity"] == "high"
    assert change_password["initiator"] == "admin"

    drop_user = take_until_title(events, "drop_user")
    assert drop_user is not None
    assert drop_user["user"] == "ymir"
    assert drop_user["message"] == f"dropped user `{drop_user['user']}`"
    assert drop_user["severity"] == "medium"
    assert drop_user["initiator"] == "pico_service"


def test_role(instance: Instance):
    instance.start()

    setup = [
        """
        create user "bubba" with password 'T0psecret' using chap-sha1
        """,
        """
        grant create role to "bubba"
        """,
    ]
    for query in setup:
        instance.sudo_sql(query)

    with instance.connect(timeout=1, user="bubba", password="T0psecret") as c:
        c.sql(
            """
            create role "skibidi"
            """
        )
        c.sql(
            """
            create role "dummy"
            """
        )
        c.sql(
            """
            grant "dummy" to "skibidi"
            """
        )
        c.sql(
            """
            revoke "dummy" from "skibidi"
            """
        )
        c.sql(
            """
            drop role "skibidi"
            """
        )
    instance.terminate()

    events = AuditFile(instance.audit_flag_value).events()

    create_role = take_until_title(events, "create_role")
    assert create_role is not None
    assert create_role["role"] == "skibidi"
    assert create_role["message"] == f"created role `{create_role['role']}`"
    assert create_role["severity"] == "high"
    assert create_role["initiator"] == "bubba"

    grant_role = take_until_title(events, "grant_role")
    assert grant_role is not None
    assert grant_role["role"] == "dummy"
    assert grant_role["grantee"] == "skibidi"
    assert grant_role["grantee_type"] == "role"
    assert (
        grant_role["message"]
        == f"granted role `{grant_role['role']}` to role `{grant_role['grantee']}`"
    )
    assert grant_role["severity"] == "high"
    assert grant_role["initiator"] == "bubba"

    revoke_role = take_until_title(events, "revoke_role")
    assert revoke_role is not None
    assert revoke_role["role"] == "dummy"
    assert revoke_role["grantee"] == "skibidi"
    assert revoke_role["grantee_type"] == "role"
    assert (
        revoke_role["message"]
        == f"revoked role `{grant_role['role']}` from role `{revoke_role['grantee']}`"
    )
    assert revoke_role["severity"] == "high"
    assert revoke_role["initiator"] == "bubba"

    drop_role = take_until_title(events, "drop_role")
    assert drop_role is not None
    assert drop_role["role"] == "skibidi"
    assert drop_role["message"] == f"dropped role `{drop_role['role']}`"
    assert drop_role["severity"] == "medium"
    assert drop_role["initiator"] == "bubba"


def assert_instance_expelled(expelled_instance: Instance, instance: Instance):
    info = instance.call(".proc_instance_info", expelled_instance.instance_id)
    grades = (info["current_grade"]["variant"], info["target_grade"]["variant"])
    assert grades == ("Expelled", "Expelled")


def test_join_expel_instance(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    audit_i1 = AuditFile(i1.audit_flag_value)
    for _ in audit_i1.events():
        pass
    events = audit_i1.events()

    i2 = cluster.add_instance(instance_id="i2")

    join_instance = take_until_title(events, "join_instance")
    assert join_instance is not None
    assert join_instance["instance_id"] == "i2"
    assert join_instance["raft_id"] == str(i2.raft_id)
    assert join_instance["severity"] == "low"
    assert join_instance["initiator"] == "admin"

    # Joining of a new instance is treated as creation of a local database on it
    create_db = take_until_title(events, "create_local_db")
    assert create_db is not None
    assert create_db["instance_id"] == "i2"
    assert create_db["raft_id"] == str(i2.raft_id)
    assert create_db["severity"] == "low"
    assert create_db["initiator"] == "admin"

    cluster.expel(i2)
    retrying(lambda: assert_instance_expelled(i2, i1))

    expel_instance = take_until_title(events, "expel_instance")
    assert expel_instance is not None
    assert expel_instance["instance_id"] == "i2"
    assert expel_instance["raft_id"] == str(i2.raft_id)
    assert expel_instance["severity"] == "low"
    assert expel_instance["initiator"] == "admin"

    # Expelling an instance is treated as removal of a local database on it
    drop_db = take_until_title(events, "drop_local_db")
    assert drop_db is not None
    assert drop_db["instance_id"] == "i2"
    assert drop_db["raft_id"] == str(i2.raft_id)
    assert drop_db["severity"] == "low"
    assert drop_db["initiator"] == "admin"


def test_join_connect_instance(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    i2 = cluster.add_instance(instance_id="i2")
    i2.terminate()

    events = AuditFile(i2.audit_flag_value).events()

    create_db = take_until_title(events, "create_local_db")
    assert create_db is not None
    assert create_db["instance_id"] == "i1"
    assert create_db["raft_id"] == str(i1.raft_id)
    assert create_db["severity"] == "low"
    assert create_db["initiator"] == "admin"

    create_db = take_until_title(events, "create_local_db")
    assert create_db is not None
    assert create_db["instance_id"] == "i2"
    assert create_db["raft_id"] == str(i2.raft_id)
    assert create_db["severity"] == "low"
    assert create_db["initiator"] == "admin"

    # Joining instance should emit 'connect_local_db' audit event.
    # connect_local_db is a local event as only joining instance emits it
    # in contrast to create_local_db
    connect_db = take_until_title(events, "connect_local_db")
    assert connect_db is not None
    assert connect_db["instance_id"] == "i2"
    assert connect_db["raft_id"] == str(i2.raft_id)
    assert connect_db["severity"] == "low"
    assert connect_db["initiator"] == "admin"


def test_auth(instance: Instance):
    instance.start()

    instance.sudo_sql(
        """
        create user "ymir" with password 'T0psecret' using chap-sha1
        """
    )
    instance.sudo_sql(
        """
        alter user "ymir" login
        """
    )

    audit = AuditFile(instance.audit_flag_value)
    for _ in audit.events():
        pass
    events = audit.events()

    with instance.connect(4, user="ymir", password="T0psecret"):
        pass

    auth_ok = take_until_title(events, "auth_ok")
    assert auth_ok is not None
    assert auth_ok["user"] == "ymir"
    assert auth_ok["severity"] == "high"
    assert auth_ok["initiator"] == "ymir"
    assert auth_ok["verdict"] == "user is not blocked"

    for _ in range(MAX_LOGIN_ATTEMPTS):
        with pytest.raises(
            NetworkError, match="User not found or supplied credentials are invalid"
        ):
            with instance.connect(4, user="ymir", password="wrong_pwd"):
                pass

        auth_fail = take_until_title(events, "auth_fail")
        assert auth_fail is not None
        assert auth_fail["message"] == "failed to authenticate user `ymir`"
        assert auth_fail["severity"] == "high"
        assert auth_fail["verdict"] == "user is not blocked"
        assert auth_fail["user"] == "ymir"
        assert auth_fail["initiator"] == "ymir"

    with pytest.raises(NetworkError, match="Maximum number of login attempts exceeded"):
        with instance.connect(4, user="ymir", password="Wr0ng_pwd"):
            pass

    auth_fail = take_until_title(events, "auth_fail")
    assert auth_fail is not None
    assert auth_fail["message"] == "failed to authenticate user `ymir`"
    assert auth_fail["severity"] == "high"
    assert auth_fail["verdict"] == (
        "Maximum number of login attempts exceeded; user blocked"
    )
    assert auth_fail["user"] == "ymir"
    assert auth_fail["initiator"] == "ymir"


def test_access_denied(instance: Instance):
    instance.start()

    instance.create_user(with_name="ymir", with_password="T0psecret")

    audit = AuditFile(instance.audit_flag_value)
    for _ in audit.events():
        pass

    events = audit.events()

    expected_error = "Create access to role 'R' is denied for user 'ymir'"
    expected_audit = "create access denied on role `R` for user `ymir`"

    with pytest.raises(
        Exception,
        match=expected_error,
    ):
        instance.sql('CREATE ROLE "R"', user="ymir", password="T0psecret")

    access_denied = take_until_title(events, "access_denied")
    assert access_denied is not None
    assert access_denied["message"] == expected_audit
    assert access_denied["severity"] == "medium"
    assert access_denied["privilege"] == "create"
    assert access_denied["object_type"] == "role"
    assert access_denied["object"] == "R"
    assert access_denied["initiator"] == "ymir"


def test_grant_revoke(instance: Instance):
    instance.start()

    user = "ymir"
    password = "T0psecret"

    instance.create_user(with_name=user, with_password=password)

    instance.sudo_sql(f'GRANT CREATE ROLE TO "{user}"')

    audit = AuditFile(instance.audit_flag_value)
    for _ in audit.events():
        pass

    events = audit.events()

    # wildcard privilege to user
    instance.sudo_sql(f'GRANT CREATE TABLE TO "{user}"')

    grant_privilege = take_until_title(events, "grant_privilege")

    assert grant_privilege is not None
    assert (
        grant_privilege["message"]
        == "granted privilege create on table `*` to user `ymir`"
    )
    assert grant_privilege["severity"] == "high"
    assert grant_privilege["privilege"] == "create"
    assert grant_privilege["object"] == "*"
    assert grant_privilege["object_type"] == "table"
    assert grant_privilege["grantee"] == user
    assert grant_privilege["grantee_type"] == "user"
    assert grant_privilege["initiator"] == "admin"

    instance.sudo_sql(f'REVOKE CREATE TABLE FROM "{user}"')

    revoke_privilege = take_until_title(events, "revoke_privilege")

    assert revoke_privilege is not None
    assert (
        revoke_privilege["message"]
        == f"revoked privilege create on table `*` from user `{user}`"
    )
    assert revoke_privilege["severity"] == "high"
    assert revoke_privilege["privilege"] == "create"
    assert revoke_privilege["object"] == "*"
    assert revoke_privilege["object_type"] == "table"
    assert revoke_privilege["grantee"] == user
    assert revoke_privilege["grantee_type"] == "user"
    assert revoke_privilege["initiator"] == "admin"

    # specific privilege to user
    instance.sudo_sql(f'GRANT READ ON TABLE "_pico_tier" TO "{user}"')

    grant_privilege = take_until_title(events, "grant_privilege")

    assert grant_privilege is not None
    assert (
        grant_privilege["message"]
        == f"granted privilege read on table `_pico_tier` to user `{user}`"
    )
    assert grant_privilege["severity"] == "high"
    assert grant_privilege["privilege"] == "read"
    assert grant_privilege["object_type"] == "table"
    assert grant_privilege["grantee"] == user
    assert grant_privilege["grantee_type"] == "user"
    assert grant_privilege["initiator"] == "admin"
    assert grant_privilege["object"] == "_pico_tier"

    instance.sudo_sql(f'REVOKE READ ON TABLE "_pico_tier" FROM "{user}"')

    revoke_privilege = take_until_title(events, "revoke_privilege")

    assert revoke_privilege is not None
    assert (
        revoke_privilege["message"]
        == f"revoked privilege read on table `_pico_tier` from user `{user}`"
    )
    assert revoke_privilege["severity"] == "high"
    assert revoke_privilege["privilege"] == "read"
    assert revoke_privilege["object_type"] == "table"
    assert revoke_privilege["grantee"] == user
    assert revoke_privilege["grantee_type"] == "user"
    assert revoke_privilege["initiator"] == "admin"
    assert revoke_privilege["object"] == "_pico_tier"

    instance.sql('CREATE ROLE "R"', user=user, password=password)

    # wildcard privilege to role
    instance.sudo_sql('GRANT CREATE TABLE TO "R"')

    grant_privilege = take_until_title(events, "grant_privilege")

    assert grant_privilege is not None
    assert (
        grant_privilege["message"]
        == "granted privilege create on table `*` to role `R`"
    )
    assert grant_privilege["severity"] == "high"
    assert grant_privilege["privilege"] == "create"
    assert grant_privilege["object"] == "*"
    assert grant_privilege["object_type"] == "table"
    assert grant_privilege["grantee"] == "R"
    assert grant_privilege["grantee_type"] == "role"
    assert grant_privilege["initiator"] == "admin"

    instance.sudo_sql('REVOKE CREATE TABLE FROM "R"')

    revoke_privilege = take_until_title(events, "revoke_privilege")

    assert revoke_privilege is not None
    assert (
        revoke_privilege["message"]
        == "revoked privilege create on table `*` from role `R`"
    )
    assert revoke_privilege["severity"] == "high"
    assert revoke_privilege["privilege"] == "create"
    assert grant_privilege["object"] == "*"
    assert revoke_privilege["object_type"] == "table"
    assert revoke_privilege["grantee"] == "R"
    assert revoke_privilege["grantee_type"] == "role"
    assert revoke_privilege["initiator"] == "admin"

    # specific privilege to role
    instance.sudo_sql('GRANT READ ON TABLE "_pico_user" TO "R"')

    grant_privilege = take_until_title(events, "grant_privilege")

    assert grant_privilege is not None
    assert (
        grant_privilege["message"]
        == "granted privilege read on table `_pico_user` to role `R`"
    )
    assert grant_privilege["severity"] == "high"
    assert grant_privilege["privilege"] == "read"
    assert grant_privilege["object_type"] == "table"
    assert grant_privilege["grantee"] == "R"
    assert grant_privilege["grantee_type"] == "role"
    assert grant_privilege["initiator"] == "admin"
    assert grant_privilege["object"] == "_pico_user"

    instance.sudo_sql('REVOKE READ ON TABLE "_pico_user" FROM "R"')

    revoke_privilege = take_until_title(events, "revoke_privilege")

    assert revoke_privilege is not None
    assert (
        revoke_privilege["message"]
        == "revoked privilege read on table `_pico_user` from role `R`"
    )
    assert revoke_privilege["severity"] == "high"
    assert revoke_privilege["privilege"] == "read"
    assert revoke_privilege["object_type"] == "table"
    assert revoke_privilege["grantee"] == "R"
    assert revoke_privilege["grantee_type"] == "role"
    assert revoke_privilege["initiator"] == "admin"
    assert revoke_privilege["object"] == "_pico_user"

    # role to user
    instance.sql('GRANT "R" TO "ymir"', user=user, password=password)

    grant_role = take_until_title(events, "grant_role")
    assert grant_role is not None
    assert grant_role["message"] == "granted role `R` to user `ymir`"
    assert grant_role["severity"] == "high"
    assert grant_role["role"] == "R"
    assert grant_role["grantee"] == "ymir"
    assert grant_role["grantee_type"] == "user"
    assert grant_role["initiator"] == "ymir"

    instance.sql(f'REVOKE "R" FROM "{user}"', user=user, password=password)

    revoke_role = take_until_title(events, "revoke_role")

    assert revoke_role is not None
    assert revoke_role["message"] == f"revoked role `R` from user `{user}`"
    assert revoke_role["severity"] == "high"
    assert revoke_role["role"] == "R"
    assert revoke_role["grantee"] == user
    assert revoke_role["grantee_type"] == "user"
    assert revoke_role["initiator"] == user

    # one role to another role
    instance.sql('CREATE ROLE "R2"', user=user, password=password)
    instance.sql('GRANT "R" TO "R2"', user=user, password=password)

    grant_role = take_until_title(events, "grant_role")
    assert grant_role is not None
    assert grant_role["message"] == "granted role `R` to role `R2`"
    assert grant_role["severity"] == "high"
    assert grant_role["role"] == "R"
    assert grant_role["grantee"] == "R2"
    assert grant_role["grantee_type"] == "role"
    assert grant_role["initiator"] == "ymir"

    instance.sql('REVOKE "R" FROM "R2"', user=user, password=password)

    revoke_role = take_until_title(events, "revoke_role")

    assert revoke_role is not None
    assert revoke_role["message"] == "revoked role `R` from role `R2`"
    assert revoke_role["severity"] == "high"
    assert revoke_role["role"] == "R"
    assert revoke_role["grantee"] == "R2"
    assert revoke_role["grantee_type"] == "role"
    assert revoke_role["initiator"] == user


def check_rotate(audit: AuditFile):
    rotate = take_until_title(audit.events(), "audit_rotate")
    assert rotate is not None
    assert rotate["message"] == "log file has been reopened"
    assert rotate["severity"] == "low"


def test_rotation(instance: Instance):
    instance.start()

    user = "ymir"
    password = "T0psecret"

    audit = AuditFile(instance.audit_flag_value)
    for _ in audit.events():
        pass

    instance.create_user(with_name=user, with_password=password)

    instance.sudo_sql(f'GRANT CREATE ROLE TO "{user}"')

    assert instance.process is not None
    instance.process.send_signal(sig=signal.SIGHUP)

    Retriable(timeout=5, rps=1).call(check_rotate, audit)


def test_rename_user(instance: Instance):
    instance.start()

    user = "ymir"
    password = "T0psecret"

    instance.create_user(with_name=user, with_password=password)

    instance.sudo_sql('ALTER USER "ymir" RENAME TO "TOM"')

    instance.terminate()

    events = AuditFile(instance.audit_flag_value).events()

    rename_user = take_until_title(events, "rename_user")
    assert rename_user is not None
    assert rename_user["message"] == "name of user `ymir` was changed to `TOM`"
    assert rename_user["severity"] == "high"
    assert rename_user["old_name"] == "ymir"
    assert rename_user["new_name"] == "TOM"

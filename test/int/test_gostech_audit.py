import os
import pytest

from typing import List, Dict, Any, Generator

from tarantool.error import (  # type: ignore
    NetworkError,
)

from conftest import (
    MAX_LOGIN_ATTEMPTS,
    Instance,
    Cluster,
    AuditServer,
)


@pytest.fixture
def gostech_instance(cluster: Cluster, pytestconfig) -> Generator[Instance, None, None]:
    """Returns a deployed instance forming a single-node cluster with logs sent through pipe."""
    instance = cluster.add_instance(wait_online=False)

    has_webui = bool(pytestconfig.getoption("--with-webui"))
    if has_webui:
        listen = f"{cluster.base_host}:{cluster.base_port+80}"
        instance.env["PICODATA_HTTP_LISTEN"] = listen

    password_file = f"{cluster.data_dir}/password.txt"
    with open(password_file, "w") as f:
        print("s3cr3t", file=f)
    os.chmod(password_file, 0o600)
    instance.service_password_file = password_file
    yield instance


def take_until_name(events: List[Dict[str, Any]], name: str) -> Dict[str, Any] | None:
    for event in events:
        if event["name"] == name:
            return event
    return None


def test_gostech_startup(gostech_instance: Instance) -> None:
    audit_server = AuditServer(7000)
    audit_server.start()
    gostech_instance.audit = audit_server.cmd(gostech_instance.binary_path)
    gostech_instance.start_and_wait()
    gostech_instance.terminate()
    audit_server.stop()

    events = audit_server.logs()

    # Init audit logs is skipped due to it's structure
    event = take_until_name(events, name="join_instance")
    assert event is not None
    assert event["tags"] == ["low", "i1", "1"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"

    event = take_until_name(events, name="change_target_grade")
    assert event is not None
    assert event["tags"] == ["low", "i1", "1"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"

    event = take_until_name(events, name="change_current_grade")
    assert event is not None
    assert event["tags"] == ["medium", "i1", "1"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"

    event = take_until_name(events, name="create_local_db")
    assert event is not None
    assert event["tags"] == ["low", "i1", "1"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"

    event = take_until_name(events, name="connect_local_db")
    assert event is not None
    assert event["tags"] == ["low", "i1", "1"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"


def test_gostech_recover_database(gostech_instance: Instance) -> None:
    audit_server = AuditServer(7001)
    audit_server.start()
    gostech_instance.audit = audit_server.cmd(gostech_instance.binary_path)
    gostech_instance.start_and_wait()
    gostech_instance.restart()
    gostech_instance.wait_online()
    gostech_instance.terminate()
    audit_server.stop()

    events = audit_server.logs()

    event = take_until_name(events, name="create_local_db")
    assert event is not None
    assert event["tags"] == ["low", "i1", "1"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"

    event = take_until_name(events, name="recover_local_db")
    assert event is not None
    assert event["tags"] == ["low", "i1", "1"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"

    event = take_until_name(events, name="connect_local_db")
    assert event is not None
    assert event["tags"] == ["low", "i1", "1"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"


def test_gostech_create_drop_table(gostech_instance: Instance) -> None:
    audit_server = AuditServer(7002)
    audit_server.start()
    gostech_instance.audit = audit_server.cmd(gostech_instance.binary_path)
    gostech_instance.start_and_wait()
    gostech_instance.sql(
        """
        create table "foo" ("val" int not null, primary key ("val"))
        distributed by ("val")
        """
    )
    gostech_instance.sql(
        """
        drop table "foo"
        """
    )
    gostech_instance.terminate()
    audit_server.stop()

    events = audit_server.logs()

    event = take_until_name(events, name="create_table")
    assert event is not None
    assert event["tags"] == ["medium"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "pico_service"

    event = take_until_name(events, name="drop_table")
    assert event is not None
    assert event["tags"] == ["medium"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "pico_service"


def test_gostech_user(gostech_instance: Instance) -> None:
    audit_server = AuditServer(7003)
    audit_server.start()
    gostech_instance.audit = audit_server.cmd(gostech_instance.binary_path)
    gostech_instance.start_and_wait()
    gostech_instance.sql(
        """
        create user "ymir" with password 'T0psecret' using chap-sha1
        """
    )
    gostech_instance.sudo_sql(
        """
        alter user "ymir" password 'Topsecre1'
        """,
    )
    gostech_instance.sql(
        """
        drop user "ymir"
        """
    )
    gostech_instance.terminate()
    audit_server.stop()

    events = audit_server.logs()

    event = take_until_name(events, name="create_user")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "ymir"

    event = take_until_name(events, name="change_password")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "ymir"

    event = take_until_name(events, name="drop_user")
    assert event is not None
    assert event["tags"] == ["medium"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "ymir"


def test_gostech_role(gostech_instance: Instance) -> None:
    audit_server = AuditServer(7004)
    audit_server.start()
    gostech_instance.audit = audit_server.cmd(gostech_instance.binary_path)
    gostech_instance.start_and_wait()

    setup = [
        """
        create user "bubba" with password 'T0psecret' using chap-sha1
        """,
        """
        grant create role to "bubba"
        """,
    ]
    for query in setup:
        gostech_instance.sudo_sql(query)

    with gostech_instance.connect(timeout=1, user="bubba", password="T0psecret") as c:
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
    gostech_instance.terminate()
    audit_server.stop()

    events = audit_server.logs()

    event = take_until_name(events, name="create_role")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "bubba"

    event = take_until_name(events, name="grant_role")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"

    event = take_until_name(events, name="revoke_role")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "bubba"

    event = take_until_name(events, name="drop_role")
    assert event is not None
    assert event["tags"] == ["medium"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "bubba"


def test_gostech_join_expel_instance(cluster: Cluster) -> None:
    audit_server = AuditServer(7005)
    audit_server.start()

    cluster.deploy(instance_count=1, audit=audit_server.cmd(cluster.binary_path))

    i2 = cluster.add_instance(
        instance_id="i2", audit=audit_server.cmd(cluster.binary_path)
    )
    cluster.expel(i2)
    cluster.terminate()
    audit_server.stop()

    events = audit_server.logs()

    event = take_until_name(events, "expel_instance")
    assert event is not None
    assert event["tags"] == ["low", "i2", "2"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"


def test_gostech_auth(gostech_instance: Instance) -> None:
    audit_server = AuditServer(7006)
    audit_server.start()
    gostech_instance.audit = audit_server.cmd(gostech_instance.binary_path)
    gostech_instance.start_and_wait()

    gostech_instance.sudo_sql(
        """
        create user "ymir" with password 'T0psecret' using chap-sha1
        """
    )
    gostech_instance.sudo_sql(
        """
        alter user "ymir" login
        """
    )

    with gostech_instance.connect(4, user="ymir", password="T0psecret"):
        pass

    for _ in range(MAX_LOGIN_ATTEMPTS):
        with pytest.raises(
            NetworkError, match="User not found or supplied credentials are invalid"
        ):
            with gostech_instance.connect(4, user="ymir", password="wrong_pwd"):
                pass

    with pytest.raises(NetworkError, match="Maximum number of login attempts exceeded"):
        with gostech_instance.connect(4, user="ymir", password="Wr0ng_pwd"):
            pass

    gostech_instance.terminate()
    audit_server.stop()

    events = audit_server.logs()

    event = take_until_name(events, "auth_ok")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "ymir" or event["userLogin"] == "pico_service"

    event = take_until_name(events, "auth_fail")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "ymir"


def test_gostech_access_denied(gostech_instance: Instance) -> None:
    audit_server = AuditServer(7007)
    audit_server.start()
    gostech_instance.audit = audit_server.cmd(gostech_instance.binary_path)
    gostech_instance.start_and_wait()

    gostech_instance.create_user(with_name="ymir", with_password="T0psecret")

    expected_error = "Create access to role 'R' is denied for user 'ymir'"

    with pytest.raises(
        Exception,
        match=expected_error,
    ):
        gostech_instance.sql('CREATE ROLE "R"', user="ymir", password="T0psecret")

    gostech_instance.terminate()
    audit_server.stop()

    events = audit_server.logs()

    event = take_until_name(events, "access_denied")
    assert event is not None
    assert event["tags"] == ["medium"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "ymir"


def test_gostech_grant_revoke(gostech_instance: Instance) -> None:
    audit_server = AuditServer(7008)
    audit_server.start()
    gostech_instance.audit = audit_server.cmd(gostech_instance.binary_path)
    gostech_instance.start_and_wait()

    user = "ymir"
    password = "T0psecret"

    gostech_instance.create_user(with_name=user, with_password=password)

    gostech_instance.sudo_sql(f'GRANT CREATE ROLE TO "{user}"')
    gostech_instance.sudo_sql(f'GRANT CREATE TABLE TO "{user}"')
    gostech_instance.sudo_sql(f'REVOKE CREATE TABLE FROM "{user}"')
    gostech_instance.sudo_sql(f'GRANT READ ON TABLE "_pico_tier" TO "{user}"')
    gostech_instance.sudo_sql(f'REVOKE READ ON TABLE "_pico_tier" FROM "{user}"')
    gostech_instance.sql('CREATE ROLE "R"', user=user, password=password)
    gostech_instance.sudo_sql('GRANT CREATE TABLE TO "R"')
    gostech_instance.sudo_sql('REVOKE CREATE TABLE FROM "R"')
    gostech_instance.sudo_sql('GRANT READ ON TABLE "_pico_user" TO "R"')
    gostech_instance.sudo_sql('REVOKE READ ON TABLE "_pico_user" FROM "R"')
    gostech_instance.sql('GRANT "R" TO "ymir"', user=user, password=password)
    gostech_instance.sql(f'REVOKE "R" FROM "{user}"', user=user, password=password)
    gostech_instance.sql('CREATE ROLE "R2"', user=user, password=password)
    gostech_instance.sql('GRANT "R" TO "R2"', user=user, password=password)
    gostech_instance.sql('REVOKE "R" FROM "R2"', user=user, password=password)

    gostech_instance.terminate()
    audit_server.stop()

    events = audit_server.logs()

    event = take_until_name(events, "grant_privilege")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"

    event = take_until_name(events, "revoke_privilege")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"

    event = take_until_name(events, "revoke_privilege")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"

    event = take_until_name(events, "grant_role")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "ymir" or event["userLogin"] == "pico_service"

    event = take_until_name(events, "revoke_role")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "ymir"


def test_gostech_rename_user(gostech_instance: Instance) -> None:
    audit_server = AuditServer(7009)
    audit_server.start()
    gostech_instance.audit = audit_server.cmd(gostech_instance.binary_path)
    gostech_instance.start_and_wait()

    user = "ymir"
    password = "T0psecret"

    gostech_instance.create_user(with_name=user, with_password=password)
    gostech_instance.sudo_sql('ALTER USER "ymir" RENAME TO "TOM"')
    gostech_instance.terminate()
    audit_server.stop()

    events = audit_server.logs()

    event = take_until_name(events, "rename_user")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["Datetime"] is not None
    assert event["serviceName"] == "picodata"
    assert event["userLogin"] == "admin"

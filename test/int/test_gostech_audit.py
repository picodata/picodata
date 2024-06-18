import pytest

from typing import Generator, Tuple

from tarantool.error import (  # type: ignore
    NetworkError,
)

from conftest import (
    MAX_LOGIN_ATTEMPTS,
    Instance,
    Cluster,
    AuditServer,
    PortDistributor,
)


@pytest.fixture
def instance_with_gostech_audit(
    unstarted_instance: Instance, port_distributor: PortDistributor
) -> Generator[Tuple[Instance, AuditServer], None, None]:
    audit_server = AuditServer(port_distributor.get())
    audit_server.start()
    unstarted_instance.audit = audit_server.cmd(unstarted_instance.binary_path)
    unstarted_instance.start_and_wait()
    yield (unstarted_instance, audit_server)
    unstarted_instance.terminate()
    audit_server.stop()


def test_gostech_startup(
    instance_with_gostech_audit: Tuple[Instance, AuditServer]
) -> None:
    _, audit = instance_with_gostech_audit

    # Init audit logs is skipped due to it's structure
    event = audit.take_until_name(name="join_instance")
    assert event is not None
    assert event["tags"] == ["low"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "admin"

    event = audit.take_until_name(name="create_local_db")
    assert event is not None
    assert event["tags"] == ["low"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "admin"

    event = audit.take_until_name(name="change_current_grade")
    assert event is not None
    assert event["tags"] == ["medium"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "admin"

    event = audit.take_until_name(name="change_target_grade")
    assert event is not None
    assert event["tags"] == ["low"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "admin"

    event = audit.take_until_name(name="connect_local_db")
    assert event is not None
    assert event["tags"] == ["low"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "admin"


def test_gostech_recover_database(
    instance_with_gostech_audit: Tuple[Instance, AuditServer]
) -> None:
    instance, audit = instance_with_gostech_audit

    instance.restart()
    instance.wait_online()

    event = audit.take_until_name(name="create_local_db")
    assert event is not None
    assert event["tags"] == ["low"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "admin"

    event = audit.take_until_name(name="recover_local_db")
    assert event is not None
    assert event["tags"] == ["low"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "admin"

    event = audit.take_until_name(name="connect_local_db")
    assert event is not None
    assert event["tags"] == ["low"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "admin"


def test_gostech_create_drop_table(
    instance_with_gostech_audit: Tuple[Instance, AuditServer]
) -> None:
    instance, audit = instance_with_gostech_audit
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

    event = audit.take_until_name(name="create_table")
    assert event is not None
    assert event["tags"] == ["medium"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "pico_service"

    event = audit.take_until_name(name="drop_table")
    assert event is not None
    assert event["tags"] == ["medium"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "pico_service"


def test_gostech_user(
    instance_with_gostech_audit: Tuple[Instance, AuditServer]
) -> None:
    instance, audit = instance_with_gostech_audit

    instance.sql(
        """
        create user "ymir" with password 'T0psecret' using chap-sha1
        """
    )
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

    event = audit.take_until_name(name="create_user")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "ymir"

    event = audit.take_until_name(name="change_password")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "ymir"

    event = audit.take_until_name(name="drop_user")
    assert event is not None
    assert event["tags"] == ["medium"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "ymir"


def test_gostech_role(
    instance_with_gostech_audit: Tuple[Instance, AuditServer]
) -> None:
    instance, audit = instance_with_gostech_audit

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

    event = audit.take_until_name(name="create_role")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "bubba"

    event = audit.take_until_name(name="grant_role")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "bubba"

    event = audit.take_until_name(name="revoke_role")
    assert event is not None
    assert event["tags"] == ["high"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "bubba"

    event = audit.take_until_name(name="drop_role")
    assert event is not None
    assert event["tags"] == ["medium"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "bubba"


def test_gostech_join_expel_instance(
    cluster: Cluster, port_distributor: PortDistributor
) -> None:
    audit_server = AuditServer(port_distributor.get())
    audit_server.start()

    cluster.deploy(instance_count=1, audit=audit_server.cmd(cluster.binary_path))

    i2 = cluster.add_instance(
        instance_id="i2", audit=audit_server.cmd(cluster.binary_path)
    )
    cluster.expel(i2)
    cluster.terminate()
    audit_server.stop()

    event = audit_server.take_until_name("expel_instance")
    assert event["tags"] == ["low"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "admin"


def test_gostech_auth(
    instance_with_gostech_audit: Tuple[Instance, AuditServer],
) -> None:
    instance, audit = instance_with_gostech_audit

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

    with instance.connect(4, user="ymir", password="T0psecret"):
        pass

    for _ in range(MAX_LOGIN_ATTEMPTS):
        with pytest.raises(
            NetworkError, match="User not found or supplied credentials are invalid"
        ):
            with instance.connect(4, user="ymir", password="wrong_pwd"):
                pass

    with pytest.raises(NetworkError, match="Maximum number of login attempts exceeded"):
        with instance.connect(4, user="ymir", password="Wr0ng_pwd"):
            pass

    event = audit.take_until_name("auth_ok")
    assert event["tags"] == ["high"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "ymir" or event["userLogin"] == "pico_service"

    event = audit.take_until_name("auth_fail")
    assert event["tags"] == ["high"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "ymir"


def test_gostech_access_denied(
    instance_with_gostech_audit: Tuple[Instance, AuditServer],
) -> None:
    instance, audit = instance_with_gostech_audit

    instance.create_user(with_name="ymir", with_password="T0psecret")

    expected_error = "Create access to role 'R' is denied for user 'ymir'"

    with pytest.raises(
        Exception,
        match=expected_error,
    ):
        instance.sql('CREATE ROLE "R"', user="ymir", password="T0psecret")

    event = audit.take_until_name("access_denied")
    assert event["tags"] == ["medium"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "ymir"


def test_gostech_grant_revoke(
    instance_with_gostech_audit: Tuple[Instance, AuditServer],
) -> None:
    instance, audit = instance_with_gostech_audit
    user = "ymir"
    password = "T0psecret"

    instance.create_user(with_name=user, with_password=password)

    instance.sudo_sql(f'GRANT CREATE ROLE TO "{user}"')
    instance.sudo_sql(f'GRANT CREATE TABLE TO "{user}"')
    instance.sudo_sql(f'REVOKE CREATE TABLE FROM "{user}"')
    instance.sudo_sql(f'GRANT READ ON TABLE "_pico_tier" TO "{user}"')
    instance.sudo_sql(f'REVOKE READ ON TABLE "_pico_tier" FROM "{user}"')
    instance.sql('CREATE ROLE "R"', user=user, password=password)
    instance.sudo_sql('GRANT CREATE TABLE TO "R"')
    instance.sudo_sql('REVOKE CREATE TABLE FROM "R"')
    instance.sudo_sql('GRANT READ ON TABLE "_pico_user" TO "R"')
    instance.sudo_sql('REVOKE READ ON TABLE "_pico_user" FROM "R"')
    instance.sql('GRANT "R" TO "ymir"', user=user, password=password)
    instance.sql(f'REVOKE "R" FROM "{user}"', user=user, password=password)
    instance.sql('CREATE ROLE "R2"', user=user, password=password)
    instance.sql('GRANT "R" TO "R2"', user=user, password=password)
    instance.sql('REVOKE "R" FROM "R2"', user=user, password=password)

    event = audit.take_until_name("grant_privilege")
    assert event["tags"] == ["high"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "admin"

    event = audit.take_until_name("revoke_privilege")
    assert event["tags"] == ["high"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "admin"

    event = audit.take_until_name("revoke_privilege")
    assert event["tags"] == ["high"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "admin"

    event = audit.take_until_name("grant_role")
    assert event["tags"] == ["high"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "ymir" or event["userLogin"] == "pico_service"

    event = audit.take_until_name("revoke_role")
    assert event["tags"] == ["high"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "ymir"


def test_gostech_rename_user(
    instance_with_gostech_audit: Tuple[Instance, AuditServer]
) -> None:
    instance, audit = instance_with_gostech_audit

    user = "ymir"
    password = "T0psecret"

    instance.create_user(with_name=user, with_password=password)
    instance.sudo_sql('ALTER USER "ymir" RENAME TO "TOM"')

    event = audit.take_until_name("rename_user")
    assert event["tags"] == ["high"]
    assert event["createdAt"] is not None
    assert event["module"] == "picodata"
    assert event["userLogin"] == "admin"

import pytest
import pg8000.dbapi as pg  # type: ignore
import os
from conftest import Postgres


ADMIN = "admin"
PASSWORD = "P@ssw0rd"
PLUGIN = "testplug"
PLUGIN_WITH_MIGRATION = "testplug_w_migration"
VERSION_1 = "0.1.0"
VERSION_2 = "0.2.0"
DEFAULT_TIER = "default"
SERVICE_1 = "testservice_1"
SERVICE_2 = "testservice_2"


def cursor(postgres: Postgres, user: str, password: str):
    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(
        user, password=password, host=postgres.host, port=postgres.port
    )
    conn.autocommit = True
    return conn.cursor()


def test_create_plugin(postgres: Postgres):
    postgres.instance.sql(
        f"""
        ALTER USER "{ADMIN}" WITH PASSWORD '{PASSWORD}'
        """
    )
    cur = cursor(postgres, ADMIN, PASSWORD)

    cur.execute(f"CREATE PLUGIN {PLUGIN} {VERSION_1}")
    cur.execute(f"CREATE PLUGIN IF NOT EXISTS {PLUGIN} {VERSION_1}")
    cur.execute(
        f"""
        CREATE PLUGIN IF NOT EXISTS {PLUGIN} {VERSION_1}
        OPTION (TIMEOUT = 1.1)
        """
    )
    with pytest.raises(pg.DatabaseError, match="already exists"):
        cur.execute(f"CREATE PLUGIN {PLUGIN} {VERSION_1}")

    cur.execute(f"DROP PLUGIN {PLUGIN} {VERSION_1 }")
    cur.execute(
        f"""
        CREATE PLUGIN {PLUGIN} {VERSION_1} OPTION (TIMEOUT = 1.1)
        """
    )

    cur.execute(f"DROP PLUGIN {PLUGIN} {VERSION_1}")


def test_drop_plugin(postgres: Postgres):
    postgres.instance.sql(
        f"""
        ALTER USER "{ADMIN}" WITH PASSWORD '{PASSWORD}'
        """
    )
    cur = cursor(postgres, ADMIN, PASSWORD)

    cur.execute(f"CREATE PLUGIN {PLUGIN} {VERSION_1}")
    cur.execute(f"DROP PLUGIN {PLUGIN} {VERSION_1}")
    cur.execute(f"DROP PLUGIN IF EXISTS {PLUGIN} {VERSION_1}")

    cur.execute(f"CREATE PLUGIN {PLUGIN} {VERSION_1}")
    cur.execute(f"DROP PLUGIN {PLUGIN} {VERSION_1} WITH DATA")

    cur.execute(f"CREATE PLUGIN {PLUGIN} {VERSION_1}")
    cur.execute(
        f"""
        DROP PLUGIN {PLUGIN} {VERSION_1} OPTION (TIMEOUT = 1.1)
        """
    )

    cur.execute(f"CREATE PLUGIN {PLUGIN} {VERSION_1}")
    cur.execute(
        f"""
        DROP PLUGIN {PLUGIN} {VERSION_1} WITH DATA
        OPTION (TIMEOUT = 1.1)
        """
    )


def test_alter_plugin(postgres: Postgres):
    postgres.instance.sql(
        f"""
        ALTER USER "{ADMIN}" WITH PASSWORD '{PASSWORD}'
        """
    )
    cur = cursor(postgres, ADMIN, PASSWORD)

    cur.execute(f"CREATE PLUGIN {PLUGIN} {VERSION_1}")

    cur.execute(f"ALTER PLUGIN {PLUGIN} {VERSION_1} ENABLE")
    cur.execute(f"ALTER PLUGIN {PLUGIN} {VERSION_1} DISABLE")

    cur.execute(
        f"""
        ALTER PLUGIN {PLUGIN} {VERSION_1} ENABLE
        OPTION (TIMEOUT = 1)
        """
    )
    cur.execute(
        f"""
        ALTER PLUGIN {PLUGIN} {VERSION_1} DISABLE
        OPTION (TIMEOUT = 1)
        """
    )

    cur.execute(
        f"""
        ALTER PLUGIN {PLUGIN} {VERSION_1}
        ADD SERVICE {SERVICE_1} TO TIER {DEFAULT_TIER}
        OPTION (TIMEOUT = 1)
        """
    )
    cur.execute(
        f"""
        ALTER PLUGIN {PLUGIN} {VERSION_1}
        ADD SERVICE {SERVICE_1} TO TIER {DEFAULT_TIER}
        """
    )

    cur.execute(
        f"""
        ALTER PLUGIN {PLUGIN} {VERSION_1}
        REMOVE SERVICE {SERVICE_1} FROM TIER {DEFAULT_TIER}
        OPTION (TIMEOUT = 1)
        """
    )
    cur.execute(
        f"""
        ALTER PLUGIN {PLUGIN} {VERSION_1}
        REMOVE SERVICE {SERVICE_1} FROM TIER {DEFAULT_TIER}
        """
    )

    cur.execute(f"CREATE PLUGIN {PLUGIN_WITH_MIGRATION} {VERSION_1}")
    cur.execute(f"CREATE PLUGIN {PLUGIN_WITH_MIGRATION} {VERSION_2}")
    cur.execute(
        f"""
        ALTER PLUGIN {PLUGIN_WITH_MIGRATION}
        MIGRATE TO {VERSION_2}
        OPTION (TIMEOUT = 10, ROLLBACK_TIMEOUT = 10)
        """
    )

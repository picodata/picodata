import pytest
import pg8000.dbapi as pg  # type: ignore
from conftest import Postgres
import os
from pathlib import Path
import psycopg


def test_ssl_disabled(postgres: Postgres):
    create_user(postgres)

    # disable: only try a non-SSL connection
    try_connect_pg8000(postgres, sslmode="disable")

    # prefer: first try an SSL connection; if that fails,
    #         try a non-SSL connection.
    # As ssl is not supported, server will respond to SslRequest with
    # SslRefuse and client will try a non-SSL connection.
    try_connect_pg8000(postgres, sslmode="prefer")

    # require: only try an SSL connection.
    # As ssl is not supported, server will respond to SslRequest with
    # SslRefuse and client won't try to connect again.
    # Client we will see: server does not support SSL, but SSL was required,
    # but client doesn't have to inform the server.
    with pytest.raises(pg.InterfaceError, match="Server refuses SSL"):
        try_connect_pg8000(postgres, sslmode="require")

    # now try the same with psycopg
    try_connect_psycopg(postgres, sslmode="disable")
    try_connect_psycopg(postgres, sslmode="prefer")
    with pytest.raises(psycopg.OperationalError, match="server does not support SSL, but SSL was required"):
        try_connect_psycopg(postgres, sslmode="require")


def test_ssl_enabled(postgres_with_tls: Postgres):
    create_user(postgres_with_tls)

    with pytest.raises(pg.DatabaseError, match="this server requires the client to use ssl"):
        try_connect_pg8000(postgres_with_tls, sslmode="disable")
    try_connect_pg8000(postgres_with_tls, sslmode="prefer")
    try_connect_pg8000(postgres_with_tls, sslmode="require")

    # now try the same with psycopg
    with pytest.raises(psycopg.OperationalError, match="this server requires the client to use ssl"):
        try_connect_psycopg(postgres_with_tls, sslmode="disable")
    try_connect_psycopg(postgres_with_tls, sslmode="prefer")
    try_connect_psycopg(postgres_with_tls, sslmode="require")


def test_mtls_with_known_cert(postgres_with_mtls: Postgres):
    create_user(postgres_with_mtls)

    try_connect_psycopg(postgres_with_mtls, client_tls_pair_name="server")


def test_custom_server_cert_paths(postgres_with_custom_cert_paths: Postgres):
    create_user(postgres_with_custom_cert_paths)

    try_connect_psycopg(postgres_with_custom_cert_paths, client_tls_pair_name="server")


def test_mtls_without_client_cert(postgres_with_mtls: Postgres):
    create_user(postgres_with_mtls)

    with pytest.raises(psycopg.OperationalError, match="certificate required"):
        try_connect_psycopg(postgres_with_mtls)


def test_mtls_with_unknown_cert(postgres_with_mtls: Postgres):
    create_user(postgres_with_mtls)

    with pytest.raises(psycopg.OperationalError, match="unknown ca"):
        try_connect_psycopg(postgres_with_mtls, client_tls_pair_name="self-signed")


USER = "user"
PASSWORD = "P@ssw0rd"


def create_user(postgres: Postgres):
    postgres.instance.sql(f"CREATE USER \"{USER}\" WITH PASSWORD '{PASSWORD}'")


def try_connect_pg8000(postgres: Postgres, sslmode: str = "require"):
    if sslmode == "disable":
        ssl_context = False
    elif sslmode == "prefer":
        ssl_context = None
    elif sslmode == "require":
        ssl_context = True
    else:
        raise ValueError(f"Unknown sslmode value: {sslmode}")

    pg.Connection(USER, password=PASSWORD, host=postgres.host, port=postgres.port, ssl_context=ssl_context).close()


def try_connect_psycopg(postgres: Postgres, client_tls_pair_name: str | None = None, sslmode: str = "require"):
    host = postgres.host
    port = postgres.port
    connection_string = f"\
            user = {USER} \
            password={PASSWORD} \
            host={host} \
            port={port} \
            sslmode={sslmode}"

    if client_tls_pair_name is not None:
        ssl_dir = Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"
        client_cert_path = ssl_dir / (client_tls_pair_name + ".crt")
        client_key_path = ssl_dir / (client_tls_pair_name + ".key")
        connection_string += f" sslcert={client_cert_path} sslkey={client_key_path}"
        os.chmod(client_key_path, 0o600)

    psycopg.connect(connection_string).close()

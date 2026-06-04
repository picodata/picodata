import subprocess
import hashlib
import os
import socket
import time

from dataclasses import dataclass
from typing import Literal, Generator, Any, Optional

import pytest

from framework.util import BASE_HOST
from pathlib import Path

DEFAULT_DN_FMT = "cn=$USER,dc=example,dc=org"


@dataclass
class LdapServer:
    host: str
    port: int
    process: subprocess.Popen
    user: str
    password: str
    mode: Literal["plaintext", "tls", "starttls"]

    def picodata_legacy_env_config(self, dn_fmt: str = DEFAULT_DN_FMT) -> dict[str, str]:
        """
        Return environment variables to set to configure picodata to use the this LDAP server via legacy environment variables.
        """
        return make_picodata_legacy_env_config(self.mode, self.host, self.port, dn_fmt)

    def picodata_ldap_config(
        self,
        dn_fmt: str = DEFAULT_DN_FMT,
        ca_file: Optional[str | Path] = None,
    ) -> dict[str, Any]:
        """
        Return the value for `instance.ldap` config section to configure picodata to use this LDAP server.
        """
        return make_picodata_ldap_config(self.mode, self.host, self.port, dn_fmt, ca_file)


def make_picodata_legacy_env_config(
    mode: Literal["plaintext", "tls", "starttls"],
    host: str,
    port: int,
    dn_fmt: str = DEFAULT_DN_FMT,
) -> dict[str, str]:
    """
    Return environment variables to set to configure picodata to use the specified LDAP server via legacy environment variables.
    """
    if mode == "plaintext":
        return {
            "TT_LDAP_URL": f"ldap://{host}:{port}",
            "TT_LDAP_DN_FMT": dn_fmt,
        }
    elif mode == "tls":
        return {
            "TT_LDAP_URL": f"ldaps://{host}:{port}",
            "TT_LDAP_DN_FMT": dn_fmt,
        }
    elif mode == "starttls":
        return {
            "TT_LDAP_URL": f"ldap://{host}:{port}",
            "TT_LDAP_DN_FMT": dn_fmt,
            "TT_LDAP_ENABLE_TLS": "true",
        }
    else:
        raise ValueError("Unknown value of mode: " + str(mode))


def make_picodata_ldap_config(
    mode: Literal["plaintext", "tls", "starttls"],
    host: str,
    port: int,
    dn_fmt: str = "cn=$USER,dc=example,dc=org",
    ca_file: Optional[str | Path] = None,
) -> dict[str, Any]:
    """
    Return the value for `instance.ldap` config section to configure picodata to use the specified LDAP server.
    """
    return {
        "enabled": True,
        "connect": f"{host}:{port}",
        "dn_format": dn_fmt,
        "tls": {
            "enabled": mode != "plaintext",
            "method": "start_tls" if mode == "starttls" else "implicit",
            "ca_file": None if ca_file is None else str(ca_file),
        },
    }


def make_glauth_config_file(
    username: str, password: str, host: str, port: int, mode: Literal["plaintext", "tls", "starttls"], ssl_dir: Path
) -> str:
    password_sha256 = hashlib.sha256(password.encode("utf8")).hexdigest()
    client_cert_path = ssl_dir / "server-ldap-fullchain.crt"
    client_key_path = ssl_dir / "server.key"

    if mode == "plaintext":
        listen_section = f"""
        [ldap]
            enabled = true
            listen = "{host}:{port}"
            tls = false

        [ldaps]
            enabled = false
        """
    elif mode == "tls":
        listen_section = f"""
        [ldap]
            enabled = false

        [ldaps]
            enabled = true
            listen = "{host}:{port}"
            cert = "{client_cert_path}"
            key = "{client_key_path}"
        """
    elif mode == "starttls":
        listen_section = f"""
        [ldap]
            enabled = true
            listen = "{host}:{port}"
            tls = true
            tlsCertPath = "{client_cert_path}"
            tlsKeyPath = "{client_key_path}"

        [ldaps]
            enabled = false
        """
    else:
        raise ValueError("Unknown value of mode: " + str(mode))

    return f"""
        {listen_section}

        [backend]
            datastore = "config"
            baseDN = "dc=example,dc=org"

        [[users]]
            name = "{username}"
            uidnumber = 5001
            primarygroup = 5501
            passsha256 = "{password_sha256}"
                [[users.capabilities]]
                    action = "search"
                    object = "*"

        [[groups]]
            name = "ldapgroup"
            gidnumber = 5501
    """


def is_glauth_available():
    try:
        subprocess.Popen(["glauth", "--version"])
    except Exception:
        return False

    return True


def configure_ldap_server(
    username: str, password: str, data_dir: str, port: int, mode: Literal["plaintext", "tls", "starttls"]
) -> LdapServer:
    subprocess.Popen(["glauth", "--version"])

    ldap_cfg_path = f"{data_dir}/ldap.cfg"
    with open(ldap_cfg_path, "x") as f:
        ssl_dir = Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"

        f.write(make_glauth_config_file(username, password, BASE_HOST, port, mode, ssl_dir))

    process = subprocess.Popen(["glauth", "-c", ldap_cfg_path], stdout=subprocess.PIPE)

    # wait for glauth to open the socket
    deadline = time.time() + 3
    while deadline > time.time():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((BASE_HOST, port))
            break
        except ConnectionRefusedError:
            sock.close()
            time.sleep(0.1)

    return LdapServer(
        host=BASE_HOST,
        port=port,
        process=process,
        user=username,
        password=password,
        mode=mode,
    )


def ldap_server_fixture_impl(
    data_dir: str, port: int, mode: Literal["plaintext", "tls", "starttls"]
) -> Generator[LdapServer, None, None]:
    if not is_glauth_available():
        pytest.skip("need installed glauth")

    server = configure_ldap_server(
        username="ldapuser",
        password="ldappass",
        data_dir=data_dir,
        port=port,
        mode=mode,
    )

    yield server

    server.process.terminate()

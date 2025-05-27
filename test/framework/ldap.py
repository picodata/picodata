import subprocess
import hashlib
import os
import socket
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass
class LdapServer:
    host: str
    port: int
    process: subprocess.Popen
    user: str
    password: str
    tls: bool


def is_glauth_available():
    try:
        subprocess.Popen(["glauth", "--version"])
    except Exception:
        return False

    return True


def configure_ldap_server(username: str, password: str, data_dir: str, port: int, tls: bool) -> LdapServer:
    subprocess.Popen(["glauth", "--version"])

    LDAP_SERVER_HOST = "127.0.0.1"

    ldap_cfg_path = f"{data_dir}/ldap.cfg"
    with open(ldap_cfg_path, "x") as f:
        password_sha256 = hashlib.sha256(password.encode("utf8")).hexdigest()
        tls_value = "true" if tls else "false"
        ssl_dir = Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"
        client_cert_path = ssl_dir / "server-fullchain.crt"
        client_key_path = ssl_dir / "server.key"
        f.write(
            f"""
            [ldap]
                enabled = true
                listen = "{LDAP_SERVER_HOST}:{port}"
                tls = {tls_value}
                tlsCertPath = "{client_cert_path}"
                tlsKeyPath = "{client_key_path}"

            [ldaps]
                enabled = false

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
        )

    process = subprocess.Popen(["glauth", "-c", ldap_cfg_path], stdout=subprocess.PIPE)

    deadline = time.time() + 3
    while deadline > time.time():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((LDAP_SERVER_HOST, port))
            break
        except ConnectionRefusedError:
            sock.close()
            time.sleep(0.1)

    return LdapServer(
        host=LDAP_SERVER_HOST,
        port=port,
        process=process,
        user=username,
        password=password,
        tls=tls,
    )

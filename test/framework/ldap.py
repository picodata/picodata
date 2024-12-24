import subprocess
import hashlib
import socket
import time
from dataclasses import dataclass


@dataclass
class LdapServer:
    host: str
    port: int
    process: subprocess.Popen
    user: str
    password: str


def is_glauth_available():
    try:
        subprocess.Popen(["glauth", "--version"])
    except Exception:
        return False

    return True


def configure_ldap_server(
    username: str, password: str, instance_dir: str, port: int
) -> LdapServer:
    subprocess.Popen(["glauth", "--version"])

    LDAP_SERVER_HOST = "127.0.0.1"

    ldap_cfg_path = f"{instance_dir}/ldap.cfg"
    with open(ldap_cfg_path, "x") as f:
        password_sha256 = hashlib.sha256(password.encode("utf8")).hexdigest()
        f.write(
            f"""
            [ldap]
                enabled = true
                listen = "{LDAP_SERVER_HOST}:{port}"

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

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    deadline = time.time() + 3

    while deadline > time.time():
        try:
            sock.connect((LDAP_SERVER_HOST, port))
            break
        except ConnectionRefusedError:
            time.sleep(0.1)

    return LdapServer(
        host=LDAP_SERVER_HOST,
        port=port,
        process=process,
        user=username,
        password=password,
    )

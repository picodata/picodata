from typing import Dict, Any, Tuple, Optional

from conftest import (
    Cluster,
    Instance,
    TarantoolError,
)
from urllib.request import urlopen, Request
from urllib.error import HTTPError
import pytest
import json

USERNAME = "test_auth"
PASSWORD = "test_auth1A!"


def create_user(instance: Instance):
    try:
        instance.sql(f"CREATE USER \"{USERNAME}\" WITH PASSWORD '{PASSWORD}' USING chap-sha1 OPTION (TIMEOUT = 3.0);")
    except Exception as e:
        if not isinstance(e, TarantoolError):
            raise e


def set_jwt_enabled(instance: Instance, enabled: bool):
    try:
        instance.sql("ALTER SYSTEM RESET jwt_secret" if enabled else "ALTER SYSTEM SET jwt_secret = ''")
    except Exception as e:
        if not isinstance(e, TarantoolError):
            raise e


def authorize(instance: Instance, username: str, password) -> Tuple[Dict[str, str], int]:
    create_user(instance)
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]
    request = Request(
        f"http://{http_listen}/api/v1/session",
        data=json.dumps({"username": username, "password": password}).encode(),
        method="POST",
    )
    try:
        with urlopen(request) as response:
            response_data = response.read().decode()
            result = json.loads(response_data)
            return result, response.status
    except HTTPError as e:
        response_data = e.read().decode("utf-8")
        result = json.loads(response_data)
        return result, e.status  # type: ignore


def get_auth_token(instance: Instance) -> str:
    return authorize(instance, USERNAME, PASSWORD)[0]["auth"]


@pytest.fixture
def auth_token(request, instance: Instance) -> Optional[str]:
    auth_type = request.param
    if auth_type == "unauthorized":
        set_jwt_enabled(instance, False)
        return None

    set_jwt_enabled(instance, True)
    return get_auth_token(instance)


def get_authorized(url: str, auth_token: str) -> Any:
    request = Request(url, headers={"Authorization": f"Bearer {auth_token}"}, method="GET")
    return urlopen(request)


def get_unauthorized(url: str) -> Any:
    request = Request(url, method="GET")
    return urlopen(request)


def get_url(url: str, auth_token: Optional[str]) -> Any:
    if auth_token is None:
        return get_unauthorized(url)

    return get_authorized(url, auth_token)


@pytest.mark.webui
def test_http_routes(instance: Instance):
    instance.eval(
        """
        pico.httpd:route({path = '/hello', method = 'GET'}, function(req)
            return {
                status = 200,
                body = 'world'
            }
        end)
        """
    )
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]
    with urlopen(f"http://{http_listen}/hello") as response:
        assert response.read() == b"world"


@pytest.mark.webui
@pytest.mark.parametrize("auth_token", ["authorized", "unauthorized"], indirect=True)
def test_webui_basic(instance: Instance, auth_token: Optional[str]):
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]

    instance_version = instance.eval("return pico.PICODATA_VERSION")
    instance_slab = instance.call("box.slab.info")
    capacity_usage = round(instance_slab["quota_used"] * 100 / instance_slab["quota_size"], 1)

    with urlopen(f"http://{http_listen}/") as response:
        assert response.headers.get("content-type") == "text/html"

    with get_url(f"http://{http_listen}/api/v1/tiers", auth_token) as response:
        assert response.headers.get("content-type") == "application/json"
        assert json.load(response) == [
            {
                "replicasets": [
                    {
                        "state": "Online",
                        "version": instance_version,
                        "instances": [
                            {
                                "failureDomain": {},
                                "isLeader": True,
                                "currentState": "Online",
                                "targetState": "Online",
                                "name": "default_1_1",
                                "version": instance_version,
                                "httpAddress": http_listen,
                                "binaryAddress": instance.iproto_listen,
                                "pgAddress": instance.pg_listen,
                            }
                        ],
                        "instanceCount": 1,
                        "capacityUsage": capacity_usage,
                        "memory": {
                            "usable": instance_slab["quota_size"],
                            "used": instance_slab["quota_used"],
                        },
                        "uuid": instance.replicaset_uuid(),
                        "name": "default_1",
                    }
                ],
                "replicasetCount": 1,
                "rf": 1,
                "bucketCount": 3000,
                "instanceCount": 1,
                "can_vote": True,
                "name": "default",
                "services": [],
            }
        ]

    with get_url(f"http://{http_listen}/api/v1/cluster", auth_token) as response:
        assert response.headers.get("content-type") == "application/json"
        assert json.load(response) == {
            "capacityUsage": capacity_usage,
            "clusterName": instance.cluster_name,
            "replicasetsCount": 1,
            "instancesCurrentStateOffline": 0,
            "currentInstaceVersion": instance_version,
            "memory": {
                "usable": instance_slab["quota_size"],
                "used": instance_slab["quota_used"],
            },
            "instancesCurrentStateOnline": 1,
            "plugins": [],
        }


@pytest.mark.webui
def test_webui_with_plugin(cluster: Cluster):
    cluster_cfg = """
    cluster:
        name: test
        tier:
            red:
                replication_factor: 1
            blue:
                replication_factor: 1
            green:
                replication_factor: 1
    """
    cluster.set_config_file(yaml=cluster_cfg)

    plugin_1 = "testplug"
    plugin_1_services = ["testservice_1", "testservice_2"]
    plugin_2 = "testplug_small"
    plugin_2_service = "testservice_1"
    version_1 = "0.1.0"

    i1 = cluster.add_instance(wait_online=True, tier="red", enable_http=True)
    i2 = cluster.add_instance(wait_online=True, tier="blue")
    i3 = cluster.add_instance(wait_online=True, tier="green")

    i1.call("pico.install_plugin", plugin_1, version_1)
    i1.call("pico.install_plugin", plugin_2, version_1)
    i1.call(
        "pico.service_append_tier",
        plugin_1,
        version_1,
        plugin_1_services[0],
        "red",
    )
    i1.call(
        "pico.service_append_tier",
        plugin_1,
        version_1,
        plugin_1_services[1],
        "blue",
    )
    i1.call(
        "pico.service_append_tier",
        plugin_2,
        version_1,
        plugin_2_service,
        "blue",
    )

    http_listen = i1.env["PICODATA_HTTP_LISTEN"]
    instance_version = i1.eval("return pico.PICODATA_VERSION")

    with urlopen(f"http://{http_listen}/") as response:
        assert response.headers.get("content-type") == "text/html"

    create_user(i1)
    auth_token = get_auth_token(i1)

    instance_template = {
        "failureDomain": {},
        "isLeader": True,
        "currentState": "Online",
        "targetState": "Online",
        "version": instance_version,
    }
    instance_1 = {
        **instance_template,
        "name": "red_1_1",
        "binaryAddress": i1.iproto_listen,
        "pgAddress": i1.pg_listen,
        "httpAddress": http_listen,
    }
    instance_2 = {
        **instance_template,
        "name": "blue_1_1",
        "binaryAddress": i2.iproto_listen,
        "pgAddress": i2.pg_listen,
        "httpAddress": "",
    }
    instance_3 = {
        **instance_template,
        "name": "green_1_1",
        "binaryAddress": i3.iproto_listen,
        "pgAddress": i3.pg_listen,
        "httpAddress": "",
    }

    replicaset_template = {
        "state": "Online",
        "version": instance_version,
        "instanceCount": 1,
        "capacityUsage": 0.0,
        "memory": {
            "usable": 67108864,
            "used": 0,
        },
        "uuid": i1.replicaset_uuid(),
        "name": "r1",
    }
    r1 = {
        **replicaset_template,
        "uuid": i1.replicaset_uuid(),
        "name": "red_1",
        "instances": [instance_1],
    }
    r2 = {
        **replicaset_template,
        "uuid": i2.replicaset_uuid(),
        "name": "blue_1",
        "instances": [instance_2],
    }
    r3 = {
        **replicaset_template,
        "uuid": i3.replicaset_uuid(),
        "name": "green_1",
        "instances": [instance_3],
    }

    tier_template = {
        "replicasetCount": 1,
        "rf": 1,
        "bucketCount": 3000,
        "instanceCount": 1,
        "can_vote": True,
    }

    tier_red = {
        **tier_template,
        "name": "red",
        "services": [plugin_1_services[0]],
        "replicasets": [r1],
    }
    tier_blue = {
        **tier_template,
        "name": "blue",
        "services": [plugin_1_services[1], plugin_2_service],
        "replicasets": [r2],
    }
    tier_green = {**tier_template, "name": "green", "services": [], "replicasets": [r3]}

    with get_url(f"http://{http_listen}/api/v1/tiers", auth_token) as response:
        assert response.headers.get("content-type") == "application/json"
        assert sorted(json.load(response), key=lambda tier: tier["name"]) == [
            tier_blue,
            tier_green,
            tier_red,
        ]

    with get_url(f"http://{http_listen}/api/v1/cluster", auth_token) as response:
        assert response.headers.get("content-type") == "application/json"
        assert json.load(response) == {
            "capacityUsage": 0.0,
            "clusterName": cluster.id,
            "replicasetsCount": 3,
            "instancesCurrentStateOnline": 3,
            "instancesCurrentStateOffline": 0,
            "currentInstaceVersion": instance_version,
            "memory": {"usable": 201326592, "used": 0},
            "plugins": [
                plugin_1 + " " + version_1,
                plugin_2 + " " + version_1,
            ],
        }


@pytest.mark.webui
def test_webui_replicaset_state(cluster: Cluster):
    cluster_cfg = """
    cluster:
        name: test
        tier:
            red:
                replication_factor: 2
    """
    cluster.set_config_file(yaml=cluster_cfg)

    i1 = cluster.add_instance(wait_online=True, tier="red")
    i2 = cluster.add_instance(wait_online=True, tier="red")
    i3 = cluster.add_instance(wait_online=True, tier="red")
    i4 = cluster.add_instance(wait_online=True, tier="red", enable_http=True)

    # 1. Make sure i3 is leader
    # 2. Kill i3
    # 3. Make sure i4 was promoted
    # 4. Check that replicaset status == Online
    i3.promote_or_fail()
    i3.kill()
    cluster.wait_has_states(i3, "Offline", "Offline")

    i4.promote_or_fail()

    # we have to query i4 to avoid incosistency in cluster view
    # data from global tables (as instance state etc) could be
    # distributed in cluster with delay significant enough to
    # cause flaks in tests
    http_listen = i4.env["PICODATA_HTTP_LISTEN"]
    instance_version = i1.eval("return pico.PICODATA_VERSION")

    with urlopen(f"http://{http_listen}/") as response:
        assert response.headers.get("content-type") == "text/html"

    create_user(i4)
    auth_token = get_auth_token(i4)

    instance_template = {
        "failureDomain": {},
        "currentState": "Online",
        "targetState": "Online",
        "version": instance_version,
        "httpAddress": "",
    }
    instance_1 = {
        **instance_template,
        "name": "red_1_1",
        "isLeader": True,
        "binaryAddress": i1.iproto_listen,
        "pgAddress": i1.pg_listen,
    }
    instance_2 = {
        **instance_template,
        "name": "red_1_2",
        "isLeader": False,
        "binaryAddress": i2.iproto_listen,
        "pgAddress": i2.pg_listen,
    }
    instance_3 = {
        **instance_template,
        "name": "red_2_1",
        "isLeader": False,
        "currentState": "Offline",
        "targetState": "Offline",
        "binaryAddress": i3.iproto_listen,
        "pgAddress": i3.pg_listen,
        "version": "",
    }
    instance_4 = {
        **instance_template,
        "name": "red_2_2",
        "isLeader": True,
        "binaryAddress": i4.iproto_listen,
        "pgAddress": i4.pg_listen,
        "httpAddress": http_listen,
    }

    replicaset_template = {
        "state": "Online",
        "version": instance_version,
        "instanceCount": 2,
        "capacityUsage": 0,
    }
    r1 = {
        **replicaset_template,
        "state": "Online",
        "uuid": i1.replicaset_uuid(),
        "name": "red_1",
        "instances": [instance_1, instance_2],
        "memory": {
            "usable": 67108864,
            "used": 0,
        },
    }
    r2 = {
        **replicaset_template,
        "state": "Online",
        # use i4, as i3 is already dead
        "uuid": i4.replicaset_uuid(),
        "name": "red_2",
        "instances": [instance_3, instance_4],
        "memory": {
            "usable": 67108864,
            "used": 0,
        },
    }

    tier_red = {
        "replicasetCount": 2,
        "rf": 2,
        "bucketCount": 3000,
        "instanceCount": 4,
        "can_vote": True,
        "name": "red",
        "services": [],
        "replicasets": [r1, r2],
    }

    with get_url(f"http://{http_listen}/api/v1/tiers", auth_token) as response:
        assert response.headers.get("content-type") == "application/json"
        assert sorted(json.load(response), key=lambda tier: tier["name"]) == [
            tier_red,
        ], "/api/v1/tier"

    with get_url(f"http://{http_listen}/api/v1/cluster", auth_token) as response:
        assert response.headers.get("content-type") == "application/json"
        assert json.load(response) == {
            "capacityUsage": 0,
            "clusterName": cluster.id,
            "replicasetsCount": 2,
            "instancesCurrentStateOnline": 3,
            "instancesCurrentStateOffline": 1,
            "currentInstaceVersion": instance_version,
            "memory": {"usable": 134217728, "used": 0},
            "plugins": [],
        }, "/api/v1/cluster"


@pytest.mark.webui
def test_webui_can_vote_flag(cluster: Cluster):
    cluster_cfg = """
    cluster:
        name: test
        tier:
            red:
                replication_factor: 1
            blue:
                replication_factor: 1
                can_vote: false
    """
    cluster.set_config_file(yaml=cluster_cfg)

    i1 = cluster.add_instance(wait_online=True, tier="red", enable_http=True)
    i2 = cluster.add_instance(wait_online=True, tier="blue")

    http_listen = i1.env["PICODATA_HTTP_LISTEN"]
    instance_version = i1.eval("return pico.PICODATA_VERSION")

    with urlopen(f"http://{http_listen}/") as response:
        assert response.headers.get("content-type") == "text/html"

    create_user(i1)
    auth_token = get_auth_token(i1)

    instance_template = {
        "failureDomain": {},
        "isLeader": True,
        "currentState": "Online",
        "targetState": "Online",
        "version": instance_version,
    }
    instance_1 = {
        **instance_template,
        "name": "red_1_1",
        "binaryAddress": i1.iproto_listen,
        "pgAddress": i1.pg_listen,
        "httpAddress": http_listen,
    }
    instance_2 = {
        **instance_template,
        "name": "blue_1_1",
        "binaryAddress": i2.iproto_listen,
        "pgAddress": i2.pg_listen,
        "httpAddress": "",
    }

    replicaset_template = {
        "state": "Online",
        "version": instance_version,
        "instanceCount": 1,
        "capacityUsage": 0.0,
        "memory": {
            "usable": 67108864,
            "used": 0,
        },
        "uuid": i1.replicaset_uuid(),
        "name": "r1",
    }
    r1 = {
        **replicaset_template,
        "uuid": i1.replicaset_uuid(),
        "name": "red_1",
        "instances": [instance_1],
    }
    r2 = {
        **replicaset_template,
        "uuid": i2.replicaset_uuid(),
        "name": "blue_1",
        "instances": [instance_2],
    }

    tier_template = {
        "replicasetCount": 1,
        "rf": 1,
        "bucketCount": 3000,
        "instanceCount": 1,
    }

    tier_red = {
        **tier_template,
        "name": "red",
        "services": [],
        "replicasets": [r1],
        "can_vote": True,
    }
    tier_blue = {
        **tier_template,
        "can_vote": False,
        "name": "blue",
        "services": [],
        "replicasets": [r2],
    }

    with get_url(f"http://{http_listen}/api/v1/tiers", auth_token) as response:
        assert response.headers.get("content-type") == "application/json"
        assert sorted(json.load(response), key=lambda tier: tier["name"]) == [
            tier_blue,
            tier_red,
        ]


@pytest.mark.webui
def test_jwt_session_login_success(instance: Instance):
    response_data, status_code = authorize(instance, USERNAME, PASSWORD)

    assert status_code == 200
    assert "auth" in response_data
    assert "refresh" in response_data
    assert isinstance(response_data["auth"], str)
    assert isinstance(response_data["refresh"], str)
    assert len(response_data["auth"].split(".")) == 3
    assert len(response_data["refresh"].split(".")) == 3


@pytest.mark.webui
def test_jwt_session_login_invalid_credentials(instance: Instance):
    response_data, status_code = authorize(instance, USERNAME, "wrongpass")

    assert status_code == 401
    assert response_data["error"] == "wrongCredentials"
    assert response_data["errorMessage"] == "invalid credentials"

    response_data, status_code = authorize(instance, "someuser", "wrongpass")

    assert status_code == 401
    assert response_data["error"] == "wrongCredentials"
    assert response_data["errorMessage"] == "invalid credentials"


@pytest.mark.webui
def test_jwt_session_refresh_success(instance: Instance):
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]

    response_data, _ = authorize(instance, USERNAME, PASSWORD)
    refresh_token = response_data["refresh"]

    with get_authorized(f"http://{http_listen}/api/v1/session", refresh_token) as response:
        new_tokens = json.loads(response.read().decode())
        status_code = response.status

    assert status_code == 200

    assert "auth" in new_tokens
    assert "refresh" in new_tokens
    assert isinstance(new_tokens["auth"], str)
    assert isinstance(new_tokens["refresh"], str)
    assert len(new_tokens["auth"].split(".")) == 3
    assert len(new_tokens["refresh"].split(".")) == 3


@pytest.mark.webui
@pytest.mark.parametrize("auth_token", ["authorized", "unauthorized"], indirect=True)
def test_ui_config(instance: Instance, auth_token):
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]

    with get_url(f"http://{http_listen}/api/v1/config", auth_token) as response:
        assert response.headers.get("content-type") == "application/json"
        assert response.status == 200
        assert json.loads(response.read().decode()) == {"isAuthEnabled": auth_token is not None}

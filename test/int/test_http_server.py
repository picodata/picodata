from conftest import (
    Cluster,
    Instance,
    _PLUGIN,
    _PLUGIN_SERVICES,
    _PLUGIN_SMALL,
    _PLUGIN_SMALL_SERVICES,
    _PLUGIN_VERSION_1,
)
from urllib.request import urlopen
import pytest
import json
import requests  # type: ignore


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
def test_webui_basic(instance: Instance):
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]

    instance_version = instance.eval("return pico.PICODATA_VERSION")

    with urlopen(f"http://{http_listen}/") as response:
        assert response.headers.get("content-type") == "text/html"

    with urlopen(f"http://{http_listen}/api/v1/tiers") as response:
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
                                "name": "i1",
                                "version": instance_version,
                                "httpAddress": http_listen,
                                "binaryAddress": instance.listen,
                            }
                        ],
                        "instanceCount": 1,
                        "capacityUsage": 50,
                        "memory": {
                            "usable": 67108864,
                            "used": 33554432,
                        },
                        "uuid": instance.replicaset_uuid(),
                        "id": "r1",
                    }
                ],
                "replicasetCount": 1,
                "rf": 1,
                "instanceCount": 1,
                "can_vote": True,
                "name": "default",
                "services": [],
            }
        ]

    with urlopen(f"http://{http_listen}/api/v1/cluster") as response:
        assert response.headers.get("content-type") == "application/json"
        assert json.load(response) == {
            "capacityUsage": 50,
            "replicasetsCount": 1,
            "instancesCurrentStateOffline": 0,
            "currentInstaceVersion": instance_version,
            "memory": {"usable": 67108864, "used": 33554432},
            "instancesCurrentStateOnline": 1,
            "plugins": [],
        }


@pytest.mark.webui
def test_webui_with_plugin(cluster: Cluster):
    cluster_cfg = """
    cluster:
        cluster_id: test
        tier:
            red:
                replication_factor: 1
            blue:
                replication_factor: 1
            green:
                replication_factor: 1
    """
    cluster.set_config_file(yaml=cluster_cfg)

    i1 = cluster.add_instance(wait_online=True, tier="red", enable_http=True)
    i2 = cluster.add_instance(wait_online=True, tier="blue")
    i3 = cluster.add_instance(wait_online=True, tier="green")

    i1.call("pico.install_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    i1.call("pico.install_plugin", _PLUGIN_SMALL, _PLUGIN_VERSION_1)
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "red",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[1],
        "blue",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN_SMALL,
        _PLUGIN_VERSION_1,
        _PLUGIN_SMALL_SERVICES[0],
        "blue",
    )

    http_listen = i1.env["PICODATA_HTTP_LISTEN"]
    instance_version = i1.eval("return pico.PICODATA_VERSION")

    with urlopen(f"http://{http_listen}/") as response:
        assert response.headers.get("content-type") == "text/html"

    instance_template = {
        "failureDomain": {},
        "isLeader": True,
        "currentState": "Online",
        "targetState": "Online",
        "version": instance_version,
    }
    instance_1 = {
        **instance_template,
        "name": "i1",
        "binaryAddress": i1.listen,
        "httpAddress": http_listen,
    }
    instance_2 = {
        **instance_template,
        "name": "i2",
        "binaryAddress": i2.listen,
        "httpAddress": "",
    }
    instance_3 = {
        **instance_template,
        "name": "i3",
        "binaryAddress": i3.listen,
        "httpAddress": "",
    }

    replicaset_template = {
        "state": "Online",
        "version": instance_version,
        "instanceCount": 1,
        "capacityUsage": 50,
        "memory": {
            "usable": 67108864,
            "used": 33554432,
        },
        "uuid": i1.replicaset_uuid(),
        "id": "r1",
    }
    r1 = {
        **replicaset_template,
        "uuid": i1.replicaset_uuid(),
        "id": "r1",
        "instances": [instance_1],
    }
    r2 = {
        **replicaset_template,
        "uuid": i2.replicaset_uuid(),
        "id": "r2",
        "instances": [instance_2],
    }
    r3 = {
        **replicaset_template,
        "uuid": i3.replicaset_uuid(),
        "id": "r3",
        "instances": [instance_3],
    }

    tier_template = {
        "replicasetCount": 1,
        "rf": 1,
        "instanceCount": 1,
        "can_vote": True,
    }

    tier_red = {
        **tier_template,
        "name": "red",
        "services": [_PLUGIN_SERVICES[0]],
        "replicasets": [r1],
    }
    tier_blue = {
        **tier_template,
        "name": "blue",
        "services": [_PLUGIN_SERVICES[1], _PLUGIN_SMALL_SERVICES[0]],
        "replicasets": [r2],
    }
    tier_green = {**tier_template, "name": "green", "services": [], "replicasets": [r3]}

    with urlopen(f"http://{http_listen}/api/v1/tiers") as response:
        assert response.headers.get("content-type") == "application/json"
        assert sorted(json.load(response), key=lambda tier: tier["name"]) == [
            tier_blue,
            tier_green,
            tier_red,
        ]

    with urlopen(f"http://{http_listen}/api/v1/cluster") as response:
        assert response.headers.get("content-type") == "application/json"
        assert json.load(response) == {
            "capacityUsage": 50,
            "replicasetsCount": 3,
            "instancesCurrentStateOnline": 3,
            "instancesCurrentStateOffline": 0,
            "currentInstaceVersion": instance_version,
            "memory": {"usable": 201326592, "used": 100663296},
            "plugins": [
                _PLUGIN + " " + _PLUGIN_VERSION_1,
                _PLUGIN_SMALL + " " + _PLUGIN_VERSION_1,
            ],
        }


@pytest.mark.webui
def test_metrics_ok(instance: Instance) -> None:
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]
    response = requests.get(f"http://{http_listen}/metrics")
    assert response.ok

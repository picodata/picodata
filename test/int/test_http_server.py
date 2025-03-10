from conftest import (
    Cluster,
    Instance,
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
    instance_slab = instance.call("box.slab.info")

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
                                "name": "default_1_1",
                                "version": instance_version,
                                "httpAddress": http_listen,
                                "binaryAddress": instance.iproto_listen,
                            }
                        ],
                        "instanceCount": 1,
                        "capacityUsage": 50,
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
        "httpAddress": http_listen,
    }
    instance_2 = {
        **instance_template,
        "name": "blue_1_1",
        "binaryAddress": i2.iproto_listen,
        "httpAddress": "",
    }
    instance_3 = {
        **instance_template,
        "name": "green_1_1",
        "binaryAddress": i3.iproto_listen,
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
                plugin_1 + " " + version_1,
                plugin_2 + " " + version_1,
            ],
        }


@pytest.mark.webui
def test_metrics_ok(instance: Instance) -> None:
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]
    response = requests.get(f"http://{http_listen}/metrics")
    assert response.ok

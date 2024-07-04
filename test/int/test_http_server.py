from conftest import Instance
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
def test_webui(instance: Instance):
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
                "plugins": [],
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
        }


@pytest.mark.webui
def test_metrics_ok(instance: Instance) -> None:
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]
    response = requests.get(f"http://{http_listen}/metrics")
    assert response.ok

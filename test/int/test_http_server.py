from conftest import Cluster, Instance
from urllib.request import urlopen
import pytest
import json


@pytest.fixture
def instance(cluster: Cluster):
    instance = cluster.add_instance(wait_online=False)
    instance.env["PICODATA_HTTP_LISTEN"] = f"{cluster.base_host}:{cluster.base_port+80}"
    instance.start()
    instance.wait_online()
    return instance


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


def test_webui(cluster_with_webui: Cluster):
    c = cluster_with_webui
    instance = c.add_instance(wait_online=False)
    http_listen = f"{c.base_host}:{c.base_port+80}"
    instance.env["PICODATA_HTTP_LISTEN"] = http_listen
    instance.start()
    instance.wait_online()
    with urlopen(f"http://{http_listen}/") as response:
        assert response.headers.get("content-type") == "text/html"

    with urlopen(f"http://{http_listen}/api/v1/replicaset") as response:
        assert response.headers.get("content-type") == "application/json"
        assert json.load(response) == [
            {
                "grade": "Online",
                "version": "??.??",
                "instances": [
                    {
                        "failureDomain": {},
                        "isLeader": True,
                        "currentGrade": "Online",
                        "targetGrade": "Online",
                        "name": "i1",
                        "version": "??.??",
                    }
                ],
                "instanceCount": 1,
                "capacity": 100,
                "uuid": instance.replicaset_uuid(),
                "id": "r1",
            }
        ]

    with urlopen(f"http://{http_listen}/api/v1/cluster") as response:
        assert response.headers.get("content-type") == "application/json"
        assert json.load(response) == {
            "capacityUsage": 100,
            "replicasetsCount": 1,
            "instancesCurrentGradeOffline": 0,
            "currentInstaceVersion": "23.06.0",
            "memory": {"usable": 33554432, "used": 33554432},
            "instancesCurrentGradeOnline": 1,
        }

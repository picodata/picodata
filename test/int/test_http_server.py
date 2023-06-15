from conftest import Cluster, Instance
from urllib.request import urlopen
import pytest


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


def test_webui(instance: Instance):
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]
    with urlopen(f"http://{http_listen}/") as response:
        assert response.headers.get("content-type") == "text/html"

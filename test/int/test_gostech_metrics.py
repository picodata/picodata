import requests  # type: ignore

from conftest import (
    Instance,
    MetricsServer,
)


def test_metrics_ok(instance: Instance) -> None:
    server = MetricsServer(instance)
    server.start()
    response = requests.get(server.url)
    assert response.ok
    server.stop()

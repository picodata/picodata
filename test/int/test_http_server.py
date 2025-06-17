from conftest import (
    Cluster,
    Instance,
    TarantoolError,
    ErrorCode,
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
                                "pgAddress": instance.pg_listen,
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
            "clusterName": instance.cluster_name,
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
            "clusterName": cluster.id,
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

    with urlopen(f"http://{http_listen}/api/v1/tiers") as response:
        assert response.headers.get("content-type") == "application/json"
        assert sorted(json.load(response), key=lambda tier: tier["name"]) == [
            tier_blue,
            tier_red,
        ]


@pytest.mark.webui
def test_metrics_ok(instance: Instance) -> None:
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]
    response = requests.get(f"http://{http_listen}/metrics")
    assert response.ok


# Verifies that all picodata metrics are present
# and accessible on /metrics endpoint
@pytest.mark.webui
def test_picodata_metrics(cluster: Cluster) -> None:
    instance = cluster.add_instance(name="i1", init_replication_factor=2, enable_http=True)
    i2 = cluster.add_instance(name="i2", enable_http=True)

    instance.wait_online()
    instance.sql("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT) DISTRIBUTED GLOBALLY")
    instance.sql("INSERT INTO test VALUES (1, 'one')")

    with pytest.raises(TarantoolError) as e:
        i2.sql("INSERT INTO test VALUES (1, 'one')")
        assert e.value.args[:2] == (ErrorCode.SbroadError, "sbroad: Lua error (IR dispatch)")

    i2.sql("INSERT INTO test VALUES (2, 'two')")
    with pytest.raises(TarantoolError) as e:
        instance.sql("INSERT INTO test VALUES (2, 'two')")
        assert e.value.args[:2] == (ErrorCode.SbroadError, "sbroad: Lua error (IR dispatch)")

    http_listen = instance.env["PICODATA_HTTP_LISTEN"]
    url = f"http://{http_listen}/metrics"
    response = requests.get(url)
    assert response.ok, f"Metrics endpoint {url} did not return OK: {response.status_code}"
    metrics_output = response.text
    expected_metrics = [
        "pico_governor_changes_total",
        "pico_sql_query_total",
        "pico_sql_query_errors_total",
        "pico_sql_query_duration",
        "pico_rpc_request_total",
        "pico_rpc_request_errors_total",
        "pico_rpc_request_duration",
        "pico_cas_records_total",
        "pico_cas_errors_total",
        "pico_cas_ops_duration",
        "pico_instance_state",
        "pico_raft_applied_index",
        "pico_raft_commit_index",
        "pico_raft_term",
        "pico_raft_state",
        "pico_raft_leader_id",
    ]

    for metric in expected_metrics:
        assert metric in metrics_output, f"Metric '{metric}' not found in /metrics output"

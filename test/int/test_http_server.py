from conftest import Cluster
from urllib.request import urlopen


def test_server_works(cluster: Cluster):
    instance = cluster.add_instance(wait_online=False)
    script = f"{cluster.data_dir}/add_routes.lua"
    with open(script, "w") as f:
        f.write(
            """
            function handler(req)
                return {
                    status = 200,
                    headers = { ['content-type'] = 'text/html; charset=utf8' },
                    body = 'world'
                }
            end
            pico.httpd:route({path = '/hello', method = 'GET'}, handler)
            """
        )
    instance.env["PICODATA_SCRIPT"] = script
    instance.env["PICODATA_HTTP_LISTEN"] = ":8080"
    instance.start()
    instance.wait_online()
    with urlopen("http://127.0.0.1:8080/hello") as response:
        assert b"world" == response.read()


def test_webui(cluster: Cluster):
    instance = cluster.add_instance(wait_online=False)
    instance.env["PICODATA_HTTP_LISTEN"] = ":8081"
    instance.start()
    instance.wait_online()
    with urlopen("http://127.0.0.1:8081") as response:
        assert "text/html" == response.headers.get("content-type")

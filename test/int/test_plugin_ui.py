import json
import os
from pathlib import Path
from typing import Any, List, Optional
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest
from conftest import Cluster, Instance, TarantoolError

from test_http_server import create_user, get_auth_token, get_unauthorized, get_url, set_jwt_enabled
from test_plugin import init_dummy_plugin

# The whole plugin-ui feature (manifest `webui:` section, `/api/v1/plugin-ui/pages`,
# `/plugin-ui/<plugin>/<version>/*`) is compiled only with `--features webui`.
pytestmark = pytest.mark.webui

_PLUGIN = "testplug_webui"
_PLUGIN_VERSION_1 = "0.1.0"
_PLUGIN_VERSION_2 = "0.2.0"
# webui pages are a plugin-level concept, independent of services,
# so these fixtures don't declare any services -
# that sidesteps needing to match `testplug`'s service config schemas.
_SERVICES: List[str] = []

_DEFAULT_PAGES = [
    {"slug": "dashboard", "title": "dashboard.title", "entry": "dashboard.js"},
    {"slug": "public", "title": "public.title", "entry": "public.js", "auth": "plugin"},
]


def init_dummy_plugin_with_webui(
    cluster: Cluster,
    plugin: str,
    version: str,
    *,
    services: List[str] = _SERVICES,
    pages: Optional[List[dict]] = None,
    create_entries: bool = True,
) -> Path:
    """
    Like `init_dummy_plugin`, but also appends a `webui:` section to the
    generated manifest, so the resulting plugin directory is a valid fixture
    for `GET /api/v1/plugin-ui/pages` and `GET /plugin-ui/<plugin>/<version>/*`.

    By default also creates each page's `entry` file under `assets/webui/`;
    pass `create_entries=False` to declare pages whose entry files don't
    exist on disk (e.g. to test manifest validation of a missing entry).
    """
    if pages is None:
        pages = _DEFAULT_PAGES

    plugin_dir = init_dummy_plugin(cluster, plugin, version, services=services)

    assets_dir = plugin_dir / "assets" / "webui"
    os.makedirs(assets_dir, exist_ok=True)

    manifest_path = plugin_dir / "manifest.yaml"
    with open(manifest_path, "a") as f:
        print("webui:", file=f)
        for page in pages:
            print(f"  - slug: {page['slug']}", file=f)
            print(f"    title: {page['title']}", file=f)
            print(f"    entry: {page['entry']}", file=f)
            if page.get("auth"):
                print(f"    auth: {page['auth']}", file=f)

    if create_entries:
        for page in pages:
            entry_path = assets_dir / page["entry"]
            content = page.get("content", f"// {plugin} {version} {page['slug']}\n")
            entry_path.write_text(content)

    return plugin_dir


def pages_url(instance: Instance) -> str:
    return f"http://{instance.http_listen}/api/v1/plugin-ui/pages"


def asset_url(instance: Instance, plugin: str, version: str, path: str) -> str:
    return f"http://{instance.http_listen}/plugin-ui/{plugin}/{version}/{path}"


def get_json(url: str, auth_token: Optional[str] = None) -> Any:
    with get_url(url, auth_token) as response:
        assert response.headers.get("content-type") == "application/json"
        return json.load(response)


# ---------------------------------- Manifest validation -------------------------------------------


def test_manifest_webui_duplicate_slug_rejected(cluster: Cluster):
    init_dummy_plugin_with_webui(
        cluster,
        "testplug_webui_dup",
        "0.1.0",
        pages=[
            {"slug": "dashboard", "title": "dashboard.title", "entry": "a.js"},
            {"slug": "dashboard", "title": "other.title", "entry": "b.js"},
        ],
    )
    i1 = cluster.add_instance(wait_online=True)

    with pytest.raises(TarantoolError, match="duplicate webui page slug"):
        i1.sql("CREATE PLUGIN testplug_webui_dup 0.1.0")


def test_manifest_webui_missing_entry_file_rejected(cluster: Cluster):
    init_dummy_plugin_with_webui(
        cluster,
        "testplug_webui_missing",
        "0.1.0",
        pages=[{"slug": "dashboard", "title": "dashboard.title", "entry": "does_not_exist.js"}],
        create_entries=False,
    )
    i1 = cluster.add_instance(wait_online=True)

    with pytest.raises(TarantoolError, match="non-existent entry file"):
        i1.sql("CREATE PLUGIN testplug_webui_missing 0.1.0")


def test_manifest_webui_valid_manifest_installs_ok(cluster: Cluster):
    init_dummy_plugin_with_webui(cluster, _PLUGIN, _PLUGIN_VERSION_1)
    i1 = cluster.add_instance(wait_online=True)

    # Should not raise.
    i1.sql(f"CREATE PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1}")


# ---------------------------------- GET /api/v1/plugin-ui/pages -------------------------------------


def test_plugin_ui_pages_endpoint_auth_filtering_and_order(cluster: Cluster):
    init_dummy_plugin_with_webui(cluster, _PLUGIN, _PLUGIN_VERSION_1)
    i1 = cluster.add_instance(wait_online=True)
    i1.sql(f"CREATE PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1}")
    i1.sql(f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} ENABLE")

    create_user(i1)
    set_jwt_enabled(i1, True)
    auth_token = get_auth_token(i1)

    # Authenticated: full list, in manifest declaration order.
    pages = get_json(pages_url(i1), auth_token)
    assert pages == [
        {
            "plugin": _PLUGIN,
            "slug": "dashboard",
            "title": "dashboard.title",
            "entry": "dashboard.js",
            "version": _PLUGIN_VERSION_1,
            "auth": "cluster",
        },
        {
            "plugin": _PLUGIN,
            "slug": "public",
            "title": "public.title",
            "entry": "public.js",
            "version": _PLUGIN_VERSION_1,
            "auth": "plugin",
        },
    ]

    # Anonymous: only `auth: plugin` pages, no 401.
    anon_pages = get_json(pages_url(i1))
    assert anon_pages == [
        {
            "plugin": _PLUGIN,
            "slug": "public",
            "title": "public.title",
            "entry": "public.js",
            "version": _PLUGIN_VERSION_1,
            "auth": "plugin",
        },
    ]


def test_plugin_ui_pages_regression_existing_routes_still_require_auth(cluster: Cluster):
    """DoD: existing routes (tiers/cluster/memory/instance) behavior must be unaffected."""
    i1 = cluster.add_instance(wait_online=True)
    create_user(i1)
    set_jwt_enabled(i1, True)

    # The new optional-auth endpoint never rejects an anonymous caller.
    with get_url(pages_url(i1), None) as response:
        assert response.status == 200

    # Existing strict endpoints still reject an anonymous caller.
    with pytest.raises(HTTPError) as e:
        get_unauthorized(f"http://{i1.http_listen}/api/v1/tiers")
    assert e.value.code == 401

    with pytest.raises(HTTPError) as e:
        get_unauthorized(f"http://{i1.http_listen}/api/v1/cluster")
    assert e.value.code == 401


def test_plugin_ui_pages_aggregation_across_plugins_and_lifecycle(cluster: Cluster):
    plugin_a = "testplug_webui_zz"
    plugin_b = "testplug_webui_aa"
    for plugin in (plugin_a, plugin_b):
        init_dummy_plugin_with_webui(
            cluster,
            plugin,
            "0.1.0",
            pages=[{"slug": "only", "title": "only.title", "entry": "only.js"}],
        )

    i1 = cluster.add_instance(wait_online=True)

    for plugin in (plugin_a, plugin_b):
        i1.sql(f"CREATE PLUGIN {plugin} 0.1.0")
        i1.sql(f"ALTER PLUGIN {plugin} 0.1.0 ENABLE")

    create_user(i1)
    set_jwt_enabled(i1, True)
    auth_token = get_auth_token(i1)

    # Anonymous sees nothing (both plugins' pages are `auth: cluster`-only).
    pages = get_json(pages_url(i1))
    assert pages == []

    pages = get_json(pages_url(i1), auth_token)
    # Groups are ordered alphabetically by plugin name, regardless of install order.
    assert [p["plugin"] for p in pages] == [plugin_b, plugin_a]

    # Disabling a plugin removes its pages from the aggregated response.
    i1.sql(f"ALTER PLUGIN {plugin_b} 0.1.0 DISABLE")
    pages = get_json(pages_url(i1), auth_token)
    assert [p["plugin"] for p in pages] == [plugin_a]


# ---------------------------------- GET /plugin-ui/<plugin>/<version>/* -----------------------------


def test_plugin_ui_static_assets_lifecycle(cluster: Cluster):
    init_dummy_plugin_with_webui(cluster, _PLUGIN, _PLUGIN_VERSION_1)
    i1 = cluster.add_instance(wait_online=True)

    # Not installed/enabled yet: no route.
    with pytest.raises(HTTPError) as e:
        urlopen(asset_url(i1, _PLUGIN, _PLUGIN_VERSION_1, "dashboard.js"))
    assert e.value.code == 404

    i1.sql(f"CREATE PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1}")
    i1.sql(f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} ENABLE")

    with urlopen(asset_url(i1, _PLUGIN, _PLUGIN_VERSION_1, "dashboard.js")) as response:
        assert response.status == 200
        assert response.headers.get("content-type") == "application/javascript"
        body = response.read().decode()
    assert f"{_PLUGIN} {_PLUGIN_VERSION_1} dashboard" in body

    # Unknown file under an existing plugin/version prefix: 404, not a crash.
    with pytest.raises(HTTPError) as e:
        urlopen(asset_url(i1, _PLUGIN, _PLUGIN_VERSION_1, "no-such-file.js"))
    assert e.value.code == 404

    # Path traversal is rejected.
    with pytest.raises(HTTPError) as e:
        urlopen(asset_url(i1, _PLUGIN, _PLUGIN_VERSION_1, "../manifest.yaml"))
    assert e.value.code in (400, 404)

    i1.sql(f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} DISABLE")

    # No restart required: the route stops responding immediately after disable.
    with pytest.raises(HTTPError) as e:
        urlopen(asset_url(i1, _PLUGIN, _PLUGIN_VERSION_1, "dashboard.js"))
    assert e.value.code == 404

    # Re-enabling brings the route back.
    i1.sql(f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} ENABLE")
    with urlopen(asset_url(i1, _PLUGIN, _PLUGIN_VERSION_1, "dashboard.js")) as response:
        assert response.status == 200


def test_plugin_ui_static_assets_version_upgrade_keeps_old_route_until_disabled(cluster: Cluster):
    init_dummy_plugin_with_webui(cluster, _PLUGIN, _PLUGIN_VERSION_1)
    init_dummy_plugin_with_webui(cluster, _PLUGIN, _PLUGIN_VERSION_2)
    i1 = cluster.add_instance(wait_online=True)

    i1.sql(f"CREATE PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1}")
    i1.sql(f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} ENABLE")

    with urlopen(asset_url(i1, _PLUGIN, _PLUGIN_VERSION_1, "dashboard.js")) as response:
        v1_body = response.read().decode()
    assert f"{_PLUGIN_VERSION_1} dashboard" in v1_body

    # Install (but don't enable) v2: v1's route must keep responding untouched,
    # and v2's route must not exist yet.
    i1.sql(f"CREATE PLUGIN {_PLUGIN} {_PLUGIN_VERSION_2}")

    with urlopen(asset_url(i1, _PLUGIN, _PLUGIN_VERSION_1, "dashboard.js")) as response:
        assert response.read().decode() == v1_body

    with pytest.raises(HTTPError) as e:
        urlopen(asset_url(i1, _PLUGIN, _PLUGIN_VERSION_2, "dashboard.js"))
    assert e.value.code == 404

    # Switch: disable v1, enable v2.
    i1.sql(f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} DISABLE")
    i1.sql(f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_2} ENABLE")

    with pytest.raises(HTTPError) as e:
        urlopen(asset_url(i1, _PLUGIN, _PLUGIN_VERSION_1, "dashboard.js"))
    assert e.value.code == 404

    with urlopen(asset_url(i1, _PLUGIN, _PLUGIN_VERSION_2, "dashboard.js")) as response:
        v2_body = response.read().decode()
    assert f"{_PLUGIN_VERSION_2} dashboard" in v2_body
    assert v2_body != v1_body

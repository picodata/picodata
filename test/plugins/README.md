This folder contains tested plugins source code and the `share_dir` folder of `testplug`, which is passed to `picodata run` as the `--share-dir` parameter. It has a regular hierarchy of the [`share-dir`](https://docs.picodata.io/picodata/stable/reference/cli/#run_share_dir).

The `testplug` folder contains a workspace crate, which represents a single plugin with multiple services used for testing. The plugin uses the latest version of the plugin SDK (i.e. the `picodata-plugin` crate). This is the main plugin for testing new features.

Other plugins (such as `plug_wrong_version` and `plug_26_1`) are used for testing version guarantees or compatibility between minor releases. They do not use the latest SDK version, so they are considered "external".

For automatization purposes, all "external" plugin names should start with `plug_`.

See the `test_plugin_on_cluster_leader_change_not_present` for a test example.

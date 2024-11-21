# Load testing

## Navigation
* [General information](#general)
* [How to run using docker-compose](#run-docker-compose)
* [How to run locally](#run-locally)
   * [using makefile](#make-rules)
   * [using manual commands](#manually)
* [How to use results](#results-comparison)

## <a name="general"></a>General information
We use [k6](https://k6.io/docs/getting-started/running-k6/) for load testing with tarantool go package that allows to connect to tarantool instance. At the moment we have several scenarios that can be run separately or together (consequentially) using docker-compose or with local tools.

For each scenario, the result will be a file named `k6_summary.json` or `k6_summary_local.json` containing summarized metrics within the chosen stress test folder.
## <a name="run-docker-compose"></a>Run using docker-compose
* Run from `sbroad` directory
   ```bash
   make stress test=projection
   ```
   Parameter `test` is type of stress test (corresponding to the name of folder). To run all stress tests consequentially, use:
      ```bash
      make stress_all
      ```
* You can find summarized metrics by `stress-test/[test]/k6_summary.json` path.

## <a name="run-locally"></a>Run using k6 and tarantool
* Firstly, build [k6](https://k6.io/docs/getting-started/running-k6/) with tarantool module
   ```bash
   xk6 build --with github.com/WeCodingNow/xk6-tarantool --output sbroad/sbroad-cartridge/stress-test/k6
   ```
* You can run stress tests locally (without docker-compose) by using make rules or running them manually. Summarized metrics will be placed in `stress-test/[test]/k6_summary_local.json`.

### <a name="make-rules"></a>run by make rules
Run a chosen stress test from the `sbroad` or `sbroad/sbroad-cartridge` directory. You can specify the path to your k6 binary. If the parameter `K6_PATH` is not set, the default path `sbroad/sbroad-cartridge/stress-test/k6` will be used:
```bash
make stress_local test=projection K6_PATH=path_to_k6_binary
```
If you want to run all stress tests use
```bash
make stress_all_local K6_PATH=path_to_k6_binary
```
### <a name="manually"></a>run manually
1. Run [test application](../test_app)
    ```bash
    cd ../test_app && cartridge start
    ```

   If the cluster is not configured then use the the web-based administrative panel or run the following command to configure it:
   ```bash
      cartridge replicasets setup --file replicasets.yml --bootstrap-vshard
   ```
1. Open a folder with a stress test. Initialize the schema and (optionally) populate the data with a define the number of records:
    ```bash
    ./init.lua 1000
    ```

1. Run the [k6](https://k6.io/docs/getting-started/running-k6/) script
    ```bash
   ./k6 run -u 10 -d 1m k6.js --summary-export k6_summary_local.json
   ```
   **Note:**
If you run stress tests sequentially, you may need to do `cartridge clean` before next test run because tests might have
conflicting schemas.

## <a name="results-comparison"></a>Compare results
   If you need to compare results serveral test, you may run script `compare.lua` with lua or tarantool
   ```bash
   [lua|tarantool] compare.lua path_to_k6_summary_1.json path_to_k6_summary_2.json
   ```
It is assumed that the first result will have a lower or equal RPS and will result in an exit code of 0. Otherwise, it prints error message and return exit code equals to 1. An example of the output is as follows:

```
$ lua compare.lua path_to_k6_summary_2.json path_to_k6_summary_1.json
+----------------+-----------------+------------------+
| metrics        | left result     | right result     |
+----------------+-----------------+------------------+
| Success value  | 0.3333689238811 | 0.33428112947271 |
| Passes         | 71812           | 71244            |
| Fails          | 143601          | 141882           |
| VUs            | 10              | 10               |
| Iterations rps | 3539.5409122402 | 3503.5619844057  |
+----------------+-----------------+------------------+
Left (first) result must have lower or equal rps
```

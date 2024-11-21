# Cartridge integration test application

The application was created for integration testing of the `sbroad` library. It may also serve as a usage example of `sbroad`.

## Navigation
* [Build the application](#build-app)
* [Run test application](#run-test)
* [Architecture](#architecture)
    * [sbroad.execute](#sbroad-execute)
    * [sbroad.calculate_bucket_id](#sbroad-calculate_bucket_id)
* [General observations](#general-observations)
* [Local load testing](#load-test)

## <a name="build-app"></a>Build the application

``` bash
cd .. && make build_integration
```

## <a name="run-tests"></a>Run test application and bootstrap shards

```bash
cartridge start -d
cartridge replicasets setup --bootstrap-vshard
 ```
Before proceding with queries, make sure you have applied a valid schema for your application. A working example is [provided](https://git.picodata.io/picodata/picodata/sbroad/-/blob/main/sbroad-cartridge/test_app/test/data/config.yml) with this test app.

```bash
cartridge enter router-1
```
Name of the router (`router-1`) may differ. In order to get the actual one go to http://localhost:8081/
and find the name of the router.

**Note**: In case you just want to run integration tests, you may call
`make run_integration` after build command (or just call `make test_integration` in
order to unite build and run execution).

## <a name="architecture"></a>Architecture

The application includes two roles:

- `api` (includes `sbroad-router` and enables these functions: `sbroad.calculate_bucket_id`, `sbroad.execute`)
- `storage` (includes `sbroad-storage` and enables the `sbroad.calculate_bucket_id` function)

Notice that sbroad function are placed into global [_G](https://www.lua.org/pil/14.html), so you can call sbroad functions at any place of app. Let's have a look at sbroad functions:

### <a name="sbroad-execute"></a>`sbroad.execute(sql_query,params)`
execute the parametrized sql query. Currently query supports only _SELECT_, _INSERT_ and _EXPLAIN_ statements. For details see [SQL feature taxonomy](https://git.picodata.io/picodata/picodata/sbroad/-/blob/main/doc/sql/feature_taxonomy.md).

**Parameters**:
* sql_query (string) -- sql query to execute on cluster
* params (array) -- params for sql query. The number and order of the arguments correspond to the `?` signs inside `sql_query`.

Note that the use of parameters is obligatory. If you don't have any, use the `, {}` stub.

**Return**:
* result of query (table|nil)
* error

**Examples**:


```
sbroad.execute([[insert into "testing_space" ("id", "name") values (?, ?), (?, ?)]], {1, "123", 2, "123"})
---
- {row_count = 2}
- null
...
sbroad.execute([[insert into "testing_space" ("id", "name") values (3, '123'), (4, '123')]], {})
---
- {row_count = 2}
- null
...

sbroad.execute([[select "name" from "testing_space" where "id" = 2]], {})
---
- {{{metadata = {name = 'id', type = 'integer'}, {name = 'name', type = 'string'}}, {rows = {2, "123"}}}
- null
...
sbroad.execute([[select "name" from "testing_space" where "id" = ?]], {3})
---
- {{{metadata = {name = 'id', type = 'integer'}, {name = 'name', type = 'string'}}, {rows = {3, "123"}}}
- null
...

sbroad.execute([[EXPLAIN SELECT * FROM (
    SELECT "f1", "f2" FROM "space_1" WHERE "f3" <= 0
    UNION ALL
    SELECT "f1", "f2" FROM "space_2" WHERE "f3" > 0
    ) as "t1"
    WHERE "f1" = 1 ]], {}
)
---
- "projection (\"t1\".\"f1\" -> \"f1\", \"t1\".\"f2\" -> \"f2\")",
  "    selection ROW(\"t1\".\"f1\") = ROW(1)",
  "        scan \"t1\"",
  "            union all",
  "                projection (\"space_1\".\"f1\" -> \"f1\", \"space_1\".\"f2\" -> \"f2\")",
  "                    selection ROW(\"space_1\".\"f3\") <= ROW(0)",
  "                        scan \"space_1\"",
  "                projection (\"space_2\".\"f1\" -> \"f1\", \"space_2\".\"f2\" -> \"f2\")",
  "                    selection ROW(\"space_2\".\"f3\") > ROW(0)",
  "                        scan \"space_2\"",
...
```

You can find more examples inside [integration tests](https://git.picodata.io/picodata/picodata/sbroad/-/tree/main/sbroad-cartridge/test_app/test/integration). To run it use
```
cd .. && test_integration
```
### <a name="sbroad-calculate_bucket_id"></a>`sbroad.calculate_bucket_id(value[,space_name])`
calculate bucket id.

**Parameters**:
* value (string|tuple|table) -- value on the basis of which to calculate
* space_name (string) -- name of space.

**Restrictions**:
* String type value does not need space_name
* Tuple type value (array or box.tuple) needs `space_name`. The number and order of the elements must be corresponding to space format or less by one (skip bucket_id field).
* Table type value needs `space_name`. Table must contain all space sharding_key elements.

**Return**:
* result (number|nil) -- bucket_id
* error

**Example**:
```
sbroad.calculate_bucket_id("1123")
---
- 360
...
sbroad.calculate_bucket_id({ id = 1, name = "123" }, "testing_space"})
---
- 360
...
sbroad.calculate_bucket_id"({ box.tuple.new{ 1, "123" }, "testing_space" })
---
- 360
...
sbroad.calculate_bucket_id"({ 1, "123", 1 }, "testing_space" })
---
- 360
...
```

##  <a name="general-observations"></a>General observations

`Sbroad` library uses internal lua functions in the cartridge executor and preloads them with `load_lua_extra_function` call in the `init` cartridge function.

As the `sbroad` library caches the cluster cartridge schema internally, any `sbroad` function that is called checks the internal cluster schema, and if that is empty it loads the schema from the main app. If the app schema was updated then the internal cache needs to be cleared. To clear the cache we need to add the `invalidate_cached_schema` call to the `apply_config` cartridge function.

## <a name="load-test"></a>Local load testing

More information in [stress-test](../stress-test) folder.

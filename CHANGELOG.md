# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Calendar Versioning](https://calver.org/#scheme)
with the `YY.MINOR.MICRO` scheme.

<img src="https://img.shields.io/badge/calver-YY.MINOR.MICRO-22bfda.svg">

## [26.2.1rc1] - 2026-07-15

### Breaking changes

* remove deprecated plugin_dir following a major release in [!3018](https://git.picodata.io/core/picodata/-/merge_requests/3018)
* remove deprecated advertise_address following a major release in [!3018](https://git.picodata.io/core/picodata/-/merge_requests/3018)
* remove deprecated --cluster-name argument for expel in [!3018](https://git.picodata.io/core/picodata/-/merge_requests/3018)
* undeprecate CancellationTokenHandle in [!3018](https://git.picodata.io/core/picodata/-/merge_requests/3018)
* remove deprecated ServiceWorkerManager in [!3018](https://git.picodata.io/core/picodata/-/merge_requests/3018)
* drop deprecated path for picodata_plugin::internal::authenticate in [!3018](https://git.picodata.io/core/picodata/-/merge_requests/3018)
* remove deprecated RegionBuffer::get in [!3018](https://git.picodata.io/core/picodata/-/merge_requests/3018)
* **(cli)** drop deprecated `--init-cfg` and `PICODATA_INIT_CFG` in [!3375](https://git.picodata.io/core/picodata/-/merge_requests/3375)
* **(cfg)** drop deprecated `instance.cluster_name` in favor of `cluster.name` in [!3375](https://git.picodata.io/core/picodata/-/merge_requests/3375)

### Features
* **(/api/v1/instance)** add memtx configuration details in [!3369](https://git.picodata.io/core/picodata/-/merge_requests/3369)
* **(/api/v1/tiers)** add instance uuid in [!3317](https://git.picodata.io/core/picodata/-/merge_requests/3317)
* **(/tiers)** add replicaset.state to /tiers in [!3049](https://git.picodata.io/core/picodata/-/merge_requests/3049)
* **(auth/ldap)** implement `ldap` auth method in rust in [!3252](https://git.picodata.io/core/picodata/-/merge_requests/3252)
* **(ci)** make audit job available in MR pipeline in [!3345](https://git.picodata.io/core/picodata/-/merge_requests/3345)
* **(config)** add wal_dir tier configuration parameter in [!3294](https://git.picodata.io/core/picodata/-/merge_requests/3294)
* **(config)** allow user to set WAL mode at the tier level in [!3260](https://git.picodata.io/core/picodata/-/merge_requests/3260)
* **(config)** add memtx.dir and vinyl.dir instance configuration parameters in [!3385](https://git.picodata.io/core/picodata/-/merge_requests/3385)
* **(domain/webui)** modal instance card in [!3392](https://git.picodata.io/core/picodata/-/merge_requests/3392)
* **(http)** add /memory endpoint that provides memory usage separately in [!3060](https://git.picodata.io/core/picodata/-/merge_requests/3060)
* **(http /cluster)** add info on voters and raft leader to instances in [!3041](https://git.picodata.io/core/picodata/-/merge_requests/3041)
* **(join)** prefer replicaset matching instance's naming scheme on join in [!3387](https://git.picodata.io/core/picodata/-/merge_requests/3387)
* **(metrics)** label local SQL metrics with tier/replicaset in [!3157](https://git.picodata.io/core/picodata/-/merge_requests/3157)
* **(metrics)** add sql storage query duration metric and grafana panels in [!3175](https://git.picodata.io/core/picodata/-/merge_requests/3175)
* **(monitoring)** rework duration histogram panels in [!3163](https://git.picodata.io/core/picodata/-/merge_requests/3163)
* **(monitoring)** add Local SQL query panels in [!3157](https://git.picodata.io/core/picodata/-/merge_requests/3157)
* **(monitoring)** tier/replicaset-filterable cache panels and per-instance breakdowns in [!3175](https://git.picodata.io/core/picodata/-/merge_requests/3175)
* **(monitoring)** scale statement-counter panels for large clusters in [!3175](https://git.picodata.io/core/picodata/-/merge_requests/3175)
* **(plugin)** allow access to plugin listener configuration without binding a socket in [!3174](https://git.picodata.io/core/picodata/-/merge_requests/3174)
* **(plugin)** add on_raft_leader_change callback to plugin API in [!3224](https://git.picodata.io/core/picodata/-/merge_requests/3224)
* **(plugin)** deprecate on_leader_change and rename on_raft_leader_change in [!3224](https://git.picodata.io/core/picodata/-/merge_requests/3224)
* **(replication)** add synchronous replication support for tiers in [!2962](https://git.picodata.io/core/picodata/-/merge_requests/2962)
* **(rpc)** add .proc_instance_details in [!3195](https://git.picodata.io/core/picodata/-/merge_requests/3195)
* **(sql)** EXPLAIN now only quotes names when necessary in [!2999](https://git.picodata.io/core/picodata/-/merge_requests/2999)
* **(sql)** EXPLAIN no longer prints unnecessary aliases in [!2999](https://git.picodata.io/core/picodata/-/merge_requests/2999)
* **(sql)** EXPLAIN (FMT) is now supported for all facettes in [!2999](https://git.picodata.io/core/picodata/-/merge_requests/2999)
* **(sql)** drop obscure text pieces from EXPLAIN in [!2999](https://git.picodata.io/core/picodata/-/merge_requests/2999)
* **(sql)** introduce proper operator precedence to IR in [!2999](https://git.picodata.io/core/picodata/-/merge_requests/2999)
* **(sql)** improve formatting of EXPLAIN {INSERT,DELETE} in [!2999](https://git.picodata.io/core/picodata/-/merge_requests/2999)
* **(sql)** EXPLAIN (FMT) now splits long AND chains in [!2999](https://git.picodata.io/core/picodata/-/merge_requests/2999)
* **(sql)** EXPLAIN (FMT) now properly formats CASE operator in [!2999](https://git.picodata.io/core/picodata/-/merge_requests/2999)
* **(sql)** add BUCKETS mode (facet) to EXPLAIN in [!3113](https://git.picodata.io/core/picodata/-/merge_requests/3113)
* **(sql)** add LOGICAL mode (facet) to EXPLAIN in [!3113](https://git.picodata.io/core/picodata/-/merge_requests/3113)
* **(sql)** add equality-facts analysis over the relational plan in [!3089](https://git.picodata.io/core/picodata/-/merge_requests/3089)
* **(sql)** support DELETE inside transactional DO blocks in [!3149](https://git.picodata.io/core/picodata/-/merge_requests/3149)
* **(sql)** make EXPLAIN (RAW) representation more informative in [!3147](https://git.picodata.io/core/picodata/-/merge_requests/3147)
* **(sql)** support INSERT ... VALUES inside transactional DO blocks in [!3146](https://git.picodata.io/core/picodata/-/merge_requests/3146)
* **(sql)** use fancy decorations in EXPLAIN for better readability in [!3171](https://git.picodata.io/core/picodata/-/merge_requests/3171)
* **(sql)** support array columns in create table in [!3148](https://git.picodata.io/core/picodata/-/merge_requests/3148)
* **(sql)** introduce pico_instance_health_status() function in [!3162](https://git.picodata.io/core/picodata/-/merge_requests/3162)
* **(sql)** extend boolean constant folding with AND/OR and equality identities in [!3193](https://git.picodata.io/core/picodata/-/merge_requests/3193)
* **(sql)** compile transactional blocks into a single VDBE program in [!3170](https://git.picodata.io/core/picodata/-/merge_requests/3170)
* **(sql)** support LET variables in transactional blocks in [!3170](https://git.picodata.io/core/picodata/-/merge_requests/3170)
* **(sql)** support IF statements in transactional blocks in [!3170](https://git.picodata.io/core/picodata/-/merge_requests/3170)
* **(sql)** forbid deleting/truncating from _pico_db_config in [!3169](https://git.picodata.io/core/picodata/-/merge_requests/3169)
* **(sql)** allow unused LET statements inside transactional blocks in [!3184](https://git.picodata.io/core/picodata/-/merge_requests/3184)
* **(sql)** cache assembled VDBE for transactional blocks in [!3310](https://git.picodata.io/core/picodata/-/merge_requests/3310)
* **(sql)** cache rendered block patterns on the router in [!3310](https://git.picodata.io/core/picodata/-/merge_requests/3310)
* **(sql)** place JOIN motions using equality facts in [!3261](https://git.picodata.io/core/picodata/-/merge_requests/3261)
* **(sql)** provide more accurate execution location in EXPLAIN (RAW) in [!3330](https://git.picodata.io/core/picodata/-/merge_requests/3330)
* **(sql)** rename temporary table prefix from TMP_ to _tmp_ in [!3371](https://git.picodata.io/core/picodata/-/merge_requests/3371)
* **(sql)** provide more detailed EXPLAIN output for queries with UNION in [!3357](https://git.picodata.io/core/picodata/-/merge_requests/3357)
* **(sql)** add per-relation restriction clause list in [!3356](https://git.picodata.io/core/picodata/-/merge_requests/3356)
* **(sql)** support array literal in [!3180](https://git.picodata.io/core/picodata/-/merge_requests/3180)
* **(sql)** INSERT ON CONFLICT DO UPDATE in transactional blocks in [!3202](https://git.picodata.io/core/picodata/-/merge_requests/3202)
* **(sql)** show insert conflict hooks in raw explain in [!3202](https://git.picodata.io/core/picodata/-/merge_requests/3202)
* **(sql)** reject repeated IOCDU hits for the same conflict key. in [!3202](https://git.picodata.io/core/picodata/-/merge_requests/3202)
* **(tarantool)** expose `box_txn_set_flags`, add `set_force_async` and `transaction_force_async` helpers in [!2962](https://git.picodata.io/core/picodata/-/merge_requests/2962)
* **(tarantool)** expose `box_promote` call in [!2962](https://git.picodata.io/core/picodata/-/merge_requests/2962)
* **(webui)** add /api/v1/instance/<uuid> in [!3195](https://git.picodata.io/core/picodata/-/merge_requests/3195)
* tarantool::space::UpdateOps new method implementations in [!2994](https://git.picodata.io/core/picodata/-/merge_requests/2994)
* minor improvements to the "ready banner" in [!3084](https://git.picodata.io/core/picodata/-/merge_requests/3084)
* use bindgen to generate bindings to VDBE at compile time in [!3087](https://git.picodata.io/core/picodata/-/merge_requests/3087)
* introduce `forward` option in [!2844](https://git.picodata.io/core/picodata/-/merge_requests/2844)
* introduce CONTEXT mode of EXPLAIN in [!3182](https://git.picodata.io/core/picodata/-/merge_requests/3182)
* experimental_sharding_implementation parameter turns on the new sharding implementation in [!3002](https://git.picodata.io/core/picodata/-/merge_requests/3002)
* validate cluster.tier config section against system tables in [!3002](https://git.picodata.io/core/picodata/-/merge_requests/3002)
* add AddressSanitizer instrumentation script in [!3123](https://git.picodata.io/core/picodata/-/merge_requests/3123)
* add Picodata SQL-aware update ops in [!3192](https://git.picodata.io/core/picodata/-/merge_requests/3192)
* document system allocator and expose its metrics in [!3178](https://git.picodata.io/core/picodata/-/merge_requests/3178)
* automate CHANGELOG and RELEASE_NOTES generation in [!3205](https://git.picodata.io/core/picodata/-/merge_requests/3205)
* specify query number from EXPLAIN (RAW) in sql execution error in [!3262](https://git.picodata.io/core/picodata/-/merge_requests/3262)
* support LOGICAL, BUCKETS, and FORWARD modes of EXPLAIN for transactions in [!3184](https://git.picodata.io/core/picodata/-/merge_requests/3184)
* introduce per query bucket estimation in EXPLAIN (RAW) output in [!3280](https://git.picodata.io/core/picodata/-/merge_requests/3280)
* implement initial bucket distribution in _pico_bucket in [!3019](https://git.picodata.io/core/picodata/-/merge_requests/3019)
* bump tarantool-sys in [!3180](https://git.picodata.io/core/picodata/-/merge_requests/3180)

### Bug fixes
* **(sql)** invalid local SQL in empty_motion1_test and exec_plan_order_by in [!2940](https://git.picodata.io/core/picodata/-/merge_requests/2940)
* **(sql)** harden function argument cast rewrite for cloned and aliased nodes in [!2981](https://git.picodata.io/core/picodata/-/merge_requests/2981)
* **(sql)** forbid revoking privileges from admin in [!2986](https://git.picodata.io/core/picodata/-/merge_requests/2986)
* **(sql)** cache storage issue with temporary table TMP_ not found in [!3023](https://git.picodata.io/core/picodata/-/merge_requests/3023)
* **(sql)** look up unlogged tables with admin privileges in [!3029](https://git.picodata.io/core/picodata/-/merge_requests/3029)
* **(sql)** wait for box_is_ro and vshard readiness when restarting the nodes in [!2988](https://git.picodata.io/core/picodata/-/merge_requests/2988)
* **(sql)** validate tier existence in ALTER PLUGIN ADD SERVICE TO TIER in [!3035](https://git.picodata.io/core/picodata/-/merge_requests/3035)
* **(sql)** broken custom plans with cte unions in [!3039](https://git.picodata.io/core/picodata/-/merge_requests/3039)
* **(sql)** EXPLAIN prints subqueries properly and allocates less memory in [!2999](https://git.picodata.io/core/picodata/-/merge_requests/2999)
* **(sql)** clarify index exists when queried for wrong table in [!3093](https://git.picodata.io/core/picodata/-/merge_requests/3093)
* **(sql)** retry vshard NO_ROUTE_TO_BUCKET error when restarting single replicaset node in [!3032](https://git.picodata.io/core/picodata/-/merge_requests/3032)
* **(sql)** schema version has changed during yield due to cache eviction in [!3099](https://git.picodata.io/core/picodata/-/merge_requests/3099)
* **(sql)** correct spelling of "facette" to "facet" in [!3113](https://git.picodata.io/core/picodata/-/merge_requests/3113)
* **(sql)** prevent as_explain() call on Plan without explain facet set in [!3113](https://git.picodata.io/core/picodata/-/merge_requests/3113)
* **(sql)** do not panic on attempt to inherit privileges from another user in [!3110](https://git.picodata.io/core/picodata/-/merge_requests/3110)
* **(sql)** use direct RPC for query metadata on DQL cache miss in [!3038](https://git.picodata.io/core/picodata/-/merge_requests/3038)
* **(sql)** create a fresh ScanCte per CTE reference in [!3156](https://git.picodata.io/core/picodata/-/merge_requests/3156)
* **(sql)** NOT push-down no longer skips operand subtrees and Cast in [!3193](https://git.picodata.io/core/picodata/-/merge_requests/3193)
* **(sql)** asterisk alias resolution in CTE in [!3254](https://git.picodata.io/core/picodata/-/merge_requests/3254)
* **(sql)** forbid dropping primary index in [!3222](https://git.picodata.io/core/picodata/-/merge_requests/3222)
* **(sql)** point block errors at the offending statement in [!3290](https://git.picodata.io/core/picodata/-/merge_requests/3290)
* **(sql)** stop discarding constants in mixed param/const equality classes in [!3261](https://git.picodata.io/core/picodata/-/merge_requests/3261)
* **(sql)** check that scalar subqueries return 1 row in transactions in [!3339](https://git.picodata.io/core/picodata/-/merge_requests/3339)
* **(sql)** frozen execution plan is still referenced in [!3347](https://git.picodata.io/core/picodata/-/merge_requests/3347)
* **(sql)** correct query enumeration in vdbe errors in [!3354](https://git.picodata.io/core/picodata/-/merge_requests/3354)
* **(sql)** recompile cached block VDBE on stale schema version in [!3358](https://git.picodata.io/core/picodata/-/merge_requests/3358)
* **(sql)** stack overflow due to very long DNF chains in [!3353](https://git.picodata.io/core/picodata/-/merge_requests/3353)
* **(sql)** distinct cache keys for LET vars with different names in [!3373](https://git.picodata.io/core/picodata/-/merge_requests/3373)
* **(sql)** max tuple size mismatch in raft log in [!3142](https://git.picodata.io/core/picodata/-/merge_requests/3142)
* **(sql)** substitute params with actual values in EXPLAIN (RAW) in [!3407](https://git.picodata.io/core/picodata/-/merge_requests/3407)
* **(.proc_instance_details)** fix upgrade in [!3214](https://git.picodata.io/core/picodata/-/merge_requests/3214)
* **(build)** add a missing backslash to Makefile in [!2998](https://git.picodata.io/core/picodata/-/merge_requests/2998)
* **(cas)** detect secondary unique conflicts in [!3201](https://git.picodata.io/core/picodata/-/merge_requests/3201)
* **(ci)** fix docker-compose job in [!3301](https://git.picodata.io/core/picodata/-/merge_requests/3301)
* **(ci)** set up proper volume mappings in docker-compose.yml in [!3342](https://git.picodata.io/core/picodata/-/merge_requests/3342)
* **(ci)** only upload flake-tracker reports on the master branch in [!3370](https://git.picodata.io/core/picodata/-/merge_requests/3370)
* **(cli)** do not panic on forced expel if cluster has only one node in [!3106](https://git.picodata.io/core/picodata/-/merge_requests/3106)
* **(config)** use configured max_tuple_size instead of hardcoded value in [!3221](https://git.picodata.io/core/picodata/-/merge_requests/3221)
* **(debian)** support arm64 architecture in debian/control in [!3282](https://git.picodata.io/core/picodata/-/merge_requests/3282)
* **(discovery)** support multi-address nodes in [!3312](https://git.picodata.io/core/picodata/-/merge_requests/3312)
* **(docker)** remove PICODATA_PG_LISTEN env variable in [!3311](https://git.picodata.io/core/picodata/-/merge_requests/3311)
* **(domain/webui)** filter text tag in [!3286](https://git.picodata.io/core/picodata/-/merge_requests/3286)
* **(domain/webui)** state translation in [!3403](https://git.picodata.io/core/picodata/-/merge_requests/3403)
* **(governor)** use master-only replication config in cold restart in [!2971](https://git.picodata.io/core/picodata/-/merge_requests/2971)
* **(justfile)** align test-rust and lint-rust recipes with Makefile in [!3065](https://git.picodata.io/core/picodata/-/merge_requests/3065)
* **(monitoring)** wire tier/replicaset variables into SQL panels in [!3163](https://git.picodata.io/core/picodata/-/merge_requests/3163)
* **(pgproto)** upgrade pgwire to 0.40 to lift ParameterDescription size limit in [!3287](https://git.picodata.io/core/picodata/-/merge_requests/3287)
* **(raft)** bump table schema version on index ddl in [!3201](https://git.picodata.io/core/picodata/-/merge_requests/3201)
* **(raft)** do not spam the logs when a DdlCommit cannot be applied on a replica in [!3362](https://git.picodata.io/core/picodata/-/merge_requests/3362)
* **(raft)** improve max tuple size error message in [!3384](https://git.picodata.io/core/picodata/-/merge_requests/3384)
* **(rpc)** fix random candidate selection for Any target in [!3114](https://git.picodata.io/core/picodata/-/merge_requests/3114)
* **(sentinel)** fix sentinel panic on long activation wait in [!3037](https://git.picodata.io/core/picodata/-/merge_requests/3037)
* **(test)** bucket balance check divided by instances instead of replicasets in [!3065](https://git.picodata.io/core/picodata/-/merge_requests/3065)
* **(test)** remove wait_balanced, migrate callers to wait_until_buckets_balanced in [!3065](https://git.picodata.io/core/picodata/-/merge_requests/3065)
* **(test)** add missing dots in rolling upgrade binary name in [!3065](https://git.picodata.io/core/picodata/-/merge_requests/3065)
* **(test)** correct bool-to-Lua formatting in KeyPart.__str__ in [!3080](https://git.picodata.io/core/picodata/-/merge_requests/3080)
* **(test)** fix flaky test_pico_service_invalid_requirements_password in [!3103](https://git.picodata.io/core/picodata/-/merge_requests/3103)
* **(test)** wait instance expelled before checking its process dead in [!3241](https://git.picodata.io/core/picodata/-/merge_requests/3241)
* **(test)** reduce `test_bucket_count_custom_and_default` flakiness in [!3281](https://git.picodata.io/core/picodata/-/merge_requests/3281)
* **(test)** terminate instances in reverse order in [!2962](https://git.picodata.io/core/picodata/-/merge_requests/2962)
* **(test)** stabilize test_sync_replication_raft_and_synchro_quorum_loss in [!3367](https://git.picodata.io/core/picodata/-/merge_requests/3367)
* **(test)** stabilize test_sync_replication_master_death_and_return_rf2 in [!3367](https://git.picodata.io/core/picodata/-/merge_requests/3367)
* **(test)** make box_region tests pass under ASAN in [!3378](https://git.picodata.io/core/picodata/-/merge_requests/3378)
* **(test)** remove `test_restart_timing` in [!3390](https://git.picodata.io/core/picodata/-/merge_requests/3390)
* **(test)** fix config in integration and unit test in [!3395](https://git.picodata.io/core/picodata/-/merge_requests/3395)
* **(test/tls)** embed test TLS certificates in the executables in [!3252](https://git.picodata.io/core/picodata/-/merge_requests/3252)
* **(tools)** update VersionAlias import path in build_rolling_binaries in [!3065](https://git.picodata.io/core/picodata/-/merge_requests/3065)
* **(tools)** skip missing versions in build_rolling_binaries in [!3065](https://git.picodata.io/core/picodata/-/merge_requests/3065)
* **(tuple)** fix heap-use-after-free in TupleBuilder::into_tuple in [!3059](https://git.picodata.io/core/picodata/-/merge_requests/3059)
* **(webui)** change existing deps & add the missing ones for eslint in [!3111](https://git.picodata.io/core/picodata/-/merge_requests/3111)
* **(webui)** fix webui's lint rules in [!3111](https://git.picodata.io/core/picodata/-/merge_requests/3111)
* **(webui)** increase instance columns widths in [!3340](https://git.picodata.io/core/picodata/-/merge_requests/3340)
* **(webui mock cluster)** add isVoter and isRaftLeader fields in [!3048](https://git.picodata.io/core/picodata/-/merge_requests/3048)
* **(webui mock cluster)** add replicasetState field in [!3053](https://git.picodata.io/core/picodata/-/merge_requests/3053)
* **(yarn)** pin the version of plugin-cyclonedx in .yarnrc.yml in [!3377](https://git.picodata.io/core/picodata/-/merge_requests/3377)
* handle fiber cancellation during SQL preemption in [!2908](https://git.picodata.io/core/picodata/-/merge_requests/2908)
* fix(domain/webui) replacing crypto with uuid

Closes #2820

Since the cryptographic function module is not supported in some environments, an error occurs accessing the crypto.randomUUID function. It was decided to replace it with the uuid library. in [!2990](https://git.picodata.io/core/picodata/-/merge_requests/2990)
* allow using scalar functions in GROUP BY in [!2987](https://git.picodata.io/core/picodata/-/merge_requests/2987)
* lifetime annotation in Index::meta return type is now more general in [!2994](https://git.picodata.io/core/picodata/-/merge_requests/2994)
* compatibility for iproto_{listen,advertise}/http_listen in cli/env in [!3021](https://git.picodata.io/core/picodata/-/merge_requests/3021)
* add #[track_caller] to PicoContext::register_job in [!2995](https://git.picodata.io/core/picodata/-/merge_requests/2995)
* add replicaset name to vshard config in [!2995](https://git.picodata.io/core/picodata/-/merge_requests/2995)
* log sleep duration with less precision in [!2995](https://git.picodata.io/core/picodata/-/merge_requests/2995)
* fix(domain/webui) highlighting the missing instances

Closes #2828

The display and highlighting of missing instances has been added to the replicaset card in [!3072](https://git.picodata.io/core/picodata/-/merge_requests/3072)
* fix(domain/webui) filter icon

Closes #2848

The search icon has been added to the filter component in [!3073](https://git.picodata.io/core/picodata/-/merge_requests/3073)
* auto-abort backup DDL when there are offline nodes in [!2871](https://git.picodata.io/core/picodata/-/merge_requests/2871)
* remove `Changes` section from CHANGELOG.md in [!3154](https://git.picodata.io/core/picodata/-/merge_requests/3154)
* used to not be able to bootstrap a cluster without at least one voter in the `--peer` set in [!3155](https://git.picodata.io/core/picodata/-/merge_requests/3155)
* prevent crash when disabling plugin with background jobs in [!3133](https://git.picodata.io/core/picodata/-/merge_requests/3133)
* fix(domain/webui) change capacity progress label

Closes #1956 in [!3083](https://git.picodata.io/core/picodata/-/merge_requests/3083)
* fix(domain/webui) display raft leader and voter

The raft leader display has been added to the instance card. A display has been added for shooting range and replicaset cards indicating whether it includes an instance that is a raft ledaer. A "voting" status display has been added for the instance. The display of the "votes" status for the shooting gallery card has been changed.

Closes #1725, #1723 in [!3185](https://git.picodata.io/core/picodata/-/merge_requests/3185)
* more consistent ASan build via Makefile in [!3187](https://git.picodata.io/core/picodata/-/merge_requests/3187)
* Updated the docker-compose file, now all instance settings are carried out through configuration files in [!3198](https://git.picodata.io/core/picodata/-/merge_requests/3198)
* fix(domain/webui) separate memory request

The get data about the memory of the shooting gallery and the instance, a separate request was added that occurs every 2 minutes. The logic of collecting initialized data for the list of ranges, replicases, and instances has been somewhat redesigned.

Closes #2710 in [!3194](https://git.picodata.io/core/picodata/-/merge_requests/3194)
* increase default timeout scaling factor for ASan to 5.0 in [!3209](https://git.picodata.io/core/picodata/-/merge_requests/3209)
* Makefile is now compatible with make 3.81 in [!3213](https://git.picodata.io/core/picodata/-/merge_requests/3213)
* add --with-webui and --skip-asan flags to asan-test-py in [!3219](https://git.picodata.io/core/picodata/-/merge_requests/3219)
* don't build tests in `make build-*` in [!3227](https://git.picodata.io/core/picodata/-/merge_requests/3227)
* `make coverage-report` now uses a named file for binary list in [!3228](https://git.picodata.io/core/picodata/-/merge_requests/3228)
* fix(domain/webui) voter and raft leader filter

New filtering options have been added to the filter, such as searching for an instance based on voter and isRaftLeader.

Closes #2866 in [!3217](https://git.picodata.io/core/picodata/-/merge_requests/3217)
* retry check for process is dead in test_cross_cluster_isolation in [!3243](https://git.picodata.io/core/picodata/-/merge_requests/3243)
* wait instance expelled before checking its process dead in [!3241](https://git.picodata.io/core/picodata/-/merge_requests/3241)
* disable auto-online to check wait_online handles replication error in [!3242](https://git.picodata.io/core/picodata/-/merge_requests/3242)
* fix(domain/webui) replicaset ready state

A display of the status of the replicaset that has not entered the ready state has been added to the replicaset card.

Closes #1782 in [!3270](https://git.picodata.io/core/picodata/-/merge_requests/3270)
* get user name in on_access_denied without reading memtx spaces in [!3196](https://git.picodata.io/core/picodata/-/merge_requests/3196)
* always add master to replication config if no active master transitioning in [!2962](https://git.picodata.io/core/picodata/-/merge_requests/2962)
* use increased RPC timeouts sender-side for all governor RPC in [!3019](https://git.picodata.io/core/picodata/-/merge_requests/3019)
* update package 'crossbeam-epoch' to avoid audit error in [!3394](https://git.picodata.io/core/picodata/-/merge_requests/3394)
* reduce errors in tests with ASAN turned on in [!3398](https://git.picodata.io/core/picodata/-/merge_requests/3398)

### Refactor
* **(auth)** implement `md5` auth method in Rust in [!3225](https://git.picodata.io/core/picodata/-/merge_requests/3225)
* **(config)** use a decl macro to reduce repetition of address new-types in [!3252](https://git.picodata.io/core/picodata/-/merge_requests/3252)
* **(governor)** remove unneeded check in [!2962](https://git.picodata.io/core/picodata/-/merge_requests/2962)
* **(http_server.rs)** split http into multiple files in [!3368](https://git.picodata.io/core/picodata/-/merge_requests/3368)
* **(justfile)** simplify test-python, drop verbose recipe in [!3065](https://git.picodata.io/core/picodata/-/merge_requests/3065)
* **(pgproto)** rename EXPLAIN output column from "QUERY PLAN" to "EXPLAIN" in [!3113](https://git.picodata.io/core/picodata/-/merge_requests/3113)
* **(sql)** do not derive Serialize for EXPLAIN structs in [!2999](https://git.picodata.io/core/picodata/-/merge_requests/2999)
* **(sql)** EXPLAIN now uses crate `indenter` in [!2999](https://git.picodata.io/core/picodata/-/merge_requests/2999)
* **(sql)** reorganize and simplify ColExpr for EXPLAIN in [!2999](https://git.picodata.io/core/picodata/-/merge_requests/2999)
* **(sql)** use TinyFmtBuffer's contents in EXPLAIN in [!2999](https://git.picodata.io/core/picodata/-/merge_requests/2999)
* **(sql)** many fixes to EXPLAIN in [!3113](https://git.picodata.io/core/picodata/-/merge_requests/3113)
* **(sql)** set explicit EXPLAIN modes in mock tests in [!2844](https://git.picodata.io/core/picodata/-/merge_requests/2844)
* **(sql)** replace take_subtree with subtree views in [!3143](https://git.picodata.io/core/picodata/-/merge_requests/3143)
* **(sql)** avoid cloning IR for single-rs dispatch in [!3244](https://git.picodata.io/core/picodata/-/merge_requests/3244)
* **(sql)** pre-resolve LEFT JOIN equality scopes at freeze in [!3261](https://git.picodata.io/core/picodata/-/merge_requests/3261)
* **(sql)** remove serde attributes from `Plan` struct in [!3261](https://git.picodata.io/core/picodata/-/merge_requests/3261)
* **(sql)** remove equality propagation transformation in [!3261](https://git.picodata.io/core/picodata/-/merge_requests/3261)
* **(sql)** split into old and new AST in [!3351](https://git.picodata.io/core/picodata/-/merge_requests/3351)
* **(sql)** share the optimizer pass order between production and tests in [!3356](https://git.picodata.io/core/picodata/-/merge_requests/3356)
* **(sql)** split sql-planner crate into 3 smaller ones in [!3379](https://git.picodata.io/core/picodata/-/merge_requests/3379)
* **(webui)** fix sonarqube warnings in [!3007](https://git.picodata.io/core/picodata/-/merge_requests/3007)
* **(webui)** fix sonar issues in [!3010](https://git.picodata.io/core/picodata/-/merge_requests/3010)
* replace duplicated expression match blocks with ExprChildren trait
* define_str_enum_or_smol_str! macro for better string enums in [!2995](https://git.picodata.io/core/picodata/-/merge_requests/2995)
* extract define_smolstr_newtype into a separate file in [!2995](https://git.picodata.io/core/picodata/-/merge_requests/2995)
* use `BackupGuard` to reliably call `box.backup.stop` on backup error in [!2871](https://git.picodata.io/core/picodata/-/merge_requests/2871)
* rename health status fields for clarity in [!3088](https://git.picodata.io/core/picodata/-/merge_requests/3088)
* use std::time::Duration for all SQL timeouts in [!3032](https://git.picodata.io/core/picodata/-/merge_requests/3032)
* move "picodata demo" behind feature flag in [!3115](https://git.picodata.io/core/picodata/-/merge_requests/3115)
* remove unused reqwest dev-dependency in [!3118](https://git.picodata.io/core/picodata/-/merge_requests/3118)
* unify TLS configuration handling between `picodata-plugin` and `picodata` in [!2926](https://git.picodata.io/core/picodata/-/merge_requests/2926)
* log TLS configuration for every configured listener in [!2926](https://git.picodata.io/core/picodata/-/merge_requests/2926)
* remove now unused `ListenAddress::conflicts_with` in [!2926](https://git.picodata.io/core/picodata/-/merge_requests/2926)
* use struct to avoid unnamed bool parameters in `load_listener_tls_config_from_files` in [!2926](https://git.picodata.io/core/picodata/-/merge_requests/2926)
* do not expose APIs exported for `picodata` to plugins in [!2926](https://git.picodata.io/core/picodata/-/merge_requests/2926)
* introduce helper methods for iteration over plugin configuration in [!2926](https://git.picodata.io/core/picodata/-/merge_requests/2926)
* replace `cargo::warning` helper with `cargo::warning!` macro in [!3087](https://git.picodata.io/core/picodata/-/merge_requests/3087)
* cleanup read-only parameter handling in [!3002](https://git.picodata.io/core/picodata/-/merge_requests/3002)
* some tweaks to sharding governor code in preparation for future work in [!3019](https://git.picodata.io/core/picodata/-/merge_requests/3019)

### Documentation
* **(auth/ldap)** document the new LDAP YAML configuration in [!3350](https://git.picodata.io/core/picodata/-/merge_requests/3350)
* **(changelog)** add webui changes in [!3217](https://git.picodata.io/core/picodata/-/merge_requests/3217)
* **(config)** add wal_dir description in [!3294](https://git.picodata.io/core/picodata/-/merge_requests/3294)
* **(config)** add `cluster.tier.wal_mode` description in [!3260](https://git.picodata.io/core/picodata/-/merge_requests/3260)
* **(header)** sync menu content with new picodata.io in [!3138](https://git.picodata.io/core/picodata/-/merge_requests/3138)
* **(header)** mirror Astro Header DOM and styling from picodata.io in [!3138](https://git.picodata.io/core/picodata/-/merge_requests/3138)
* **(kirovets)** rename exporter to importer in architecture scheme in [!3318](https://git.picodata.io/core/picodata/-/merge_requests/3318)
* **(kirovets)** add system params documentation in [!3316](https://git.picodata.io/core/picodata/-/merge_requests/3316)
* **(kirovets)** add is_public endpoint flag description in [!3315](https://git.picodata.io/core/picodata/-/merge_requests/3315)
* **(kirovets)** improve KafkaDebezium importer params documentation in [!3314](https://git.picodata.io/core/picodata/-/merge_requests/3314)
* **(kirovets)** improve parquet importer source params documentation in [!3313](https://git.picodata.io/core/picodata/-/merge_requests/3313)
* **(kirovets)** add using meta in user defined function in [!3061](https://git.picodata.io/core/picodata/-/merge_requests/3061)
* **(sirin)** update plugin documentation for 1.2.1 release in [!3238](https://git.picodata.io/core/picodata/-/merge_requests/3238)
* **(sirin)** update plugin documentation for 1.3.0 release in [!3337](https://git.picodata.io/core/picodata/-/merge_requests/3337)
* **(sql)** add pico_instance_health_status() description in [!3200](https://git.picodata.io/core/picodata/-/merge_requests/3200)
* document anonymous blocks / transactions support in [!2938](https://git.picodata.io/core/picodata/-/merge_requests/2938)
* document unified socket configuration for Picodata config file and settings reference in [!2965](https://git.picodata.io/core/picodata/-/merge_requests/2965)
* document K8S endpoints for health check API in [!2965](https://git.picodata.io/core/picodata/-/merge_requests/2965)
* fix and update configuration options names in [!2977](https://git.picodata.io/core/picodata/-/merge_requests/2977)
* describe LSM compaction for glossary in [!2983](https://git.picodata.io/core/picodata/-/merge_requests/2983)
* review note for alter_user.md in [!2986](https://git.picodata.io/core/picodata/-/merge_requests/2986)
* describe opts for _pico_table in [!3008](https://git.picodata.io/core/picodata/-/merge_requests/3008)
* new group by article in [!3017](https://git.picodata.io/core/picodata/-/merge_requests/3017)
* update Argus description, fix style and typos in [!3031](https://git.picodata.io/core/picodata/-/merge_requests/3031)
* updates and fixes for Radix in [!3043](https://git.picodata.io/core/picodata/-/merge_requests/3043)
* add dev doc, catalog upgrade guide in [!2922](https://git.picodata.io/core/picodata/-/merge_requests/2922)
* updates and fixes for config.md in [!3056](https://git.picodata.io/core/picodata/-/merge_requests/3056)
* recover comments about the directory structure used by build.rs in [!3075](https://git.picodata.io/core/picodata/-/merge_requests/3075)
* new article about JDBC and Weblogic in [!3078](https://git.picodata.io/core/picodata/-/merge_requests/3078)
* fix typos, prettify screenshots in [!3090](https://git.picodata.io/core/picodata/-/merge_requests/3090)
* update documentation regarding current state of Sirin plugin in [!3036](https://git.picodata.io/core/picodata/-/merge_requests/3036)
* improve documentation for transactional blocks in [!3101](https://git.picodata.io/core/picodata/-/merge_requests/3101)
* unify GA4 measurement ID with main site in [!3138](https://git.picodata.io/core/picodata/-/merge_requests/3138)
* add Yandex.Metrika counter 99962757 (unified with picodata.io) in [!3138](https://git.picodata.io/core/picodata/-/merge_requests/3138)
* fix incorrect header bar overlay in [!3140](https://git.picodata.io/core/picodata/-/merge_requests/3140)
* add marge-bot info to CONTRIBUTING.md in [!3141](https://git.picodata.io/core/picodata/-/merge_requests/3141)
* fix URLs of Picodata certificates in [!3145](https://git.picodata.io/core/picodata/-/merge_requests/3145)
* update install instructions for Alt p10 and p11 in [!3152](https://git.picodata.io/core/picodata/-/merge_requests/3152)
* hide Helm chart occurrences in [!3166](https://git.picodata.io/core/picodata/-/merge_requests/3166)
* adjust CREATE TABLE IN TIER description in [!3167](https://git.picodata.io/core/picodata/-/merge_requests/3167)
* update Radix documentation for Radix 1.0.2 in [!3160](https://git.picodata.io/core/picodata/-/merge_requests/3160)
* add new article on K8S Operator in [!3220](https://git.picodata.io/core/picodata/-/merge_requests/3220)
* update docker-compose tutorial in [!3203](https://git.picodata.io/core/picodata/-/merge_requests/3203)
* fix example command for ALTER SYSTEM SET iproto_net_msg_max in [!3230](https://git.picodata.io/core/picodata/-/merge_requests/3230)
* edit Expression BNF, add _pico_bucket description in [!3208](https://git.picodata.io/core/picodata/-/merge_requests/3208)
* fix and update LDAP+StartTLS description in [!3259](https://git.picodata.io/core/picodata/-/merge_requests/3259)
* document LET in transactional blocks in [!3170](https://git.picodata.io/core/picodata/-/merge_requests/3170)
* document IF in transactional blocks in [!3170](https://git.picodata.io/core/picodata/-/merge_requests/3170)
* describe Picodata use cases in [!3020](https://git.picodata.io/core/picodata/-/merge_requests/3020)
* update documentation for Radix 1.0.4 in [!3285](https://git.picodata.io/core/picodata/-/merge_requests/3285)
* show value/label encoding for state gauges in [!3319](https://git.picodata.io/core/picodata/-/merge_requests/3319)
* remove traces of the obsolete K8S article in [!3324](https://git.picodata.io/core/picodata/-/merge_requests/3324)
* remove Riot section in [!3334](https://git.picodata.io/core/picodata/-/merge_requests/3334)
* update Radix config file example and add some details in [!3338](https://git.picodata.io/core/picodata/-/merge_requests/3338)
* update plugin-configure description in cli.md in [!3338](https://git.picodata.io/core/picodata/-/merge_requests/3338)

### Performance
* **(http)** reduce RPC calls in /api/v1/tiers and /api/v1/cluster in [!2991](https://git.picodata.io/core/picodata/-/merge_requests/2991)
* **(sql)** memoize block statement constant ids across executions in [!3393](https://git.picodata.io/core/picodata/-/merge_requests/3393)
* **(sql-planner)** avoid extra clone in single-rs dispatch in [!3102](https://git.picodata.io/core/picodata/-/merge_requests/3102)
* add `pg-gonnect-bench` - a tool for measuring throughput of postgresql connection establishment in [!3225](https://git.picodata.io/core/picodata/-/merge_requests/3225)

### Testing
* **(auth)** add regression test for role authentication from plugin API in [!2985](https://git.picodata.io/core/picodata/-/merge_requests/2985)
* **(auth/ldap)** update `ldap` auth tests in [!3252](https://git.picodata.io/core/picodata/-/merge_requests/3252)
* **(cluster)** fix bucket balance wait skipping all instances when no instances are excluded in [!3071](https://git.picodata.io/core/picodata/-/merge_requests/3071)
* **(cluster)** support per-tier wait for bucket balancing in [!3091](https://git.picodata.io/core/picodata/-/merge_requests/3091)
* **(cluster)** integrate discovery check into bucket balancing in [!3091](https://git.picodata.io/core/picodata/-/merge_requests/3091)
* **(config/ldap)** add unit tests for `ldap` configuration in [!3252](https://git.picodata.io/core/picodata/-/merge_requests/3252)
* **(framework)** extend version handling utilities in [!2677](https://git.picodata.io/core/picodata/-/merge_requests/2677)
* **(health)** add new entity for structured error expectations in [!2502](https://git.picodata.io/core/picodata/-/merge_requests/2502)
* **(health)** replace boolean flags with new structured error entity in [!2502](https://git.picodata.io/core/picodata/-/merge_requests/2502)
* **(health)** introduce proper health check module in [!2502](https://git.picodata.io/core/picodata/-/merge_requests/2502)
* **(health)** wait for vshard readiness before verifying distributed ops in [!3065](https://git.picodata.io/core/picodata/-/merge_requests/3065)
* **(health)** bump DDL timeout to reduce flakiness in rolling upgrades in [!3382](https://git.picodata.io/core/picodata/-/merge_requests/3382)
* **(pgproto)** combine all cache-related tests into a single file in [!3042](https://git.picodata.io/core/picodata/-/merge_requests/3042)
* **(registry)** replace slot-based registry with version-keyed runtime map in [!2677](https://git.picodata.io/core/picodata/-/merge_requests/2677)
* **(registry)** adopt suite to the new lookup API in [!2677](https://git.picodata.io/core/picodata/-/merge_requests/2677)
* **(registry)** add tests for Registry and Executable in [!3137](https://git.picodata.io/core/picodata/-/merge_requests/3137)
* **(restart)** fix flaky test_wait_vshard_storage in [!3080](https://git.picodata.io/core/picodata/-/merge_requests/3080)
* **(rolling)** migrate to new version of the registry API in [!2677](https://git.picodata.io/core/picodata/-/merge_requests/2677)
* **(rolling)** fall back to previous patch when tagged binary is not yet published in [!2677](https://git.picodata.io/core/picodata/-/merge_requests/2677)
* **(rolling)** remove data fill methods from cluster deployment in [!2982](https://git.picodata.io/core/picodata/-/merge_requests/2982)
* **(rolling)** migrate rolling tests to new health checks in [!2502](https://git.picodata.io/core/picodata/-/merge_requests/2502)
* **(rolling)** do not skip previously temporarily skipped tests in [!2502](https://git.picodata.io/core/picodata/-/merge_requests/2502)
* **(rolling)** wait for governor to finish operations after cluster upgrade in [!2502](https://git.picodata.io/core/picodata/-/merge_requests/2502)
* **(rolling)** update `Executable.version` when latest tagged patch is not available yet in [!3071](https://git.picodata.io/core/picodata/-/merge_requests/3071)
* **(sentinel)** add test for sentinel panic on long activation wait in [!3037](https://git.picodata.io/core/picodata/-/merge_requests/3037)
* **(sql)** move known defects to a dedicated dir in [!2984](https://git.picodata.io/core/picodata/-/merge_requests/2984)
* **(sql)** introduce inline_snapshot to some tests in [!3042](https://git.picodata.io/core/picodata/-/merge_requests/3042)
* **(sql)** move sql-specific tests to test/int/sql in [!3042](https://git.picodata.io/core/picodata/-/merge_requests/3042)
* **(sql)** implement snapshot tests in sql test framework in [!3025](https://git.picodata.io/core/picodata/-/merge_requests/3025)
* **(sql)** move known defects of SQL generation to dedicated directory in [!3327](https://git.picodata.io/core/picodata/-/merge_requests/3327)
* **(sql)** validate that preemption doesn't happen in a transactional block in [!3348](https://git.picodata.io/core/picodata/-/merge_requests/3348)
* **(sql)** cover iocdu params through SQL test framework in [!3202](https://git.picodata.io/core/picodata/-/merge_requests/3202)
* **(tcp)** remove flaky external DNS check from `get_libc_addrs` test in [!3274](https://git.picodata.io/core/picodata/-/merge_requests/3274)
* fix expected catalog versions in upgrade in [!2969](https://git.picodata.io/core/picodata/-/merge_requests/2969)
* temporarily skip test_upgrade_25_5_to_25_6_check_opts after bump to 26.2.0 in [!2969](https://git.picodata.io/core/picodata/-/merge_requests/2969)
* fix flaky config tests in [!2970](https://git.picodata.io/core/picodata/-/merge_requests/2970)
* use pytest-timeout deadline for all retry loops in [!2955](https://git.picodata.io/core/picodata/-/merge_requests/2955)
* reduce test_process_management flakiness in [!2995](https://git.picodata.io/core/picodata/-/merge_requests/2995)
* fix bug in _get_target_state_reason, make it easier to detect such bugs in future in [!3057](https://git.picodata.io/core/picodata/-/merge_requests/3057)
* mark test_wait_vshard_storage flaky in [!3064](https://git.picodata.io/core/picodata/-/merge_requests/3064)
* make test_rotation less flaky in [!3027](https://git.picodata.io/core/picodata/-/merge_requests/3027)
* add a set of tests that validate how TLS configuration is printed in [!2926](https://git.picodata.io/core/picodata/-/merge_requests/2926)
* add --skip flag to inner test harness in [!3122](https://git.picodata.io/core/picodata/-/merge_requests/3122)
* use current timezone offset instead of historical 1970 offset in [!3139](https://git.picodata.io/core/picodata/-/merge_requests/3139)
* decouple cargo build and binary resolution from CI mode in [!3144](https://git.picodata.io/core/picodata/-/merge_requests/3144)
* add test for concurrent waiters on background job token in [!3133](https://git.picodata.io/core/picodata/-/merge_requests/3133)
* add skip_if attribute to conditionally skip tests in [!3177](https://git.picodata.io/core/picodata/-/merge_requests/3177)
* skip ASan-incompatible unit tests in [!3177](https://git.picodata.io/core/picodata/-/merge_requests/3177)
* move peformance tests to separate test/perf directory in [!3151](https://git.picodata.io/core/picodata/-/merge_requests/3151)
* move test_large_snapshot to test/perf directory in [!3151](https://git.picodata.io/core/picodata/-/merge_requests/3151)
* add timeout scaling factor for slow environments in [!3151](https://git.picodata.io/core/picodata/-/merge_requests/3151)
* add skip_asan marker for sanitizer builds in [!3151](https://git.picodata.io/core/picodata/-/merge_requests/3151)
* mark ASan-incompatible tests with skip_asan in [!3151](https://git.picodata.io/core/picodata/-/merge_requests/3151)
* tweak Instance.__repr__ in [!3002](https://git.picodata.io/core/picodata/-/merge_requests/3002)
* move test_config_storage_conflicts_on_restart into test_config_file.py in [!3002](https://git.picodata.io/core/picodata/-/merge_requests/3002)
* use terminate() over kill() for shutting instances down in [!3096](https://git.picodata.io/core/picodata/-/merge_requests/3096)
* pass LLVM_PROFILE_NAME to instance subprocess in [!3096](https://git.picodata.io/core/picodata/-/merge_requests/3096)
* respect CARGO_BUILD_TARGET in the test framework in [!3096](https://git.picodata.io/core/picodata/-/merge_requests/3096)
* replace build-asan-dev with new ASan targets using sanitizer.py in [!3209](https://git.picodata.io/core/picodata/-/merge_requests/3209)
* auto-detect correct path to `plug_wrong_version` with codecov in [!3216](https://git.picodata.io/core/picodata/-/merge_requests/3216)
* fix flaky backup tests in [!3295](https://git.picodata.io/core/picodata/-/merge_requests/3295)
* replace wait_until_instance_has_this_many_active_buckets with wait_until_buckets_balanced in [!3295](https://git.picodata.io/core/picodata/-/merge_requests/3295)
* move `md5` and `ldap` auth tests to picodata in [!3225](https://git.picodata.io/core/picodata/-/merge_requests/3225)
* add more tests for transactional blocks in [!3344](https://git.picodata.io/core/picodata/-/merge_requests/3344)
* add SQL tests for UUID insertions in [!3349](https://git.picodata.io/core/picodata/-/merge_requests/3349)
* restructurize the plugin testing system in [!3224](https://git.picodata.io/core/picodata/-/merge_requests/3224)
* add more tests for compute_bucket_distribution_imbalance in [!3374](https://git.picodata.io/core/picodata/-/merge_requests/3374)
* fix flaky test_call_normalization in [!3391](https://git.picodata.io/core/picodata/-/merge_requests/3391)
* fetch current leader to fix flaky test in [!3391](https://git.picodata.io/core/picodata/-/merge_requests/3391)

### Miscellaneous Tasks
* **(cert)** remove remaining golang related files in third_party/nghttp2 in [!3288](https://git.picodata.io/core/picodata/-/merge_requests/3288)
* **(cfg)** parametrize `init_core_logger`, drop config dependency in [!3375](https://git.picodata.io/core/picodata/-/merge_requests/3375)
* **(ir)** replace BlockStatement getters with iterators in [!3170](https://git.picodata.io/core/picodata/-/merge_requests/3170)
* **(ir)** store let var name as SmolStr in [!3290](https://git.picodata.io/core/picodata/-/merge_requests/3290)
* **(lua)** use Tarantool's NULL instead of lua nil in [!3094](https://git.picodata.io/core/picodata/-/merge_requests/3094)
* **(notes)** fix syntax error in [!3320](https://git.picodata.io/core/picodata/-/merge_requests/3320)
* **(sbroad)** remove unnecessary Eq and PartialEq impls in [!3269](https://git.picodata.io/core/picodata/-/merge_requests/3269)
* **(sql)** replace 'Trying to add motions' and 'motion key position must exist' panics with errors in [!3266](https://git.picodata.io/core/picodata/-/merge_requests/3266)
* **(sql)** separate block patterns from parameter values in [!3310](https://git.picodata.io/core/picodata/-/merge_requests/3310)
* **(sql)** split define_str_enum macro in [!3180](https://git.picodata.io/core/picodata/-/merge_requests/3180)
* **(test)** move anon block tests to test/int/sql folder in [!3170](https://git.picodata.io/core/picodata/-/merge_requests/3170)
* **(webui)** try to upgrade various js deps to fix security advisory in [!3055](https://git.picodata.io/core/picodata/-/merge_requests/3055)
* **(webui)** run yarn dedupe in [!3055](https://git.picodata.io/core/picodata/-/merge_requests/3055)
* **(webui)** refactor webui's build rules in [!3109](https://git.picodata.io/core/picodata/-/merge_requests/3109)
* add 26.1 to install_rolling_binaries.sh in [!2969](https://git.picodata.io/core/picodata/-/merge_requests/2969)
* update minimum python version in pyproject to 3.11 in [!3005](https://git.picodata.io/core/picodata/-/merge_requests/3005)
* hide `picodata run --interactive` from `--help` in [!2972](https://git.picodata.io/core/picodata/-/merge_requests/2972)
* tweak sbom infra to fix webui sbom and extend checks in [!2997](https://git.picodata.io/core/picodata/-/merge_requests/2997)
* introduce tlua::Ignore - a helper struct for ignoring values returned from lua in [!2994](https://git.picodata.io/core/picodata/-/merge_requests/2994)
* update prepare_source_tree_for_stat_analysis.py in [!2828](https://git.picodata.io/core/picodata/-/merge_requests/2828)
* remove dead code from prepare_source_tree_for_stat_analysis.py in [!3028](https://git.picodata.io/core/picodata/-/merge_requests/3028)
* apply python formatting to tools and benchmarks in [!3055](https://git.picodata.io/core/picodata/-/merge_requests/3055)
* sort alphabetically list of files to be deleted before stat analysis in [!3055](https://git.picodata.io/core/picodata/-/merge_requests/3055)
* update python deps to fix security advisory in [!3055](https://git.picodata.io/core/picodata/-/merge_requests/3055)
* exclude more supplementary files from stat analysis in [!3055](https://git.picodata.io/core/picodata/-/merge_requests/3055)
* report all password policy violations at once in [!2888](https://git.picodata.io/core/picodata/-/merge_requests/2888)
* print ready banner with connection info after startup in [!2891](https://git.picodata.io/core/picodata/-/merge_requests/2891)
* update sbom meta for changed deps in [!3074](https://git.picodata.io/core/picodata/-/merge_requests/3074)
* update 'rand' crate in [!3076](https://git.picodata.io/core/picodata/-/merge_requests/3076)
* Expression::may_need_parentheses now uses exhaustive match in [!3081](https://git.picodata.io/core/picodata/-/merge_requests/3081)
* bump raft-rs commit in [!3082](https://git.picodata.io/core/picodata/-/merge_requests/3082)
* bump rustls-webpki version in [!3086](https://git.picodata.io/core/picodata/-/merge_requests/3086)
* update axios and follow-redirects webui deps in [!3085](https://git.picodata.io/core/picodata/-/merge_requests/3085)
* turn on check_quorum property in [!2475](https://git.picodata.io/core/picodata/-/merge_requests/2475)
* explain CARGO_FLAGS vs. CARGO_FLAGS_EXTRA in Makefile in [!3115](https://git.picodata.io/core/picodata/-/merge_requests/3115)
* move explain_forward() to a different module in [!3171](https://git.picodata.io/core/picodata/-/merge_requests/3171)
* clippy warning in [!3002](https://git.picodata.io/core/picodata/-/merge_requests/3002)
* add code coverage tools in [!3096](https://git.picodata.io/core/picodata/-/merge_requests/3096)
* adjust Makefile & build.rs in the presence of code coverage in [!3096](https://git.picodata.io/core/picodata/-/merge_requests/3096)
* add `make coverage-trim-target` for CI jobs
* place skip_asan marker on tests with short timeouts in [!3219](https://git.picodata.io/core/picodata/-/merge_requests/3219)
* add a Makefile rule to merge code coverage reports in [!3226](https://git.picodata.io/core/picodata/-/merge_requests/3226)
* make tools/find-executables.sh more robust in [!3226](https://git.picodata.io/core/picodata/-/merge_requests/3226)
* delete more files before sonar cube analysis in [!3247](https://git.picodata.io/core/picodata/-/merge_requests/3247)
* bump vshard in [!3255](https://git.picodata.io/core/picodata/-/merge_requests/3255)
* add new features to tools/coverage.py in [!3233](https://git.picodata.io/core/picodata/-/merge_requests/3233)
* give preference to llvm-cxxfilt in tools/coverage.py in [!3233](https://git.picodata.io/core/picodata/-/merge_requests/3233)
* rename coverage-related Makefile rules and add new ones in [!3233](https://git.picodata.io/core/picodata/-/merge_requests/3233)
* update js deps for sonar in [!3256](https://git.picodata.io/core/picodata/-/merge_requests/3256)
* regenerate Cargo.lock to update to latest version of openssl bindings in [!3256](https://git.picodata.io/core/picodata/-/merge_requests/3256)
* add `make coverage-demo-diff` for local code reviews in [!3268](https://git.picodata.io/core/picodata/-/merge_requests/3268)
* don't show trivial branch coverage in codecov reports in [!3268](https://git.picodata.io/core/picodata/-/merge_requests/3268)
* unify rand crate dependency in [!3291](https://git.picodata.io/core/picodata/-/merge_requests/3291)
* update allowed git sources for audit in [!3293](https://git.picodata.io/core/picodata/-/merge_requests/3293)
* add missing changelog from older patch releases in [!3297](https://git.picodata.io/core/picodata/-/merge_requests/3297)
* adjust tile width so that it stays within sane limits in [!3300](https://git.picodata.io/core/picodata/-/merge_requests/3300)
* call absolute as a method in install rolling binaries script in [!3304](https://git.picodata.io/core/picodata/-/merge_requests/3304)
* ignore unmaintained proc-macro-error2 in [!3309](https://git.picodata.io/core/picodata/-/merge_requests/3309)
* update postgres-protocol, postgres-types, pgwire in [!3329](https://git.picodata.io/core/picodata/-/merge_requests/3329)
* override tarantool's symbol name demangler in [!3336](https://git.picodata.io/core/picodata/-/merge_requests/3336)
* update memmap2 crate in [!3345](https://git.picodata.io/core/picodata/-/merge_requests/3345)
* update Ubuntu dependencies in CONTRIBUTING.md in [!3355](https://git.picodata.io/core/picodata/-/merge_requests/3355)
* bump anyhow to 1.0.103 in [!3366](https://git.picodata.io/core/picodata/-/merge_requests/3366)
* bump 'tarantool-sys' submodule in [!3380](https://git.picodata.io/core/picodata/-/merge_requests/3380)
* bump 'tarantool-sys' submodule in [!3380](https://git.picodata.io/core/picodata/-/merge_requests/3380)
* bump 'tarantool-sys' submodule in [!3380](https://git.picodata.io/core/picodata/-/merge_requests/3380)
* remove empty file cbo.rs in [!3383](https://git.picodata.io/core/picodata/-/merge_requests/3383)
* replace DerivedType hashing with bincode in [!3180](https://git.picodata.io/core/picodata/-/merge_requests/3180)
* port old changelog entries to new format in [!3409](https://git.picodata.io/core/picodata/-/merge_requests/3409)
* edit fragments in [!3409](https://git.picodata.io/core/picodata/-/merge_requests/3409)
* freeze submodules for 26.3.0
* update AUTHORS

### Build
* **(rolling)** cache binary installation in a dedicated job in [!2677](https://git.picodata.io/core/picodata/-/merge_requests/2677)
* fix build_rolling_binaries.py in [!2969](https://git.picodata.io/core/picodata/-/merge_requests/2969)
* trigger pycopin ci to run tests after picodata release in [!2979](https://git.picodata.io/core/picodata/-/merge_requests/2979)
* disable automatic pipeline for user branch pushes in [!2978](https://git.picodata.io/core/picodata/-/merge_requests/2978)
* only run certlinters manually in [!2993](https://git.picodata.io/core/picodata/-/merge_requests/2993)
* Revert "ci: disable automatic pipeline for user branch pushes" in [!3006](https://git.picodata.io/core/picodata/-/merge_requests/3006)
* upload pytest junit report XMLs to flake-tracker in [!3024](https://git.picodata.io/core/picodata/-/merge_requests/3024)
* use _SLUG variables for cache key values in [!3092](https://git.picodata.io/core/picodata/-/merge_requests/3092)
* disable automatic pipeline for user branch pushes in [!3107](https://git.picodata.io/core/picodata/-/merge_requests/3107)
* use `needs` instead of `stages` in tarantool-module CI in [!3132](https://git.picodata.io/core/picodata/-/merge_requests/3132)
* fix test-vanilla-* job in tarantool-module pipeline in [!3132](https://git.picodata.io/core/picodata/-/merge_requests/3132)
* make build-base-image-* job dependencies optional in [!3159](https://git.picodata.io/core/picodata/-/merge_requests/3159)
* fix docker build add clang dev dependency in [!3150](https://git.picodata.io/core/picodata/-/merge_requests/3150)
* add /usr/share/picodata directory to docker images in [!3150](https://git.picodata.io/core/picodata/-/merge_requests/3150)
* added code coverage data collection in [!3211](https://git.picodata.io/core/picodata/-/merge_requests/3211)
* move distroless images to separate namespace in [!3204](https://git.picodata.io/core/picodata/-/merge_requests/3204)
* collect code coverage data only for BUILD_PROFILE==dev in [!3240](https://git.picodata.io/core/picodata/-/merge_requests/3240)
* add jobs to run build and tests with AddressSanitizer in [!3237](https://git.picodata.io/core/picodata/-/merge_requests/3237)
* split CMake downloads from base cache in [!3239](https://git.picodata.io/core/picodata/-/merge_requests/3239)
* fix audit job image tag in [!3250](https://git.picodata.io/core/picodata/-/merge_requests/3250)
* use fs.picodata.io to store code coverage artifacts in [!3249](https://git.picodata.io/core/picodata/-/merge_requests/3249)
* fix asan-build-dev image tag in [!3275](https://git.picodata.io/core/picodata/-/merge_requests/3275)
* fix missing CI_COMMIT_BRANCH variable in [!3271](https://git.picodata.io/core/picodata/-/merge_requests/3271)
* remove `rolling_binaries_cache` from `test-py-sql-linux` job in [!3273](https://git.picodata.io/core/picodata/-/merge_requests/3273)
* remove `flaky-finder` job in [!3273](https://git.picodata.io/core/picodata/-/merge_requests/3273)
* add comment with code coverage report url to commits in [!3264](https://git.picodata.io/core/picodata/-/merge_requests/3264)
* use pypi mirror for poetry and pip in [!3284](https://git.picodata.io/core/picodata/-/merge_requests/3284)
* build distroless image from package in [!3282](https://git.picodata.io/core/picodata/-/merge_requests/3282)
* remove deprecated changelog checkbox from MR description in [!3296](https://git.picodata.io/core/picodata/-/merge_requests/3296)
* enable sccache for rustc in [!3298](https://git.picodata.io/core/picodata/-/merge_requests/3298)
* validate commit headers with prek instead of pre-commit in [!3305](https://git.picodata.io/core/picodata/-/merge_requests/3305)
* validate fragment well-formedness in [!3321](https://git.picodata.io/core/picodata/-/merge_requests/3321)
* specify rust version for macos cargo command in [!3322](https://git.picodata.io/core/picodata/-/merge_requests/3322)
* move docs jobs to child pipeline in [!3323](https://git.picodata.io/core/picodata/-/merge_requests/3323)
* trigger examples pipeline on release tag in [!3306](https://git.picodata.io/core/picodata/-/merge_requests/3306)
* add cargo cache for deploy-coverage job in [!3386](https://git.picodata.io/core/picodata/-/merge_requests/3386)
* feature freeze 26.2 branch## [26.2.0] - 2026-03-25

### Breaking changes

* prepend "picodata " to SQL version string in [!2968](https://git.picodata.io/core/picodata/-/merge_requests/2968)

### Features

### Testing
* fix git describe parsing when running exactly on a tag in [!2969](https://git.picodata.io/core/picodata/-/merge_requests/2969)

## [26.1.4] - 2026-05-28

### Fixes

- [picodata#2926] Reduced log verbosity in vshard for routine replicaset events.
- Vinyl: fix page index min-key corruption in LCP groups

## [26.1.3] - 2026-05-21

### Breaking changes

- `CancellationTokenHandle::cancel()` now returns `Rc<OnceEvent>` instead of
  `Channel<()>`. Use `finish_event.is_finished()` to check completion or
  `finish_event.wait_timeout(duration)` to wait with a timeout.
- Rename fields in `/api/v1/health/status` response: `reasons` to `issues`,
  status level `unhealthy` to `broken`.
- `picodata demo` subcommand is now gated behind the `demo` Cargo feature,
  disabled by default. To build with demo, use `CARGO_FLAGS_EXTRA="--features demo"`.

### Features

- Improved accuracy of `space:len()` for Vinyl tables.
- Support `DELETE` statementes inside transactional `DO` blocks.
- Support `INSERT` statements inside transactional `DO` blocks.
- [picodata#1596] Added support for `ARRAY` columns in `CREATE TABLE` and
  `ALTER TABLE ADD COLUMN`. Supported syntax: `T[]`, `T[N]`, `T[N][M]`,
  `T[][]`, `T ARRAY`, `T ARRAY[N]`. Declared type and sizes are documentation
  only and do not affect the internal implementation for now.

### Fixes

- Fixed crash when disabling a plugin with slow background jobs. Previously,
  if a job didn't finish within the shutdown timeout, the plugin library could
  be unloaded while the job fiber was still running.
- Fixed the `NO_ROUTE_TO_BUCKET` error that occurred when sending a request to
  the single node of a replicaset during its restart. This error is now retried
  by the router.
- [picodata#2842] Fixed the `schema version has changed: need to re-compile SQL statement`
  error, which could occur when you execute multiple DQL queries due to
  yield during cache eviction.
- Fixed sentinel panic on long activation wait.
- [picodata#2812] Use direct RPC for query metadata on DQL cache miss
  - Replace vshard-based Lua dispatch with ConnectionPool::call_raw for
    the proc_query_metadata callback. This fixes SQL query execution from
    arbiter tier instances (bucket_count=0) where no vshard router exists.
- [picodata#2888] Fixed a bug where cluster would fail to bootstrap if there were no voter
  instances in the initial `--peer` set.
- Revoking privileges from `admin` user caused a panic. Now, revoking priviliges
  from `admin` user is forbidden, for same reasons as for the `pico_service` user.
- Fixed `ALTER PLUGIN ADD SERVICE TO TIER` accepting nonexistent tier names.
  Now validates that the specified tier exists and returns an error otherwise.
- Backup operation will now be automatically aborted if there are offline instances.
  This prevents cluster being locked in a readonly state.
- [picodata#2732] Fixed a panic when attempting to inherit privileges via SQL
  (e.g., `GRANT admin TO somebody`). We do not support privilege inheritance
  via `GRANT user1 TO user2`. The system now validates the grantee type and
  returns a proper `NoSuchRole` error instead of panicking.

## [26.1.2] - 2026-04-14

### Changed

- Optimize `/api/v1/tiers` and `/api/v1/cluster` endpoints to reduce RPC calls.
  HTTP addresses are now read from `_pico_peer_address` storage instead of RPC,
  and memory info is only fetched from replicaset leaders. This reduces the
  number of RPC calls from O(N×RF) to O(N) where N is the number of replicasets.
  Offline instances now show their HTTP address (from storage) instead of empty string.

### Fixes

- Fixed a SQL planner panic caused by stale type metadata after clone-based
  rewrites such as `BETWEEN` normalization and `GROUP BY` alias expansion.
- Fixed a bug with the storage cache that caused an error "Temporary table TMP_ not found".
- Fixed a regression in config parsing. `--iproto-listen`, `--iproto-advertise` and
  `--http-listen` were triggering an error instead of overriding corresponding value
  in yaml config when deprecated listen options were used in the config.
- Fixed a permission error occured during query planning for non-admin users.
- Fix cold restart deadlock where all instances in a replicaset would get empty
  replication configs, preventing synchronization. The governor now includes
  only the master in the fallback replication config, preserving conflict
  isolation while allowing the cluster to recover.
- Fixed the errors `box.cfg.read_only is true` and `Failed to add a storage reference`,
  which occurred when restarting a storage instance and previously required a retry.
- Fixed the `query for request_id with plan_id not found` error that occurred
  on queries with `UNION` of `CTE` on a cluster consisting of multiple instances.
- Fixes an issue when http and plugin addresses were inserted into `_pico_peer_address`
  when an instance joined a mixed-version cluster, causing a panic on older instances.
  It is also now not possible to bootstrap an 26.1.x instance defining a plugin listener
  address into a mixed 25.5.x and 26.1.x cluster - cluster has to be fully updated to do that.


## [26.1.1] - 2026-03-24

### Features

- Unified socket configuration: introduce new `instance.iproto`, `instance.http`,
  and `instance.pgproto` config sections that consolidate listen/advertise/TLS settings
  per protocol. Old top-level parameters (`instance.iproto_listen`,
  `instance.iproto_advertise`, `instance.http_listen`, `instance.pg`) are deprecated
  but remain functional. HTTP server is now enabled by default on port 5327.
  HTTP and pgproto peer addresses are stored in `_pico_peer_address` system table
  with corresponding connection types (`http`, `pgproto`, `plugin:<name>.<service>`).
- Rework SQL execution protocol for DML queries to reduce data transfer.
- Support JSON_EXTRACT_PATH function.
- Introduce non-blocking SQL execution to prevent fiber starvation.
- New column `sync_incarnation` is added to `_pico_instance` system table.
- New ALTER SYSTEM parameter `governor_check_replication_error` (default: true)
  enables the checking if replication is broken on any instance, in which case
  the instance will be automatically made Offline.
- New ALTER SYSTEM parameter `sql_log` (default: false)
  enables logging of all SQL statements to log file.
- Add CREATE TABLE syntax "PRIMARY KEY (bucket_id, ...)":
  - When this syntax is used, there is no separate 'bucket_id' index;
  - Instead, 'bucket_id' is included as the first part of the primary key index.
- New columns `target_state_reason` & `target_state_change_time` in `_pico_instance` system table
- Added env option PICODATA_UNSAFE_FORCE_RECOVERY.
  - Possible values: true, false.
  - This option is passed to Tarantool as `force_recovery` option.
  - If force_recovery equals true, Tarantool tries to continue
    if there is an error while reading a snapshot file (at server instance start)
    or a write-ahead log file (at server instance start or when applying
    an update at a replica.
- Introduce `read_preference` option for routing DQL queries to replicas in specific scenarios.
- Introduce `pico_stmt_invalidation` option for getting errors when binding invalid statements.
- Introduce `pico_query_metadata` option for getting distribution key metadata.
- Introduce `sql_preemption_opcode_max` to control the VDBE opcode interval
  between execution time checks when `sql_preemption` is enabled.
- New ALTER SYSTEM parameter `sql_runtime_concurrency_max` (default: `1`)
  limits the number of simultaneously executing SQL requests per instance.
- Support cluster update to next major version (26.1.0).
- Support compatibility between the next major Picodata version and older plugin versions.
- Add support for `EXPLAIN (RAW)` for queries that fail at local sql execution stage.
- Add unlogged tables to SQL:
  - Unlogged tables' updates are not writeen into the WAL, so they are not persisted on restarts of
    an instance and are not replicated. On leader change, all unlogged tables are truncated to
    prevent inconsistencies. Creating unlogged tables is possible with the `CREATE UNLOGGED TABLE ...`
    syntax. Unlogged tables are implemented as Tarantool data-temporary spaces, so it is not possible to
    store them using the vinyl engine.
- \[breaking\] Instead of always being a tier with name `default`, default tier is now the first tier mentioned in the config.
- Add support for `compression_level` Vinyl option for secondary indices created with `CREATE INDEX`.
- Add support for Vinyl index options (`bloom_fpr`, `page_size`, `range_size`, `run_count_per_level`,
  `run_size_ratio`, `compression_level`) in `CREATE TABLE ... WITH (...)` syntax for configuring
  implicit primary key and bucket_id indices.
- Add suppoort for anonymous blocks. An anonymous block is a sequence of statements that execute
  queries transactionally. Blocks are single-bucket, meaning that all the queries within the
  block must be executed on the same bucket (or have distribution any).
- Add optimization for Limit + Distinct and Limit + OrderBy.
  When certain conditions are met, the Limit node is added to the local stage of SQL query plan.

  Supported statements are:
  - QUERY: execute the given query;
  - RETURN QUERY: execute the given query and return its result.

  Example of a block query:
  ```sql
  DO $$ BEGIN
    RETURN QUERY SELECT a FROM t WHERE pk = 1;
    UPDATE t SET a = a + 1 WHERE pk = 1;
  END $$;
  ```
- Add detailed health status endpoint (`/api/v1/health/status`) with instance, Raft, bucket, and cluster information.
- Add support for Kubernetes startup, liveness and readiness probes.
- Support `bucket_count=0` for tiers. A tier with `bucket_count=0` has no sharded data
  (only global system tables) and is intended for "arbiter" tiers used in Raft consensus.
  Vshard bootstrap and configuration are skipped for such tiers, and replicaset expel
  proceeds without waiting for bucket transfer. Creating sharded tables on a zero-bucket
  tier is rejected with a clear error.
- Add support for `EXPLAIN (RAW)` for block queries.
- Speed up instance restart by actively trying to identify the raft leader instead of waiting for it to send a heartbeat to us.
- Refactor the plan id calculation for more accurate and faster caching.
- ACL/ALTER SYSTEM/ALTER INDEX RENAME operations now support WAIT APPLIED GLOBALLY / WAIT APPLIED LOCALLY syntax and
  default to globally, matching DDL behavior.
- Add bucket estimation for INSERT queries in explain.
- Support reading from global tables in anonymous blocks; writing is not supported yet.
- Upgrade Tarantool from 2.11.5 to 2.11.8.
- Add migration context validation API into plugin SDK.
- Introduce a local SQL execution path for eligible queries that bypasses
  `iproto` on the current instance; usage is exposed via the
  `pico_sql_local_query_total` and `pico_sql_local_query_duration` metrics.
- Remove unnecessary `Motion(Full)` for queries that are guaranteed to be routed to a single node due to the sharding key filter.
- SQL function `version` now prefixes picodata version with `picodata `, allowing PostgreSQL clients to easily detect the use of picodata.

### CLI
- Completely re-architected `picodata demo` subcommand:
  - Fixed improper signal handling (SIGINT, SIGTERM) and process termination.
  - Added graceful shutdown and guaranteed cleanup of child processes.
  - Introduced cluster orchestration model for simplified lifecycle management.
  - Added configurable command-line parameters and cluster information display.
  - Add machine-readable output formats to `picodata admin`
  - Add long version output (-VV) with more info

### WebUI
- Webui now displays the value of `cluster_version` instead of current
  instance's version. That way you can easily tell if the cluster has been
  upgraded successfully or not yet.
- the display of the target state in the instance card has been removed
- the ability to group by replicas has been removed
- added a visual indication of the problem status for the offline instance counter
- virtualization has been applied to the tiers and instance list
- the cluster information is displayed in the header
- the filter has been redesigned, now it is constantly displayed in front of the list of shooting ranges or instances. Added the ability to filter by text and by tags, such as dash name, replica set name, instance name, version, status.

### Fixes

- Fixed that governor would hang indefinitely if an Offline replicaset had
  target_master_name != current_master_name.
- Fixed that instance would hang indefinitely when trying to join the cluster if
  the cluster becomes too big.
  NOTE: The fix requires modifying the proc_raft_join RPC response format
  which technically breaks compatibility with previous versions of picodata.
  However picodata explicitly doesn't support heterogeneous joins (when version
  of joining instances mismatches version of cluster), so this shouldn't be a
  problem for anybody. NOTE also that this doesn't affect restarting instances
  which already joined the cluster.
- Fixed a crash when SQL request arrives before instance is properly initialized
- Fixed that instances would be made Offline immediately after a raft entry is
  applied if there weren't any entries applied for a long time before that
- Fixed that instances would randomly fail with ER_READONLY during bootstrap
- Fixed ER_BOOTSTRAP_CONNECTION_NOT_TO_ALL failure during instance join stage.
- Fixed that governor would send redundant proc_sharding RPCs which would make
  it impossible to deploy huge clusters. Now RPCs from governor are split into
  batches of configurable size (default 200, ALTER SYSTEM parameter `governor_rpc_batch_size`).
- Improve upgrade flow for creating Lua stored functions (exported to SQL).
- Node construction is now deferred until actually needed, avoiding unnecessary
  work for cached queries on any instance execution
- Fixed a memory leak in SQL API of plugin SDK
- `picodata status` no longer panics when `stdout`, `stderr`,
  or both are redirected to a broken pipe.
- Fixed that the whole replicaset would be broken if one instance get's a
  replication conflict. (See also https://git.picodata.io/core/picodata/-/issues/2231).
- Fixed that governor would sometimes be blocked in read_only on a DDL operation
  mode not being able to apply any subsequent raft operations.
- Introduce unnamed_join alias for motions with joins under them to distinguish columns with identical names
- Governor RPC batching is also implemented for proc_apply_schema_change.
- Governor RPC batching is also implemented for proc_apply_backup.
- Datetime literals should support `yyyy-mm-dd` format, e.g. `select '2026-01-17'::datetime`.
- Fix type inference for the `a BETWEEN b AND c` expression; now types of `a`, `b` and `c` should be
  properly unified, meanining that `select '2026-01-13' between '2026-01-01'::datetime and '2026-01-20'`
  will work as expected.
- Fixed that upgrading between patch versions wouldn't run upgrade scripts.
- Fixed assertion failure in CAS right after raft leader change followed by
  persisted raft log tail truncation.
- Fixed instance.vinyl.* options to be applied to primary and bucket_id indices.
- Fixed a crash in proc_runtime_info when the last applied raft entry contained
  a unicode string where a 100th byte position was not on a character boundary.
- Fixed that sentinel_loop was broken during upgrade from versions before 25.5.3.
- Fixed ignoring `NULLS FIRST` and `NULLS LAST` in unnamed window queries with ordering.
- Fixed metadata loss in queries with LIMIT clause in picodata admin.
- Fixed invalid volatile flag for rust-implemented builtin functions.
- Fixed governor's `ConfigureReplication` step was broken during upgrade from before 25.5.3
- Fixed that instances from tiers with can_vote=false attempting to promote to raft leader.
- Fixed that `--pg-advertise` CLI argument was erroneously disallowed to be used
  simultaneously with `--iproto-advertise`.
- Fixed a race condition between DDL (i.e., TRUNCATE) and DQL when the preemption option is enabled.
- Fixed concurrent access to storage temporary tables by synchronizing their lifecycle
  and execution with a per-plan lock.
- Fixed a number of vinyl issues by backporting upstream patches
- Fixed an issue where upgrade operations were inserted incorrectly
  when applying system catalog changes for several catalog versions.
- Fixed `picodata plugin configure` panic on attempt to update non-existent plugin or a non-existing
  service of an existing plugin.
- Fixed `proposal dropped` errors sometimes being returned from DDL commands for example when raft is unknown.
- Fixed an RPC to avoid skipping metrics collection code path on early return in procedure implementation.
- Make sure that single-tiered clusters upgraded from 25.3.x always have a default tier.
- Fixed that instances would fail with ER_READONLY while joining a replicaset
  whose master was still bootstrapping. Governor no longer triggers mastership
  failover for a master that is in the initial Offline(0) join state and has
  not yet had a chance to become Online.
- Fixed local SQL iterators to survive fiber yields during table truncation.
- Fixed a caching bug affecting `UNION` queries with global and sharded tables in a cluster of several replicasets.
- Fixed a caching bug that caused some different queries to tables with `bucket_id` in the primary key to have the same plan id.
- Fixed SUM/AVG type resolution for Double
- Fixed incorrect filter pushdown into compound queries containing window functions.

### Observability

- All duration-based metrics now report in fractional seconds instead of
  milliseconds for consistency with Prometheus and more precision.
- RPC request durations now use a monotonic high-precision clock instead
  of the event-loop clock to improve timing accuracy.
- Added SQL temp-table lock metrics:
  `pico_sql_temp_table_leases_total` and
  `pico_sql_temp_table_lock_waits_total`.

### Breaking changes

- Remove `tros` and `tarolog` dependencies from `picodata-plugin`. These
  libraries can still be used as direct dependencies when needed.
- Hashing behavior changed for `DOUBLE` type fields in primary keys
  and distribution keys. Previously, values were always re-encoded as MP_DOUBLE
  (9 bytes) before hashing. Now, integer-representable doubles (e.g., `1.0`)
  are converted to integer encoding before hashing, making them hash identically
  to their integer equivalents (e.g., `1`). This is correct behavior that allows
  lookups like `SELECT * FROM t WHERE double_col = 1` to find rows inserted with
  `double_col = 1.0`. However, existing data sharded on `DOUBLE` keys containing
  integer values may have different bucket assignments after upgrade.


## [25.5.9] - 2026-03-20

### Fixes

- Support cluster update to next major version (26.1.0).
- Fixed incorrect filter pushdown into compound queries containing window functions.


## [25.5.8] - 2026-02-25

### Features

- New ALTER SYSTEM parameter `sql_log` (default: false)
  enables logging of all SQL statements to log file.
- Introduce `sql_preemption_opcode_max` to control the VDBE opcode interval
  between execution time checks when `sql_preemption` is enabled.
- Fixed local SQL iterators to survive fiber yields during table truncation.

### Fixes

- Fixed that `--pg-advertise` CLI argument was erroneously disallowed to be used
  simultaneously with `--iproto-advertise`.
- Fixed an issue where upgrade operations were inserted incorrectly
  when applying system catalog changes for several catalog versions.


## [25.5.7] - 2026-02-10

### Fixes

- Fixed a number of vinyl issues by backporting upstream patches


## [25.5.6] - 2026-02-06

### WebUI
- Webui now displays the value of `cluster_version` instead of current
  instance's version. That way you can easily tell if the cluster has been
  upgraded successfully or not yet.

### Fixes

- Fixed governor's `ConfigureReplication` step was broken during upgrade from before 25.5.3
- Fixed that instances from tiers with can_vote=false attempting to promote to raft leader.
- Fixed a crash in case of any error during TRUNCATE operation.
- Fixed a race condition between DDL (i.e., TRUNCATE) and DQL when the preemption option is enabled.


## [25.5.5] - 2026-01-26

### Fixes

- Fixed assertion failure in CAS right after raft leader change followed by
  persisted raft log tail truncation.
- Fixed a crash in proc_runtime_info when the last applied raft entry contained
  a unicode string where a 100th byte position was not on a character boundary.
- Added env option PICODATA_UNSAFE_FORCE_RECOVERY.
  - Possible values: true, false.
  - This option is passed to Tarantool as `force_recovery` option.
  - If force_recovery equals true, Tarantool tries to continue
    if there is an error while reading a snapshot file (at server instance start)
    or a write-ahead log file (at server instance start or when applying
    an update at a replica.
- Always open vylog files with O_SYNC.
- Fixed that sentinel_loop was broken during upgrade from versions before 25.5.3.


## [25.5.4] - 2026-01-21

### Fixes

- Introduce unnamed_join alias for motions with joins under them to distinguish columns with identical names
- Fix erroneous logic of counting rows returned from replicasets which led to undercovered limit exceedance
  errors.
- Fixed that upgrading between patch versions wouldn't run upgrade scripts.
- Datetime literals should support `yyyy-mm-dd` format, e.g. `select '2026-01-17'::datetime`.
- Fix type inference for the `a BETWEEN b AND c` expression; now types of `a`, `b` and `c` should be
  properly unified, meanining that `select '2026-01-13' between '2026-01-01'::datetime and '2026-01-20'`
  will work as expected.


## [25.5.3] - 2026-01-15

### Features

- Support JSON_EXTRACT_PATH function.
- New column `sync_incarnation` is added to `_pico_instance` system table.
- New ALTER SYSTEM parameter `governor_check_replication_error` (default: true)
  enables the checking if replication is broken on any instance, in which case
  the instance will be automatically made Offline.
- New columns `target_state_reason` & `target_state_change_time` in `_pico_instance` system table

### Fixes

- Fixed that the whole replicaset would be broken if one instance get's a
  replication conflict. (See also https://git.picodata.io/core/picodata/-/issues/2231).
- Fixed that governor would sometimes be blocked in read_only on a DDL operation
  mode not being able to apply any subsequent raft operations.


## [25.5.2] - 2025-12-26

### Features

- Rework SQL execution protocol for DML queries to reduce data transfer.
- Introduce non-blocking SQL execution to prevent fiber starvation.

### Fixes

- Fixed that governor would hang indefinitely if an Offline replicaset had
  target_master_name != current_master_name.
- Fixed that instance would hang indefinitely when trying to join the cluster if
  the cluster becomes too big.
  NOTE: The fix requires modifying the proc_raft_join RPC response format
  which technically breaks compatibility with previous versions of picodata.
  However picodata explicitly doesn't support heterogeneous joins (when version
  of joining instances mismatches version of cluster), so this shouldn't be a
  problem for anybody. NOTE also that this doesn't affect restarting instances
  which already joined the cluster.
- Fixed a crash when SQL request arrives before instance is properly initialized
- Fixed that instances would be made Offline immediately after a raft entry is
  applied if there weren't any entries applied for a long time before that
- Fixed that instances would randomly fail with ER_READONLY during bootstrap
- Improve upgrade flow for creating Lua stored functions (exported to SQL).
- Fixed that governor would send redundant proc_sharding RPCs which would make
  it impossible to deploy huge clusters. Now RPCs from governor are split into
  batches of configurable size (default 200, ALTER SYSTEM parameter `governor_rpc_batch_size`).
- Node construction is now deferred until actually needed, avoiding unnecessary
  work for cached queries on any instance execution
- Fixed a memory leak in SQL API of plugin SDK


## [25.5.1] - 2025-12-19

### Features

- Change the default memtx_checkpoint_count from 2 to 1. If a user needs a checkpoint, they can use SQL BACKUP statement, no need to occupy extra space by default.
- Reduce the frequency of heartbeats sent to raft learners, reducing the cluster traffic.
- Refactor SQL executor to reduce memory allocations.
- Plugins and webUI now support scram-sha256 auth.
- SQL optimizer may now select indices for comparison operators with mixed number
  types (e.g. pk int < decimal) and datetime.
- Vinyl improvements to decrease in-memory page index size.
- Add HTTPS support for metrics and WebUI
- Add the _pico_bucket(tier_name) function to show vshard bucket distribution.
- Rework SQL execution protocol for DQL queries to reduce data transfer.

### Fixes
- Fix compilation error on ARM architecture.
- Fix eliminate erroneous ER_READONLY message in logs when executing TRUNCATE on
  global tables sometimes (See also https://git.picodata.io/core/picodata/-/issues/2274).
- Fix possible crash when using TLS for iproto communication within cluster.
- Specifying `instance.iproto_tls.enable` parameter via configuration file.
- Prevent conflicts when creating tables with renamed table names.
- Fix CREATE TABLE hanging on index conflict.
- Print upper bound of a signed 64-bit integer for ALTERing the `sql_storage_cache_count_max`,
  instead of an unsigned 64-bit integer, after `UNSIGNED` has been deleted internally and range
  has been changed.
- Resolve hang when using TLS (hang in `ssl_iostream_destroy` when fiber is cancelled).
- Resolve hang when using TLS (fix infinite loop in coio_writev_timeout on SSL_ERROR_ZERO_RETURN).
- Fix Service::on_leader_change callbacks not being called after master goes Offline
  due to sentinel auto-offline policy (See also https://git.picodata.io/core/picodata/-/issues/2303).
- Fixed a bug where the raft snapshot would sometimes contain incorrect term
  which would result in an unrecoverable failure of the receiving instance.
- DML to global tables no longer will fail with errors such as "Compacted"
  (See also https://git.picodata.io/core/picodata/-/issues/2273).
- Fix `use-after-poison` in `proc_sql_execute`.
- Fixed a bug which made the effective value of `pg_portal_max` be the same as `pg_statement_max`
- Fix trigger execution and privilege initialization for scram-sha256 auth in pgproto.
- Fixed possible ER_BOOTSTRAP_READONLY failure during instance bootstrap.
- Fixed a possible bootstrap failure when joining multiple instances at the same
  time. NOTE: The fix requires modifying the proc_raft_join RPC response format
  which technically breaks compatibility with previous versions of picodata.
  However picodata explicitly doesn't support heterogeneous joins (when version
  of joining instances mismatches version of cluster), so this shouldn't be a
  problem for anybody. NOTE also that this doesn't affect restarting already
  instances which already joined the cluster.
- Fix pico_instance_state metric to report correct value on all instances
- Fixed a bug where some queries that changed the table schema checked WRITE permissions instead of ALTER permissions.
- Fix `DO NOTHING` conflict policy error on unique indices for globally distributed tables.
- Lua datetime functions `to_char` and `to_date` are now strict (returns `NULL` when at least one argument is `NULL`).
- Fix name resolution in HAVING if projection contains a param.
- Fixed that vshard would consume non-negligible amount of resources when idling
  on big cluster setups.

### WebUI
- Introduce display of used memory in terms of tier
- Make offline instance count red in case if there are offline instances
- Get rid of "target state" field due to its uselessness

### ACL
- Support granting CREATE privilege on specific table.

### SQL
- Support indexing arrays, i.e. `a[1][2]` expressions.
- Support indexing maps with string keys, i.e. `m['key']` expressions.
- Add IF NOT EXISTS support for ALTER TABLE ADD COLUMN.
- Support for a new set of volatile scalar functions, providing service information:
  - `pico_instance_name()`: returns a name of the current instance.
  - `pico_replicaset_name()`: returns a name of the replicaset in which current instance is.
  - `pico_tier_name()`: returns a name of the tier in which current replicaset of current instance is.
  - `pico_instance_dir()`: returns an absolute path to the current instance working directory
    (do not confuse with share directory, which is used for plugins).
  - `pico_config_file_path()`: return an absolute path to the current instance configuration file. It
    does not check whether the config exist, it will still return the initial path. If the instance was
    started without specifying an instance config, it will return `null`.
- Support ALTER INDEX RENAME command to rename an existing index.
- Introduced various optimizations reducing memory allocations and
  eliminating unnecessary plan traversals and modifications.
  Some highlights:
  - Eliminated unnecessary parameter clones.
  - Skipped optimizations for plans that cannot benefit from them.
  - Cached results of some transformations.
  - Eliminated allocations in various places.
  - Avoided sending unused vtables for INSERT queries.

  As a result:
  - INSERT performance increased by 20%-70% depending on the size of the rows.
  - TPC-B performance increased by ~10%.
- Support `EXPLAIN (RAW)` for DML queries.

- Add INDEXED BY clause. It is sqlite feature which allows to specify index for
  table lookup.

### Pgproto
- Add support for configuring pgproto TLS certificates via:
  - `instance.pg.ssl` (enable/disable TLS)
  - `instance.pg.cert_file` (client certificate path)
  - `instance.pg.key_file` (private key path)
  - `instance.pg.ca_file` (CA certificate path).

### Observability
- Added metrics `pico_sql_global_dml_query` and `pico_sql_global_dml_query_retries`
  which report respectively the total number of SQL DML operations on global
  tables and number of times these operations had to be retried due to CAS conflicts.

### Configuration
- Changed default value for `raft_wal_count_max` ALTER SYSTEM parameter from 64 to 16384.

## [25.4.4] - 2025-11-12

### Fixes
- Fixed a bug where the raft snapshot would sometimes contain incorrect term
  which would result in an unrecoverable failure of the receiving instance.
- Resolve hang when using TLS (hang in `ssl_iostream_destroy` when fiber is cancelled).
- Resolve hang when using TLS (fix infinite loop in coio_writev_timeout on SSL_ERROR_ZERO_RETURN).
- DML to global tables no longer will fail with errors such as "Compacted"
  (See also https://git.picodata.io/core/picodata/-/issues/2273).
- Fix trigger execution and privilege initialization for scram-sha256 auth in pgproto.

### SQL
- Add IF NOT EXISTS support for ALTER TABLE ADD COLUMN.

### Observability
- Added metrics `pico_sql_global_dml_query` and `pico_sql_global_dml_query_retries`
  which report respectively the total number of SQL DML operations on global
  tables and number of times these operations had to be retried due to CAS conflicts.

## [25.4.3] - 2025-10-15

### Fixes
- Fix possible crash when using TLS for iproto communication within cluster.
- Specifying `instance.iproto_tls.enable` parameter via configuration file.
- Prevent conflicts when creating tables with renamed table names.
- Fix CREATE TABLE hanging on index conflict.
- Fix Service::on_leader_change callbacks not being called after master goes Offline
  due to sentinel auto-offline policy (See also https://git.picodata.io/core/picodata/-/issues/2303).

### Pgproto
- Add support for configuring pgproto TLS certificates via:
  - `instance.pg.ssl` (enable/disable TLS)
  - `instance.pg.cert_file` (client certificate path)
  - `instance.pg.key_file` (private key path)
  - `instance.pg.ca_file` (CA certificate path).

## [25.4.2] - 2025-10-09

### Fixes
- Fix compilation error on ARM architecture.
- Fix eliminate erroneous ER_READONLY message in logs when executing TRUNCATE on
  global tables sometimes (See also https://git.picodata.io/core/picodata/-/issues/2274).

### WebUI
- Add a login form and JWT-based session management. The JWT secret is stored in
  the `_pico_db_config` table as `jwt_secret`. Set `jwt_secret` to an empty string ("")
  to disable authentication. On upgrade, Picodata leaves the existing `jwt_secret`
  unchanged. To enable web authentication on existing clusters, reset `jwt_secret`
  via the `ALTER SYSTEM` API.

## [25.4.1] - 2025-10-02

### Features

- Support unique indexes with non-sharded columns on sharded tables, with sharding key prefix provided
- Introduce the governor script to add `is_default` column to the `_pico_tier` table
- Pgproto now reports more verbose TLS error messages
- Validate cluster UUID during IPROTO handshake and propagate `cluster_uuid` via IPROTO_ID. Connections with a mismatching `cluster_uuid` are rejected, ensuring cross-cluster isolation.
- Instance's which loose ability to apply raft log updates will automatically
  become Offline. (See also https://git.picodata.io/core/picodata/-/issues/2238).
- Support TLS in IPROTO for intra-cluster communication and CLI.
  - Configure via new config section: `instance.iproto_tls`;
  - Run CLI with new arguments: `--tls-cert`, `--tls-key`, `--tls-ca`.
- Support TLS certificate authentication
  - Use the certificate's Common Name (CN) field as the username;
  - For example, use `CN=dbuser@company.com` or `CN=dbuser` in the certificate to authenticate the user "dbuser";
  - This method takes precedence over other authentication mechanisms.
- New ALTER SYSTEM parameter `plugin_check_migration_hash` (default: true)
  allows disabling plugin migration file checksum validation. This allows plugin
  authors to more easily fix mistakes in migration files.
- It is now possible to use `scram-sha256` auth method for old and new users.
  Currently, the method is only compatible with pgproto, which means it
  won't be available for picodata plugins or iproto connections.

### Observability
- `instance_name` is now the primary label for Prometheus metrics.
- Added the metric `pico_info_uptime{instance_name, instance_dir_name, replicaset, tier, cluster_name}`.
- Updated Grafana dashboard: legends now use `{{instance_name}}`.

### SQL
- SQL supports scalar function `abs()`.
- Multiple `OPTIONS` specified in an SQL query no longer result in an error.
  Instead, the rightmost option takes precedence.
- SQL now supports the `AUDIT POLICY` operation to enable or disable audit logging of DML operations for specific users.
  - Use cases:
    - `AUDIT POLICY dml_default BY dbuser` - enables audit logging of DML operations for user "dbuser"
    - `AUDIT POLICY dml_default EXCEPT dbuser` - disables audit logging of DML operations for user "dbuser"
  - Currently, only one policy named `dml_default` is supported.
- Maximum value for `Integer` and `Unsigned` type is ***9223372036854775807***.

### Fixes
- Fixed "instance is already joined" error if picodata crashes during reboostrap
  (see also https://git.picodata.io/core/picodata/-/issues/2077).
- Supported detecting and fixing broken replication.
- Remove unnecessary timeout on local Unix domain sockets in `picodata admin` command.
  Unlike network sockets, local sockets don't silently hang - a server crash
  immediately breaks the connection, making timeouts redundant. This fixes an issue
  where long-running plugin commands could be incorrectly aborted due to the
  artificial timeout.
- Supported backoff strategies when configuring vshard from the governor.
- Fixed restart the whole cluster at once taking too long.
- Fixed an issue where `cluster.shredding` option does not get applied on instance restart.
- `EXPLAIN` queries now support `OPTION` (e.g. `EXPLAIN SELECT 1 OPTION (SQL_VDBE_OPCODE_MAX = 6)`)
- Fixed joining a replica after expel with the same instance name as the
  expelled one (see also https://git.picodata.io/core/picodata/-/issues/2173).
- Fixed an out-of-memory crash when using Response::encode_rmp with large
  collections in plugin API (see also https://git.picodata.io/core/picodata/-/issues/2028).
- Upgraded luajit to fix several issues.
  See https://git.picodata.io/core/tarantool/-/merge_requests/282 for details.
- Fixed bizarre row order produced by ORDER BY <uuid>.
- Fixed restarting the cluster without some of the voters
  (see also https://git.picodata.io/core/picodata/-/issues/2202).
- Fixed potential undefined behavior in plugin RPC handler arguments.
- Picodata now loads all certificates from `ca.crt`, not just the first one.
- Fixed a possible state corruption in case picodata crashes after receiving a
  raft snapshot with a stale schema version.
- Fixed TRUNCATE operation for global tables.

### CLI

- Authentication method flags now use automatic recognition system, instead
  of relying on default values, delivering less error-prone and complex user
  experience.
  See <https://git.picodata.io/core/picodata/-/issues/1973>.

- `picodata status` and `picodata plugin configure` now support execution with
  custom user, determining authentication method automatically on it's own.
  WARNING: If you do not specify an authentication method, it will be found
  out by brute force, which may lead to the user being blocked if the number of
  authorization attempts exceeds the limits.
  See <https://git.picodata.io/core/picodata/issues/1734>.

- `picodata plugin configure` now prints a message on success for better UX.
  See <https://git.picodata.io/core/picodata/-/issues/1904>.

- `picodata plugin configure` now returns a success code, instead of non-zero
  when trying to change plugin service parameters with the same values.
  See <https://git.picodata.io/core/picodata/-/issues/2222>.

### Plugin API

- `internal::authenticate` is now deprecated, and is a re-export of the
  same auth function in a separate module `authentication::authenticate`.
  See <https://git.picodata.io/core/picodata/-/issues/2007>.

### WebUI
 - display expelled state

## [25.3.3] - 2025-09-10

### CLI

- Authentication method flags now use automatic recognition system, instead
  of relying on default values, delivering less error-prone and complex user
  experience.
  See <https://git.picodata.io/core/picodata/-/issues/1973>.

- `picodata status` and `picodata plugin configure` now support execution with
  custom user, determining authentication method automatically on it's own.
  WARNING: If you do not specify an authentication method, it will be found
  out by brute force, which may lead to the user being blocked if the number of
  authorization attempts exceeds the limits.
  See <https://git.picodata.io/core/picodata/issues/1734>.

- `picodata plugin configure` now prints a message on success for better UX.
  See <https://git.picodata.io/core/picodata/-/issues/1904>.

### Features

- Pgproto now reports more verbose TLS error messages

### Fixes

- Record `pico_sql_query_errors_total`, `pico_sql_query_duration` and `pico_sql_query_errors_total` metrics
  for queries executed through pgproto
- Fixed joining a replica after expel with the same instance name as the
  expelled one (see also https://git.picodata.io/core/picodata/-/issues/2173).

## [25.3.2] - 2025-08-04

### Fixes

- Fixed "instance is already joined" error if picodata crashes during reboostrap
  (see also https://git.picodata.io/core/picodata/-/issues/2077).
- Remove unnecessary timeout on local Unix domain sockets in `picodata admin` command.
  Unlike network sockets, local sockets don't silently hang - a server crash
  immediately breaks the connection, making timeouts redundant. This fixes an issue
  where long-running plugin commands could be incorrectly aborted due to the
  artificial timeout.

## [25.3.1] - 2025-07-25

### Features

- Introduce automatic system catalog schema upgrade on new releases
  (automatically executing DDL/DML and creating internal functions).

### ACL
- The maximum number of users and roles that can be created has been increased from 32 to 128.

### SQL
- SQL supports `ORDER BY ... NULLS FIRST/LAST` operation.
- SQL supports `INSERT INTO ... ON CONFLICT DO FAIL/REPLACE/NOTHING` operation for globally distributed tables.
- SQL supports modulo operator `%` for integers.
- SQL supports window function `last_value()`.
- SQL supports new volatile scalar functions `pico_raft_leader_id()` and `pico_raft_leader_uuid()`.
- SQL scalar function `instance_uuid` is now marked as deprecated, consider using `pico_instance_uuid`.
- SQL supports new stable scalar function `version()`, that returns version of the current instance.
- SQL supports `CURRENT_TIMESTAMP` scalar function now.
- \[breaking\] `LOCALTIMESTAMP` now returns time with correct timezone, instead of always marking it as UTC.
- Remove `Unsigned` type from casts and general type operations:
  - The `Unsigned` type can no longer be used in casts (`CAST(x AS unsigned)`)
  - `Unsigned` no longer appears in `EXPLAIN` output
  - The type remains available only for column definitions in DDL (`CREATE TABLE`, `ALTER TABLE`)

- A new `BACKUP` SQL command is now supported for performing consistent, clusterwide backups.
  A new `--backup-dir` configuration parameter has been introduced (available via environment variable and
  config file). By default, `--backup-dir` is set to `<instance-dir>/backup`. When `BACKUP` is executed,
  each instance in the cluster saves its data into a subdirectory under `backup-dir`, named in the
  format YYYYMMDDThhmmss. The following data is included in the backup:
  * `.snap` files
  * the instance configuration file
  * `.picodata-cookie`
  * plugin data from `share-dir`

  **Note**: Backup cleanup is not automated — database administrators are responsible for
  managing and deleting old backups
- A new `picodata restore` CLI command has been added to restore data from previously created backups.
  Use the `--path` parameter to specify the path to the backup directory. The recommended restore
  procedure is:
  * Stop all instances in the cluster
  * Run `picodata restore --path <backup-path>` on each instance, using the same timestamped
    backup folder (e.g., YYYYMMDDThhmmss)
  * Restart all instances

### Pgproto

- Support LDAP with TLS (StartTLS) for LDAP authentication menthod
  - use TT_LDAP_ENABLE_TLS=true environment variable to turn on

- More metrics:
  - pico_pgproto_connections_closed_total
  - pico_pgproto_connections_opened_total
  - pico_pgproto_portals_closed_total
  - pico_pgproto_portals_opened_total
  - pico_pgproto_statements_closed_total
  - pico_pgproto_statements_opened_total
- Record `pico_sql_query_errors_total`, `pico_sql_query_duration` and `pico_sql_query_errors_total` metrics
  for queries executed through pgproto

### Config
- Config supports `instance.pg.advertise` option for pgproto server advertise address,
  which defaults to `instance.pg.listen`. It also can be set in the CLI via `--pg-advertise` option.

### WebUI

- Replicaset state now always display current leader state
- Display cluster ID and instance PG address
- Cluster info and other data refreshes automatically every 10 seconds if the window is focused
- Offline instances are now marked with a contrasting indicator

### Fixes

- Fixed numerous panics when setting invalid values for alter system parameters
- Fixed the modification of `_pico_service_route`; it is now retriable.

- Fixed a performance issue with global table DML which resulted in an up to 95x performance increase!

- Fixed sorting in WebUI - it's now consistent across reloads
- Fixed WebUI dependency on internet access for displaying fonts
- Fixed WebUI copy icons: now they copy related text on-click

- Fixed a bug which broke intra-replicaset replication when upgrading from 25.1.* to 25.2.*.
  Note that clusters broken by this bug require some manual actions in addition
  to this fix, but newer version should upgrade fine. Note also that this fix
  does not include the actual schema upgrade procedure, so the cluster will
  still have a schema of the older version of picodata. For details see
  https://git.picodata.io/core/picodata/-/issues/1946#note_157146

- Preserve parameter types provided by client.
  Previously, we'd completely replace parameter types array with the one
  derived by the Sbroad's type system. As a result, sometimes the type
  could implicitly change from VARCHAR to TEXT, causing various problems
  to client drivers.

- Fixed a bug when parsing RFC 3339 timestamps with a date-time delimiter other than `T`
  resulted in an error.

- Fixes unnecessary vshard router and storage config updates.

- Fixed a bug which could sometimes lead to unconstrained stream of requests
  from a failed instance (implemented expontential backoff for sentinel requests).

- Fixed a bug where cluster-wide settings for options `sql_vdbe_opcode_max` and `sql_motion_row_max`
  were not applied to queries made through pgproto.

- Fixed a panic in sentinel loop because of integer overflow when multiplying.

- Fixed a bug where picodata would sometimes indefinitely block when applying
  DDL operations which could result in full replicaset failure.

- Fixed a bug where picodata would sometimes indefinitely block after a
  restart and/or network failure when receiving a raft snapshot.

- Fixed a bug when a non-voting instance would sometimes be chosen as
  a bootstrap leader which would always lead to failure.

- Fixed a bug when pgproto expected users to pass parameters from the
  procedure body in CREATE PROCEDURE queries.

- Fixed a bug when pgproto failed to execute CALL queries with the following error:
  `sbroad: invalid node: node is not Relational type: Block(Procedure(Procedure { name: "proc1", values: [NodeId { offset: 0, arena_type: Arena32 }] }))`.

- Fixed a bug when config inheritance during plugin upgrades
  lead to loss of new config keys. Config inheritance was reworked and now we:
  - Inherit keys that present in both configs
  - Keep new keys untouched
  - Preserve missing keys and plugin-level config

- Fixed a bug where setting `pg_statement_max` or `pg_portal_max` to `0` could cause queries to
  fail with access error to a system table.

### Lua API

- Remove `pico.exit` function as it is no longer used.

### Plugin API

- Allowed to use different patch level versions of SDK (ex. allow to run plugin built with 25.3.1 to run on Picodata 25.3.2)
- Added `cluster_uuid()` API and corresponding `pico_ffi_cluster_uuid`
  FFI binding to retrieve the cluster UUID of the current instance.

## [25.2.3] - 2025-07-07

### Fixes

- Fixed a bug which broke intra-replicaset replication when upgrading from 25.1.* to 25.2.*.
  Note that clusters broken by this bug require some manual actions in addition
  to this fix, but newer version should upgrade fine. Note also that this fix
  does not include the actual schema upgrade procedure, so the cluster will
  still have a schema of the older version of picodata. For details see
  https://git.picodata.io/core/picodata/-/issues/1946#note_157146

- Fixed incorrect validation check for max number of users.

- Preserve parameter types provided by client.
  Previously, we'd completely replace parameter types array with the one
  derived by the Sbroad's type system. As a result, sometimes the type
  could implicitly change from VARCHAR to TEXT, causing various problems
  to client drivers.

- Fixed a bug which could sometimes lead to unconstrained stream of requests
  from a failed instance (implemented expontential backoff for sentinel requests).

- Fixes unnecessary vshard router and storage config updates.

- Fixed a bug when parsing RFC 3339 timestamps with a date-time delimiter other than `T`
  resulted in an error.

## [25.2.2] - 2025-06-25

### Features

- Increase max number of users to 128
- Support LDAP with TLS (StartTLS)

### Fixes

- Fixed a performance issue with global table DML which resulted in an up to 95x performance increase!
- Used to fail to start when raft election was in progress
- row_number marked as non-deterministic function
- Re-enable basic auth for credentials in URL

### Plugin API

- The `authentication` function has been introduced, which determines the authentication method automatically.

## [25.2.1] - 2025-05-26

### CLI
- `picodata admin` in \lua mode no longer requires a delimiter for executing commands.
- allow only the full format for address command-line arguments
  - allow `HOST:PORT`,
  - disallow `HOST` or `:PORT`,
  - this is technically a breaking change.
- provide a more detailed message for address binding errors
- set permissions of admin socket file to 0660 by default.
- `picodata expel` no longer requires the `--cluster-name` parameter; it is now marked as deprecated and will be removed in the future major release (version 26).

### Pgproto

- Do not allow connections without ssl when ssl is enabled on the server.
  - In previous versions `pg.ssl = true` meant "allow clients to connect either plaintext or via SSL",
  - Now it more strict: "only allow clients to connect via SSL".
  - This is technically a breaking change.

- The WAIT APPLIED GLOBALLY option now waits for all instances rather than just replicaset masters.

### Plugin API

- Plugin RPC requests will now be executed locally whenever possible. Previously
  the behavior was inverted - we preferred remote execution, which was counter
  productive.

- Plugin RPC context has new named field "call_was_local" which is set to `true`
  when the call is made locally (without network access). Note that in the
  opposite case "call_was_local" may be unset, so the absence of this fields
  should be interpreted as a non local call.

### ACL

- For all users with role `public` the privileges to `read` system tables `_pico_instance` and `_pico_peer_address` are now granted.

- Forbid the granting of DROP privileges on system tables.

### Type system

- Improved type checking now catches more errors earlier with clearer error messages.
  Valid queries should note no difference.

  Example with `SELECT 1 = false`:

  Old behavior:
  ```sql
  picodata> select 1 = false;
  ---
  - null
  - 'sbroad: failed to create tarantool: Tarantool(BoxError { code: 171, message: Some("Type
    mismatch: can not convert boolean(FALSE) to number"), error_type: Some("ClientError"),
    errno: None, file: Some("./src/box/sql/mem.c"), line: Some(2784), fields: {}, cause:
    None })'
  ...
  ```

  New behavior:
  ```sql
  picodata> select 1 = false;
  ---
  - null
  - 'sbroad: could not resolve operator overload for =(usigned, bool)'
  ...
  ```

  Some queries with parameters or subqueries that previously worked might
  now require explicit type casts. For instance, `select 1 + (select $1)`
  now leads to "could not resolve operator overload for +(unsigned, unknown)"
  error, that can be fixed with explicit type cast: `select 1 + (select $1)::int`

- Sbroad now infers parameter types from query context, allowing to
  prepare statements in pgproto without explicit type specification,
  which is a quite common case.

  For iproto nothing actually changes, because parameter types are
  inferred from the actual parameter values.

  For example, query `SELECT * FROM t WHERE a = $1` used to fail in
  pgproto with "could not determine datatype for parameter $1" error,
  if parameter type wasn't specified by the client. Now the
  parameter type is inferred from the context to the type of column `a`.

  In addition, parameter types can be inferred from the column types
  when inserting values. For instance, in `INSERT INTO t (int_col) VALUES ($1)`
  or `UPDATE t SET int_col = $1` query, parameter type will be inferred to
  the type of the column (int).

  Note that there are 2 methods to fix inference errors:
   1) Explicitly provide parameter type on protocol level.
   2) Explicitly provide parameter type by using CAST.

  Limitations:

   - `SELECT $1`
   Works in PostgreSQL, fails in sbroad, because there is no context
   for type inference. Such queries require parameter type defaulting rules.

   - `SELECT 1 UNION SELECT $1`
   Parameter type could be inferred from the left select statement,
   like PostgreSQL does, but this is not implemented yet.

- SQL type system supports parameter type defaulting to text, allowing
  to handle parameterized queries with context for parameter types
  inference. For instance, `SELECT $1` used to result in
  "could not infer data type of parameter $1" error.
  Now the parameter type will be defaulted to text, making the query valid.

- SQL now can coerce string literals to more suitable type according to
  the context where they are used. For instance, string literal will be
  automatically coerced to a datetime value when it's being inserted in a
  column of type datetime. If a string doesn't represent a valid value of
  the inferred type, the result will be a parsing error.

  Query examples:
   - `SELECT 1 + '1'` is the same as `SELECT 1 + 1`
   - `INSERT INTO t (datetime_col) VALUES ('2023-07-07T12:34:56Z')` is the
     same as `INSERT INTO t (datetime_col) VALUES ('2023-07-07T12:34:56Z'::datetime)`

### Observability
- Added picodata metrics in prometheus format to the `/metrics` endpoint. Metrics allow to monitor SQL, RPC, CAS, Raft, instance and governor states. List of metrics:
  - pico_governor_changes_total
  - pico_sql_query_total
  - pico_sql_query_errors_total
  - pico_sql_query_duration
  - pico_rpc_request_total
  - pico_rpc_request_errors_total
  - pico_rpc_request_duration
  - pico_cas_records_total
  - pico_cas_errors_total
  - pico_cas_ops_duration
  - pico_instance_state
  - pico_raft_applied_index
  - pico_raft_commit_index
  - pico_raft_term
  - pico_raft_state
  - pico_raft_leader_id

### Fixes
- Display correct value for "can_vote" property in webUI

- Disallow DDL creation operations in a heterogeneous cluster. A cluster is considered
  heterogeneous if any instance differs from another by major or minor version.

- Changed `picodata status` output format to more minimalistic and unix-stylished.

- Fixed panic when dropping system user pico_service.

- PostgreSQL protocol initialization now happens at the stage of instance becoming online,
  preventing possible problems in UX.

- User creation or altering in SQL with a password and LDAP authentication method has been forbidden.

- `.proc_before_online`'s PostgreSQL protocol initialization stage will be
  skipped if it was already initialized before, making this RPC idempotent.

- `--pg-listen` parameter is now checked at bootstrap (`postjoin`) stage, so
  it is not possible anymore to bind a busy port when instance is offline.

- Provide the client with a detailed error message when using LDAP for authentication and the LDAP server is unavailable.

- Fixed secondary indexes not working after raft log compaction.

- Fixed a bug where replication master switchover would sometimes fail.

- No longer dumps backtrace files by default. Logs error if dump failed.

- compare_and_swap in plugins now implicitly calls wait_index and validates term of the applied record
  to exclude situations when election caused different entry to become applied at expected index.
  With new behavior users of the API do not need to take that into account.

- Fixed an issue where an instance cannot join after we expelled another instance.

- `ALTER PLUGIN MIGRATE TO` command's timeout option is now handled more accurately.

- Fixed a bug where a timeout during `ALTER PLUGIN MIGRATE` command would make it
  impossible to run migrations on the current instance.

- Fixed a bug where _pico_service_route was incorrectly loaded into the topology cache.


### RPC API

- `.proc_before_online` is a successor to `.proc_enable_all_plugins` due to added
  step of initialization of a PostgreSQL protocol at it's call: see "deprecation".

### Deprecation

- `.proc_enable_all_plugins` will be deprecated in the next major release.
- `cluster_name` parameter from `.proc_expel` RPC call will be deprecated in the next major release.

### SQL
- SQL supports `TRUNCATE` operation.
- SQL supports `ALTER TABLE t ADD COLUMN` operation.
- SQL supports `ALTER TABLE old_table_name RENAME TO new_table_name` operation.
- SQL supports `ALTER TABLE t RENAME COLUMN old_column_name TO new_column_name` operation.
- SQL supports volatile scalar functions: `instance_uuid`.

### Configuration

- Minimal supported Rust version has been bumped from `1.76.0` to `1.85`.

- Allow to configure `boot_timeout` parameter per-instance in config file (7200 sec by default) for auto-shutdown.

## [25.1.2] - 2025-05-20

### Fixes

- vshard is bumped to 0.1.30. This fixes a bug lead to out of lua memory

- remove requirement for a delimiter in console when in lua mode

- `picodata status` output is tuned for better readability

- Fixed a bug where _pico_service_route was incorrectly loaded into the topology cache.

## [25.1.1] - 2025-02-21

### Configuration

- New alter system parameters - `sql_vdbe_opcode_max` and `sql_motion_row_max`.

- Default config name is `picodata.yaml`.

- The default names for replicaset and instance are now generated using the following patterns:
  - ReplicaSet Name: `{tier_name}_{replicaset_number_in_this_tier}`
  - Instance Name: `{tier_name}_{replicaset_number_in_tier}_{instance_number_in_replicaset}`

- New parameters for Vinyl configuration: `bloom_fpr`, `max_tuple_size`, `page_size`, `range_size`,
  `run_count_per_size`, `run_size_ratio`, `read_threads`, `write_threads` and `timeout`.

- New parameter for Memtx configuration: `max_tuple_size`.

- `plugin_dir` parameter is renamed to `share_dir` (--plugin-dir -> --share-dir,
  PICODATA_PLUGIN_DIR -> PICODATA_SHARE_DIR, config: instance.plugin_dir ->
  instance.share_dir)

- The following parameters has been moved from configuration file to `_pico_db_config` system table:
  - checkpoint_interval
  - checkpoint_count
  - max_concurrent_messages

- `max_heartbeat_period` removed from `_pico_db_config`

- CLI parameter `service_password_file` removed from arguments. To set a password for connecting to the cluster, create a file named `.picodata-cookie` in the instance's directory and store the password there.

- Major renaming of parameters from _pico_db_config:
 - `max_concurrent_messages` renamed to `iproto_net_msg_max`
 - `password_min_length` renamed to `auth_password_length_min`
 - `password_enforce_uppercase` renamed to `auth_password_enforce_uppercase`
 - `password_enforce_lowercase` renamed to `auth_password_enforce_lowercase`
 - `password_enforce_digits` renamed to `auth_password_enforce_digits`
 - `password_enforce_specialchars` renamed to `auth_password_enforce_specialchars`
 - `max_login_attempts` renamed to `auth_login_attempt_max`
 - `auto_offline_timeout` renamed to `governor_auto_offline_timeout`
 - `max_pg_statements` renamed to `pg_statement_max`
 - `max_pg_portals` renamed to `pg_portal_max`
 - `snapshot_chunk_max_size` renamed to `raft_snapshot_chunk_size_max`
 - `snapshot_read_view_close_timeout` renamed to `raft_snapshot_read_view_close_timeout`
 - `cluster_wal_max_size` renamed to `raft_wal_size_max`
 - `cluster_wal_max_count` renamed to `raft_wal_count_max`

- `listen`, `advertise` parameters are renamed to `iproto_listen`, `iproto_advertise`

- Added scopes to all parameters from `_pico_db_config`. There are two scopesright now - `tier`
and `global`. Parameters with scope `tier`
can be different on different tiers.
For example `ALTER SYSTEM SET parameter_with_scope_tier FOR ALL TIERS` or
`ALTER SYSTEM SET parameter_with_scope_tier FOR TIER default`.
Parameters with scope `global` are the same on each instance.

- `instance.shredding` moved to `cluster` section and is now defined at bootstrap only.

- Added box `sql_cache_size` to _pico_db_config under `sql_storage_cache_size_max` alias.

- Added `sql_storage_cache_count_max` to _pico_db_config.

### CLI

- `picodata expel` takes instance uuid instead of instance name.

- `picodata expel` now doesn't allow expelling Online instances and replicaset
  masters by default.
  New `--force` flag can be used to forcefully expel an Online instance which
  will shutdown once it finds out it got Expelled.

- String cells are now output without double quotes during SELECT.

- `picodata connect` and `picodata admin` return a non-zero exit code for file inputs with errors.

- `picodata --version` now provides verbose output, including the build type (static or dynamic) and the build configuration (release or debug)

- New command `picodata status` which prints all current members of the cluster and their status.

### Pgproto

- Support LDAP authentication method

### Compatibility

- Added unique index on column `uuid` for `_pico_instance` table. IDs
of `_pico_instance_raft_id` and `_pico_instance_replicaset_name` now equals
to 2 and 3.

- New special command `\set delimiter enter` to change the default delimiter to EOL (End Of Line). Introduced a new inner prompt prefix to indicate when input is waiting for a delimiter. EOF is now treated as a delimiter when reading files.

- New field `_pico_property.system_catalog_version` representing version of a system catalog.
  It may not be changed at every release, so this is not autoincrementing value.

- From now on, when joining a cluster, an instance's version must be the same as the cluster's version or one minor version higher. For example, if the cluster's version is 25.1, only instances with versions 25.1 or 25.2 can join.
  - In the `_pico_property` table, there is a new field called `cluster_version`, which shows the global version of the cluster.
  - In the `_pico_instance` table, there is a new field called `picodata_version` that displays the version of the executable running on the instance.
  - The global `cluster_version` is updated by the governor only when every instance in the cluster has been upgraded to the new minor version.

- System table `_pico_peer_address` has a new column `conncetion_type` that indicates the connection type of the peer. It can be `iproto` or `pgproto`.

- Global rename
  - Config File Changes:
    - `data_dir` renamed to `instance_dir`

  - Source Code Changes:
    - `data_dir` renamed to `instance_dir`

  - Environment Variable Changes:
    - `PICODATA_DATA_DIR` renamed to `PICODATA_INSTANCE_DIR`

- PgProto is now enabled by default and listens at `127.0.0.1:4327`.

- Prevented non-admin users with the DROP TABLE privilege from dropping system tables.

- SQL query parameters renamed:
  - `vdbe_max_steps` to `sql_vdbe_opcode_max`
  - `vtable_max_rows` to `sql_motion_row_max`

- `SCALAR` and `NUMBER` data types are not supported anymore.

### RPC API

- `.proc_expel` and `.proc_expel_redirect` takes instance uuid instead of instance name.

### Fixes

- It's no longer possible to execute DML queries for tables that are not operable
- Fixed panic on user/role creation when max user number was exceeded

- `picodata expel` used to finish before the instance got finally expelled.
  Now it will block until the instance is completely expelled, or the timeout
  is exceeded.

- Fixed a bug, where we would allow to create more than 2 versions of the same
  plugin (and panic when compiled in debug mode).

- `DROP PLUGIN` now leaves the plugin's data in the database if `WITH DATA`
  wasn't specified. Previously we would return an error instead.

### SQL

- SQL support `SUBSTRING` function
- SQL support window functions

## [24.6.1] - 2024-10-28

### Configuration

- New feature `tier` - a group of instances with own replication factor.
  Tiers can span multiple failure domains and a single cluster can have
  multiple tiers. Going forward it will be possible to specify which
  tier a table belongs to.

- Default authentication method changed from `CHAP-SHA1` to `MD5` both for user creation and in connect CLI.
  This change affects new user creation and all system users (except the `pico_service` user), as a command-line interface of `picodata connect` and `picodata expel`. Also, default schema version at cluster boot is now `1`, not `0` as it was previously.
  Connection via `Pgproto` no longer requires additional manual step to change the authentication method. However if you use `iproto` the admin will have to manually change the authentication type.

- Support human numbers to configure memtx.memory, vinyl.memory and vinyl.cache parameters.
  Supported suffixes: K, M, G, T, 1K = 1024
  (e.g picodata run --memtx-memory 10G)

- Add `cluster_wal_max_size` and `cluster_wal_max_count` alter system parameters
  which control the new auto-compaction feature.
  Whenever the total size of tuples in raft log system space exceeds `cluster_wal_max_size`
  or the number of tuples exceeds `cluster_wal_max_count` the log will be automatically compacted.

### Plugins

- New ability to write custom plugins for picodata. Plugins are supposed to be written in Rust
  using our official SDK crate: `picodata-plugin`. Plugins are compiled to shared objects which
  are loaded directly into picodata process. Plugins have in-process access to various picodata API's.
  Plugins do not use special sandboxing mechanisms for maximum performance. Thus require special care
  during coding. Make sure you do not install plugins from untrusted sources.
- Plugins are able to use dedicated RPC subsystem for communication inside the cluster.
- Plugins are able to provide migrations written in SQL to define objects they need for operation.
- Plugins are cluster aware, they're deployed on entire cluster. It is possible to specify particular
  `tier` for plugins to run on.
- Plugins are managed with SQL API, i e `CREATE PLUGIN` and such. For details consult with SQL reference.

### CLI

- New `picodata connect` and `picodata expel` argument `--timeout` for specifying
  the timeout for address resolving operation.

- Replace the use of `localhost` with `127.0.0.1` in `picodata run --listen` default
  value and everywhere across documentation and examples to reduce ambiguity.

### RPC API

- New rpc entrypoint: `.proc_get_vshard_config` which returns the vshard configuration of tier.

- Unused types of IPROTO requests have been forbidden for the sake of integrity.
  Here is the list (all start with "IPROTO_" prefix): INSERT, REPLACE,
  UPDATE, DELETE, CALL_16, UPSERT, NOP, PREPARE, BEGIN, COMMIT, ROLLBACK.
  In future verisons, SELECT will also be forbidden.

- New rpc endpoint: `.proc_replication_sync` which waits until replication on
  the instance progresses until the provided vclock value.

- Parameters `traceable` and `id` have been removed from `.proc_sql_dipstach`.

### Compatibility

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots

- Order of columns in `_pico_service_route` table has changed.

- Global rename
  - Config File Changes:
    - `cluster_id` renamed to `name`
    - `instance_id` renamed to `name`
    - `replicaset_id` renamed to `replicaset_name`

  - Source Code Changes:
    - `cluster_id` renamed to `cluster_name`
    - `instance_id` renamed to `instance_name`
    - `instance.instance_uuid` renamed to `instance.uuid`
    - `replicaset.replicaset_uuid` renamed to `replicaset.uuid`
    - `replicaset.replicaset_name` renamed to `replicaset.name`
    - `replicaset_id` renamed to `replicaset_name`
    - corresponding tables' columns and indexes changed accordingly

  - Environment Variable Changes:
    - `PICODATA_CLUSTER_ID` renamed to `PICODATA_CLUSTER_NAME`
    - `PICODATA_INSTANCE_ID` renamed to `PICODATA_INSTANCE_NAME`
    - `PICODATA_REPLICASET_ID` renamed to `PICODATA_REPLICASET_NAME`

- Default delimiter in `picodata connect` and `picodata admin` cli sessions is now `;`

- Paramters of `.proc_sql_dipstach` RPC have been changed.

### Lua API

- Update `pico.LUA_API_VERSION`: `4.0.0` -> `5.0.0`
- The following functions removed in favor of SQL commands and RPC API
  stored procedures:
* `pico.create_table` -> [CREATE TABLE]
* `pico.drop_table` -> [DROP TABLE]
* `pico.raft_read_index` -> [.proc_read_index]
* `pico.raft_wait_index` -> [.proc_wait_index]

[CREATE TABLE]: https://docs.picodata.io/picodata/devel/reference/sql/create_table/
[DROP TABLE]: https://docs.picodata.io/picodata/devel/reference/sql/drop_table/
[.proc_read_index]: https://docs.picodata.io/picodata/devel/architecture/rpc_api/#proc_read_index
[.proc_wait_index]: https://docs.picodata.io/picodata/devel/architecture/rpc_api/#proc_wait_index

### SQL

- SQL supports `LIKE` operator
- SQL supports `ILIKE` operator
- SQL supports `lower` and `upper` string functions
- Execute option `sql_vdbe_max_steps` was renamed to
`vdbe_max_steps`
- SQL supports `SELECT` statements without scans: `select 1`
- `CREATE TABLE`, `CREATE INDEX`, `CREATE PROCEDURE`, `CREATE USER` and `CREATE ROLE` support `IF NOT EXISTS` option
- `DROP TABLE`, `DROP INDEX`, `DROP PROCEDURE`, `DROP USER` and `DROP ROLE` support `IF EXISTS` option
- `CREATE TABLE`, `CREATE INDEX`, `CREATE PROCEDURE`, `DROP TABLE`, `DROP INDEX` and `DROP PROCEDURE`
  support WAIT APPLIED (GLOBALLY | LOCALLY) options, allowing users to wait for operations to be
  committed across all replicasets or only on the current one
- EXPLAIN estimates query buckets
- SQL supports `COALESCE` function
- `CREATE USER` no need to specify `PASSWORD` for LDAP authentification
- SQL supports `STRING_AGG` as an alias for `GROUP_CONCAT` aggregate function
- SQL support `LOCALTIMESTAMP` function
### Pgproto
- "vdbe_max_steps" and "vtable_max_rows" options are supported in connection
  string. These options allow to override the defalt values of the
  corresponding execution options used in sql (VDBE_MAX_STEPS and VTABLE_MAX_ROWS).

  For example, the following connection string sets both options to 42:
  postgres://postgres:Passw0rd@localhost:5432?options=vtable_max_rows%3D42,vdbe_max_steps%3D42

### Fixes

- Fixed bucket rebalancing for sharded tables
- Fixed panic when applying snapshot with the same index

## [24.5.1] - 2024-09-04

### SQL

- SQL now infers sharding key from primary key, when
  the former is not specified in `create table` clause
- SQL normalizes unquoted identifiers to lowercase instead of
  uppercase
- SQL supports `LIMIT` clause
- SQL supports `SUBSTR` function
- SQL supports postgres [cast notation]: `expr::type`
- SQL internally uses new protocol for cacheable requests,
which improves perfomance

### Pgproto

- pgproto supports tab-completion for tables names in psql:
  ```sql
  postgres=> select * from _pico_<TAB>
  _pico_index             _pico_plugin            _pico_privilege         _pico_routine           _pico_table
  _pico_instance          _pico_plugin_config     _pico_property          _pico_service           _pico_tier
  _pico_peer_address      _pico_plugin_migration  _pico_replicaset        _pico_service_route     _pico_user
  ```

- pgproto supports explicit parameter type declarations in SQL via casting.
  This is helpful for drivers that do not specify parameters types, such as
  pq and pgx drivers for Go. In such drivers, users need to explicitly cast all
  query parameters.

  If the driver doesn't specify the type and the parameter isn't cast, the query
  will fail. For instance, running `SELECT * FROM t WHERE id = $1` in pgx will
  return "could not determine data type of parameter $1" error. To resolve this,
  users must specify the expected type of the parameter:
  `SELECT * FROM t WHERE id = $1::INT`.

- Mutual TLS authentication for Pgproto.

    1. Set `instance.pg.ssl` configuration parameter to `true`
    1. Put PEM-encoded `ca.crt` file into instance's data directory along with `server.crt` and `server.key`.

  As a result pgproto server will only accept connection if client has presented a certificate
  which was signed by `ca.crt` or it's derivatives.

  If `ca.crt` is absent in instance's data directory, then client certificates are not requested and not validated.

### Configuration

- Set up password for admin with `PICODATA_ADMIN_PASSWORD` environment variable

- Multiline input is available in `picodata admin` and `picodata connect`

- Set delimiter for multiline input with `\set delimiter my-shiny-delimiter`

- Ability to change cluster properties via SQL `ALTER SYSTEM` command

### Fixes

- Fix error "Read access to space '_raft_state' is denied"
  when executing a DML query on global tables

- Fix error "Maximum number of login attempts exceeded" in picodata admin

### Compatibility

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots

- New index for the system table `_pico_replicaset` - `_pico_replicaset_uuid`

- Changed `weight` column type to DOUBLE in `_pico_replicaset`

- Option `picodata run --peer` now defaults to `--advertise` value.
  The previous was `localhost:3301`. This leads to the important behavior change.
  Running `picodata run --listen :3302` without implicit `--peer` specified
  now bootstraps a new cluster. The old behavior was to join `:3301` by default

- DdlAbort raft log entry now contains the error information.

- Add `promotion_vclock` column to `_pico_replicaset` table.

- Add `current_config_version` column to `_pico_replicaset` table.

- Add `target_config_version` column to `_pico_replicaset` table.

- `Replicated` is no longer a valid instance state.

### RPC API

- Removed stored procedure `.proc_replication_promote`.

- New rpc entrypoint: `.proc_get_config` which returns the effective
  picodata configuration

### Lua API

- Update `pico.LUA_API_VERSION`: `3.1.0` -> `4.0.0`
- The following functions removed in favor of SQL commands and RPC API
  stored procedures:
  * `pico.change_password` -> [ALTER USER]
  * `pico.create_role` -> [CREATE ROLE]
  * `pico.create_user` -> [CREATE USER]
  * `pico.drop_role` -> [DROP ROLE]
  * `pico.drop_user` -> [DROP USER]
  * `pico.grant_privilege` -> [GRANT]
  * `pico.raft_get_index` -> [.proc_get_index]
  * `pico.revoke_privilege` -> [REVOKE]

[ALTER USER]: https://docs.picodata.io/picodata/devel/reference/sql/alter_user/
[CREATE ROLE]: https://docs.picodata.io/picodata/devel/reference/sql/create_role/
[CREATE USER]: https://docs.picodata.io/picodata/devel/reference/sql/create_user/
[DROP ROLE]: https://docs.picodata.io/picodata/devel/reference/sql/drop_role/
[DROP USER]: https://docs.picodata.io/picodata/devel/reference/sql/drop_user/
[GRANT]: https://docs.picodata.io/picodata/devel/reference/sql/grant/
[REVOKE]: https://docs.picodata.io/picodata/devel/reference/sql/revoke/
[.proc_get_index]: https://docs.picodata.io/picodata/devel/architecture/rpc_api/#proc_get_index
[cast notation]: https://docs.picodata.io/picodata/devel/reference/sql/cast/

--------------------------------------------------------------------------------
## [24.4.1] - 2024-06-21

### Pgproto

- Allow connecting to the cluster using PostgreSQL protocol, see
  [Tutorial — Connecting — Pgproto]:

  ```
  picodata run --pg-listen localhost:5432
  psql

  CREATE TABLE ...
  INSERT ...
  SELECT ...
  ```

- The feature is currently in beta. It does NOT automatically imply
  complete compatibility with PostgreSQL's extensive features, SQL
  syntax, etc

[Tutorial — Connecting — Pgproto]:
  https://docs.picodata.io/picodata/24.4/tutorial/connecting/#pgproto

### SQL

- New commands [CREATE INDEX] and [DROP INDEX]
- Support `SELECT ... ORDER BY`
- Support `SELECT ... UNION ... SELECT ... UNION`
- Support common table expressions (CTE)
- Support [CASE][sql_case] expression
- New function [TRIM][sql_trim]
- New functions `TO_CHAR`, `TO_DATE`
- Allow `PRIMARY KEY` next to column declaration
- Support `SET ...` and `SET TRANSACTION ...` but they are ignored
- Support inferring not null constraint on primary key columns
- Support `INSERT`, `UPDATE`, `DELETE` in global tables

[CREATE INDEX]: https://docs.picodata.io/picodata/24.4/reference/sql/create_index/
[DROP INDEX]: https://docs.picodata.io/picodata/24.4/reference/sql/drop_index/
[sql_case]: https://docs.picodata.io/picodata/24.4/reference/sql/case/
[sql_trim]: https://docs.picodata.io/picodata/24.4/reference/sql/trim/

### Configuration

- Provide a new way of configuring instances via config file in a yaml
  format, see [Reference — Configuration file]. It extends the variety
  of previously available methods — environment variables and
  command-line arguments

- New option `picodata run --config` provides a path to the config file

- New option `picodata run -c` overrides single parameter using the same
  naming

- New command `picodata config default` generates contents of the
  config file with default parameter values

- New RPC API `.proc_get_config` returns the effective configuration

[Reference — Configuration file]:
  https://docs.picodata.io/picodata/24.4/reference/config/

### Compatibility

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots

- System table `_pico_table` format changed, the field `distribution`
  now is a map, a new field `description` was added (_string_)

- System table `_pico_tier` format changed, a new field `can_vote` was
  added (_boolean_)

- Rename all indexes adding a prefix containing the table name, e.g.
  `name` -> `_pico_table_name`

- System table `_pico_index` format changed, now `parts` are stored by
  field name instead of an index, other fields were rearranged
  significantly, see [Architecture — System tables]

- Rename RPC APIs related to SQL: dispatch_query -> proc_sql_dispatch;
  execute -> proc_sql_execute

[Architecture — System tables]:
  https://docs.picodata.io/picodata/24.4/architecture/system_tables/

--------------------------------------------------------------------------------
## [24.3.3] - 2024-07-03

- Fix invalid socket path error at startup
- Fix insufficient privileges for sbroad's temp tables breaks legit sql queries
- Finalize static analysis patches

--------------------------------------------------------------------------------
## [24.3.2] - 2024-06-10

- New HTTP endpoint `/metrics` exposes instance metrics in prometheus format

--------------------------------------------------------------------------------
## [24.2.4] - 2024-06-03

- Fix invalid socket path error at startup
- Fix insufficient privileges for sbroad's temp tables breaks legit sql queries
- Fix panic in case of reference used in sql query

--------------------------------------------------------------------------------
## [24.2.3] - 2024-05-28

- Fix `picodata admin` 100\% CPU usage when server closes the socket
- Fix `picodata connect` error after granting a role to the user
- Fix `ALTER USER alice WITH NOLOGIN`

--------------------------------------------------------------------------------
## [24.2.2] - 2024-04-03

- Fix panic after `CREATE USER alice; DROP ROLE alice;`

- Fix SQL chain of joins without sub-queries
  `SELECT * FROM ... JOIN ... JOIN ...`

- Fix SQL grammar support for `table.*`

- Refine audit log [events][audit_events] list: remove
 'new_database_created', add 'create_local_db', 'drop_local_db',
 'connect_local_db', 'recover_local_db', 'integrity_violation'

- Revise [picodata expel][cli_expel] command-line arguments and
  [tutorial][tutorial_expel]

[audit_events]: https://docs.picodata.io/picodata/devel/reference/audit_events/
[cli_expel]: https://docs.picodata.io/picodata/24.2/reference/cli/#expel
[tutorial_expel]: https://docs.picodata.io/picodata/24.2/tutorial/deploy/#expel

--------------------------------------------------------------------------------
## [24.2.1] - 2024-03-20

### SQL

- Introduce stored procedures:

  ```sql
  CREATE PROCEDURE my_proc(int, text)
  LANGUAGE SQL
  AS $$
    INSERT INTO my_table VALUES($1, $2)
  $$;

  CALL my_proc(42, 'the answer');

  SELECT * FROM my_table;
  ```

- The following new queries are supported:

  ```
  CREATE PROCEDURE
  DROP PROCEDURE
  CALL PROCEDURE
  ALTER PROCEDURE ... RENAME TO
  GRANT ... ON PROCEDURE
  REVOKE ... ON PROCEDURE

  ALTER USER ... RENAME TO
  ```

### Security

- All inter-instance communications now occur under `pico_service`
  builtin user. The user is secured with a password in a file provided
  in `picodata run --service-password-file` command-line option

- New requirements on password complexity — enforce uppercase,
  lowercase, digits, special symbols

### Implementation details

- Make RPC API the main communication interface, see [Architecture — RPC
  API]. Lua API is deprecated and will be removed soon

### Compatibility

- System table `_pico_role` was deleted

- System table `_pico_user` format changed, a new field `type` was added
  (_string_, `"user" | "role"`)

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots

[Architecture — RPC API]:
  https://docs.picodata.io/picodata/devel/architecture/rpc_api/

--------------------------------------------------------------------------------
## [24.1.1] - 2024-02-09

- Slightly change calendar versioning semantics, it's `YY.MINOR` now
  instead of `YY.0M`.

### CLI

- New `picodata admin` command connects to an instance via unix socket
  under the admin account, see [Tutorial — Connecting — Admin console].

- New `picodata connect` implementation provides a console interface to
  the distributed SQL, see [Tutorial — Connecting — SQL console].

- New option `picodata run --admin-sock` replaces `--console-sock` which
  is removed. The default value is `<data_dir>/admin.sock`.

- New option `picodata run --shredding` enables secure removing of data
  files (snap, xlog).

- New option `picodata run --log` configures the diagnotic log.

- New option `picodata run --memtx-memory` controls the amount of memory
  allocated for the database engine.

[Tutorial — Connecting — Admin console]:
  https://docs.picodata.io/picodata/24.1/tutorial/connecting/#admin_console

[Tutorial — Connecting — SQL console]:
  https://docs.picodata.io/picodata/24.1/tutorial/connecting/#sql_console

### SQL

- Global tables now can be used in the following queries:

  ```
  SELECT
  SELECT ... EXCEPT
  SELECT ... UNION ALL
  SELECT ... WHERE ... IN (SELECT ...)
  SELECT ... JOIN
  SELECT ... GROUP BY
  ```

- `ALTER USER ... WITH LOGIN` can now unblock a user, who was blocked due to exceeding login attempts.

### Fixes

- Revoke excess privileges from `guest`
- Fix panic after `ALTER USER "alice" WITH NOLOGIN`
- Repair `picodata connect --auth-type=ldap`
- Picodata instances will no longer ingore raft entries which failed to apply.
  Instead now the raft loop will keep retrying the operation forever, so that
  admin has an opportunity to fix the error manually. Raft entries should never
  fail to apply, so if this happens please report a bug to us.

### Compatibility

- System table `_pico_replicaset` now has a different format: the field `master_name`
  is replaced with 2 fields `current_master_name` and `target_master_name`.

- All `.proc_*` stored procedures changed their return values. An extra top level
  array of 1 element is removed.

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots.

--------------------------------------------------------------------------------
## [23.12.1] - 2023-12-21

### Fixes

- Correct `picodata -V`
- Web UI appeared to be broken in 23.12.0
- And `picodata connect --unix` too

--------------------------------------------------------------------------------
## [23.12.0] - 2023-12-08

### Features

- Clusterwide SQL is available via `\set language sql` in the
  interactive console with the support of global and sharded tables,
  see [Reference — SQL Queries]. The basic features are:

  ```
  # DDL
  CREATE TABLE ... USING memtx DISTRIBUTED GLOBALLY
  CREATE TABLE ... USING memtx/vinyl DISTRIBUTED BY
  DROP TABLE

  # ACL
  CREATE USER
  CREATE ROLE
  ALTER USER
  DROP USER
  DROP ROLE
  GRANT ... TO
  REVOKE ... FROM

  # DQL
  SELECT
  SELECT ... JOIN
  SELECT ... LEFT JOIN
  SELECT ... WHERE
  SELECT ... WHERE ... IN
  SELECT ... UNION ALL
  SELECT ... GROUP BY

  # DML
  INSERT
  UPDATE
  DELETE

  # other
  EXPLAIN
  ```

- Implement request authorization based on access control lists (ACL),
  see [Tutorial — Access Control].

- Implement security audit log, see [Tutorial — Audit log].

- Implement automatic failover of replicaset leaders,
  see [Architecture — Topology management].

- Introduce Web UI. It includes cluster status panel and replicaset
  list. Current status of Web UI is still beta.

[Reference — SQL Queries]: https://docs.picodata.io/picodata/reference/sql_queries/
[Tutorial — Access Control]: https://docs.picodata.io/picodata/tutorial/access_control/
[Tutorial — Audit log]: https://docs.picodata.io/picodata/tutorial/audit_log/
[Architecture — Topology management]: https://docs.picodata.io/picodata/architecture/topology_management/

### CLI

- Interactive console is disabled by default. Enable it implicitly with
  `picodata run -i`.

- Allow connecting interactive console over a unix socket `picodata run --console-sock`.
  Use `picodata connect --unix` to connect. Unlike connecting to a `--listen` address,
  console communication occurs in plain text and always operates under the admin account.

- New option `--password-file` for `picodata connect' allows supplying
  password in a plain-text file.

- Allow specifying `picodata connect [user@][host][:port]` format. It
  overrides the `--user` option.

- Enable the audit log with `picodata run --audit`.

- Allow connecting interactive console over a unix socket `picodata run --console-sock`.
  Use `picodata connect --unix` to connect. Unlike connecting to a `--listen` address,
  console communication occurs in plain text and always operates under the admin account.

- Block a user after 4 failed login attempts.

### Lua API

- Changes in terminology - all appearances of `space` changed to `table`
- Update `pico.LUA_API_VERSION`: `1.0.0` -> `3.1.0`
- New semantics of `pico.create_table()`. It's idempotent now.
- `pico.create_table()` has new optional parameter: `engine`.
  Note: global spaces can only have memtx engine.
- `pico.whoami()` and `pico.instance_info()` returns new field `tier`
- Add `pico.sql()`
- Add `pico.drop_table()`
- Add `pico.create_user()`, `pico.drop_user()`
- Add `pico.create_role()`, `pico.drop_role()`
- Add `pico.grant_privilege()`, `pico.revoke_privilege()`
- Add `pico.raft_term()`
- Add `pico.change_password()`
- Add `pico.wait_ddl_finalize()`
- Make `pico.cas` follow access control rules
- `pico.cas` now verifies dml operations before applying them
- Change `pico.raft_log()` arguments
- Make `opts.timeout` optional in most functions

### Compatibility

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots.

--------------------------------------------------------------------------------
## [23.06.0] - 2023-06-16

### Features

- Expose the Public Lua API `pico.*`. It's supplemented with a
  built-in reference manual, see `pico.help()`.

- _Compare and Swap_ (CaS) is an algorithm that allows to combine Raft
  operations with a predicate checking. It makes read-write access to
  global spaces serializable. See `pico.help('cas')`.

- _Clusterwide schema_ now allows to create global and sharded spaces.
  The content of global spaces is replicated to every instance in the
  cluster. In sharded spaces the chuncs of data (buckets) are
  distributed across different replicasets. See
  `pico.help('create_space')`.

- _Raft log compaction_ allows stripping old raft log entries in order
  to prevent its infinite growth. See `pico.help('raft_compact_log')`.

- Remove everything related to "migrations". They're superseeded with
  "clusterwide schema" mentioned above.

### CLI

- `picodata run --script` command-line argument sets a path to a Lua
  script executed at startup.

- `picodata run --http-listen` command-line argument sets the [HTTP
  server](https://github.com/tarantool/http) listening address. If no
  value is provided, the server won't be initialized.

- `picodata connect` CLI command provides interactive Lua console.

### Implementation details

- Picodata automatically demotes Raft voters that go offline and
  promotes a replacement. See docs/topology.md for more details.
  Replicaset leadership is switched too.

### Compatibility

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots.

--------------------------------------------------------------------------------
## [22.11.0] - 2022-11-22

### Features

- Brand new algorithm of cluster management based on the _"governor"_
  concept — a centralized actor that maintains cluster topology and
  performs instances configuration.

- Instance states are now called _"grades"_. This new term more clearly
  denotes how an instance is currently perceived by other instances (eg.
  how they are configured in its regard) rather than what it assumes
  about itself.

- Built-in _sharding_ configuration based on the `vshard` library. Once
  a replicaset is up to the given replication factor, Picodata will
  automatically re-balance data across replicasets.

- Clusterwide schema and data _migrations_ are introduced.

- Instances can now be _expelled_ in order to shrink the cluster.

### Compatibility

- The current version is NOT compatible with `22.07.0`. It cannot be
  started with the old snapshots.

--------------------------------------------------------------------------------
## [22.07.0] - 2022-07-08

### Basic functionality

- Command line interface for cluster deployment.
- Dynamic topology configuration, cluster scaling by launching more instances.
- Support for setting the replication factor for the whole cluster.
- Failure domains-aware replicasets composition.
- Two kinds of storages:
  - based on Raft consensus algorithm (clusterwide),
  - based on Tarantool master-master async replication.
- Graceful instance shutdown.
- Automatic Raft group management (voters provision, Raft leader failover).
- Dead instance rebootstrap without data loss.
- Automatic peers discovery during initial cluster configuration.

<!--
The template is not strict. You can use any ADR structure that feels best for a particular case.
See these ADR examples for inspiration:
- [Cassandra SEP - Ganeral Purpose Transactions](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-15%3A+General+Purpose+Transactions)
- [Rust RFC - Lifetime Ellision](https://github.com/rust-lang/rfcs/blob/master/text/0141-lifetime-elision.md)
- [TiKV - Use Joint Consensus](https://github.com/tikv/rfcs/blob/master/text/0054-joint-consensus.md)
-->
status: approved

decision-makers: @kostja, @funbringer, @d.rodionov, @kdy, @vifley

reviewers: @a.badulin, @gmoshkin

informed: @kbezuglyi

<!--
consulted: list everyone whose opinions are sought (typically subject-matter experts); and with whom there is a two-way communication
informed: list everyone who is kept up-to-date on progress; and with whom there is a one-way communication
-->

--------------------------------

# Health Check API

## Context and Problem Statement

<!-- Describe the context and problem statement, e.g., in free form using two to three sentences or in the form of an illustrative story.
You may want to articulate the problem in form of a question and add links to gitlab issues. -->

We would like to implement a health check HTTP/REST API that would be used by orchestration and monitoring tools as well as integrated into the WebUI and provide comprehensive information about a Picodata instance and cluster.

Initially, this has been asked for in the context of K8s liveness & readiness probes support [#211](https://git.picodata.io/core/picodata/-/issues/211), but with multiple comments requesting more and more information to be provided, the scope has grown beyond the original use case.

The document below covers both K8s-specific probes (liveness, readiness & startup) that are all binary in their nature, and a health status endpoint that returns detailed information about instance's health.

It does not cover cluster-wide health checks as those are more complex and heavyweight and therefore mandate a separate design and implementation track on top of the current one.

This document lists the requirements, provides an overview of existing solutions in distributed databases and proposes ways to implement the health check API in Picodata.

### Health check API in distributed databases
<details open>
  <summary>Prior state of the art</summary>

  Many popular distributed DBs do not expose HTTP endpoints for liveness/readiness checks or status info; rather, they use their own tools (like `nodetool` for Scylla/Cassandra, `redis-cli` for Redis) or expect a command to be run over its native protocol (`db.runCommand({ping: 1})` for MongoDB). 

  For some of those databases that do expose HTTP/REST API for liveness/readiness and health checks, below is a quick summary.

  #### CockroachDB
  **CockroachDB** exposes a `/health` [endpoint](https://www.cockroachlabs.com/docs/stable/monitoring-and-alerting#health-endpoints) that can be queried in two ways.

  Requesting `/health` checks basic liveness and returns `200` if the node is up and running.

  Querying `/health?ready=1` checks readiness and returns `200` if the node is ready, `503` if not. For `503`, error details are sent in a JSON message.

  Readiness implies that the node has completed initialization, can serve SQL queries and is not draining or decommissioning.
  
  #### TiDB
  **TiDB Server** has a `/status` [endpoint](https://docs.pingcap.com/tidb/stable/tidb-monitoring-api/#use-the-status-interface) that returns 200 with JSON info in the body.

  The info contains the current version and the number of currently served client connections.

  #### etcd
  **etcd** provides [three endpoints](https://kubernetes.io/docs/reference/using-api/health-checks/): `/health`, `/livez` and `/readyz`

  `/health` is a legacy endpoint that has been superseded by the other two.

  `/livez` returns `200` if the service is functioning normally, `500` if the service is non-recoverable.

  `/readyz` returns `200` if the node is ready to accept traffic. This implies that Raft is healthy and has a leader.

  #### ThingsDB
  **ThingsDB** provides [three endpoints](https://book.thingsdb.io/) that are used to check node liveness/readiness for upgrade purposes:

  `/ready` - responds with `200` if the node is ready for an upgrade, `503` if not.

  `/status` - returns the current node status as a string (`READY`, `AWAY`, `SYNCHRONIZING`, etc)

  `/health` - always responds with `200`, indicating that the node is up and alive (liveness)

  #### ElasticSearch
  **ElasticSearch** provides a `/_cluster/health/{index}` [endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-health) to retrieve the health status of the entire cluster or specific data streams and indices in it.

  It returns info in JSON format.

  Notably, it includes the `status` field which can be `green`/`yellow`/`red`, indicating various levels of health in the cluster:
  > A green status is better than yellow and yellow is better than red.
  > `green`: All shards are assigned.
  > `yellow`: All primary shards are assigned, but one or more replica shards are unassigned. If a node in the cluster fails, some data could be unavailable until that node is repaired.
  > `red`: One or more primary shards are unassigned, so some data is unavailable. This can occur briefly during cluster startup as primary shards are assigned.

  #### ArangoDB
  **ArangoDB** has a `/_admin/metrics/v2` [endpoint](https://docs.arango.ai/arangodb/3.10/develop/http-api/monitoring/metrics/) for the instance's metrics in Prometheus format which can be used to track health-related information.

  For cluster health, a dedicated `/_admin/cluster/health` [endpoint](https://docs.arango.ai/arangodb/3.10/develop/http-api/cluster/#get-the-cluster-health) that returns data about the state of the cluster in JSON.

</details>

### Requirements

The following requirements are based on the [original issue #211](https://git.picodata.io/core/picodata/-/issues/211) description and comments as well as comparison with existing solutions across other databases.

Requirements 1-3 are mainly driven by Kubernetes integration, 4 is for general monitoring.

1. **Startup probe.**

Used by the orchestrator to find out when the instance has started and completed initialization.
This probe is needed to let K8s wait for instances that take long to initialize and not accidentally consider them broken or dead and attempt to restart them.

2. **Liveness probe.**

Can be used to check if the system is alive, i.e., accepts traffic and is responsive (not stuck/crashed).
If this check fails, the instance should be restarted or even expelled if damaged beyond possible recovery.

3. **Readiness probe.**

Used to check if the instance is ready to accept user traffic.

For this, the following conditions are required to be met:
   - The instance must be in the `Online` state
   - The Raft leader must be known

An instance that is not ready can be (temporarily) excluded by the orchestrator from the set of instances receiving user traffic until it recovers.

4. **Detailed instance health status.**

Returns comprehensive information about the current status of various subsystems of the instance which can be used both for automated monitoring, reporting and analysis and to show the info in WebUI.

Most of the information checked for readiness conditions and some other is included in the response:
   - Instance info: name, UUID, version, tier, replicaset, if the instance is a replicaset master, current state, target state, target state reason and change time
   - Raft info: id, state, leader id, term, applied/commited/compacted/persisted indices
   - Limbo owner
   - Upgrade info: is in progress or not, pending version if any
   - Resharding info: number of active, sending, receiving, garbage, pinned, and total buckets.

## Considered Options

### Proposal: Separate HTTP Endpoint for Each Scenario

The proposed design is to have a dedicated endpoint for each of the listed requirements, all scoped under `/api/v1/health`:

|        Endpoint        |               Purpose               | HTTP codes  |   Auth   | 
| :--------------------: | :----------------------------------:| :---------: | :------: |
| `/api/v1/health/startup` | Startup: has instance started yet?  |  `200`/`503`    | Optional |
| `/api/v1/health/live`    | Liveness: is instance alive?        |  always `200` | Optional |
| `/api/v1/health/ready`   | Readiness: can serve user traffic?  |  `200`/`503`    | Optional |
| `/api/v1/health/status`  | Detailed health related information |  always `200` | Required |

1. Startup endpoint `/api/v1/health/startup`

This endpoint returns `200` when the instance has completed boot-up and initialization, and `503` if not yet completed.

The following conditions must be met for the startup endpoint to return `200`:
 - current state is `Online`
 - Raft leader is known
 - Replicaset the instance belongs to is in the `ready` state meaning that the number of replicas is not less than the replication factor.

|           Check           |                  Rationale             |          Failure Message      | 
| :-----------------------: | :-------------------------------------:| :---------------------------: |
| `current_state == Online`   | Instance is in Online state            | "instance state is Offline"   |
| `leader_id != None`         | Instance does not know Raft leader     | "no Raft leader known"        |
| `replicaset_state == ready` | count(replicas) >= replication factor  | "replicaset state is not-ready" |

We can include the following JSON response for the `200` reply:

```json
{"status": "ok"}

```

For `503` code, a short human-readable message indicating the failed startup check wrapped into JSON is included.

```json
{
  "status": "not_ready",
  "reason": "instance state is Offline"
}
```

Access to this endpoint should normally not require authentication, however, we should optionally support it to satisfy customers that have stringent security requirements.

2. Liveness endpoint `/api/v1/health/live`

This is a very lightweight endpoint that always replies with an empty `200 OK` response.

This verifies that the Tarantool event loop is running and the embedded HTTP server is up.

The endpoint integrates naturally with the K8s liveness probe.

Access to this endpoint should normally not require authentication, however, we should optionally support it to satisfy customers that have stringent security requirements.

Note that according to K8s specification the orchestrator only starts to query this endpoint after the startup probe succeeds.

3. Readiness endpoint `/api/v1/health/ready`

This endpoint performs all critical instance health checks and summarizes them in the status code in the response.

The following conditions must be met for the startup endpoint to return `200`:
 - current state is `Online`
 - Raft leader is known

**NB:** even though we consider the instance ready when it knows who is the Raft leader, in fact, it is possible today that the leader is known but the majority is lost. When leader step-down logic is productized, this check should become more robust - see [!2475](https://git.picodata.io/core/picodata/-/merge_requests/2475) for more details.
 

|           Check          |                  Rationale               |          Failure Message      | 
| :----------------------: | :---------------------------------------:| :---------------------------: |
| `current_state != Online`  | Instance is in Online state | "instance state is Offline"   |
| `leader_id != None`         | Instance does not know Raft leader     | "no Raft leader known"        |


The `200` reply code means that the instance is healthy and functional.
We can include the following JSON response:

```json
{"status": "ok"}

```

For `503` code, a short human-readable message indicating the failed health check wrapped into JSON is included.

```json
{
  "status": "not_ready",
  "reason": "instance is expelled"
}
```

The endpoint should be used with the K8s readiness probe. When the instance is not ready, the orchestrator stops routing any user traffic to it until it becomes ready again. Note that intra-POD and POD-to-POD traffic is still allowed which should let the governor bring the instance back to the Online state.

Although this endpoint should normally not require authentication, we may want to optionally support it because the checks performed by the background, albeit reasonably lightweight (table lookups, Lua evals), still open a path for creating extra load on the server (think DoS). Besides, some customers may have very stringent security requirements.

Note that according to K8s specification the orchestrator only starts to query this endpoint after the startup probe succeeds.

4. Health status endpoint `/api/v1/health/status`

This endpoint provides comprehensive health information for the WebUI dashboard and monitoring tools.  

It always returns `200` code.

In addition to various instance-related details, it includes a `status` field that provides a subjective yet intuitive summary as follows:
 - `healthy` means the instance is functional and does not show signs of service deterioration, i.e., operating normally
 - `degraded` means the instance has certain ongoing issues that affect its functionality, performance or capabilities, but remains at least partially operational
 - `unhealty` means the instance is currently non-functional: down, in Offline state, being expelled, etc.


The `status` field draws inspiration from ElasticSearch's "traffic light" statuses as well as various other use cases: [ASP.NET Core](https://learn.microsoft.com/en-us/aspnet/core/host-and-deploy/health-checks?view=aspnetcore-10.0#basic-health-probe), [Azure](https://docs.azure.cn/en-us/service-health/resource-health-overview#health-status), [Envoy](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/health_checking) and others.

This also aligns with the proposal for `picodata doctor` utility in [#1789](https://git.picodata.io/core/picodata/-/issues/1789#note_151431).

For `degraded` and `unhealty` statuses, a human-readable `reason` field is added.

The following conditions can result in the `degraded` status:
|              Check          |                  Rationale               |          Failure Message      | 
| :-------------------------: | :---------------------------------------:| :---------------------------: |
|  `limbo.owner == 0`         | Tarantool sync replication is turned off | "limbo owner is X"            |
|  No ongoing resharding    | Known bug when resharding [#1971](https://git.picodata.io/core/picodata/-/issues/1971) | "currently resharding"        |

Note that certain conditions for declaring an instance degraded can be removed in later releases as we improve stability and fix bugs - see the "No ongoing resharding" check caused by [#1971](https://git.picodata.io/core/picodata/-/issues/1971) as an example.

For the `unhealthy` status, the `reason` should be the same as in the readiness probe API (`api/v1/health/ready`).

Example response:

```json
{
    "limboOwner": 1,
    "status": "degraded",
    "reason": "limbo owner is 1",
    "timestamp": "2026-01-15T10:30:00Z",
    "uptimeSeconds": 86400,
    "uuid": "451b5e9a-d91c-4fc0-9180-fd6ef3b7b52a",
    "version": "26.1",
    "name": "i1",
    "raftId": 1,
    "currentState": "Online",
    "targetState": "Online",
    "targetStateReason": "wakeup",
    "targetStateChangeTime": "2026-01-12T11:20:00Z",
    "tier": "default",
    "replicaset": "r1",
    "isReplicasetMaster": true,
    "systemCatalogVersion": "25.5.3",
    "raft": {
        "state": "Leader",
        "term": 5,
        "leaderId": 1,
        "leaderName": "i1",
        "appliedIndex": 1234,
        "commitedIndex": 1237,
        "compactedIndex": 1220,
        "persistedIndex": 1235
    },
    "buckets": {
        "active": 20,
        "garbage": 0,
        "pinned": 2,
        "receiving": 1,
        "sending": 0,
        "total": 23
    },
    cluster: {
      "uuid": "781b5e9a-d91c-2fc0-9180-ac6ef3b7b52a",
      "version": "25.5.5"
    }
}
```

This endpoint requires authentication because it exposes a lot of information about the instance's state and internals which should not be available to unauthorized users.

## Decision Outcome

<!-- Chosen option: "title of option 1", because justification. e.g., only option, which meets k.o. criterion decision driver 
| which resolves … | … | comes out best (see below). -->

The plan is to go with the proposed design as described in the latest revision of this document.
Cluster-wide health checks are moved out of the scope of this project.

### Deliverables

Four endpoints should be implemented as a result of this project as listed in the proposal.

<!--
Describe what the deliverables are and how the implementation of/compliance with the ADR can/will be confirmed.
-->

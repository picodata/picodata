# Описание параметров запуска

```console
$ picodata run --help
picodata-run
Run the Picodata instance

USAGE:
    picodata run [OPTIONS]

OPTIONS:
        --advertise <[host][:port]>
            Address the other instances should use to connect to this instance. Defaults to
            `--listen` value [env: PICODATA_ADVERTISE=]

        --cluster-id <name>
            Name of the cluster. The instance will refuse to join a cluster with a different name
            [env: PICODATA_CLUSTER_ID=] [default: demo]

        --data-dir <path>
            Here the instance persists all of its data [env: PICODATA_DATA_DIR=] [default: .]

    -e, --tarantool-exec <expr>
            Execute tarantool (Lua) script

        --failure-domain <key=value>
            Comma-separated list describing physical location of the server. Each domain is a key-
            value pair. Until max replicaset count is reached, picodata will avoid putting two
            instances into the same replicaset if at least one key of their failure domains has the
            same value. Instead, new replicasets will be created. Replicasets will be populated with
            instances from different failure domains until the desired replication factor is reached
            [env: PICODATA_FAILURE_DOMAIN=]

    -h, --help
            Print help information

        --init-replication-factor <INIT_REPLICATION_FACTOR>
            Total number of replicas (copies of data) for each replicaset in the current group.
            It's only accounted upon the group creation (adding the first instance in the group),
            and ignored aftwerwards. [env: PICODATA_INIT_REPLICATION_FACTOR=] [default: 1]

        --instance-id <name>
            Name of the instance Empty value means that instance_id of a Follower will be generated
            on the Leader [env: PICODATA_INSTANCE_ID=] [default: ]

    -l, --listen <[host][:port]>
            Socket bind address [env: PICODATA_LISTEN=] [default: localhost:3301]

        --log-level <LOG_LEVEL>
            Log level [env: PICODATA_LOG_LEVEL=] [default: info] [possible values: fatal, system,
            error, crit, warn, info, verbose, debug]

        --peer <[host][:port]>
            Address(es) of other instance(s) [env: PICODATA_PEER=] [default: localhost:3301]

        --replicaset-id <name>
            Name of the replicaset [env: PICODATA_REPLICASET_ID=]
```

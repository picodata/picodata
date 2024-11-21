# SQL Broadcaster (sbroad)

Currently the library contains a query planner and an executor for the distributed SQL in a Tarantool `cartridge` cluster.

## Supported Tarantool versions
Sbroad is only supported by Picodata Tarantool currently.
You can download it [here](https://picodata.io/download)

## Getting started

First you need to have `rust`, `tarantool` and `cartridge-cli` CLI tools
installed and available in your environment, plus some other development
packages Sbroad depends on, including `openssl`, `libicu` and
`libreadline`

An example of the `sbroad` integration with Tarantool can be found in the `test_app` folder.
Check out the separate [README.md](test_app/README.md) for using Sbroad with the test Cartridge application.

### Install as lua module

```bash
tarantoolctl rocks --only-server https://download.picodata.io/luarocks/ install sbroad <version>
```
(use one of the version numbers in [Tags](https://git.picodata.io/picodata/picodata/sbroad/-/tags))

For a [Cartridge](https://github.com/tarantool/cartridge) application add the command above into `cartridge.pre-build` file and [sbroad roles](#cartridge-roles) into your role's dependencies (for example see [test_app](test_app/)) 

### Build from source

```bash
git clone https://gitlab.com/picodata/picodata/sbroad.git
cd sbroad
make test_all
```

## Cartridge roles

`cartridge.roles.sbroad-storage` role initializes functions that accept a local SQL query from the router for execution. Depends on the `vshard-storage` role.

`cartridge.roles.sbroad-router` role initializes functions that transform a distributed SQL into a sequence on local SQL queries and dispatch them to the storage. Depends on the `vshard-router` role.

## Architecture

The `sbroad` library consists of three main parts:

- SQL frontend
- planner
- executor

We try to keep the planner independent from other modules. For example, we can implement some other frontend and use the already implemented planner and executor.

More information about the ideas behind `sbroad` can be found in the [Sbroad internal design presentation](doc/design/sbroad.pdf). You can also find out more details in the built-in module documentation:
```
cargo doc --open
```

### SQL frontend

We use a custom [PEG](sbroad-core/src/frontend/sql/query.pest) for the `pest` parser generator to compile an SQL query and a `vshard` cluster schema into the planner's intermediate representation (IR).
The SQL query passes three main steps:

- the parse tree (PT) iterator, which is produced by the `pest` parser generated for our grammar.
- abstract syntax tree (AST). It is a wrapper over the PT nodes to transform them into a more IR-friendly tree. Also we have our own iterator to traverse the AST in the convenient order.
- planning intermediate representation (IR), which is a complete self-describing tree with an information derived from the SQL query and the table schema. It can be used by the planner for further transformations.

### Planner

The main goal of the planner is to insert the data motion nodes in the IR tree to instruct the executor where and what portions of data to transfer from one Tarantool instance to another within a cluster.

To make these motions efficient (i.e. transfer as less data amount as possible) we rely on the tuple distribution information. In case of a distribution conflict we decide to solve it by inserting a motion node. The better information about distribution we have in every tuple of the IR tree, the less data we move over the cluster. To save on data transfer the planner applies many IR transformations other than the motion derivation.

### Executor
The executor is located on the coordinator node in the `vshard` cluster. It collects all the intermediate results of the plan execution in memory and executes the IR plan tree in the bottom-up manner. It goes like this:

1. The executor collects all the motion nodes from the bottom layer. In theory all the motions in the same layer can be executed in parallel (this feature is yet to come).
1. For every motion the executor:
   - inspects the IR sub-tree and detects the buckets to execute the query for.
   - performs map-reduce for that IR subtree (we send it to the shards deduced from the buckets):
     - serialize IR subtree to send with message pack
     - deserialize it on the executor and build a valid SQL
     - execute SQL and return results
   - builds a virtual table with query results that correspond to the original motion.
1. Moves to the next motion layer in the IR tree.
1. For every motion the executor then:
   - links the virtual table results of the motion from the previous layer we depend on.
   - inspects the IR sub-tree and detects the buckets to execute the query.
   - performs map-reduce for that IR sub-tree
     - on the map stage we use virtual table to build a temporary space (and populate it with dispatched virtual table's data)
     - build and execute a valid SQL from IR sub-tree and this new temporary space
     - remove temporary space
   - builds a virtual table with query results that correspond to the original motion.
1. Repeats step 3 till we are done with motion layers.
1. Executes the final IR top subtree and returns the final result to the user.

The most complicated logic here can be found in the IR to SQL serialization (SQL backend) and bucket deduction (to execute the resulting SQL query on).

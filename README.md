# Picodata –  Distributed, PostgreSQL-compatible in-memory database

This repository contains the source code of Picodata, a distributed
in-memory database with plugins in Rust.

## What is Picodata

Picodata is a PostgreSQL-compatible distributed DBMS with plugins
in Rust. It is based on [shared-nothing architecture] and partitions the
entire data set across independent processes each running on an own CPU core.
Picodata is SQL- and wire-compatible with PostgreSQL.

Learn more about our software at the [picodata.io] web site.

[picodata.io]: https://picodata.io/picodata/
[shared-nothing architecture]: https://en.wikipedia.org/wiki/Shared-nothing_architecture

## Getting Picodata

We provide pre-built Picodata packages for Debian, Ubuntu, and
RHEL-based Linux distributions. Head over to [picodata.io/download] to
see what is available.

[picodata.io/download]: https://picodata.io/download/

## Running Picodata

Running a Picodata instance only takes one simple command:

```bash
picodata run
```

Getting a basic distributed cluster made of two instances running on
different hosts involves two commands, like this:

```bash
picodata run --listen 192.168.0.1:3301 --pg-listen 192.168.0.1:5432
picodata run --listen 192.168.0.2:3301 --pg-listen 192.168.0.2:5432 --peer 192.168.0.1:3301
```

Learn more about getting started with Picodata by heading over to our
documentation site at [docs.picodata.io].

[docs.picodata.io]: https://docs.picodata.io/picodata/stable/

## Building Picodata from source

Please refer to the [CONTRIBUTING.md](CONTRIBUTING.md) document for more
detailed prerequisites, compilation instructions as well as steps
required to run integration tests.

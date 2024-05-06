# Picodata – Professional Data Management System for High Loads

This repository contains the source code of Picodata, an in-memory
database with plugins in Rust.

## What is Picodata

Picodata is a software for building professional data management
systems. It provides an in-memory database together with a development
platform and a runtime for custom plugins written in Rust. Learn more
about our software at the [picodata.io] web site.

[picodata.io]: https://picodata.io/picodata/

## Getting Picodata

We provide pre-built Picodata packages for select Linux distributions
including CentOS and Ubuntu. Head over to
[picodata.io/download] to see what is available.

[picodata.io/download]: https://picodata.io/download/

## Running Picodata

Running a Picodata instance only takes one simple command:

```bash
picodata run
```

Getting a basic distributed cluster made of two instances running on
different hosts involves two commands, like this:

```bash
picodata run --listen 192.168.0.1:3301
picodata run --listen 192.168.0.2:3301 --peer 192.168.0.1:3301
```

You can find out more about getting started procedures and first steps
by heading to [docs.picodata.io].

[docs.picodata.io]: https://docs.picodata.io/picodata/stable/

## Building Picodata from source

Please refer to the [CONTRIBUTING.md](CONTRIBUTING.md) document for more
detailed prerequisites, compilation instructions as well as steps
required to run integration tests.

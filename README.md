# Picodata – Professional Data Management System for High Loads
This repository contains the source code of Picodata, a distributed application server with a built-in in-memory database.

## What is Picodata
Picodata is a software for building professional data management systems. It provides a data storage system together with a development platform and a runtime for persistent applications written in Rust. Learn more about our software at the [picodata.io](picodata.io){:target="_blank"} web site.

## Getting Picodata
We provide pre-built Picodata packages for select Linux distributions including CentOS and Ubuntu. Head over to our [downloads page](https://picodata.io/download/) to find out installation details. 

## Running Picodata
Running a Picodata instance only takes one simple command: `picodata run`. Getting a basic distributed cluster made of two instances running on different hosts involves two commands, like this:

```bash
picodata run --listen 192.168.0.1:3301
picodata run --listen 192.168.0.2:3301 --peer 192.168.0.1:3301 
```
You can find out more about getting started procedures and first steps by heading to our [documentation site](https://docs.picodata.io/picodata/install/#_2).

## Building Picodata from source
Our pre-built packages provide a statically linked `picodata` binary that have no extra dependencies other than a recent `glibc` library version. However, you may want or need to compile the software from the source. Please refer to the [CONTRIBUTING.md](CONTRIBUTING.md) document for compilation instructions and steps required to run integration tests.

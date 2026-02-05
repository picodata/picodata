<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="webui/src/assets/logo-dark.svg">
    <source media="(prefers-color-scheme: dark)" srcset="webui/src/assets/logo.svg">
    <img src="webui/src/assets/logo-dark.svg" alt="Picodata" width="600"/>
  </picture>
  <br/>
  <br/>
</div>

<div align="center">
<h2>Distributed, PostgreSQL-Compatible In-Memory Database</h2>
</div>

<div align="center">
  <h3>
    <a href="https://picodata.io/en">Website</a> |
    <a href="https://docs.picodata.io/picodata/stable/">Docs</a>
  </h3>

  <div>
    <img alt="GitHub Tag" src="https://img.shields.io/github/v/tag/picodata/picodata?label=release">
    <img alt="GitHub last commit" src="https://img.shields.io/github/last-commit/picodata/picodata">
    <img alt="GitHub commit activity" src="https://img.shields.io/github/commit-activity/m/picodata/picodata">
    <img alt="GitLab License" src="https://img.shields.io/gitlab/license/core%2Fpicodata?gitlab_url=https%3A%2F%2Fgit.picodata.io">
  </div>

  <div>
    <a href="https://t.me/picodataru">
      <img src="https://img.shields.io/badge/telegram-follow_us-e23956.svg?style=for-the-badge" alt="Telegram"/>
    </a>
    <a href="https://www.linkedin.com/company/picodata/">
      <img src="https://img.shields.io/badge/linkedin-connect_with_us-e23956.svg?style=for-the-badge" alt="LinkedIn"/>
    </a>
  </div>
  <br/>
  <br/>
</div>

> This repository contains the source code of Picodata, a distributed
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

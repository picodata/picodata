# Contributing to Picodata
This document describes contributing to Picodata, primarily the ways you can build and test it.

## Building Picodata from source

### Required build tools
- Rust and Cargo 1.65 or newer
- Cmake 3.16 or newer
- gcc, g++
- libstc++-static
- (*optional* to build with Web UI) node v15+, yarn

### Prerequisites for CentOS 8
Use the following commands to install the required build prerequisites. Note that you'll need recent Rust and Cargo versions installed using the recommended way from [rustup.rs](rustup.rs):
```bash
sudo dnf config-manager --set-enabled powertools
sudo dnf in -y gcc gcc-c++ make cmake git patch libstdc++-static

# Optional - to build with Web UI
sudo dnf module install nodejs:19
sudo corepack enable
```
### Prerequisites for Ubuntu 22.04 and 24.04
Use the following command to install the required build prerequisites. Note that Ubuntu 22.04 provides recent Rust and Cargo versions, so it's preferable to install it via `apt-get`:
```bash
sudo apt-get install build-essential git cmake autoconf libtool curl pkg-config -y

# Optional - to build with Web UI
curl -sL https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add -
echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list
sudo apt install yarn npm -y
sudo curl --compressed -o- -L https://yarnpkg.com/install.sh | bash
```

### Prerequisites for Alt Workstation p10
Use the following commands to install the required build prerequisites. Note that you'll need recent Rust and Cargo versions installed using the recommended way from [rustup.rs](rustup.rs):
```bash
su -
apt-get install gcc gcc-c++ cmake git patch libstdc++10-devel-static libgomp10-devel-static -y && exit
```

### Prerequisites for MacOs
Use the following commands to install the required build prerequisites.
Note that you'll need recent Rust and Cargo versions installed using the
recommended way from [rustup.rs](rustup.rs):
```shell
brew install git cmake make curl gcc msgpack protobuf

# Optional - to build with Web UI
brew install node yarn
```

### Prerequisites for Fedora 37+

#### Static build

```shell
dnf install cmake gcc gcc-c++ git libstdc++-static perl
```

#### Dynamic build

```shell
dnf install curl-devel libicu-devel libyaml-devel libzstd-devel openldap-devel openssl-devel readline-devel zlib-devel
```

#### Build with Web UI

```shell
dnf install nodejs yarnpkg
```

### Install Rust

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```

### Getting and building the source code
```bash
git clone https://git.picodata.io/picodata/picodata/picodata.git
cd picodata
git submodule update --init --recursive
```
Compile the project:
```bash
make build-dev
```

This will build the debug version. If you want the optimized build we ship to users:

```bash
make build-release-pkg
```

For other build options consult with our `Makefile`.

By default picodata is compiled with webui feature.

When running `picodata` `--http-listen` should be supplied to serve Web UI.

The resulting binaries should appear under the `target` subdirectory.

> NOTE:<br>
> Picodata supports both dynamic and static linking. Instruction above produces statically linked binary. When built with `dynamic_build` feature dynamic linking is used:
>
> ```bash
> cargo build --features dynamic_build
> ```
>
> Dynamic linking requires for additional dependencies to be installed on the system. For example see [Dynamic build](#dynamic-build). For full list see [Dockerfile](docker-build-base/Dockerfile).

## Integration testing with pytest
The following refers to Ubuntu 20.04 LTS. The mileage with other distributions may vary.

### Ubuntu

#### Installation


1. Install Python 3.10

     ```bash
   sudo add-apt-repository ppa:deadsnakes/ppa
   sudo apt install python3.10 python3.10-distutils
   curl -sSL https://bootstrap.pypa.io/get-pip.py -o get-pip.py
   python3.10 get-pip.py
   ```

3. Install poetry:
   See [instructions](https://python-poetry.org/docs/#installation).

4. Install dependencies

    ```
    poetry install
    ```


#### Adding dependencies

```bash
poetry install <dependency-package-name>
```

#### Running

```bash
python3.10 -m poetry run pytest
```

or

```bash
python3.10 -m poetry shell
# A new shell will be opened inside the poetry environment
pytest
```

#### Running specific test
```bash
poetry run pytest -k test_sql_acl
```

#### Running tests in parallel with pytest-xdist

```bash
poetry run pytest -n 20
```

#### Debugging tests

There are a few tricks that help in debugging python tests.

First one is to use NOLOG env var like that: `NOLOG=1 pytest ...`. `NOLOG` removes instance log output printed on test failure.
This is useful when you're debugging the test itself and instance logs just pollute the output.

Second tip is to use debugger, namely `ipdb`. You just need to place `import ipdb; ipdb.set_trace()` on the line and
run pytest with `-s` argument to avoid stdout interception. This will open a REPL when you run the test, which is a convenient
way to look around, run arbitrary code, etc. It is more productive compared to endlessly rerunning the test with various
print statements.

#### Running manual/test_scaling.py::test_cas_conflicts

This test is not ran by default, but can be used for benchmarking instance join time and cas conflicts.
The test plots a chart and therefore also requires `matplotlib` dependency to be avaliable.

Install it with:
```bash
poetry install matplotlib
```

But do not commit the changes to `pyproject.toml` and `poetry.lock` that this installation generates. As we
do not want to have `matplotlib` installed by default.
---

### macOS

Note: some details regarding python version tweaking might be outdated, feel free to submit a correction.

#### Installation

The installation sequence requires Python 3.10 or newer, preferably installed via Homebrew.

First check if the required version is available:
```shell
brew info python
```

If it is, go and install it:
```shell
brew install python@3.11
brew link python
python3 --version
```
Check if the default `python3` executable is available and provided by the Homebrew installation:

```shell
which python3
```
_Note_: Python versions <=3.9 such as the one provided by the older `Xcode Developer Tools` will not work.

If the `python3` executable is provided by Homebrew, skip the following part and jump to `poetry` installation.
Otherwise, that needs to be fixed. Let's find out the local Homebrew installation details:

```shell
brew config
```
The `HOMEBREW_PREFIX` variable should point to the directory where `brew`
installs packages. Let's create a symlink:

```shell
ln -s "$(brew config | sed -n "s/^HOMEBREW_PREFIX: //p" | tr -d "\n")/bin/python@3.10" /usr/local/bin/python3
```
_Note_: Make sure that `/usr/local/bin` is in your `PATH`.

Check `python3` location and version. Now it should be provided by Homebrew:

```shell
which python3
python3 --version
```

Make sure the `pip3` executable also points to the homebrew directory.
```shell
which pip3
```
If it doesn't, then similarly create a simlink for `pip3`:

```shell
ln -s "$(brew config | sed -n "s/^HOMEBREW_PREFIX: //p" | tr -d "\n")/bin/pip@3.10" /usr/local/bin/pip3
```
After that you can install poetry, see [instructions](https://python-poetry.org/docs/#installation):

#### Adding dependencies

```shell
poetry install --deploy
```

#### Running

```shell
poetry run pytest -n auto
```

## Benchmarks and flamegraphs

### Benchmarks

There is a simple benchmark based on `pytest` scenario. Quick run it with

```bash
make benchmark
```

The benchmark consists of single part: SQL query handling via `IPROTO_EXECUTE` vs `.proc_sql_dispatch`.
You will see resulting table with time delta.

### Flamegraphs

It is possible to make [flamegraphs](https://github.com/brendangregg/FlameGraph) while benchmarking a debug build.

- Install `perf`:
  ```bash
  apt install -y linux-tools-common linux-tools-generic linux-tools-`uname -r`
  ```

- Install FlameGraph script:
  ```bash
  cd .. && git clone https://github.com/brendangregg/FlameGraph.git ; cd -
  ```
  Your folder structure will look like this:
  ```
  ..
  picodata (you are here)
  FlameGraph
  (other folders and files)
  ```

- Set kernel options for `perf`:
  ```bash
  sudo echo 0 > /proc/sys/kernel/perf_event_paranoid
  ```
  ```bash
  sudo echo 0 > /proc/sys/kernel/kptr_restrict
  ```

- Now run
  ```bash
  make flamegraph
  ```

  Flamegraphs will be placed in `tmp/flamegraph` dir.

### SQL performance

To benchmark SQL, a custom K6 build ([xk6](https://github.com/grafana/xk6)) with
Tarantool protocol support is used.

- Install [golang](https://go.dev/doc/install) and [xk6](https://github.com/grafana/xk6).

- Run K6:
  ```bash
  make k6
  ```
  Performance summary can then be found in the `test/manual/sql/` directory.

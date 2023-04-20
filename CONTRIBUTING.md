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
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# Optional - to build with Web UI
sudo dnf module install nodejs:19
sudo corepack enable
```
### Prerequisites for Ubuntu 22.04
Use the following command to install the required build prerequisites. Note that Ubuntu 22.04 provides recent Rust and Cargo versions, so it's preferable to install it via `apt-get`:
```bash
sudo apt-get install build-essential cargo git cmake -y

# Optional - to build with Web UI
curl -fsSL https://deb.nodesource.com/setup_19.x | sudo -E bash - &&\
sudo apt-get install -y nodejs
sudo corepack enable
```

### Prerequisites for Alt Workstation p10
Use the following commands to install the required build prerequisites. Note that you'll need recent Rust and Cargo versions installed using the recommended way from [rustup.rs](rustup.rs):
```bash
su -
apt-get install gcc gcc-c++ cmake git patch libstdc++10-devel-static libgomp10-devel-static -y && exit
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```

### Prerequisites for MacOs
Use the following commands to install the required build prerequisites. 
Note that you'll need recent Rust and Cargo versions installed using the 
recommended way from [rustup.rs](rustup.rs):
```shell
brew install git cmake make curl gcc msgpack
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# Optional - to build with Web UI
brew install node yarn
```

### Getting and building the source code
```bash
git clone https://git.picodata.io/picodata/picodata/picodata.git
cd picodata
git submodule update --init --recursive
```
Compile the project:
```bash
cargo build
```

This will build the debug version. If you want the release version, try this instead:

```bash
cargo build --release
```

If you want to enable Web UI for administration, build with these flags:

```bash
cargo build --features webui
```
When running `picodata` `-http-listen` should be supplied to serve Web UI.

The resulting binaries should appear under the  `target` subdirectory.

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

3. Install pipenv:

    ```
    python3.10 -m pip install pipenv==2022.4.8
    ```

4. Install dependencies

    ```
    python3.10 -m pipenv install --deploy
    ```


#### Adding dependencies

```bash
python3.10 -m pipenv install <dependency-package-name>
```

#### Running

```bash
python3.10 -m pipenv run pytest
python3.10 -m pipenv run lint
```

or

```bash
python3.10 -m pipenv shell
# A new shell will be opened inside the pipenv environment
pytest
pipenv run lint
```

#### Running tests in parallel with pytest-xdist

```bash
python3.10 -m pipenv run pytest -n 20
```

---

### MacOs

#### Installation

The first step is to see which version is currently linked homebrew.
```shell
brew info python
```

If the python version is equal to `3.10`, go to install pipenv.
```shell
brew install python@3.10
brew link python
python3 --version
```

Since with a high probability you have already installed
`Xcode Developer Tools`, check which executable file you have called by
command:
```shell
which python3
```
If it is a python3 installed by homebrew then proceed to the installation 
pipenv. Also, if you not make sure the `/usr/local/bin` directory is included 
in the `PATH`, add it. After that, check homebrew environment:
```shell
brew config
```
Variable `HOMEBREW_PREFIX` will point to the directory in which brew 
installs packages. Depending on where the homebrew executables are located,
create a symlink.
```shell
ln -s "$(brew config | sed -n "s/^HOMEBREW_PREFIX: //p" | tr -d "\n")/bin/python@3.10" /usr/local/bin/python3
```

Check `python3` location and version. Now it should be python installed 
homebrew.
```shell
which python3
python3 --version
```

Make sure the `pip3` executable also points to the homebrew directory.
```shell
which pip3
```
If not using "right" (located in homebrew directory) an executable, then
repeat the steps similar to `python3` before proceeding with installing
`pipenv`.
```shell
ln -s "$(brew config | sed -n "s/^HOMEBREW_PREFIX: //p" | tr -d "\n")/bin/pip@3.10" /usr/local/bin/pip3
```
After that you can install pipenv:
```shell
pip3 install pipenv==2022.4.8
```
Set python path for pipenv:
```shell
pipenv --python /usr/local/bin/python3
 ```

#### Adding dependencies

```shell
pipenv install --deploy
```

#### Running

```shell
pipenv run pytest -n auto
pipenv run lint
```

## Benchmarks and flamegraphs

### Benchmarks

There is simple benchmark based on pytest scenario. Quick run it with

```bash
make benchmark
```

The benchmark consist of two parts: benchmark of tarantool space operations (replaces) and benchmark of picodata's raft writes to leader (NOPs).
You will see both results.

The benchmark designed for quick estimate of current application performance.

### Flamegraphs

It is possible to make [flamegraphs](https://github.com/brendangregg/FlameGraph) while benchmarking debug build.

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
  picodata (you here)
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

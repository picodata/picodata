# Contributing to Picodata
This document describes contributing to Picodata, primarily the ways you can build and test it.

## Building Picodata from source
### Required build tools
- Rust and Cargo 1.65 or newer
- Cmake 3.16 or newer
- gcc, g++
- libstc++-static

### Prerequisites for CentOS 8
Use the following commands to install the required build prerequisites. Note that you'll need recent Rust and Cargo versions installed using the recommended way from [rustup.rs](rustup.rs):
```bash
sudo dnf config-manager --set-enabled powertools
sudo dnf in -y gcc gcc-c++ make cmake git patch libstdc++-static
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```
### Prerequisites for Ubuntu 22.04
Use the following command to install the required build prerequisites. Note that Ubuntu 22.04 provides recent Rust and Cargo versions, so it's preferable to install it via `apt-get`:
```bash
sudo apt-get install build-essential cargo git cmake -y
```

### Prerequisites for Alt Workstation p10
Use the following commands to install the required build prerequisites. Note that you'll need recent Rust and Cargo versions installed using the recommended way from [rustup.rs](rustup.rs):
```bash
su -
apt-get install gcc gcc-c++ cmake git patch libstdc++10-devel-static libgomp10-devel-static -y && exit
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
cargo build
```

This will build the debug version. If you want the release version, try this instead:

```bash
cargo build --release
```
The resulting binaries should appear under the  `target` subdirectory.

## Integration testing with pytest
The following refers to Ubuntu 20.04 LTS. The mileage with other distributions may vary.

### Installation

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

### Adding dependencies

```bash
python3.10 -m pipenv install <dependency-package-name>
```

### Running

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

### Running tests in parallel with pytest-xdist

```bash
python3.10 -m pipenv run pytest -n 20
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

# pgproto

**TODO**: describe the project

## Building

To build the project, simply execute the following command:

```bash
cargo build
```

## Testing

Firstly, clone and build [picodata](https://git.picodata.io/picodata/picodata/picodata)
by following the instructions from the repo. Don't forget to build `pgproto` as well!

Then, install the test harness:

```bash
pipenv install
```

Then, export the needed environment variables:

```bash
# Required: expose both debug and release builds for Linux and Mac
export LUA_CPATH='target/debug/?.so;target/release/?.so;target/debug/?.dylib;target/release/?.dylib'

# Optional: skip this one if picodata is already in $PATH
export PICODATA_EXECUTABLE=...
```

After that, you may finally run the tests:

```bash
pipenv run pytest
```

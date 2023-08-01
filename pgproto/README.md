# pgproto

**TODO**: add a descritprion

## Prerequisites
[Picodata](https://git.picodata.io/picodata/picodata/picodata) and
[MsgPuck](https://github.com/rtsisyk/msgpuck) library.


**TODO**: TEST REQUIREMENTS

## Building

Firstly, clone and build [picodata](https://git.picodata.io/picodata/picodata/picodata)
by following the instructions from the repo.

Then you can build the pgproto library. Use PICODATA_TARGET_DIR to specify the directory
target directory where picodata was built.
```bash
mkdir build && cd build
cmake .. -DPICODATA_DIR=~/picodata/picodata/target/debug
make -j
```

If `MsgPuck` lib was no found, you need to specify where to find
`libmsgpuck.a` and headers via `MSGPUCK_LIBRARY` and `MSGPUCK_INCLUDE_DIR`
variables.

Here is how cmake configuration command changes in this case:
```bash
cmake ..	\
	-DPICODATA_DIR=~/picodata/picodata/target/debug	\
	-DMSGPUCK_LIBRARY=~/install/msgpuck/libmsgpuck.a	\
	-DMSGPUCK_INCLUDE_DIR=~/install/msgpuck/
```

## Testing
Just run test target
```bash
make test
```

> **NOTE**: If tests are failed because of missed symbol `mp_*`,
> `msgpuck` library is missed so you should back to *Building* step
> and configure `MSGPUCK_*` variables.

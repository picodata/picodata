# picodata-tarantool

A `tarantool` executable backed by `picodata tarantool`. Put this directory on 
`PATH` to run Lua code, e.g. tarantool-sys tests, with picodata instead of 
vanilla tarantool:

```bash
PATH=$PWD/tools/picodata-tarantool:$PATH tarantool script.lua
```

`tools/test-tarantool-sys.py` uses it this way.

You need to create a symlink to the `picodata` binary next to it or add it to 
the `PATH`.

Many Lua tests want to self-execute Tarantool and do so via `argv[0]`, which is 
why a wrapper is needed.

**If you're using Mac OS, install [GNU bash](https://formulae.brew.sh/formula/bash) and add it to your `PATH`.**

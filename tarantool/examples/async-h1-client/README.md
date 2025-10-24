# Example async_h1 HTTP client

This is a simple example of how you can use **tarantool** with an
**async_h1** based async http client running on our built-in fiber based runtime.

Build the code:
```bash
cargo build -p async-h1-client
```

_(Optional)_ Build the code with TLS support, to make requests also via HTTPS:
```bash
cargo build -p async-h1-client --features tls
```
_For this to work OpenSSL has to be installed in the system._

Make sure `LUA_CPATH` environment variable is setup correctly, e.g. if your
target director is `./target`:
```bash
export LUA_CPATH='./target/debug/lib?.so'
```

Start tarantool with the provided entry script:
```bash
tarantool examples/async-h1-client/src/init.lua
```

Send a `GET` request to any url:
```bash
tarantool> box.func['async_h1_client.get']:call({"http://example.org"})
```

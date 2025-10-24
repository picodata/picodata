# Example tokio + hyper

This is a simple example of how you can use **tarantool** with a
**tokio** + **hyper** based web server.

Simply build the code:
```bash
cargo build -p tokio-hyper
```

Make sure `LUA_CPATH` environment variable is setup correctly, e.g. if your
target director is `./target`:
```bash
export LUA_CPATH='./target/debug/lib?.so'
```

Start tarantool with the provided entry script:
```bash
tarantool examples/tokio-hyper/src/init.lua
```

Now (in another terminal) you can send requests to the web server for example using `curl`:
```bash
curl '127.0.0.1:3000/add-fruit' -XPOST -d '{"id": 1, "name": "apple", "weight": 3.14 }'
curl '127.0.0.1:3000/add-fruit' -XPOST -d '{"id": 2, "name": "banana", "weight": 2.18 }'

curl '127.0.0.1:3000/list-fruit'
```

You should see the list of fruit:
```
Fruit { id: 1, name: "apple", weight: 3.14 }
Fruit { id: 2, name: "banana", weight: 2.18 }
```

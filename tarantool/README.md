# Tarantool Rust SDK

[![Latest Version]][crates.io] [![Docs badge]][docs.rs]

[Latest Version]: https://img.shields.io/crates/v/tarantool.svg
[crates.io]: https://crates.io/crates/tarantool

[Docs badge]: https://img.shields.io/badge/docs.rs-rustdoc-green
[docs.rs]: https://docs.rs/tarantool/

Tarantool Rust SDK offers a library for interacting with Tarantool from Rust applications. This document describes the Tarantool API bindings for Rust and includes the following API's:

- Box: spaces, indexes, sequences
- Fibers: fiber attributes, conditional variables, latches, async runtime
- CoIO
- Transactions
- Schema management
- Protocol implementation (`net.box`): CRUD, stored procedure call, triggers
- Tuple utils
- Logging (see https://docs.rs/log/)
- Error handling

Links:

- [Crate page][crates.io]
- [API Documentation][docs.rs]
- [Repository](https://github.com/picodata/tarantool-module)

See also:

- https://tarantool.io
- https://github.com/tarantool/tarantool

> **Caution!** The library is currently under development.
> API may be unstable until version 1.0 is released.

## Getting Started

The following instructions will help you get a copy of the project up and running on your local machine.
For deployment check out the deployment notes at the end of this file.

### Prerequisites

- Rust 1.61 or newer + Cargo
- Tarantool 2.2

#### Linking issues in macOS

On macOS you may encounter linking errors like this: `ld: symbol(s) not found for architecture x86_64`. To solve it please put these lines to your `$CARGO_HOME/config.toml` (`~/.cargo/config.toml` by default):

```toml
[target.x86_64-apple-darwin]
rustflags = [
    "-C", "link-arg=-undefined",  "-C", "link-arg=dynamic_lookup"
]
```

### Usage

Add the following lines to your project's Cargo.toml:
```toml
[dependencies]
tarantool = "0.6"

[lib]
crate-type = ["cdylib"]
```

See https://github.com/picodata/brod for example usage. 

### Features

- `net_box` - Enables protocol implementation (enabled by default)
- `schema` - Enables schema manipulation utils (WIP as of now)

### Stored procedures

There are several ways Tarantool can call a Rust code. It can use either a plugin, a Lua to Rust FFI code generator, or a stored procedure.
In this file we only cover the third option, namely Rust stored procedures. Even though Tarantool always treats Rust routines just as "C functions", we keep on using the "stored procedure" term as an agreed convention and also for historical reasons.

This tutorial contains the following simple steps:
1. `examples/easy` - prints "hello world";
1. `examples/harder` - decodes a passed parameter value;
1. `examples/hardest` - uses this library to do a DBMS insert;
1. `examples/read` - uses this library to do a DBMS select;
1. `examples/write` - uses this library to do a DBMS replace.

Our examples are a good starting point for users who want to confidently start writing their own stored procedures.

#### Creating a Cargo project

After getting the prerequisites installed, follow these steps:

1. Create a Cargo project:

```console
$ cargo init --lib
```

2. Add the following lines to `Cargo.toml`:

```toml
[package]
name = "easy"
version = "0.1.0"
edition = "2018"
# author, license, etc

[dependencies]
tarantool = "0.6.0"
serde = "1.0"

[lib]
crate-type = ["cdylib"]
```

3. Create the server entry point named `init.lua` with the following script:

```lua
box.cfg({listen = 3301})
box.schema.func.create('easy', {language = 'C', if_not_exists = true})
box.schema.func.create('easy.easy2', {language = 'C', if_not_exists = true})
box.schema.user.grant('guest', 'execute', 'function', 'easy', {if_not_exists = true})
box.schema.user.grant('guest', 'execute', 'function', 'easy.easy2', {if_not_exists = true})
```

To learn more about the commands used above, look up their syntax and usage details in the Tarantool documentation:
- [box.cfg()](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_cfg/);
- [box.schema.func.create()](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema/func_create/);
- [box.schema.user.grant()](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema/user_grant/).

4. Edit `lib.rs` file and add the following lines:

```rust
use tarantool::proc;

#[proc]
fn easy() {
    println!("hello world");
}

#[proc]
fn easy2() {
    println!("hello world -- easy2");
}
```
We are now ready to provide some usage examples. We will show three difficulty levels of calling a function, from a basic usage example (`easy`), to a couple of more complex shared libraries (`harder` and `hardest`) examples. Additionally, there will be separate examples for reading and writing data.

#### Basic usage example

Compile the application and start the server:

```console
$ cargo build
$ LUA_CPATH=target/debug/lib?.so tarantool init.lua
```
Check that the generated `.so` file is on the `LUA_CPATH` that was specified earlier for the next lines to work.

Although Rust and Lua layout conventions are different, we can take hold of Lua flexibility and fix it by explicitly setting the [LUA_CPATH](https://www.lua.org/pil/8.1.html) environmental variable, as shown above.

Now you're ready to make some requests. Open separate console window and run Tarantool as a client. Paste the following into the console:  

```lua
conn = require('net.box').connect(3301)
conn:call('easy')
```

Again, check out the [net.box](https://www.tarantool.io/en/doc/latest/reference/reference_lua/net_box/)
module documentation if necessary.

The code above establishes a server connection and calls the 'easy' function. Since the `easy()` function in
`lib.rs` begins with `println!("hello world")`, the "hello world" string will appear in the server console output.

The code also checks that the call was successful. Since the `easy()` function in `lib.rs` ends
with return 0, there is no error message to display and the request is over.

Now let's call another function in lib.rs, namely `easy2()`. The sequence is almost the same as with he `easy()`
function, but there's a difference: if the file name does not match the function name,  we have to explicitly specify _{file-name}_._{function-name}_.

```lua
conn:call('easy.easy2')
```

... and this time the result will be `hello world -- easy2`.

As you can see, calling a Rust function is as straightforward as it can be.

#### Retrieving call arguments

Create a new crate called "harder". Put these lines to `lib.rs`:

```rust
#[tarantool::proc]
fn harder(fields: Vec<i32>) {
    println!("field_count = {}", fields.len());

    for val in fields {
        println!("val={}", val);
    }
}
```
The above code defines a stored proc which accepts a sequence of integers.

Compile the program into the `harder.so` library using `cargo build`.

Now go back to the client and execute these requests:
```lua
box.schema.func.create('harder', {language = 'C'})
box.schema.user.grant('guest', 'execute', 'function', 'harder')
passable_table = {}
table.insert(passable_table, 1)
table.insert(passable_table, 2)
table.insert(passable_table, 3)
capi_connection:call('harder', {passable_table})
```

This time the call is passing a Lua table (`passable_table`) to the `harder()` function. The `harder()` function will detect it as that was coded in the `args` part of our example above.

The console output should now look like this:
```
tarantool> capi_connection:call('harder', {passable_table})
field_count = 3
val=1
val=2
val=3
---
- []
...
```

As you can see, decoding parameter values passed to a Rust function may be tricky since it requires coding extra routines.

#### Accessing Tarantool space

Create a new crate called "hardest". Put these lines to `lib.rs`:
```rust
use serde::{Deserialize, Serialize};

use tarantool::{
    proc,
    space::Space,
    tuple::{Encode, Tuple},
};

#[derive(Serialize, Deserialize)]
struct Row {
    pub int_field: i32,
    pub str_field: String,
}

impl Encode for Row {}

#[proc]
fn hardest() -> Tuple {
    let mut space = Space::find("capi_test").unwrap();
    let result = space.insert(&Row {
        int_field: 10000,
        str_field: "String 2".to_string(),
    });
    result.unwrap()
}
```
This time the Rust function does the following:
1. Finds the `capi_test` space by calling the `Space::find()` method;
1. Serializes the row structure to a tuple in auto mode;
1. Inserts the tuple using `.insert()`.

Compile the program into the `hardest.so` library using `cargo build`.

Now go back to the client and execute these requests:
```lua
box.schema.func.create('hardest', {language = "C"})
box.schema.user.grant('guest', 'execute', 'function', 'hardest')
box.schema.user.grant('guest', 'read,write', 'space', 'capi_test')
capi_connection:call('hardest')
```

Additionally, execute another request at the client:
```lua
box.space.capi_test:select()
```

The result should look like this:
```
tarantool> box.space.capi_test:select()
---
- - [10000, 'String 2']
...
```

The above proves that the `hardest()` function has succeeded.

#### Reading example

Create a new crate "read". Put these lines to `lib.rs`:
```rust
use serde::{Deserialize, Serialize};

use tarantool::{
    proc,
    space::Space,
    tuple::Encode,
};

#[derive(Serialize, Deserialize, Debug)]
struct Row {
    pub int_field: i32,
    pub str_field: String,
}

impl Encode for Row {}

#[proc]
fn read() {
    let space = Space::find("capi_test").unwrap();

    let key = 10000;
    let result = space.get(&(key,)).unwrap();
    assert!(result.is_some());

    let result = result.unwrap().decode::<Row>().unwrap();
    println!("value={:?}", result);
}
```
The above code does the following:
1. Finds the `capi_test` space by calling `Space::find()`;
1. Formats a search key = 10000 using Rust tuple literal (an alternative to serializing structures);
1. Gets the tuple using `.get()`;
1. Deserializes the result.

Compile the program into the `read.so` library using `cargo build`.

Now go back to the client and execute these requests:
```lua
box.schema.func.create('read', {language = "C"})
box.schema.user.grant('guest', 'execute', 'function', 'read')
box.schema.user.grant('guest', 'read,write', 'space', 'capi_test')
capi_connection:call('read')
```

The result of `capi_connection:call('read')` should look like this:

```
tarantool> capi_connection:call('read')
uint value=10000.
string value=String 2.
---
- []
...
```

The above proves that the `read()` function has succeeded.

#### Writing example

Create a new crate called "write". Put these lines to `lib.rs`:
```rust
use tarantool::{
    proc,
    error::Error,
    fiber::sleep,
    space::Space,
    transaction::start_transaction,
};

#[proc]
fn write() -> Result<(i32, String), String> {
    let mut space = Space::find("capi_test")
        .ok_or_else(|| "Can't find space capi_test".to_string())?;

    let row = (1, "22".to_string());

    start_transaction(|| -> Result<(), Error> {
        space.replace(&row)?;
        Ok(())
    })
    .unwrap();

    sleep(std::time::Duration::from_millis(1));
    Ok(row)
}
```
The above code does the following:
1. Finds the `capi_test` space by calling `Space::find()`;
1. Prepares the row value;
1. Launches the transaction;
1. Replaces the tuple in `box.space.capi_test`
1. Finishes the transaction:
    - performs a commit upon receiving `Ok()` on closure
    - performs a rollback upon receiving `Error()`;
1. Returns the entire tuple to the caller and lets the caller display it.

Compile the program into the `write.so` library using `cargo build`.

Now go back to the client and execute these requests:
```lua
box.schema.func.create('write', {language = "C"})
box.schema.user.grant('guest', 'execute', 'function', 'write')
box.schema.user.grant('guest', 'read,write', 'space', 'capi_test')
capi_connection:call('write')
```

The result of `capi_connection:call('write')` should look like this:
```
tarantool> capi_connection:call('write')
---
- [[1, '22']]
...
```

The above proves that the `write()` function has succeeded.

As you can see, Rust "stored procedures" have full access to a database.

#### Cleaning up

- Get rid of each of the function tuples with `box.schema.func.drop`.
- Get rid of the `capi_test` space with `box.schema.capi_test:drop()`.
- Remove the `*.so` files that were created for this tutorial.

## Running the tests

To invoke the automated tests run:
```shell script
make
make test
```

See [test readme](./tests/README.md) for more information about test structure and adding them.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://git.picodata.io/picodata/picodata/tarantool-module/-/tags). 

## Authors

- **Anton Melnikov**
- **Dmitriy Koltsov**
- **Georgy Moshkin**
- **Egor Ivkov**

© 2020-2022 Picodata.io https://git.picodata.io/picodata
## License

This project is licensed under the BSD License - see the [LICENSE](LICENSE) file for details

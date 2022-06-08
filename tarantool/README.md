# Tarantool Rust SDK

[![Latest Version]][crates.io] [![Docs badge]][docs.rs]

[Latest Version]: https://img.shields.io/crates/v/tarantool.svg
[crates.io]: https://crates.io/crates/tarantool

[Docs badge]: https://img.shields.io/badge/docs.rs-rustdoc-green
[docs.rs]: https://docs.rs/tarantool/

Tarantool API bindings for Rust. 
This library contains the following Tarantool API's:

- Box: spaces, indexes, sequences
- Fibers: fiber attributes, conditional variables, latches
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

- rustc 1.48 or newer + cargo builder
- tarantool 2.2

#### MacOS linking issues

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
- `schema` - Enables schema manipulation utils (WIP for now)

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

After getting the prerequisies installed, follow these steps:

1. Create cargo project:

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
require('easy')
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
use std::os::raw::c_int;
use tarantool::tuple::{FunctionArgs, FunctionCtx};

#[no_mangle]
pub extern "C" fn easy(_: FunctionCtx, _: FunctionArgs) -> c_int {
    println!("hello world");
    0
}

#[no_mangle]
pub extern "C" fn easy2(_: FunctionCtx, _: FunctionArgs) -> c_int {
    println!("hello world -- easy2");
    0
}

#[no_mangle]
pub extern "C" fn luaopen_easy(_l: std::ffi::c_void) -> c_int {
    // Tarantool calls this function upon require("easy")
    println!("easy module loaded");
    0
}
```
We are now ready to provide three usage examples with varied difficulty level, from the basic usage example (`easy`), to a couple of more complex shared libraries (`harder` and `hardest`).

#### Basic usage example

Compile the application and start the server:

```console
$ cargo build
$ LUA_CPATH=target/debug/lib?.so tarantool init.lua
```

Setting the [LUA_CPATH](https://www.lua.org/pil/8.1.html) environmental variable is necessary because Rust and Lua layout conventions are different. Fortunately, Lua is rather flexible. 

Now you're ready to make some requests. Open separate console window and run tarantool as a client. Paste the following into the console:  

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

#### Compiling a library

Create a new crate called "harder". Put these lines to `lib.rs`:

```rust
use serde::{Deserialize, Serialize};
use std::os::raw::c_int;
use tarantool::tuple::{AsTuple, FunctionArgs, FunctionCtx, Tuple};

#[derive(Serialize, Deserialize)]
struct Args {
    pub fields: Vec<i32>,
}

impl AsTuple for Args {}

#[no_mangle]
pub extern "C" fn harder(_: FunctionCtx, args: FunctionArgs) -> c_int {
    let args: Tuple = args.into(); // (1)
    let args = args.into_struct::<Args>().unwrap(); // (2)
    println!("field_count = {}", args.fields.len());

    for val in args.fields {
        println!("val={}", val);
    }

    0
}
```
The above code does the following two things:
1. Extracts tuple from the `FunctionArgs` special structure
1. Deserializes tuple into the Rust structure

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

#### Compiling an advanced library

Create a new crate called "hardest". Put these lines to `lib.rs`:
```rust
use std::os::raw::c_int;

use serde::{Deserialize, Serialize};

use tarantool::space::Space;
use tarantool::tuple::{AsTuple, FunctionArgs, FunctionCtx};

#[derive(Serialize, Deserialize)]
struct Row {
    pub int_field: i32,
    pub str_field: String,
}

impl AsTuple for Row {}

#[no_mangle]
pub extern "C" fn hardest(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    let mut space = Space::find("capi_test").unwrap(); // (1)
    let result = space.insert( // (3)
        &Row { // (2)
            int_field: 10000,
            str_field: "String 2".to_string(),
        }
    );
    ctx.return_tuple(&result.unwrap().unwrap()).unwrap()
}
```
This time the Rust function does three things:
1. Finds the `capi_test` space by calling the `Space::find_by_name()` method;
1. Serializes the row structure to tuple in auto mode;
1. Inserts a tuple using `.insert()`.

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

The above proves that the `hardest()` function succeeded.

#### Read

Create a new crate "read". Put these lines to `lib.rs`:
```rust
use std::os::raw::c_int;

use serde::{Deserialize, Serialize};

use tarantool::space::Space;
use tarantool::tuple::{AsTuple, FunctionArgs, FunctionCtx};

#[derive(Serialize, Deserialize, Debug)]
struct Row {
    pub int_field: i32,
    pub str_field: String,
}

impl AsTuple for Row {}

#[no_mangle]
pub extern "C" fn read(_: FunctionCtx, _: FunctionArgs) -> c_int {
    let space = Space::find("capi_test").unwrap(); // (1)

    let key = 10000;
    let result = space.get(&(key,)).unwrap(); // (2, 3)
    assert!(result.is_some());

    let result = result.unwrap().into_struct::<Row>().unwrap(); // (4)
    println!("value={:?}", result);

    0
}
```
1. once again, finding the `capi_test` space by calling `Space::find()`;
1. formatting a search key = 10000 using rust tuple literal (an alternative to serializing structures);
1. getting a tuple using `.get()`;
1. deserializing result.

Compile the program, producing a library file named `read.so`.

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

This proves that the `read()` function succeeded.

#### Write

Create a new crate "write". Put these lines to `lib.rs`:
```rust
use std::os::raw::c_int;

use tarantool::error::{Error, TarantoolErrorCode};
use tarantool::fiber::sleep;
use tarantool::space::Space;
use tarantool::transaction::start_transaction;
use tarantool::tuple::{FunctionArgs, FunctionCtx};

#[no_mangle]
pub extern "C" fn write(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
   let mut space = match Space::find("capi_test") {
      None => {
         return tarantool::set_error!(TarantoolErrorCode::ProcC, "Can't find space capi_test")
      }
      Some(space) => space,
   };

   let row = (1, "22".to_string());

   start_transaction(|| -> Result<(), Error> {
      space.replace(&row)?;
      Ok(())
   })
           .unwrap();

   sleep(std::time::Duration::from_millis(1));
   ctx.return_mp(&row).unwrap()
}
```
1. once again, finding the `capi_test` space by calling `Space::find_by_name()`;
1. preparing row value;
1. starting a transaction;
1. replacing a tuple in `box.space.capi_test`
1. ending a transaction: 
    - commit if closure returns `Ok()`
    - rollback on `Error()`;
1. use the `.return_mp()` method to return the entire tuple to the caller and let the caller display it.

Compile the program, producing a library file named `write.so`.

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
- [[1, 22]]
...
```

This proves that the `write()` function succeeded.

Conclusion: Rust "stored procedures" have full access to the database.

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

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/picodata/tarantool-module/tags). 

## Authors

- **Anton Melnikov**
- **Dmitriy Koltsov**
- **Georgy Moshkin**

© 2020-2021 Picodata.io https://github.com/picodata

## License

This project is licensed under the BSD License - see the [LICENSE](LICENSE) file for details

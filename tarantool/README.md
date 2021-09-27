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
> API may be unstable until version 1.0 will be released.

## Getting Started

These instructions will get a copy of the project up and running on your local machine.
For deployment, check out the deployment notes at the end of the tutorial.

### Prerequisites

- rustc 1.48 or newer
- tarantool 2.2

### Usage

Add the following lines to your project Cargo.toml:
```toml
[dependencies]
tarantool = "0.4"

[lib]
crate-type = ["cdylib"]
```

See https://github.com/picodata/brod for example usage. 

### Features

- `net_box` - Enables protocol implementation (enabled by default)
- `schema` - Enables schema manipulation utils (WIP for now)

### Stored procedures

Tarantool can call Rust code via a plugin, from Lua using FFI, or as a stored procedure.
This tutorial only is about the third 
option, Rust stored procedures. In fact Rust routines are always "C
functions" to Tarantool but the phrase "stored procedure" is commonly used
for historical reasons.

This tutorial contains the following simple steps:
1. `examples/easy` - prints "hello world";
1. `examples/harder` - decodes a passed parameter value;
1. `examples/hardest` - uses this library to do a DBMS insert;
1. `examples/read` - uses this library to do a DBMS select;
1. `examples/write` - uses this library to do a DBMS replace.

By following the instructions and seeing that the results users should
become confident in writing their own stored procedures.

#### Preparation

Check that these items exist on the computer:
- Tarantool 2.2
- A rustc compiler + cargo builder. Any modern version should work

Create cargo project:
```shell script
$ cargo init --lib
```

Add the following lines to `Cargo.toml`:
```toml
[package]
name = "easy"
version = "0.1.0"
edition = "2018"
# author, license, etc

[dependencies]
tarantool = "0.4.2" # (1)
serde = "1.0" # (2)

[lib]
crate-type = ["cdylib"] # (3)
```

1. add to dependencies `tarantool` library;
1. add to dependencies [Serde](https://github.com/serde-rs/serde), this is optional and required if you want to use rust 
structures as a tuple values (see [this example](#harder));
1. you need to compile dynamic library.

Requests will be done using Tarantool as a client. Start Tarantool, and enter these requests:
```lua
box.cfg{listen=3306}
box.schema.space.create('capi_test')
box.space.capi_test:create_index('primary')
net_box = require('net.box')
capi_connection = net_box:new(3306)
```

In plain language: create a space named `capi_test`, and make a connection to self named `capi_connection`.

Leave the client running. It will be used to enter more requests later.

#### Easy

Edit `lib.rs` file and add the following lines:
```rust
use std::os::raw::c_int;
use tarantool_module::tuple::{FunctionArgs, FunctionCtx};

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
```

Compile the program:
```shell script
$ cargo build
```

Start another shell. Change directory (`cd`) so that it is the same as the directory that the client is running in.
Copy the compiled library (it is located in subfolder `target/debug` at you
project sources folder) to the current folder and rename it to `easy.so`

Now go back to the client and execute these requests:
```lua
box.schema.func.create('easy', {language = 'C'})
box.schema.user.grant('guest', 'execute', 'function', 'easy')
capi_connection:call('easy')
```

If these requests appear unfamiliar, read the descriptions of 
[box.schema.func.create()](https://www.tarantool.io/en/doc/2.2/reference/reference_lua/box_schema/#box-schema-func-create), 
[box.schema.user.grant()](https://www.tarantool.io/en/doc/2.2/reference/reference_lua/box_schema/#box-schema-user-grant) 
and [conn:call()](https://www.tarantool.io/en/doc/2.2/reference/reference_lua/net_box/#net-box-call).

The function that matters is `capi_connection:call('easy')`.

Its first job is to find the 'easy' function, which should be easy because by default Tarantool looks on the current directory
for a file named `easy.so`.

Its second job is to call the 'easy' function. Since the `easy()` function in `lib.rs` begins with `println!("hello world")`, 
the words "hello world" will appear on the screen.

Its third job is to check that the call was successful. Since the `easy()` function in `lib.rs` ends with return 0, there 
is no error message to display and the request is over.

The result should look like this:
```
tarantool> capi_connection:call('easy')
hello world
---
- []
...
```

Now let's call the other function in lib.rs - `easy2()`. This is almost the same as the `easy()` function, but there's a 
detail: when the file name is not the same as the function name, then we have to specify _{file-name}_._{function-name}_
```lua
box.schema.func.create('easy.easy2', {language = 'C'})
box.schema.user.grant('guest', 'execute', 'function', 'easy.easy2')
capi_connection:call('easy.easy2')
```

... and this time the result will be `hello world -- easy2`.

Conclusion: calling a Rust function is easy.

#### Harder

Create a new crate "harder". Put these lines to `lib.rs`:
```rust
use serde::{Deserialize, Serialize};
use std::os::raw::c_int;
use tarantool_module::tuple::{AsTuple, FunctionArgs, FunctionCtx, Tuple};

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
1. extract tuple from special structure `FunctionArgs`
1. deserialize tuple into rust structure

Compile the program, producing a library file named `harder.so`.

Now go back to the client and execute these requests:
```lua
box.schema.func.create('harder', {language = 'C'})
box.schema.user.grant('guest', 'execute', 'function', 'harder')
passable_table = {}
table.insert(passable_table, 1)
table.insert(passable_table, 2)
table.insert(passable_table, 3)
capi_connection:call('harder', passable_table)
```

This time the call is passing a Lua table (`passable_table`) to the `harder()` function. The `harder()` function will see 
it, it's in the char `args` parameter.

And now the screen looks like this:
```
tarantool> capi_connection:call('harder', passable_table)
field_count = 3
val=1
val=2
val=3
---
- []
...
```

Conclusion: decoding parameter values passed to a rust function is not easy at first, but there are routines to do the job.

#### Hardest

Create a new crate "hardest". Put these lines to `lib.rs`:
```rust
use std::os::raw::c_int;

use serde::{Deserialize, Serialize};

use tarantool_module::space::Space;
use tarantool_module::tuple::{AsTuple, FunctionArgs, FunctionCtx};

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
    ctx.return_tuple(result.unwrap().unwrap()).unwrap()
}
```
This time the rust function is doing three things:
1. finding the `capi_test` space by calling `Space::find_by_name()` method;
1. row structure can be passed as is, it will be serialized to tuple
   automaticaly;
1. inserting a tuple using `.insert()`.

Compile the program, producing a library file named `hardest.so`.

Now go back to the client and execute these requests:
```lua
box.schema.func.create('hardest', {language = "C"})
box.schema.user.grant('guest', 'execute', 'function', 'hardest')
box.schema.user.grant('guest', 'read,write', 'space', 'capi_test')
capi_connection:call('hardest')
```

Now, still on the client, execute this request:
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

This proves that the `hardest()` function succeeded.

#### Read

Create a new crate "read". Put these lines to `lib.rs`:
```rust
use std::os::raw::c_int;

use serde::{Deserialize, Serialize};

use tarantool_module::space::Space;
use tarantool_module::tuple::{AsTuple, FunctionArgs, FunctionCtx};

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

use tarantool_module::error::{set_error, Error, TarantoolErrorCode};
use tarantool_module::fiber::sleep;
use tarantool_module::space::Space;
use tarantool_module::transaction::start_transaction;
use tarantool_module::tuple::{FunctionArgs, FunctionCtx};

#[no_mangle]
pub extern "C" fn hardest(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    let mut space = match Space::find("capi_test").unwrap() { // (1)
        None => {
            return set_error(
                file!(),
                line!(),
                &TarantoolErrorCode::ProcC,
                "Can't find space capi_test",
            )
        }
        Some(space) => space,
    };

    let row = (1, 22); // (2)

    start_transaction(|| -> Result<(), Error> { // (3)
        space.replace(&row, false)?; // (4)
        Ok(()) // (5)
    })
    .unwrap();

    sleep(0.001);
    ctx.return_mp(&row).unwrap() // (6)
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

© 2020-2021 Picodata.io https://github.com/picodata

## License

This project is licensed under the BSD License - see the [LICENSE](LICENSE) file for details

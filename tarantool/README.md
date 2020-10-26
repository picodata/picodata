# Tarantool C API bindings

[![Latest Version]][crates.io] [![Docs badge]][docs.rs]

[Latest Version]: https://img.shields.io/crates/v/tarantool-module.svg
[crates.io]: https://crates.io/crates/tarantool-module

[Docs badge]: https://img.shields.io/badge/docs.rs-rustdoc-green
[docs.rs]: https://docs.rs/tarantool-module/

Tarantool module C API bindings for Rust. 
This library contains following Tarantool API's:

- Box: spaces, indexes, sequences 
- Fibers: fiber attributes, conditional variables
- CoIO
- Transactions
- Latches
- Tuple utils
- Logging (see https://docs.rs/log/0.4.11/log/)
- Error handling

See also:

- https://tarantool.io
- https://github.com/tarantool/tarantool

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

- rustc 1.45.0 or newer (other versions were not tested)
- tarantool 2.2

### Usage

Add following lines to your project Cargo.toml:
```toml
[dependencies]
tarantool-module = "0.2"

[lib]
crate-type = ["cdylib"]
```

See https://github.com/picodata/brod for example of usage. 

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

© 2020 Picodata.io https://github.com/picodata

## License

This project is licensed under the BSD License - see the [LICENSE](LICENSE) file for details

use std::env;

use clap::Parser;
use picodata::cli;
use picodata::cli::args::Picodata;

include!(concat!(env!("OUT_DIR"), "/export_symbols.rs"));

fn main() -> ! {
    export_symbols();
    match Picodata::parse() {
        Picodata::Run(args) => cli::run::main(*args),
        Picodata::Test(args) => cli::test::main(args),
        Picodata::Tarantool(args) => cli::tarantool::main(args),
        Picodata::Expel(args) => cli::expel::main(args),
        Picodata::Connect(args) => cli::connect::main(args),
        Picodata::Sql(args) => cli::sql::main(args),
    }
}

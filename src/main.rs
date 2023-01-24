use clap::Parser;
use picodata::args;
use picodata::{main_expel, main_run, main_tarantool, main_test};

fn main() -> ! {
    match args::Picodata::parse() {
        args::Picodata::Run(args) => main_run(args),
        args::Picodata::Test(args) => main_test(args),
        args::Picodata::Tarantool(args) => main_tarantool(args),
        args::Picodata::Expel(args) => main_expel(args),
    }
}

use clap::Parser;
use picodata::cli::{self, args::Picodata};

fn main() -> ! {
    // Initialize the panic hook asap.
    picodata_plugin::internal::set_panic_hook();

    match Picodata::parse() {
        Picodata::Run(args) => cli::run::main(*args),
        Picodata::Test(args) => cli::test::main(args),
        Picodata::Tarantool(args) => cli::tarantool::main(args),
        Picodata::Expel(args) => cli::expel::main(args),
        Picodata::Status(args) => cli::status::main(args),
        Picodata::Connect(args) => cli::connect::main(args),
        Picodata::Admin(args) => cli::admin::main(args),
        Picodata::Config(cli::args::Config::Default(args)) => cli::default_config::main(args),
        Picodata::Plugin(args) => cli::plugin::main(args),
    }
}

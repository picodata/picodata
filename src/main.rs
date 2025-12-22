use clap::{CommandFactory, Parser};
use picodata::cli::{
    self,
    args::{Command, Picodata},
};

fn main() -> ! {
    // Initialize the panic hook asap.
    picodata_plugin::internal::set_panic_hook();

    let args = Picodata::parse();

    // XXX: `--version` takes precedence over any subcommand.
    if args.version >= 2 {
        print!("{}", picodata::info::render_long_version());
        std::process::exit(0);
    } else if args.version == 1 {
        print!("{}", picodata::info::render_version());
        std::process::exit(0);
    }

    if let Some(command) = args.command {
        match command {
            Command::Restore(args) => cli::restore::main(args),
            Command::Run(args) => cli::run::main(*args),
            Command::Test(args) => cli::test::main(args),
            Command::Tarantool(args) => cli::tarantool::main(args),
            Command::Expel(args) => cli::expel::main(args),
            Command::Status(args) => cli::status::main(args),
            Command::Connect(args) => cli::connect::main(args),
            Command::Admin(args) => cli::admin::main(args),
            Command::Config(cli::args::Config::Default(args)) => cli::default_config::main(args),
            Command::Plugin(args) => cli::plugin::main(args),
            Command::Demo(args) => cli::demo::main(args),
        }
    } else {
        Picodata::command().print_help().unwrap();
        std::process::exit(0);
    }
}

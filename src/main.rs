use clap::Parser;
use picodata::cli::{self, args::Picodata};

fn main() -> ! {
    // Initialize the panic hook asap.
    // Even if `say` isn't properly initialized yet, we
    // still should be able to print a simplified line to stderr.
    std::panic::set_hook(Box::new(|info| {
        // Capture a backtrace regardless of RUST_BACKTRACE and such.
        let backtrace = std::backtrace::Backtrace::force_capture();
        picodata::tlog!(
            Critical,
            "\n\n{info}\n\nbacktrace:\n{backtrace}\naborting due to panic"
        );
        std::process::abort();
    }));

    match Picodata::parse() {
        Picodata::Run(args) => cli::run::main(*args),
        Picodata::Test(args) => cli::test::main(args),
        Picodata::Tarantool(args) => cli::tarantool::main(args),
        Picodata::Expel(args) => cli::expel::main(args),
        Picodata::Connect(args) => cli::connect::main(args),
        Picodata::Admin(args) => cli::admin::main(args),
        Picodata::Config(cli::args::Config::Default(args)) => cli::default_config::main(args),
    }
}

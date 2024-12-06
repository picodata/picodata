use clap::Parser;
use picodata::cli::{self, args::Picodata};

fn main() -> ! {
    // Initialize the panic hook asap.
    // Even if `say` isn't properly initialized yet, we
    // still should be able to print a simplified line to stderr.
    std::panic::set_hook(Box::new(|info| {
        // Capture a backtrace regardless of RUST_BACKTRACE and such.
        let backtrace = std::backtrace::Backtrace::force_capture();
        let message = format!("\n\n{info}\n\nbacktrace:\n{backtrace}\naborting due to panic");
        picodata::tlog!(Critical, "{message}");

        // Dump the backtrace to file for easier debugging experience.
        // In particular this is used in the integration tests.
        _ = std::fs::write("picodata.backtrace", message);
        if let Ok(mut dir) = std::env::current_dir() {
            dir.push("picodata.backtrace");
            picodata::tlog!(Info, "dumped panic backtrace to `{}`", dir.display());
        }

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

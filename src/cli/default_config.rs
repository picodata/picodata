use crate::cli::args;
use crate::config::PicodataConfig;

pub fn main(args: args::ConfigDefault) -> ! {
    if let Err(e) = main_impl(args) {
        eprintln!("{e}");
        std::process::exit(1);
    }

    std::process::exit(0);
}

fn main_impl(args: args::ConfigDefault) -> Result<(), Box<dyn std::error::Error>> {
    let config = PicodataConfig::with_defaults();

    let yaml = serde_yaml::to_string(&config)
        .map_err(|e| format!("failed converting configuration to yaml: {e}"))?;

    match &args.output_file {
        Some(filename) if filename != "-" => {
            std::fs::write(filename, &yaml)
                .map_err(|e| format!("failed saving configuration to file '{filename}': {e}"))?;
        }
        _ => {
            println!("{yaml}");
        }
    }

    Ok(())
}

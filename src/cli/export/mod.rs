use std::path::Path;

use tokio::task::JoinHandle;
use tokio_postgres as pg;
use tokio_postgres::{Client, NoTls};

use crate::cli::args;

mod catalog;
mod diagnostics;
mod error;
mod options;
mod output;
mod render;

use catalog::Catalog;
use diagnostics::Diagnostics;
use error::{describe_chain, ExportError, ExportErrorKind};
use output::DumpOutput;
use render::plain::PlainTextFormat;
use render::DumpHeader;

async fn connect(
    config: &pg::Config,
    diagnostics: Diagnostics,
) -> Result<(Client, JoinHandle<()>), ExportError> {
    let (client, connection) = config
        .connect(NoTls)
        .await
        .map_err(ExportErrorKind::Connect)?;

    // When the connection breaks, the pending queries fail with a bare "connection closed",
    // while the actual cause is only returned from here.
    let connection = tokio::spawn(async move {
        if let Err(error) = connection.await {
            diagnostics.warn(format_args!(
                "lost the connection to the cluster: {}",
                describe_chain(&error)
            ));
        }
    });

    Ok((client, connection))
}

pub fn main(arguments: args::Export) -> ! {
    let verbose = arguments.verbose;
    let result = options::build(arguments).and_then(|options| {
        // Switch it to `new_multi_thread` when `-j/--jobs` is being developed.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| {
                ExportError::other("failed initializing the tokio runtime", source)
            })?;

        let catalog = runtime.block_on(fetch_catalog(&options.pg_config, verbose))?;
        write_dump(&catalog, options.output_file.as_deref())
    });

    match result {
        Ok(()) => std::process::exit(0),
        // The reader is no longer reading, e.g. `picodata export ... | head`, so gracefully exit.
        Err(error) if error.is_broken_pipe() => std::process::exit(1),
        Err(error) => {
            crate::eprintln_buffered!("ERROR: {}", describe_chain(&error));
            if let Some(hint) = error.get_hint() {
                crate::eprintln_buffered!("HINT: {hint}");
            }
            if verbose {
                crate::eprintln_buffered!("LOCATION: {}", error.location);
            }
            std::process::exit(error.exit_code());
        }
    }
}

async fn fetch_catalog(config: &pg::Config, verbose: bool) -> Result<Catalog, ExportError> {
    let diagnostics = Diagnostics::new(verbose);

    diagnostics.report("connecting to the cluster...");
    let (client, connection) = connect(config, diagnostics).await?;

    let catalog = Catalog::fetch(&client).await;
    // Terminate the connection by dropping the client and polling its future
    // to completion so connection shuts down cleanly on the server.
    drop(client);
    _ = connection.await;
    let catalog = catalog?;
    diagnostics.report(format_args!(
        "read {} table(s), {} index(es), {} tier(s)",
        catalog.tables.len(),
        catalog.indexes.len(),
        catalog.tiers.len()
    ));

    Ok(catalog)
}

fn write_dump(catalog: &Catalog, output_file: Option<&Path>) -> Result<(), ExportError> {
    let mut output = DumpOutput::open(output_file)?;
    render::render_dump(
        &mut PlainTextFormat::new(&mut output),
        catalog,
        &DumpHeader::now(),
    )
    .map_err(|error| output.name_destination_of(error))?;

    output.finish()
}

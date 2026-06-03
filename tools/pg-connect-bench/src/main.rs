use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use std::io::Write;
use std::time::{Duration, Instant};

struct ConnectionConfig {
    pub addr: std::net::SocketAddr,
    pub user: String,
    pub password: String,
}

#[derive(Debug, Copy, Clone)]
struct RunConfig {
    pub connection_count: usize,
    pub max_concurrency: usize,
}

#[derive(Debug, Copy, Clone)]
struct RunResult {
    pub run: RunConfig,
    pub run_duration: Duration,
    pub p99_connection_duration: Duration,
}

impl RunResult {
    pub fn connections_per_second(&self) -> f64 {
        self.run.connection_count as f64 / self.run_duration.as_secs_f64()
    }
}

async fn run_concurrent_connects(
    conn: &ConnectionConfig,
    run: &RunConfig,
) -> anyhow::Result<RunResult> {
    let mut config = tokio_postgres::Config::new();
    config.hostaddr(conn.addr.ip());
    config.port(conn.addr.port());

    config.user(&conn.user);
    config.password(&conn.password);

    let run_start = Instant::now();

    let results = futures::stream::iter((0..run.connection_count).map(|_| async {
        let config = config.clone();

        let conn_start = Instant::now();

        let (client, connection) = tokio::time::timeout(
            Duration::from_secs(4),
            config.connect(tokio_postgres::NoTls),
        )
        .await
        .context("connection timeout")??;

        let conn_duration = conn_start.elapsed();

        drop(client);

        // gracefully close the execution
        connection.await?;

        Ok::<_, anyhow::Error>(conn_duration)
    }))
    .buffered(run.max_concurrency)
    .collect::<Vec<_>>()
    .await;

    let run_duration = run_start.elapsed();

    let mut perc = inc_stats::Percentiles::new();

    let mut error_count = 0;
    for result in results {
        match result {
            Ok(conn_duration) => perc.add(conn_duration.as_secs_f64()),
            Err(_) => error_count += 1,
        }
    }
    if error_count > 0 {
        panic!("{error_count} connections failed!");
    }

    let p99_connection_duration = perc.percentile(0.99).unwrap().unwrap();
    let p99_connection_duration = Duration::from_secs_f64(p99_connection_duration);

    Ok(RunResult {
        run: run.clone(),
        run_duration,
        p99_connection_duration,
    })
}

#[derive(Parser)]
struct Cli {
    experiment_name: String,
}

async fn main_impl() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let conn = ConnectionConfig {
        addr: "127.0.0.1:4327".parse().unwrap(),
        user: "user".to_string(),
        password: "Password1".to_string(),
    };

    let experiment_name = cli.experiment_name;

    let num_trials = 100;
    let output_filename = "trials.csv";

    let csv_header = "experiment,concurrency,throughput,p99_conn_time";

    let mut output = match std::fs::File::create_new(output_filename) {
        Ok(mut new_file) => {
            eprintln!("Created a new output file {}", output_filename);

            // the file is new, need to add a header
            writeln!(new_file, "{csv_header}").unwrap();
            new_file
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            eprintln!("Opening existing output file {}", output_filename);

            // file already exists, append to it
            std::fs::File::options()
                .append(true)
                .open(output_filename)
                .unwrap()
        }
        Err(e) => {
            panic!("Couldn't open file: {e}")
        }
    };

    println!("{csv_header}");

    for max_concurrency in 1..8 {
        let run = RunConfig {
            connection_count: 400 * max_concurrency,
            max_concurrency,
        };

        for _trial in 0..num_trials {
            let result = run_concurrent_connects(&conn, &run).await?;
            let cps = result.connections_per_second();
            let p99_duration = result.p99_connection_duration.as_secs_f64() * 1000.0;

            let csv_line =
                format!("{experiment_name},{max_concurrency},{cps:.02},{p99_duration:.06}",);

            println!("{csv_line}");
            writeln!(output, "{csv_line}").unwrap();

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    main_impl().await.unwrap();
}

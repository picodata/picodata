use crate::cli::args::Demo;
use crate::cli::Result;
use crate::config::DEFAULT_LISTEN_HOST;
use crate::schema::ADMIN_NAME;
use nix::errno::Errno;
use nix::sys::signal::SigSet;
use nix::sys::signal::Signal;
use nix::sys::wait::WaitPidFlag;
use nix::sys::wait::WaitStatus;
use nix::unistd::Pid;
use pike::cluster as builder;
use std::collections::BTreeMap;
use std::path::Path;

const ADMIN_PASSWORD: &'static str = "T0psecret";
const WORKING_DIRECTORY: &'static str = "picodata_demo";

struct Meta<'a> {
    node_name: &'a str,
    node_pid: Pid,
    node_dir: &'a Path,
    node_pg_port: u16,
    node_http_port: u16,
    node_iproto_port: u16,
}

impl Meta<'_> {
    fn print(&self) {
        let Meta {
            node_name,
            node_pid,
            node_dir,
            ..
        } = self;
        let node_http = format!("http://{}:{}", DEFAULT_LISTEN_HOST, self.node_http_port);
        let node_logs = node_dir.join("picodata.log");
        let node_logs = node_logs.display();
        let node_dir = node_dir.display();
        let node_iproto = self.node_iproto_port;
        let node_pgproto = format!(
            "postgres://{}:{}@{}:{}",
            ADMIN_NAME, ADMIN_PASSWORD, DEFAULT_LISTEN_HOST, self.node_pg_port,
        );

        println!(
            "\
Instance '{node_name}':
  - pid: `{node_pid}`
  - dir: `{node_dir}`
  - logs: `{node_logs}`
  - http: `{node_http}`
  - iproto: `{node_iproto}`
  - pgproto: `{node_pgproto}`\n\
",
        );
    }
}

fn shutdown_instances(cluster_meta: &[Meta]) -> Result<()> {
    for node_meta in cluster_meta {
        let Meta {
            node_name,
            node_pid,
            ..
        } = node_meta;

        println!("Killing instance '{node_name}' (pid {node_pid})...");
        match nix::sys::signal::kill(*node_pid, Some(Signal::SIGKILL)) {
            Ok(()) => {}
            Err(Errno::ESRCH) => {}
            Err(error) => Err(error)?,
        }

        println!("Waiting instance '{node_name}' (pid {node_pid}) termination...");
        match nix::sys::wait::waitpid(*node_pid, None) {
            Ok(_) | Err(Errno::ECHILD) => {
                println!("Killed Instance '{node_name}' successfully after shutdown request.");
            }
            Err(e) => {
                eprintln!("Instance '{node_name}': waitpid syscall failed ({e}).");
            }
        }
    }

    Ok(())
}

fn fetch_meta(cluster_nodes: &[pike::cluster::PicodataInstance]) -> Result<Vec<Meta<'_>>> {
    let mut result_meta = Vec::with_capacity(cluster_nodes.len());
    for cluster_node in cluster_nodes {
        let node_properties = cluster_node.properties();

        let node_name = node_properties.instance_name;
        let node_dir = node_properties.data_dir;
        let node_pg_port = *node_properties.pg_port;
        let node_http_port = *node_properties.http_port;
        let node_iproto_port = *node_properties.bin_port;

        // HACK: pike stores child handler in private state, which means we cannot access
        // it directly, but pike also writes a text file with pid of an instance in its
        // working directory which makes possible to parse and use it.
        let pid_path = node_dir.join("pid");
        let raw_pid = std::fs::read_to_string(pid_path)?.trim().parse()?;
        let node_pid = Pid::from_raw(raw_pid);

        result_meta.push(Meta {
            node_name,
            node_pid,
            node_dir,
            node_pg_port,
            node_http_port,
            node_iproto_port,
        });
    }

    Ok(result_meta)
}

fn clean_data(condition: bool, directory: impl AsRef<Path>) -> Result<()> {
    if condition && directory.as_ref().exists() {
        std::fs::remove_dir_all(&directory)?;
    }

    Ok(())
}

fn reap_children(cluster_meta: &[Meta]) -> Result<()> {
    loop {
        match nix::sys::wait::waitpid(None, Some(WaitPidFlag::WNOHANG)) {
            Ok(status) => {
                let instance_pid = status.pid();
                let instance_meta = cluster_meta
                    .iter()
                    .find(|meta| instance_pid.is_some_and(|pid| pid == meta.node_pid));
                let instance_name = instance_meta.map(|meta| meta.node_name);

                match status {
                    WaitStatus::Exited(_, status) => {
                        let node_name = instance_name
                            .expect("waitpid with wait status should always have a pid");
                        eprintln!("Instance {node_name} exited with status {status}.");
                    }
                    WaitStatus::Signaled(_, signal, _) => {
                        let node_name = instance_name
                            .expect("waitpid with wait status should always have a pid");
                        eprintln!("Instance {node_name} killed by {signal} signal.");
                    }
                    WaitStatus::StillAlive => break,
                    _ => continue,
                }
            }
            Err(errno) => match errno {
                Errno::ECHILD => break,
                Errno::EINTR => continue, // retry on double SIGCHLD.
                error => {
                    eprintln!("`waitpid` syscall failed ({error})");
                    Err(error)?
                }
            },
        }
    }

    Ok(())
}

fn handle_signals(cluster_meta: &[Meta]) -> Result<()> {
    let mut signals_set_mask = SigSet::empty();
    signals_set_mask.add(Signal::SIGINT);
    signals_set_mask.add(Signal::SIGTERM);
    signals_set_mask.add(Signal::SIGQUIT);
    signals_set_mask.add(Signal::SIGCHLD);
    signals_set_mask.thread_block()?;

    loop {
        match signals_set_mask.wait()? {
            signal @ (Signal::SIGINT | Signal::SIGTERM | Signal::SIGQUIT) => {
                println!("Received {signal}, shutting down the cluster...");
                return shutdown_instances(cluster_meta);
            }
            Signal::SIGCHLD => reap_children(cluster_meta)?,
            _ => unreachable!("received signal should not have been caught as it was not masked"),
        }
    }
}

/////////////////////////////////////////////////////////////////////
// main
/////////////////////////////////////////////////////////////////////

pub fn main(args: Demo) -> ! {
    if let Err(error) = main_impl(args) {
        eprintln!("{error}");
        std::process::exit(1)
    }
    std::process::exit(0)
}

fn main_impl(args: Demo) -> Result<()> {
    // STEP: command line arguments validation.

    if args.replicaset_count == 0 {
        return Err("too few replicasets: got 0, expected >0".into());
    }

    if args.replication_factor == 0 {
        return Err("too small replication factor: got 0, expected >0".into());
    }

    // STEP: command line arguments transformation.

    let working_directory = match args.working_directory {
        Some(path) => {
            std::fs::create_dir_all(&path)?;
            path
        }
        None => std::env::current_dir()?.join(WORKING_DIRECTORY),
    };
    let data_directory = working_directory.clone();

    let picodata_executable = match args.picodata_executable {
        Some(path) => {
            if !path.exists() {
                let path = path.display();
                let error = format!("{path} does not exist");
                return Err(error.into());
            }

            if path.is_dir() {
                let path = path.display();
                let error = format!("{path} is a directory");
                return Err(error.into());
            }

            path
        }
        None => std::env::current_exe()?,
    };

    // STEP: cluster configuration parameters formation.

    let mut tiers_config = BTreeMap::new();
    tiers_config.insert(
        "default".to_owned(),
        builder::Tier {
            replicasets: args.replicaset_count,
            replication_factor: args.replication_factor,
        },
    );

    let topology_config = builder::Topology {
        tiers: tiers_config,
        ..Default::default()
    };

    let parameters = builder::RunParamsBuilder::default()
        .topology(topology_config)
        .data_dir(data_directory)
        .picodata_path(picodata_executable)
        .daemon(true)
        .build()
        .expect("required fields should be initialized");

    // STEP: run cluster after setup.
    // XXX: if shutdown initiated before or right at the cluster start, it will
    // still cleanup working directory even if user asked not to do so in the
    // arguments list and unfortunately we cannot do anything with that as it is
    // coming from pike itself. Maybe, that is not even a problem to consider?

    std::env::set_var("PICODATA_ADMIN_PASSWORD", ADMIN_PASSWORD);
    std::env::set_var("PICODATA_DEMO_CHILD", "yes");

    println!("Initializing cluster start...\n");
    let cluster_nodes = builder::run(&parameters)?;

    println!("Cluster started successfully:");
    println!("  - replicaset count: {}", args.replicaset_count);
    println!("  - replication factor: {}", args.replication_factor);
    println!("  - orchestrator pid: {}", std::process::id());

    println!("\nSee nodes information below.\n");

    // STEP: print overall cluster information.

    let cluster_meta = fetch_meta(&cluster_nodes)?;
    cluster_meta.iter().for_each(Meta::print);

    // STEP: handle all types of shutdowns by signals.

    handle_signals(&cluster_meta)?;

    // STEP: clean working directory if asked by user.

    clean_data(args.clean_data, &working_directory)?;

    Ok(())
}

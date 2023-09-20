use std::fmt::Display;
use std::ops::ControlFlow;
use std::path::Path;
use std::{env, fs, io, process};

use clap::Parser;
use comfy_table::{ContentArrangement, Table};
use nix::sys::signal;
use nix::sys::termios::{tcgetattr, tcsetattr, SetArg::TCSADRAIN};
use nix::sys::wait::{waitpid, WaitStatus};
use nix::unistd::{self, fork, ForkResult};
use picodata::args::{self, Address, DEFAULT_USERNAME};
use picodata::ipc;
use picodata::tlog;
use picodata::util::{unwrap_or_terminate, validate_and_complete_unix_socket_path};
use picodata::Entrypoint;
use picodata::IpcMessage;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use tarantool::fiber;
use tarantool::network::{client, AsClient, Client, Config};

include!(concat!(env!("OUT_DIR"), "/export_symbols.rs"));

mod test;

fn main() -> ! {
    export_symbols();
    match args::Picodata::parse() {
        args::Picodata::Run(args) => main_run(*args),
        args::Picodata::Test(args) => test::main_test(args),
        args::Picodata::Tarantool(args) => main_tarantool(args),
        args::Picodata::Expel(args) => main_expel(args),
        args::Picodata::Connect(args) => main_connect(args),
        args::Picodata::Sql(args) => main_sql(args),
    }
}

#[macro_export]
macro_rules! tarantool_main {
    (
        $tt_args:expr,
         callback_data: $cb_data:tt,
         callback_data_type: $cb_data_ty:ty,
         callback_body: $cb_body:expr
    ) => {{
        let tt_args: Vec<_> = $tt_args;
        // `argv` is a vec of pointers to data owned by `tt_args`, so
        // make sure `tt_args` outlives `argv`, because the compiler is not
        // gonna do that for you
        let argv = tt_args.iter().map(|a| a.as_ptr()).collect::<Vec<_>>();
        extern "C" fn trampoline(data: *mut libc::c_void) {
            let args = unsafe { Box::from_raw(data as _) };
            let cb = |$cb_data: $cb_data_ty| $cb_body;
            cb(*args)
        }
        let cb_data: $cb_data_ty = $cb_data;
        unsafe {
            picodata::tarantool::main(
                argv.len() as _,
                argv.as_ptr() as _,
                Some(trampoline),
                Box::into_raw(Box::new(cb_data)) as _,
            )
        }
    }};
}

fn main_run(args: args::Run) -> ! {
    // Tarantool implicitly parses some environment variables.
    // We don't want them to affect the behavior and thus filter them out.
    for (k, _) in std::env::vars() {
        // NB: For the moment we'd rather allow LDAP-related variables,
        // but see https://git.picodata.io/picodata/tarantool/-/issues/25.
        let is_relevant = k.starts_with("TT_") || k.starts_with("TARANTOOL_");
        if !k.starts_with("TT_LDAP") && is_relevant {
            std::env::remove_var(k)
        }
    }

    // Tarantool running in a fork (or, to be more percise, the
    // libreadline) modifies termios settings to intercept echoed text.
    //
    // After subprocess termination it's not always possible to
    // restore the settings (e.g. in case of SIGSEGV). At least it
    // tries to. To preserve tarantool console operable, we cache
    // initial termios attributes and restore them manually.
    //
    let tcattr = tcgetattr(0).ok();

    // Intercept and forward signals to the child. As for the child
    // itself, one shouldn't worry about setting up signal handlers -
    // Tarantool does that implicitly.
    static mut CHILD_PID: Option<libc::c_int> = None;
    static mut SIGNALLED: Option<libc::c_int> = None;
    extern "C" fn sigh(sig: libc::c_int) {
        unsafe {
            // Only a few functions are allowed in signal handlers.
            // Read twice `man 7 signal-safety`.
            if let Some(pid) = CHILD_PID {
                libc::kill(pid, sig);
            }
            SIGNALLED = Some(sig);
        }
    }
    let sigaction = signal::SigAction::new(
        signal::SigHandler::Handler(sigh),
        // It's important to use SA_RESTART flag here.
        // Otherwise, waitpid() could return EINTR,
        // but we don't want dealing with it.
        signal::SaFlags::SA_RESTART,
        signal::SigSet::empty(),
    );
    unsafe {
        signal::sigaction(signal::SIGHUP, &sigaction).unwrap();
        signal::sigaction(signal::SIGINT, &sigaction).unwrap();
        signal::sigaction(signal::SIGTERM, &sigaction).unwrap();
        signal::sigaction(signal::SIGUSR1, &sigaction).unwrap();
    }

    let parent = unistd::getpid();
    let mut entrypoint = Entrypoint::StartDiscover {};
    loop {
        eprintln!("[supervisor:{parent}] running {entrypoint:?}");

        let (from_child, to_parent) =
            ipc::channel::<IpcMessage>().expect("ipc channel creation failed");
        let (from_parent, to_child) = ipc::pipe().expect("ipc pipe creation failed");

        let pid = unsafe { fork() };
        match pid.expect("fork failed") {
            ForkResult::Child => {
                drop(from_child);
                drop(to_child);

                let rc = tarantool_main!(
                    args.tt_args().unwrap(),
                    callback_data: (entrypoint, args, to_parent, from_parent),
                    callback_data_type: (Entrypoint, args::Run, ipc::Sender<IpcMessage>, ipc::Fd),
                    callback_body: {
                        // We don't want a child to live without a supervisor.
                        //
                        // Usually, supervisor waits for child forever and retransmits
                        // termination signals. But if the parent is killed with a SIGKILL
                        // there's no way to pass anything.
                        //
                        // This fiber serves as a fuse - it tries to read from a pipe
                        // (that supervisor never writes to), and if the writing end is
                        // closed, it means the supervisor has terminated.
                        let fuse = fiber::Builder::new()
                            .name("supervisor_fuse")
                            .func(move || {
                                use ::tarantool::ffi::tarantool::CoIOFlags;
                                use ::tarantool::coio::coio_wait;
                                coio_wait(*from_parent, CoIOFlags::READ, f64::INFINITY).ok();
                                tlog!(Warning, "Supervisor terminated, exiting");
                                std::process::exit(0);
                        });
                        std::mem::forget(fuse.start());

                        entrypoint.exec(args, to_parent)
                    }
                );
                std::process::exit(rc);
            }
            ForkResult::Parent { child } => {
                unsafe { CHILD_PID = Some(child.into()) };
                drop(from_parent);
                drop(to_parent);

                let msg = from_child.recv();

                let status = waitpid(child, None);

                // Restore termios configuration as planned
                if let Some(tcattr) = tcattr.as_ref() {
                    tcsetattr(0, TCSADRAIN, tcattr).unwrap();
                }

                if let Some(sig) = unsafe { SIGNALLED } {
                    eprintln!("[supervisor:{parent}] got signal {sig}");
                }

                match &msg {
                    Ok(msg) => {
                        eprintln!("[supervisor:{parent}] ipc message from child: {msg:?}");
                    }
                    Err(rmp_serde::decode::Error::InvalidMarkerRead(e))
                        if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        eprintln!("[supervisor:{parent}] no ipc message from child");
                    }
                    Err(e) => {
                        eprintln!(
                            "[supervisor:{parent}] failed reading ipc message from child: {e}"
                        );
                    }
                }

                let status = status.unwrap();
                match status {
                    nix::sys::wait::WaitStatus::Exited(pid, rc) => {
                        eprintln!("[supervisor:{parent}] subprocess {pid} exited with code {rc}");
                    }
                    nix::sys::wait::WaitStatus::Signaled(pid, signal, core_dumped) => {
                        eprintln!(
                            "[supervisor:{parent}] subprocess {pid} was signaled with {signal}"
                        );
                        if core_dumped {
                            eprintln!("[supervisor:{parent}] core dumped");
                        }
                    }
                    status => {
                        eprintln!(
                            "[supervisor:{parent}] subprocess finished with status: {status:?}"
                        );
                    }
                }

                if let Ok(msg) = msg {
                    entrypoint = msg.next_entrypoint;
                    if msg.drop_db {
                        rm_tarantool_files(&args.data_dir);
                    }
                } else {
                    let rc = match status {
                        WaitStatus::Exited(_, rc) => rc,
                        WaitStatus::Signaled(_, sig, _) => sig as _,
                        s => unreachable!("unexpected exit status {:?}", s),
                    };
                    std::process::exit(rc);
                }
            }
        };
    }
}

fn rm_tarantool_files(data_dir: &str) {
    std::fs::read_dir(data_dir)
        .expect("[supervisor] failed reading data_dir")
        .map(|entry| entry.expect("[supervisor] failed reading directory entry"))
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .filter(|f| {
            f.extension()
                .map(|ext| ext == "xlog" || ext == "snap")
                .unwrap_or(false)
        })
        .for_each(|f| {
            eprintln!("[supervisor] removing file: {}", f.to_string_lossy());
            std::fs::remove_file(f).unwrap();
        });
}

fn main_tarantool(args: args::Tarantool) -> ! {
    // XXX: `argv` is a vec of pointers to data owned by `tt_args`, so
    // make sure `tt_args` outlives `argv`, because the compiler is not
    // gonna do that for you
    let tt_args: Vec<_> = args.tt_args().unwrap();
    let argv = tt_args.iter().map(|a| a.as_ptr()).collect::<Vec<_>>();

    let rc = unsafe {
        picodata::tarantool::main(
            argv.len() as _,
            argv.as_ptr() as _,
            None,
            std::ptr::null_mut(),
        )
    };

    std::process::exit(rc);
}

fn main_expel(args: args::Expel) -> ! {
    let rc = tarantool_main!(
        args.tt_args().unwrap(),
        callback_data: (args,),
        callback_data_type: (args::Expel,),
        callback_body: {
            ::tarantool::fiber::block_on(picodata::tt_expel(args))
        }
    );
    std::process::exit(rc);
}

fn get_password_from_file(path: &str) -> Result<String, String> {
    let content = std::fs::read_to_string(path).map_err(|e| {
        format!(r#"can't read password from password file by "{path}", reason: {e}"#)
    })?;

    let password = content
        .lines()
        .next()
        .ok_or("Empty password file".to_string())?
        .trim();

    if password.is_empty() {
        return Ok(String::new());
    }

    Ok(password.into())
}

fn connect_and_start_interacitve_console(args: args::Connect) -> Result<(), String> {
    let endpoint = if args.address_as_socket {
        validate_and_complete_unix_socket_path(&args.address)?
    } else {
        let address = args
            .address
            .parse::<Address>()
            .map_err(|msg| format!("invalid format of address argument: {msg}"))?;
        let user = address.user.unwrap_or(args.user);

        let password = if user == DEFAULT_USERNAME {
            String::new()
        } else if let Some(path) = args.password_file {
            get_password_from_file(&path)?
        } else {
            let prompt = format!("Enter password for {user}: ");
            picodata::util::prompt_password(&prompt)
                .map_err(|e| format!("\nFailed to prompt for a password: {e}"))?
        };

        format!(
            "{}:{}@{}:{}?auth_type={}",
            user, password, address.host, address.port, args.auth_method
        )
    };

    tarantool::lua_state()
        .exec_with(
            r#"local code, arg = ...
            return load(code, '@src/connect.lua')(arg)"#,
            (include_str!("connect.lua"), endpoint),
        )
        .map_err(|e| e.to_string())?;

    Ok(())
}

fn main_connect(args: args::Connect) -> ! {
    let rc = tarantool_main!(
        args.tt_args().unwrap(),
        callback_data: args,
        callback_data_type: args::Connect,
        callback_body: {
            unwrap_or_terminate(connect_and_start_interacitve_console(args));
            std::process::exit(0)
        }
    );

    std::process::exit(rc);
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct ColDesc {
    name: String,
    #[serde(rename = "type")]
    ty: String,
}

impl Display for ColDesc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct RowSet {
    metadata: Vec<ColDesc>,
    rows: Vec<Vec<rmpv::Value>>,
}

impl Display for RowSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(self.metadata.iter());

        for row in &self.rows {
            table.add_row(row);
        }

        f.write_fmt(format_args!("{table}\n"))?;
        f.write_fmt(format_args!("({} rows)", self.rows.len()))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct RowCount {
    row_count: usize,
}

impl Display for RowCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.row_count))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(untagged)]
enum ResultSet {
    RowSet(Vec<RowSet>),
    RowCount(Vec<RowCount>),
    Error(Option<()>, String),
}

impl Display for ResultSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResultSet::RowSet(s) => f.write_fmt(format_args!(
                "{}",
                s.first().expect(
                    "RowSet is represented as a Vec<Vec<Rows>> where outer vec always has lenghth equal to 1"
                )
            )),
            ResultSet::Error(_, message) => f.write_str(message),
            ResultSet::RowCount(c) => f.write_fmt(format_args!("{}", c.first().expect(
                "RowCount response always consists of a Vec containing the only entry"
            ))),
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum SqlReplError {
    #[error("{0}")]
    Client(#[from] client::Error),

    #[error("Failed to prompt for a password: {0}")]
    Prompt(io::Error),

    #[error("{0}")]
    Io(io::Error),
}

const HISTORY_FILE_NAME: &str = ".picodata_history";

async fn sql_repl_main(args: args::ConnectSql) {
    unwrap_or_terminate(sql_repl(args).await);
    std::process::exit(0)
}

// Ideally we should have an enum for all commands. For now we have only two options, usual line
// and only one special command. To not overengineer things at this point just handle this as ifs.
// When the set of commands grows it makes total sense to transform this to clear parse/execute pipeline
// and separate enum variants for each command variant.
fn handle_special_sequence(line: &str) -> Result<ControlFlow<String>, SqlReplError> {
    if line != "\\e" {
        eprintln!("Unknown special sequence");
        return Ok(ControlFlow::Continue(()));
    }

    let editor = match env::var_os("EDITOR") {
        Some(e) => e,
        None => {
            eprintln!("EDITOR environment variable is not set");
            return Ok(ControlFlow::Continue(()));
        }
    };

    let temp = tempfile::Builder::new()
        .suffix(".sql")
        .tempfile()
        .map_err(SqlReplError::Io)?;
    let status = process::Command::new(&editor)
        .arg(temp.path())
        .status()
        .map_err(SqlReplError::Io)?;

    if !status.success() {
        eprintln!("{:?} returned non zero exit status: {}", editor, status);
        return Ok(ControlFlow::Continue(()));
    }

    let line = fs::read_to_string(temp.path()).map_err(SqlReplError::Io)?;
    Ok(ControlFlow::Break(line))
}

async fn sql_repl(args: args::ConnectSql) -> Result<(), SqlReplError> {
    let user = args.address.user.as_ref().unwrap_or(&args.user).clone();

    let password = if user == DEFAULT_USERNAME {
        String::new()
    } else if let Some(path) = args.password_file {
        unwrap_or_terminate(get_password_from_file(&path))
    } else {
        let prompt = format!("Enter password for {user}: ");
        match picodata::util::prompt_password(&prompt) {
            Ok(password) => password,
            Err(e) => {
                return Err(SqlReplError::Prompt(e));
            }
        }
    };

    let client = Client::connect_with_config(
        &args.address.host,
        args.address.port.parse().unwrap(),
        Config {
            creds: Some((user, password)),
        },
    )
    .await?;

    // Check if connection is valid. We need to do it because connect is lazy
    // and we want to check whether authentication have succeeded or not
    client.call("box.schema.user.info", &()).await?;

    // It is deprecated because of unexpected behavior on windows.
    // We're ok with that.
    #[allow(deprecated)]
    let history_file = env::home_dir()
        .unwrap_or_default()
        .join(Path::new(HISTORY_FILE_NAME));

    let mut rl = DefaultEditor::new().unwrap();
    rl.load_history(&history_file).ok();

    loop {
        let readline = rl.readline("picosql :) ");
        match readline {
            Ok(line) => {
                let line = {
                    if line.starts_with('\\') {
                        match handle_special_sequence(&line)? {
                            ControlFlow::Continue(_) => continue,
                            ControlFlow::Break(line) => line,
                        }
                    } else {
                        line
                    }
                };

                if line.is_empty() {
                    continue;
                }

                let response = client.call("pico.sql", &(&line,)).await?;
                let res: ResultSet = response
                    .decode()
                    .expect("Response must have the shape of ResultSet structure");
                println!("{res}");

                rl.add_history_entry(line.as_str()).ok();
                rl.save_history(&history_file).ok();
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}

fn main_sql(args: args::ConnectSql) -> ! {
    let rc = tarantool_main!(
        args.tt_args().unwrap(),
        callback_data: (args,),
        callback_data_type: (args::ConnectSql,),
        callback_body: {
            ::tarantool::fiber::block_on(sql_repl_main(args))
        }
    );
    std::process::exit(rc);
}

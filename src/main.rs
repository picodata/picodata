use clap::Parser;
use nix::sys::signal;
use nix::sys::termios::{tcgetattr, tcsetattr, SetArg::TCSADRAIN};
use nix::sys::wait::{waitpid, WaitStatus};
use nix::unistd::{self, fork, ForkResult};
use picodata::args::{self, Address};
use picodata::ipc;
use picodata::tlog;
use picodata::util::validate_and_complete_unix_socket_path;
use picodata::util::Either::{Left, Right};
use picodata::Entrypoint;
use picodata::IpcMessage;
use tarantool::fiber;

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

fn main_connect(args: args::Connect) -> ! {
    fn unwrap_or_terminate<T>(res: Result<T, String>) -> T {
        match res {
            Ok(value) => value,
            Err(msg) => {
                tlog!(Critical, "{msg}");
                std::process::exit(1);
            }
        }
    }

    let rc = tarantool_main!(
        args.tt_args().unwrap(),
        callback_data: args,
        callback_data_type: args::Connect,
        callback_body: {
            let endpoint = unwrap_or_terminate({
                if args.address_as_socket {
                    validate_and_complete_unix_socket_path(&args.address).map(Right)
                } else {
                    args.address.parse::<Address>()
                        .map_err(|msg| format!("invalid format of address argument: {msg}"))
                        .map(Left)
                }
            });

            let raw_endpoint = match endpoint {
                Left(address) => {
                    let user = address.user.as_ref().unwrap_or(&args.user).clone();
                    let prompt = format!("Enter password for {user}: ");
                    let password = unwrap_or_terminate(
                        picodata::util::prompt_password(&prompt)
                            .map_err(|e| format!("Failed to prompt for a password: {e}")),
                    );

                    let password = unwrap_or_terminate(
                        password.ok_or("\nNo password provided".into()),
                    );

                    format!(
                        "{user}:{password}@{}:{}?auth_type={}",
                        address.host, address.port, args.auth_method
                    )
                }
                Right(socket_path) => socket_path,
            };

            unwrap_or_terminate(::tarantool::lua_state().exec_with(
                r#"local code, arg = ...
                return load(code, '@src/connect.lua')(arg)"#,
                (include_str!("connect.lua"), raw_endpoint)
            ).map_err(|e| e.to_string()));
        }
    );

    std::process::exit(rc);
}

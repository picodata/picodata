use std::path::Path;

use nix::sys::signal;
use nix::sys::termios::{tcgetattr, tcsetattr, SetArg::TCSADRAIN};
use nix::sys::wait::{waitpid, WaitStatus};
use nix::unistd::{self, fork, ForkResult};
use tarantool::fiber;

use crate::cli::args;
use crate::config::PicodataConfig;
use crate::{ipc, tarantool_main, tlog, Entrypoint, IpcMessage};

pub fn main(args: args::Run) -> ! {
    let tt_args = args.tt_args().unwrap();

    let res = PicodataConfig::init(args);
    let config = crate::unwrap_ok_or!(res,
        Err(e) => {
            tlog!(Error, "{e}");
            std::process::exit(1);
        }
    );

    // Set the log level as soon as possible to not miss any messages during
    // initialization.
    tlog::set_log_level(config.instance.log_level());

    if let Some(filename) = &config.instance.service_password_file {
        if let Err(e) = crate::pico_service::read_pico_service_password_from_file(filename) {
            tlog!(Error, "{e}");
            std::process::exit(1);
        }
    }

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
                    tt_args,
                    // Note that we don't really need to pass the `config` here,
                    // because it's stored in the global variable which we can access from anywhere.
                    // But we still pass it explicitly just to make sure it's initialized at this early point.
                    callback_data: (entrypoint, config, to_parent, from_parent),
                    callback_data_type: (Entrypoint, &PicodataConfig, ipc::Sender<IpcMessage>, ipc::Fd),
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

                        if let Err(e) = entrypoint.exec(config, to_parent) {
                            tlog!(Critical, "{e}");
                            std::process::exit(1);
                        }
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
                        rm_tarantool_files(&config.instance.data_dir());
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

fn rm_tarantool_files(data_dir: impl AsRef<Path>) {
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

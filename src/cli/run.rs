use nix::sys::signal;
use nix::sys::termios::{tcgetattr, tcsetattr, SetArg::TCSADRAIN};
use nix::sys::wait::{waitpid, WaitStatus};
use nix::unistd::{self, fork, ForkResult};
use tarantool::fiber;

use crate::cli::args;
use crate::config::PicodataConfig;
use crate::{ipc, tlog, Entrypoint, IpcMessage};

pub fn main(args: args::Run) -> ! {
    let tt_args = args.tt_args().unwrap();

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

                super::tarantool::main_cb(&tt_args, || {
                    // Note: this function may log something into the tarantool's logger, which means it must be done within
                    // the `tarantool::main_cb` otherwise everything will break. The thing is, tarantool's logger needs to know things
                    // about the current thread and the way it does that is by accessing the `cord_ptr` global variable. For the main
                    // thread this variable get's initialized in the tarantool's main function. But if we call the logger before this
                    // point another mechanism called cord_on_demand will activate and initialize the cord in a conflicting way.
                    // This causes a crash when the cord gets deinitialized during the normal shutdown process, because it leads to double free.
                    // (This wouldn't be a problem if we just skipped the deinitialization for the main cord, because we don't actually need it
                    // as the OS will cleanup all the resources anyway, but this is a different story altogether)
                    let config = PicodataConfig::init(args)?;

                    if let Some(filename) = &config.instance.service_password_file {
                        crate::pico_service::read_pico_service_password_from_file(filename)?;
                    }

                    // We don't want a child to live without a supervisor.
                    //
                    // Usually, supervisor waits for child forever and retransmits
                    // termination signals. But if the parent is killed with a SIGKILL
                    // there's no way to pass anything.
                    //
                    // This fiber serves as a fuse - it tries to read from a pipe
                    // (that supervisor never writes to), and if the writing end is
                    // closed, it means the supervisor has terminated.
                    fiber::Builder::new()
                        .name("supervisor_fuse")
                        .func(move || {
                            use ::tarantool::coio::coio_wait;
                            use ::tarantool::ffi::tarantool::CoIOFlags;
                            coio_wait(*from_parent, CoIOFlags::READ, f64::INFINITY).ok();
                            tlog!(Warning, "Supervisor terminated, exiting");
                            std::process::exit(0);
                        })
                        .start_non_joinable()?;

                    std::panic::set_hook(Box::new(|info| {
                        tlog!(Critical, "{info}");
                        let backtrace = std::backtrace::Backtrace::capture();
                        tlog!(Critical, "backtrace:\n{}", backtrace);
                        tlog!(Critical, "aborting due to panic");
                        std::process::abort();
                    }));

                    config.log_config_params();

                    // Note that we don't really need to pass the `config` here,
                    // because it's stored in the global variable which we can
                    // access from anywhere. But we still pass it explicitly just
                    // to make sure it's initialized at this early point.
                    entrypoint.exec(config, to_parent)
                })
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

                let Ok(msg) = msg else {
                    let rc = match status {
                        WaitStatus::Exited(_, rc) => rc,
                        WaitStatus::Signaled(_, sig, _) => sig as _,
                        s => unreachable!("unexpected exit status {:?}", s),
                    };
                    std::process::exit(rc);
                };

                entrypoint = msg.next_entrypoint;
            }
        };
    }
}

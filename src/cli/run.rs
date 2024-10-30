use crate::cli::args;
use crate::cli::tarantool::main_cb_no_exit;
use crate::config::PicodataConfig;
use crate::ipc;
use crate::ipc::check_return_code;
use crate::start;
use crate::tlog;
use crate::traft::Result;
use crate::Entrypoint;
use std::ffi::CString;
use std::io::Read;
use std::io::Write;
use tarantool::c_str;
use tarantool::error::Error as TntError;

pub fn main(mut args: args::Run) -> ! {
    // Save the argv before enterring tarantool, because tarantool will fuss about with them
    let copied_argv = crate::cli::args::copy_argv();

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

    let input_entrypoint_pipe = args.entrypoint_fd.take();
    let mut output_entrypoint_pipe = None;

    let rc = main_cb_no_exit(&tt_args, || -> Result<()> {
        // Note: this function may log something into the tarantool's logger, which means it must be done within
        // the `tarantool::main_cb` otherwise everything will break. The thing is, tarantool's logger needs to know things
        // about the current thread and the way it does that is by accessing the `cord_ptr` global variable. For the main
        // thread this variable get's initialized in the tarantool's main function. But if we call the logger before this
        // point another mechanism called cord_on_demand will activate and initialize the cord in a conflicting way.
        // This causes a crash when the cord gets deinitialized during the normal shutdown process, because it leads to double free.
        // (This wouldn't be a problem if we just skipped the deinitialization for the main cord, because we don't actually need it
        // as the OS will cleanup all the resources anyway, but this is a different story altogether)
        let config = PicodataConfig::init(args)?;

        // Set panic hook as soon as possible (not possible to do
        // earlier, because of logging, see comment above)
        std::panic::set_hook(Box::new(|info| {
            tlog!(Critical, "{info}");
            let backtrace = std::backtrace::Backtrace::capture();
            tlog!(Critical, "backtrace:\n{}", backtrace);
            tlog!(Critical, "aborting due to panic");
            std::process::abort();
        }));

        if let Some(filename) = &config.instance.service_password_file {
            crate::pico_service::read_pico_service_password_from_file(filename)?;
        }

        config.log_config_params();

        let entrypoint = maybe_read_entrypoint_from_pipe(input_entrypoint_pipe)?;

        // Note that we don't really need to pass the `config` here,
        // because it's stored in the global variable which we can
        // access from anywhere. But we still pass it explicitly just
        // to make sure it's initialized at this early point.
        let next_entrypoint = start(config, entrypoint)?;

        if let Some(next_entrypoint) = &next_entrypoint {
            debug_assert_ne!(next_entrypoint, &Entrypoint::StartDiscover);
            // If picodata invocation starts with a --entrypoint-fd then it goes
            // into start_boot or start_join, both of which end with a normal
            // execution.
            debug_assert!(input_entrypoint_pipe.is_none());
            let pipe = write_entrypoint_to_pipe(next_entrypoint)?;
            output_entrypoint_pipe = Some(pipe);

            #[rustfmt::skip]
            tlog!(Info, "restarting process to proceed with next entrypoint {next_entrypoint:?}");

            // NOTE: we would like to just call restart_current_process here,
            // but we can't, because tarantool doesn't use CLOEXEC flag for
            // sockets, so we'll just fail with address in use error. So instead
            // we tell tarantool to shutdown explicitly, wait and only then
            // restart the process.
            crate::tarantool::exit(0);
        };

        // Return `Ok` from the callback to proceed to the tarantool event loop.
        return Ok(());
    });

    if let Some(fd) = output_entrypoint_pipe {
        // If logger is configured to stderr, tarantool closes the stderr fd
        // when destroying the logger, so we must reattach it back...
        reattach_stderr();

        // Disable the alarm, because otherwise the process terminates on the
        // SIGALRM signal when calling execvp bellow.
        // SAFETY: always safe
        unsafe {
            libc::alarm(0);
        }

        // Tarantool locks the WAL directory to prevent multiple processes
        // running in the same directory. But linux will not release the
        // file system lock automatically when exec-ing (see `man 2 flock`),
        // so we must unlock explicitly.
        if let Err(e) = unlock_wal_directory() {
            // At this point tarantool has been destroyed, so we can't use tlog! anymore
            eprintln!("teardown before rebootstrap failed: {e}");
            std::process::abort();
        }

        let mut argv = copied_argv;
        argv.push(c_str!("--entrypoint-fd").into());
        let cstr = CString::new(fd.to_string()).expect("no nuls");
        argv.push(cstr);

        // Disable the destructor, so that the read half of the pipe is not closed yet
        std::mem::forget(fd);

        restart_current_process(&argv);
    }

    std::process::exit(rc);
}

/// Reads the entrypoint from the `fd` pipe.
/// Returns `StartDiscover` if `fd` is `None`.
fn maybe_read_entrypoint_from_pipe(fd: Option<u32>) -> Result<Entrypoint, TntError> {
    let Some(fd) = fd else {
        // No fd, means it's the initial invocation
        return Ok(Entrypoint::StartDiscover);
    };

    // SAFETY: safe because we don't use the numeric `fd` anymore
    let mut fd = unsafe { ipc::Fd::from_raw(fd) };
    let mut data = vec![];
    fd.read_to_end(&mut data)?;
    let entrypoint = rmp_serde::from_slice(&data)?;
    tlog!(Info, "read entrypoint {entrypoint:?} from pipe '{fd:?}'");

    // The read half of the pipe is closed here
    drop(fd);

    Ok(entrypoint)
}

/// Opens a pipe and writes the `entrypoint` into it.
/// Returns the output pipe fd.
fn write_entrypoint_to_pipe(entrypoint: &Entrypoint) -> Result<ipc::Fd, TntError> {
    let (rx, mut tx) = ipc::pipe()?;

    #[rustfmt::skip]
    tlog!(Info, "saving entrypoint {entrypoint:?} to pipe '{tx:?}'");

    let data = rmp_serde::to_vec_named(entrypoint)?;
    tx.write_all(&data)?;

    // The write half of the pipe is closed here
    drop(tx);

    Ok(rx)
}

/// Calls execvp with the current process' argc & argv.
fn restart_current_process(args: &[CString]) -> ! {
    let mut argv = vec![];
    for arg in args {
        argv.push(arg.as_ptr());
    }
    argv.push(std::ptr::null());

    // SAFETY: safe because pointers are valid
    let rc = unsafe { libc::execvp(argv[0], argv.as_ptr()) };

    // In case of success the execution diverges
    assert_eq!(rc, -1);
    let e = std::io::Error::last_os_error();
    eprintln!("execvp failed: {e}");
    std::process::abort();
}

fn unlock_wal_directory() -> std::io::Result<()> {
    extern "C" {
        static mut wal_dir_lock: i32;
    }

    if unsafe { wal_dir_lock == -1 } {
        // Not locked
        return Ok(());
    }

    // SAFETY: always safe
    let rc = unsafe { libc::flock(wal_dir_lock, libc::LOCK_UN) };

    check_return_code(rc)?;

    Ok(())
}

fn reattach_stderr() {
    // SAFETY: always safe
    let rc = unsafe { libc::fcntl(libc::STDERR_FILENO, libc::F_GETFD) };
    if rc != -1 {
        // stderr is still attached
        return;
    }

    // SAFETY: always safe
    unsafe { libc::dup2(libc::STDOUT_FILENO, libc::STDERR_FILENO) };
}

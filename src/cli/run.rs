use crate::{
    cli::{args, tarantool::main_cb_no_exit},
    config::PicodataConfig,
    ipc::{self, check_return_code},
    start, tlog,
    traft::Result,
    Entrypoint,
};
use std::ffi::OsString;
use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::Path;
use tarantool::error::Error as TntError;

#[cfg(feature = "error_injection")]
use crate::error_injection;

pub const PICODATA_COOKIE: &'static str = ".picodata-cookie";

/// See [`write_entrypoint_to_file`].
const ENTRYPOINT_FILE: &'static str = ".entrypoint";

pub fn main(mut args: args::Run) -> ! {
    // Used to handle parent death in `Demo`.
    if std::env::var("PICODATA_DEMO_CHILD") == Ok("yes".to_string()) {
        crate::cli::util::set_parent_death_handler();
    }

    // Save the argv before entering tarantool, because tarantool will fuss about with them
    let copied_argv: Vec<OsString> = std::env::args_os().skip(1).collect();

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

    let input_entrypoint_fd = args.entrypoint_fd.take();
    let mut output_entrypoint_fd = None;

    let rc = main_cb_no_exit(&tt_args, || -> Result<()> {
        #[cfg(feature = "error_injection")]
        error_injection::set_from_env();

        tarantool::error::BoxError::set_display_fallback(crate::traft::error::box_error_display)
            .expect("first time");

        // Note: this function may log something into the tarantool's logger, which means it must be done within
        // the `tarantool::main_cb` otherwise everything will break. The thing is, tarantool's logger needs to know things
        // about the current thread and the way it does that is by accessing the `cord_ptr` global variable. For the main
        // thread this variable get's initialized in the tarantool's main function. But if we call the logger before this
        // point another mechanism called cord_on_demand will activate and initialize the cord in a conflicting way.
        // This causes a crash when the cord gets deinitialized during the normal shutdown process, because it leads to double free.
        // (This wouldn't be a problem if we just skipped the deinitialization for the main cord, because we don't actually need it
        // as the OS will cleanup all the resources anyway, but this is a different story altogether)
        let config = PicodataConfig::init(args)?;

        let info = crate::info::VersionInfo::current();
        #[rustfmt::skip]
        tlog!(Info, "Picodata {} {} {}", info.picodata_version, info.build_type, info.build_profile);

        config.log_config_params();

        let cookie_path = config.instance.instance_dir().join(PICODATA_COOKIE);
        if cookie_path.exists() {
            crate::pico_service::read_pico_service_password_from_file(cookie_path)?;
        }

        let entrypoint = maybe_read_entrypoint_from_fd(input_entrypoint_fd)?;

        // Note that we don't really need to pass the `config` here,
        // because it's stored in the global variable which we can
        // access from anywhere. But we still pass it explicitly just
        // to make sure it's initialized at this early point.
        let next_entrypoint = start(config, entrypoint)?;

        if let Some(next_entrypoint) = &next_entrypoint {
            // Next entrypoint cannot be discovery, because initialization starts
            // with discovery and is not passed afterwards anywhere. Also, no other
            // entrypoints return next entrypoint as a discovery, because it is a
            // logical error on bootstraping cluster members.
            debug_assert!(!matches!(next_entrypoint, Entrypoint::StartDiscover));

            let fd = write_entrypoint_to_file(next_entrypoint, config.instance.instance_dir())?;
            output_entrypoint_fd = Some(fd);

            // Blocks with iproto already listening and the re-exec still
            // pending, so a test can open a connection, lift the injection and
            // watch what the exec does to that connection.
            crate::error_injection!(block "BLOCK_BEFORE_REBOOTSTRAP");

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
        Ok(())
    });

    if let Some(fd) = output_entrypoint_fd {
        // SIGALRM may interrupt the restart via execvp, so we disable it.
        disable_clock_signal();

        // Tarantool locks the WAL directory to prevent multiple processes
        // running in the same directory. But linux will not release the
        // file system lock automatically when exec-ing (see `man 2 flock`),
        // so we must unlock explicitly.
        if let Err(e) = unlock_wal_directory() {
            // At this point tarantool has been destroyed, so we can't use tlog! anymore
            crate::eprintln_buffered!("teardown before rebootstrap failed: {e}");
            std::process::abort();
        }

        // If we enter the PostJoin stage, it will be our second rebootstrap. As
        // long as we don't prioritize the latest passed `--entrypoint-fd` over
        // other ones, we must remove previous entrypoint arguments, otherwise
        // we will panic with repeating arguments.
        let mut argv: Vec<OsString> = copied_argv
            .into_iter()
            .filter(|predicate| {
                let arg = predicate.to_str().unwrap();
                !arg.starts_with("--entrypoint-fd=")
            })
            .collect();

        // Append newest entrypoint file descriptor parameter to reboostrap from.
        let new_entrypoint_fd_arg = format!("--entrypoint-fd={}", *fd).into();
        argv.push(new_entrypoint_fd_arg);

        // Disable the destructor, so that the fd is not closed yet
        std::mem::forget(fd);

        restart_current_process(&argv);
    }

    std::process::exit(rc);
}

/// Reads the entrypoint from the `fd` written by [`write_entrypoint_to_file`].
/// Returns `StartDiscover` if `fd` is `None`.
fn maybe_read_entrypoint_from_fd(fd: Option<u32>) -> Result<Entrypoint, TntError> {
    let Some(fd) = fd else {
        // No fd, means it's the initial invocation
        return Ok(Entrypoint::StartDiscover);
    };

    // SAFETY: safe because we don't use the numeric `fd` anymore
    let mut fd = unsafe { ipc::Fd::from_raw(fd) };
    let mut data = vec![];
    fd.read_to_end(&mut data)?;
    let entrypoint = rmp_serde::from_slice(&data)?;
    tlog!(Info, "read entrypoint {entrypoint:?} from '{fd:?}'");

    // The fd is closed here, which frees the unlinked file
    drop(fd);

    Ok(entrypoint)
}

/// Creates an unlinked file and writes the `entrypoint` into it.
/// Returns a read-only fd of the file to pass into the self-exec.
///
/// Note that file is opened without O_CLOEXEC so that it survives the self-exec.
/// The file is also unlinked immediately after creation so the data lives until
/// the file is closed explicitly or process exits.
///
/// Note also that we used to use a self-pipe for the purpose of passing the
/// `entrypoint` during self-exec. But that approach had a limitation of the
/// pipe size, when trying to write a payload which exceeded the pipe's buffer
/// size the process would simply deadlock forever.
///
/// By default on linux the pipe buffer size is just 64K and will also drop to
/// 8K in case the current system user has exceeded it's
/// `/proc/sys/fs/pipe-user-pages-soft` limit. We were observing this in our CI
/// as flaked tests.
///
/// Using a file allows us to have a pretty much unlimitted size of the
/// `entrypoint` payload.
fn write_entrypoint_to_file(
    entrypoint: &Entrypoint,
    instance_dir: &Path,
) -> Result<ipc::Fd, TntError> {
    let data = rmp_serde::to_vec(entrypoint)?;
    let encoded_size = data.len();

    let path = instance_dir.join(ENTRYPOINT_FILE);

    #[rustfmt::skip]
    tlog!(Info, "saving entrypoint (encoded size: {encoded_size}) {entrypoint:?} to file '{}'", path.display());

    std::fs::write(&path, &data)?;
    let fd = ipc::open_read_only_inheritable(&path)?;
    std::fs::remove_file(&path)?;

    Ok(fd)
}

/// Calls execvp with the current process' argc & argv.
fn restart_current_process(args: &[OsString]) -> ! {
    let exe = std::env::current_exe().expect("must have current_exe");
    let e = std::process::Command::new(exe).args(args).exec();
    crate::eprintln_buffered!("execvp failed: {e}");
    std::process::abort();
}

/// Disable tarantool's low resolution clock timer signal.
fn disable_clock_signal() {
    // Defined in tarantool (use ctags or grep).
    extern "C" {
        fn clock_lowres_signal_reset();
    }

    // SAFETY: always safe
    unsafe { clock_lowres_signal_reset() };
}

fn unlock_wal_directory() -> std::io::Result<()> {
    // Defined in tarantool (use ctags or grep).
    extern "C" {
        // Undoes the effects of path_lock().
        // See box_cfg_xc() in tarantool for more information.
        fn path_unlock(fd: core::ffi::c_int) -> core::ffi::c_int;

        // A file descriptor for locking the wal dir.
        static wal_dir_lock: i32;
    }

    // SAFETY: wal_dir_lock is set once from the main thread.
    if unsafe { wal_dir_lock } == -1 {
        // Not locked
        return Ok(());
    }

    // SAFETY: always safe
    let rc = unsafe { path_unlock(wal_dir_lock) };
    check_return_code(rc)?;

    Ok(())
}

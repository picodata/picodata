use clap::Parser;
use nix::sys::signal;
use nix::sys::termios::{tcgetattr, tcsetattr, SetArg::TCSADRAIN};
use nix::sys::wait::{waitpid, WaitStatus};
use nix::unistd::{self, fork, ForkResult};
use picodata::args;
use picodata::ipc;
use picodata::tlog;
use picodata::Entrypoint;
use picodata::InnerTest;
use picodata::IpcMessage;
use tarantool::fiber;

fn main() -> ! {
    match args::Picodata::parse() {
        args::Picodata::Run(args) => main_run(args),
        args::Picodata::Test(args) => main_test(args),
        args::Picodata::Tarantool(args) => main_tarantool(args),
        args::Picodata::Expel(args) => main_expel(args),
    }
}

macro_rules! tarantool_main {
    (
        $tt_args:expr,
         callback_data: $cb_data:tt,
         callback_data_type: $cb_data_ty:ty,
         callback_body: $cb_body:expr
    ) => {{
        let tt_args = $tt_args;
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
        if k.starts_with("TT_") || k.starts_with("TARANTOOL_") {
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
        println!("[supervisor:{parent}] running {entrypoint:?}");

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

                let msg = from_child.recv().ok();

                let status = waitpid(child, None);

                // Restore termios configuration as planned
                if let Some(tcattr) = tcattr.as_ref() {
                    tcsetattr(0, TCSADRAIN, tcattr).unwrap();
                }

                if let Some(sig) = unsafe { SIGNALLED } {
                    println!("[supervisor:{parent}] got signal {sig}");
                }

                println!("[supervisor:{parent}] ipc message from child: {msg:?}");

                let status = status.unwrap();
                println!("[supervisor:{parent}] subprocess finished: {status:?}");

                if let Some(msg) = msg {
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
            println!("[supervisor] removing file: {}", f.to_string_lossy());
            std::fs::remove_file(f).unwrap();
        });
}

fn main_tarantool(args: args::Tarantool) -> ! {
    // XXX: `argv` is a vec of pointers to data owned by `tt_args`, so
    // make sure `tt_args` outlives `argv`, because the compiler is not
    // gonna do that for you
    let tt_args = args.tt_args();
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
            picodata::tt_expel(args)
        }
    );
    std::process::exit(rc);
}

macro_rules! color {
    (@priv red) => { "\x1b[0;31m" };
    (@priv green) => { "\x1b[0;32m" };
    (@priv clear) => { "\x1b[0m" };
    (@priv $s:literal) => { $s };
    ($($s:tt)*) => {
        ::std::concat![ $( color!(@priv $s) ),* ]
    }
}

pub fn main_test(args: args::Test) -> ! {
    // Tarantool implicitly parses some environment variables.
    // We don't want them to affect the behavior and thus filter them out.

    for (k, _) in std::env::vars() {
        if k.starts_with("TT_") || k.starts_with("TARANTOOL_") {
            std::env::remove_var(k)
        }
    }

    const PASSED: &str = color![green "ok" clear];
    const FAILED: &str = color![red "FAILED" clear];
    let mut cnt_passed = 0u32;
    let mut cnt_failed = 0u32;
    let mut cnt_skipped = 0u32;

    let now = std::time::Instant::now();

    println!();
    println!(
        "total {} tests",
        inventory::iter::<InnerTest>.into_iter().count()
    );
    for t in inventory::iter::<InnerTest> {
        if let Some(filter) = args.filter.as_ref() {
            if !t.name.contains(filter) {
                cnt_skipped += 1;
                continue;
            }
        }
        print!("test {} ... ", t.name);

        let (mut rx, tx) = ipc::pipe().expect("pipe creation failed");
        let pid = unsafe { fork() };
        match pid.expect("fork failed") {
            ForkResult::Child => {
                drop(rx);
                unistd::close(0).ok(); // stdin
                if !args.nocapture {
                    unistd::dup2(*tx, 1).ok(); // stdout
                    unistd::dup2(*tx, 2).ok(); // stderr
                }
                drop(tx);

                let rc = tarantool_main!(
                    args.tt_args().unwrap(),
                    callback_data: t,
                    callback_data_type: &InnerTest,
                    callback_body: test_one(t)
                );
                std::process::exit(rc);
            }
            ForkResult::Parent { child } => {
                drop(tx);
                let log = {
                    let mut buf = Vec::new();
                    use std::io::Read;
                    if !args.nocapture {
                        rx.read_to_end(&mut buf)
                            .map_err(|e| println!("error reading ipc pipe: {e}"))
                            .ok();
                    }
                    buf
                };

                let mut rc: i32 = 0;
                unsafe {
                    libc::waitpid(
                        child.into(),                // pid_t
                        &mut rc as *mut libc::c_int, // int*
                        0,                           // int options
                    )
                };

                if rc == 0 {
                    println!("{PASSED}");
                    cnt_passed += 1;
                } else {
                    println!("{FAILED}");
                    cnt_failed += 1;

                    if args.nocapture {
                        continue;
                    }

                    use std::io::Write;
                    println!();
                    std::io::stderr()
                        .write_all(&log)
                        .map_err(|e| println!("error writing stderr: {e}"))
                        .ok();
                    println!();
                }
            }
        };
    }

    let ok = cnt_failed == 0;
    println!();
    print!("test result: {}.", if ok { PASSED } else { FAILED });
    print!(" {cnt_passed} passed;");
    print!(" {cnt_failed} failed;");
    print!(" {cnt_skipped} skipped;");
    println!(" finished in {:.2}s", now.elapsed().as_secs_f32());
    println!();

    std::process::exit(!ok as _);
}

fn test_one(t: &InnerTest) {
    use picodata::tarantool;

    let temp = tempfile::tempdir().expect("Failed creating a temp directory");
    std::env::set_current_dir(temp.path()).expect("Failed chainging current directory");

    let cfg = tarantool::Cfg {
        listen: Some("127.0.0.1:0".into()),
        read_only: false,
        log_level: ::tarantool::log::SayLevel::Verbose as u8,
        ..Default::default()
    };

    tarantool::set_cfg(&cfg);
    tarantool::exec(
        r#"
        box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
        "#,
    )
    .unwrap();

    (t.body)();
    std::process::exit(0i32);
}

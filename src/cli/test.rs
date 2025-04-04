use crate::{cli::args, ipc};
use ::tarantool::test::TestCase;
use nix::unistd::{self, fork, ForkResult};
use std::io::{self, ErrorKind, Write};

macro_rules! color {
    (@priv red) => { "\x1b[0;31m" };
    (@priv green) => { "\x1b[0;32m" };
    (@priv clear) => { "\x1b[0m" };
    (@priv $s:literal) => { $s };
    ($($s:tt)*) => {
        ::std::concat![ $( color!(@priv $s) ),* ]
    }
}

pub fn main(args: args::Test) -> ! {
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
    let mut failed = vec![];
    let mut cnt_skipped = 0u32;

    let now = std::time::Instant::now();

    let tests = ::tarantool::test::test_cases();
    println!("total {} tests", tests.len());
    for t in tests {
        if let Some(filter) = args.filter.as_ref() {
            if !t.name().contains(filter) {
                cnt_skipped += 1;
                continue;
            }
        }
        print!("test {} ... ", t.name());
        std::io::Write::flush(&mut std::io::stdout()).unwrap();

        let (mut rx, tx) = ipc::pipe().expect("pipe creation failed");
        let pid = unsafe { fork() };
        match pid.expect("fork failed") {
            ForkResult::Child => {
                // On linux, kill child if the test runner has died.
                // Perhaps it's the easiest way to implement this.
                #[cfg(target_os = "linux")]
                unsafe {
                    libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
                }

                drop(rx);
                unistd::close(0).ok(); // stdin
                if !args.nocapture {
                    unistd::dup2(*tx, 1).ok(); // stdout
                    unistd::dup2(*tx, 2).ok(); // stderr
                }
                drop(tx);

                let tt_args = args.tt_args().unwrap();
                super::tarantool::main_cb(&tt_args, || {
                    test_one(t);
                    Ok::<_, std::convert::Infallible>(())
                });
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

                let mut rc: libc::c_int = 0;
                loop {
                    let ret = unsafe { libc::waitpid(child.into(), &mut rc, 0) };

                    // Only EINTR is allowed, other errors are uncalled for.
                    if ret == -1 {
                        match io::Error::last_os_error().kind() {
                            ErrorKind::Interrupted => continue,
                            other => panic!("waitpid() returned {other}"),
                        }
                    }

                    // Break only if process exited or was terminated by a signal.
                    if libc::WIFEXITED(rc) || libc::WIFSIGNALED(rc) {
                        break;
                    }
                }

                // If the test passed, its exit code should be zero.
                if libc::WIFEXITED(rc) && libc::WEXITSTATUS(rc) == 0 {
                    println!("{PASSED}");
                    cnt_passed += 1;
                } else {
                    println!("{FAILED}");
                    let log = if args.nocapture { None } else { Some(log) };
                    failed.push((t.name(), log));
                }
            }
        };
    }

    let ok = failed.is_empty();

    let (_, mut screen_width) = terminal_size();
    if screen_width == 0 {
        screen_width = 80;
    }
    if !ok {
        println!();
        println!("failed tests:");
        for (test, log) in &failed {
            if let Some(log) = log {
                println!("{}", "=".repeat(screen_width));
                println!("test {test} output:");
                println!("{}", "-".repeat(screen_width));

                let res = std::io::stdout().write_all(log);
                if let Err(e) = res {
                    eprintln!("failed writing stdout: {e}")
                }
                println!();
            } else {
                println!("\x1b[31m    {test}\x1b[0m");
            }
        }
        println!();
    }

    print!("test result: {}.", if ok { PASSED } else { FAILED });
    print!(" {cnt_passed} passed;");
    print!(" {} failed;", failed.len());
    print!(" {cnt_skipped} skipped;");
    println!(" finished in {:.2}s", now.elapsed().as_secs_f32());
    println!();

    std::process::exit(!ok as _);
}

/// Returns a pair (rows, columns).
fn terminal_size() -> (usize, usize) {
    // Safety: always safe
    unsafe {
        let mut screen_size: libc::winsize = std::mem::zeroed();
        libc::ioctl(libc::STDIN_FILENO, libc::TIOCGWINSZ, &mut screen_size);
        (screen_size.ws_row as _, screen_size.ws_col as _)
    }
}

fn test_one(test: &TestCase) {
    use crate::tarantool;

    let temp = tempfile::tempdir().expect("Failed creating a temp directory");
    std::env::set_current_dir(temp.path()).expect("Failed chainging current directory");

    crate::set_tarantool_compat_options();

    let cfg = tarantool::Cfg {
        listen: Some("127.0.0.1:0".into()),
        read_only: false,
        log_level: Some(::tarantool::log::SayLevel::Verbose as u8),
        ..Default::default()
    };

    tarantool::set_cfg(&cfg).unwrap();

    crate::schema::init_user_pico_service();

    ::tarantool::fiber::set_name(test.name());
    test.run();
    std::process::exit(0i32);
}

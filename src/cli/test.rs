use crate::{cli::args, ipc};
use ::tarantool::test::TestCase;
use nix::unistd::{self, fork, ForkResult};
use std::collections::VecDeque;
use std::io::{self, ErrorKind, Read, Write};

macro_rules! color {
    (@priv red) => { "\x1b[0;31m" };
    (@priv green) => { "\x1b[0;32m" };
    (@priv clear) => { "\x1b[0m" };
    (@priv $s:literal) => { $s };
    ($($s:tt)*) => {
        ::std::concat![ $( color!(@priv $s) ),* ]
    }
}

const PASSED: &str = color![green "ok" clear];
const FAILED: &str = color![red "FAILED" clear];

#[derive(Default)]
struct TestResults {
    passed_count: u64,
    skipped_count: u64,
    filtered_out_count: u64,
    failed: Vec<(&'static str, Vec<u8>)>,
    elapsed: std::time::Duration,
}

impl TestResults {
    fn is_success(&self) -> bool {
        self.failed.is_empty()
    }
}

enum TestOutcome {
    Passed,
    Failed { log: Vec<u8> },
}

/// Run the "inner" tests, i.e. tests defined via the `#[tarantool::test]`
/// attribute macro in the picodata code.
///
/// Each test runs in it's own proccess forked from the current one (no exec).
///
/// Up to [`args::Test::jobs()`] processes are spawned at a time.
///
/// NOTE: **don't combine threads and forks**.
/// During development we tried doing so and encountered problems.
///
/// The problem is that `fork` only copies the current thread, and if another
/// thread is holding a lock, that lock will forever be locked in the spawned
/// subprocess.
///
/// As a result we encountered a deadlock in [`std::process::exit`] on a mutex
/// which the standard library takes whenever a thread is spawned or exits
/// (`std::sys::pal::unix::stack_overflow::thread_info::LOCK`).
pub fn main(args: args::Test) -> ! {
    // Tarantool implicitly parses some environment variables.
    // We don't want them to affect the behavior and thus filter them out.

    for (k, _) in std::env::vars() {
        if k.starts_with("TT_") || k.starts_with("TARANTOOL_") {
            std::env::remove_var(k)
        }
    }

    let results = run_tests(&args);
    let success = results.is_success();
    report_test_results(&args, results);
    std::process::exit(if success { 0 } else { 1 });
}

////////////////////////////////////////////////////////////////////////////////
// run_tests
////////////////////////////////////////////////////////////////////////////////

fn run_tests(args: &args::Test) -> TestResults {
    let now = std::time::Instant::now();

    let tests = ::tarantool::test::test_cases();
    println!("total {} tests", tests.len());

    let mut results = TestResults::default();

    //
    // Skip filtered out tests.
    //
    let mut queue = VecDeque::new();
    for t in tests {
        if let Some(filter) = args.filter.as_ref() {
            if !t.name().contains(filter) {
                results.filtered_out_count += 1;
                continue;
            }
        }
        if args.skip.iter().any(|s| t.name().contains(s)) {
            results.filtered_out_count += 1;
            continue;
        }
        if let Some(reason) = t.skip() {
            println!("test {} ... skipped: {reason}", t.name());
            results.skipped_count += 1;
            continue;
        }

        queue.push_back(t);
    }

    //
    // Run tests
    //

    let jobs = args.jobs().min(queue.len().max(1));
    println!("running {} tests", queue.len());
    println!("number of parallel jobs: {jobs}");

    let mut running: Vec<TestSubprocess> = vec![];
    let mut poll_fds: Vec<libc::pollfd> = vec![];
    loop {
        //
        // Spawn test subprocesses if needed
        //
        while running.len() < jobs {
            let Some(test) = queue.pop_front() else {
                break;
            };

            let started = spawn_test_subprocess(args, test, &running);
            running.push(started);
        }

        if running.is_empty() {
            assert!(queue.is_empty());
            break;
        }

        //
        // Sleep until a subprocess shows signs of activity
        //
        poll_fds.clear();
        for process in &running {
            poll_fds.push(libc::pollfd {
                fd: *process.output_pipe,
                events: libc::POLLIN,
                revents: 0,
            });
        }

        // SAFETY: safe because array bounds are valid
        let ret = unsafe { libc::poll(poll_fds.as_mut_ptr(), poll_fds.len() as _, -1) };
        if ret == -1 {
            let e = io::Error::last_os_error();
            // EINTR can always happen and is never an error
            assert_eq!(e.kind(), ErrorKind::Interrupted, "poll() failed: {e}");
            continue;
        }

        let mut to_check = vec![];
        for (pollfd, i) in poll_fds.iter().zip(0..) {
            if pollfd.revents != 0 {
                to_check.push(i);
            }
        }

        //
        // Read from pipes and check if someone finished
        //
        let mut to_remove = vec![];
        for i in to_check {
            let process = &mut running[i];
            let finished = read_test_output(process);
            if finished {
                to_remove.push(process.pid);
            }
        }

        let finished;
        (finished, running) = running
            .into_iter()
            .partition(|test| to_remove.contains(&test.pid));

        //
        // Handle finished tests
        //

        for process in finished.into_iter() {
            let test_name = process.test.name();
            let outcome = finish_test_subprocess(process);
            match outcome {
                TestOutcome::Passed => {
                    println!("test {test_name} ... {PASSED}");
                    results.passed_count += 1;
                }
                TestOutcome::Failed { log } => {
                    println!("test {test_name} ... {FAILED}");
                    results.failed.push((test_name, log));
                }
            }
        }
    }

    results.elapsed = now.elapsed();
    results
}

////////////////////////////////////////////////////////////////////////////////
// report_test_results
////////////////////////////////////////////////////////////////////////////////

fn report_test_results(args: &args::Test, results: TestResults) -> bool {
    let ok = results.is_success();
    let TestResults {
        passed_count,
        skipped_count,
        filtered_out_count,
        failed,
        elapsed,
    } = results;

    let (_, mut screen_width) = terminal_size();
    if screen_width == 0 {
        screen_width = 80;
    }
    if !ok {
        println!();
        println!("failed tests:");
        for (test, log) in &failed {
            if !args.nocapture {
                println!("{}", "=".repeat(screen_width));
                println!("test {test} output:");
                println!("{}", "-".repeat(screen_width));

                let res = std::io::stdout().write_all(log);
                if let Err(e) = res {
                    crate::eprintln_buffered!("failed writing stdout: {e}")
                }
                println!();
            } else {
                println!("\x1b[31m    {test}\x1b[0m");
            }
        }
        println!();
    }

    print!("test result: {}.", if ok { PASSED } else { FAILED });
    print!(" {passed_count} passed;");
    print!(" {} failed;", failed.len());
    print!(" {skipped_count} skipped;");
    print!(" {filtered_out_count} filtered out;");
    println!();
    if !ok {
        println!("failed tests:");
        for (test, _) in &failed {
            println!("    {test}:");
        }
    }
    println!(" finished in {:.3}s", elapsed.as_secs_f32());
    println!();

    ok
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

////////////////////////////////////////////////////////////////////////////////
// TestSubprocess
////////////////////////////////////////////////////////////////////////////////

struct TestSubprocess {
    test: &'static TestCase,
    pid: libc::pid_t,
    /// Read end of the pipe connected to the test process's stdout & stderr.
    output_pipe: ipc::Fd,
    /// Everything the test process has written into [`Self::output_pipe`] so far.
    output: Vec<u8>,
}

/// Forks a process which runs a single test in it.
///
/// `running` are the tests which are already running in parallel with this one.
fn spawn_test_subprocess(
    args: &args::Test,
    test: &'static TestCase,
    running: &[TestSubprocess],
) -> TestSubprocess {
    let (rx, tx) = ipc::pipe().expect("pipe creation failed");

    // SAFETY: forking is only safe here because this process is single
    // threaded, see the doc comment on `main`.
    let pid = unsafe { fork() };
    match pid.expect("fork failed") {
        ForkResult::Child => {
            // On linux/freebsd, kill child if the test runner has died.
            // Perhaps it's the easiest way to implement this.
            #[cfg(target_os = "linux")]
            unsafe {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
            }

            #[cfg(target_os = "freebsd")]
            unsafe {
                let sig: libc::c_int = libc::SIGKILL;
                libc::procctl(
                    libc::P_PID,
                    0,
                    libc::PROC_PDEATHSIG_CTL,
                    &sig as *const _ as *mut _,
                );
            }

            drop(rx);
            // The test process has no business holding onto the pipes of the
            // tests running in parallel with it.
            for other in running {
                unistd::close(*other.output_pipe).ok();
            }

            unistd::close(0).ok(); // stdin
            if args.nocapture {
                // Don't redirect the output, but do keep the pipe open, so that
                // the test runner sees the end of file exactly when this
                // process exits.
                std::mem::forget(tx);
            } else {
                unistd::dup2(*tx, 1).ok(); // stdout
                unistd::dup2(*tx, 2).ok(); // stderr
                drop(tx);
            }

            let tt_args = args.tt_args().unwrap();
            super::tarantool::main_cb(&tt_args, || {
                test_one(test);
                Ok::<_, std::convert::Infallible>(())
            });
        }
        ForkResult::Parent { child } => {
            drop(tx);

            TestSubprocess {
                test,
                pid: child.into(),
                output_pipe: rx,
                output: Vec::new(),
            }
        }
    }
}

/// Read a portion of test output from the pipe.
///
/// Return `true` if the pipe is closed. This means the process finished.
fn read_test_output(process: &mut TestSubprocess) -> bool {
    let mut buffer = [0_u8; 16 * 1024];

    // Read a portion of output, don't try reading again as it will block the thread
    let res = process.output_pipe.read(&mut buffer);
    let count = match res {
        Ok(0) => {
            // Pipe is closed, the process finished.
            return true;
        }
        Err(e) if e.kind() == ErrorKind::Interrupted => {
            // EINTR can always happen and is never an error
            return false;
        }
        Err(e) => {
            // Other error, probably unrecoverable
            println!("error reading from ipc pipe: {e}");
            return true;
        }
        Ok(count) => count,
    };

    process.output.extend_from_slice(&buffer[..count]);
    false
}

/// Wait until process finishes and report result
fn finish_test_subprocess(process: TestSubprocess) -> TestOutcome {
    let mut rc: libc::c_int = 0;
    loop {
        let ret = unsafe { libc::waitpid(process.pid, &mut rc, 0) };

        if ret == -1 {
            // EINTR can always happen and is never an error
            let e = io::Error::last_os_error();
            assert_eq!(e.kind(), ErrorKind::Interrupted, "waitpid() failed: {e}");
            continue;
        }

        // Break only if process exited or was terminated by a signal.
        if libc::WIFEXITED(rc) || libc::WIFSIGNALED(rc) {
            break;
        }
    }

    // If the test passed, its exit code should be zero.
    if libc::WIFEXITED(rc) && libc::WEXITSTATUS(rc) == 0 {
        return TestOutcome::Passed;
    }

    TestOutcome::Failed {
        log: process.output,
    }
}

////////////////////////////////////////////////////////////////////////////////
// test_one
////////////////////////////////////////////////////////////////////////////////

fn test_one(test: &TestCase) {
    use crate::tarantool;

    let temp = tempfile::tempdir().expect("Failed creating a temp directory");
    std::env::set_current_dir(temp.path()).expect("Failed changing current directory");

    crate::set_tarantool_compat_options();

    let cert_paths = tarantool::test_util::TEST_TLS_CERT_PATHS.get_or_init(|| {
        tarantool::test_util::TEST_TLS_CERTS
            .write_to(&temp.path().join("ssl_certs"))
            .expect("failed to write TLS certs to the temp directory")
    });

    let cfg = tarantool::Cfg {
        listen: vec![
            tarantool::ListenConfig {
                uri: "127.0.0.1:0".to_string(),
                params: None,
            },
            tarantool::ListenConfig {
                uri: "127.0.0.1:0".to_string(),
                params: Some(tarantool::ListenConfigParams {
                    transport: "ssl".to_string(),
                    ssl_cert_file: Some(cert_paths.cert_file.clone()),
                    ssl_key_file: Some(cert_paths.key_file.clone()),
                    ssl_ca_file: Some(cert_paths.ca_file.clone()),
                }),
            },
        ],
        read_only: false,
        log_level: Some(::tarantool::log::SayLevel::Verbose as u8),
        wal_mode: crate::config::WalMode::None,
        checkpoint_enabled: false,
        ..Default::default()
    };

    tarantool::set_cfg(&cfg).unwrap();

    crate::schema::init_user_pico_service();

    let mut short_name = test.name();
    if let Some((_, tail)) = short_name.rsplit_once("::") {
        short_name = tail;
    };

    ::tarantool::fiber::set_name(short_name);
    test.run();
    std::process::exit(0i32);
}

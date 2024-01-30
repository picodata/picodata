use crate::args;
use crate::ipc;
use crate::tarantool_main;
use ::tarantool::test::TestCase;
use nix::unistd::{self, fork, ForkResult};

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
    let mut cnt_failed = 0u32;
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
                    callback_data_type: &TestCase,
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

fn test_one(test: &TestCase) {
    use crate::tarantool;

    let temp = tempfile::tempdir().expect("Failed creating a temp directory");
    std::env::set_current_dir(temp.path()).expect("Failed chainging current directory");

    tarantool::exec(
        "require 'compat' {
            c_func_iproto_multireturn = 'new',
        }",
    )
    .unwrap();

    let cfg = tarantool::Cfg {
        listen: Some("127.0.0.1:0".into()),
        read_only: false,
        log_level: ::tarantool::log::SayLevel::Verbose as u8,
        ..Default::default()
    };

    tarantool::set_cfg(&cfg);

    crate::schema::init_user_pico_service();

    test.run();
    std::process::exit(0i32);
}

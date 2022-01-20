use assert_cmd::Command;
use std::time::Duration;
use std::time::Instant;
use tempfile::tempdir;

fn main() {
    let now = Instant::now();

    let red = "\x1B[0;31m";
    let green = "\x1B[0;32m";
    let clear = "\x1B[0m";
    let s_passed = format!("{green}ok{clear}");
    let s_failed = format!("{red}FAILED{clear}");

    let temp = tempdir().unwrap();
    let temp_path = temp.path();

    let mut cmd = Command::cargo_bin("picodata").unwrap();
    cmd.current_dir(temp_path);
    cmd.env("PICOLIB_AUTORUN", "false");
    cmd.arg("run");
    cmd.arg("-e").arg(
        r#"
        log = require('log')
        picolib = require('picolib')
        for t, _ in pairs(picolib.test) do
            print(t)
        end
        os.exit()
        "#,
    );
    cmd.timeout(Duration::from_secs(1));

    let result = cmd.output().unwrap();
    let stdout = String::from_utf8(result.stdout).unwrap();
    let mut cnt_passed = 0u32;
    let mut cnt_failed = 0u32;

    println!();
    for t in stdout.lines() {
        print!("Running {} ... ", t);

        match run_test(t) {
            Ok(_) => {
                println!("{s_passed}");
                cnt_passed += 1;
            }
            Err(e) => {
                println!("{s_failed}");
                println!("\n{}\n", e.0);
                cnt_failed += 1;
            }
        };
    }
    println!();

    let ok = cnt_failed == 0;
    print!("test result: {}.", if ok { s_passed } else { s_failed });
    print!(" {cnt_passed} passed;");
    print!(" {cnt_failed} failed;");
    print!(" finished in {:.2}s\n", now.elapsed().as_secs_f32());
    println!();

    if !ok {
        std::process::exit(1);
    }
}

struct TestError(String);

impl std::convert::From<std::io::Error> for TestError {
    fn from(e: std::io::Error) -> Self {
        Self(format!("IoError: {}", e))
    }
}

impl From<std::string::FromUtf8Error> for TestError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Self(format!("Utf8Error: {}", e))
    }
}

fn run_test(test_name: &str) -> Result<(), TestError> {
    let temp = tempdir().unwrap();
    let temp_path = temp.path();

    let mut cmd = Command::cargo_bin("picodata").unwrap();
    cmd.current_dir(temp_path);
    cmd.arg("run");
    cmd.arg("-e");
    cmd.arg(format!("picolib.test.{}() os.exit(0)", test_name));
    cmd.timeout(Duration::from_secs(1));

    let result = cmd.output()?;
    if !result.status.success() {
        let err = format!(
            "{}\nPicodata exit status: {}",
            String::from_utf8(result.stderr)?,
            result.status
        );
        Err(TestError(err))
    } else {
        Ok(())
    }
}

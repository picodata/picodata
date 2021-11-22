use assert_cmd::Command;
use std::fs::File;
use tempfile::tempdir;

#[test]
fn positive() {
    let mut cmd = Command::cargo_bin("picodata").unwrap();
    cmd.assert().success();
}

#[test]
fn missing_tarantool() {
    let mut cmd = Command::cargo_bin("picodata").unwrap();
    cmd.env("PATH", "/nowhere");
    cmd.assert()
        .failure()
        .stderr("tarantool: No such file or directory\n");
}

#[test]
fn broken_tarantool() {
    let temp = tempdir().unwrap();
    let temp_path = temp.path();
    File::create(temp_path.join("tarantool")).unwrap();

    let mut cmd = Command::cargo_bin("picodata").unwrap();
    cmd.env("PATH", temp_path);
    cmd.assert().failure().stderr(format!(
        "{}/tarantool: {}\n",
        temp_path.display(),
        errno::Errno(libc::EACCES)
    ));
}

#[test]
fn pass_arguments() {
    let mut cmd = Command::cargo_bin("picodata").unwrap();
    cmd.args(["-e", "error('xxx', 0)"]);
    cmd.assert().failure().stderr(
        "LuajitError: xxx\n\
        fatal error, exiting the event loop\n",
    );
}

#[test]
fn pass_environment() {
    let mut cmd = Command::cargo_bin("picodata").unwrap();
    cmd.env("LUA_CPATH", "/dev/null/?");
    cmd.env("CUSTOM_VAR", "keep me");
    cmd.env("TARANTOOL_VAR", "purge me");
    cmd.env("TT_VAR", "purge me too");
    cmd.arg("-e").arg(
        r#"
        cpath = os.environ()['LUA_CPATH']
        assert(cpath and cpath:endswith(';/dev/null/?'), cpath)

        function assert_eq(l, r)
            if l ~= r then
                error(('Assertion failed: %q ~= %q'):format(l, r), 2)
            end
        end
        assert_eq(os.environ()['CUSTOM_VAR'], 'keep me')
        assert_eq(os.environ()['TARANTOOL_VAR'], nil)
        assert_eq(os.environ()['TT_VAR'], nil)
    "#,
    );
    cmd.assert().success();
}

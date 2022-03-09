use assert_cmd::Command;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn positive() {
    let temp = tempdir().unwrap();
    let temp_path = temp.path();

    let mut cmd = Command::cargo_bin("picodata").unwrap();
    cmd.current_dir(temp_path);
    cmd.env("PICOLIB_AUTORUN", "false");
    cmd.arg("run");
    cmd.arg("-e").arg(
        r#"
        function assert_eq(l, r)
            if l ~= r then
                error(('Assertion failed: %q ~= %q'):format(l, r), 2)
            end
        end
        assert_eq(picolib.args.peers[1].uri, "127.0.0.1:3301")
        assert_eq(picolib.args.listen, "3301")
        assert_eq(picolib.args.data_dir, ".")
        "#,
    );
    cmd.timeout(Duration::from_secs(1)).assert().success();
}

#[test]
fn pass_arguments() {
    let mut cmd = Command::cargo_bin("picodata").unwrap();
    cmd.env("PICOLIB_AUTORUN", "false");
    cmd.arg("run");
    cmd.args(["-e", "error('xxx', 0)"]);
    cmd.assert().failure().stderr(
        "LuajitError: xxx\n\
        fatal error, exiting the event loop\n",
    );
}

#[test]
fn pass_environment() {
    let temp = tempdir().unwrap();
    let temp_path = temp.path();

    let mut cmd = Command::cargo_bin("picodata").unwrap();
    cmd.current_dir(temp_path);
    cmd.arg("run");
    cmd.env("PICOLIB_AUTORUN", "false");
    cmd.env("LUA_CPATH", "/dev/null/?");
    cmd.env("CUSTOM_VAR", "keep me");
    cmd.env("TARANTOOL_VAR", "purge me");
    cmd.env("TT_VAR", "purge me too");
    cmd.args(["--listen", "127.0.0.1:0"]);
    cmd.args(["--cluster-id", "sam"]);
    cmd.args(["--replicaset-id", "r1"]);
    cmd.args(["--instance-id", "i1"]);
    cmd.args(["--data-dir", "./data/i1"]);
    cmd.args(["--peer", "i1,i2"]);
    cmd.args(["--peer", "i3"]);
    cmd.arg("-e").arg(
        r#"
        assert(
            type(box.cfg) == "function",
            "Picodata shouldn't be running yet"
        )

        function assert_eq(l, r)
            if l ~= r then
                error(('Assertion failed: %q ~= %q'):format(l, r), 2)
            end
        end
        assert_eq(os.environ()['LUA_CPATH'], '/dev/null/?')
        assert_eq(os.environ()['CUSTOM_VAR'], 'keep me')
        assert_eq(os.environ()['TARANTOOL_VAR'], nil)
        assert_eq(os.environ()['TT_VAR'], nil)
        assert_eq(picolib.args.listen, '127.0.0.1:0')
        assert_eq(picolib.args.cluster_id, "sam")
        assert_eq(picolib.args.replicaset_id, "r1")
        assert_eq(picolib.args.instance_id, "i1")
        assert_eq(picolib.args.peers[1].uri, "i1")
        assert_eq(picolib.args.peers[2].uri, "i2")
        assert_eq(picolib.args.peers[3].uri, "i3")
        assert_eq(picolib.args.data_dir, "./data/i1")
        picolib.run()
        os.exit(0)
    "#,
    );
    cmd.timeout(Duration::from_secs(1)).assert().success();

    assert!(
        temp_path.join("data/i1/00000000000000000000.snap").exists(),
        "tarantool didn't make a snapshot"
    );
}

#[test]
fn precedence() {
    let temp = tempdir().unwrap();
    let temp_path = temp.path();

    let mut cmd = Command::cargo_bin("picodata").unwrap();
    cmd.current_dir(temp_path);
    cmd.arg("run");
    cmd.env("PICOLIB_AUTORUN", "false");
    cmd.env("PICODATA_DATA_DIR", "./somewhere");
    cmd.env("PICODATA_LISTEN", "0.0.0.0:0");
    cmd.arg("-e").arg(
        r#"
        function assert_eq(l, r)
            if l ~= r then
                error(('Assertion failed: %q ~= %q'):format(l, r), 2)
            end
        end
        assert_eq(picolib.args.data_dir, './somewhere')
        assert_eq(picolib.args.listen, "0.0.0.0:0")
    "#,
    );
    cmd.timeout(Duration::from_secs(1)).assert().success();
}

#[test]
fn tarantool_hello() {
    Command::cargo_bin("picodata")
        .unwrap()
        .arg("tarantool")
        .arg("--")
        .arg("-e")
        .arg(r#"print("it's alive!")"#)
        .assert()
        .stdout(&b"it's alive!\n"[..])
        .success();
}

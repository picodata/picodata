fn main() {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_picodata"));
    cmd.arg("test")
        .args(["--instance-id", "i1", "--peer", "localhost"]);
    cmd.output()
        .map(|o| {
            assert!(
                o.status.success(),
                "\ncommand failed: {:?}\nstdout:\n{}\nstderr:\n{}",
                cmd,
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr),
            )
        })
        .unwrap()
}

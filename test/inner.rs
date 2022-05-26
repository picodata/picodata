fn main() {
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_picodata"))
        .arg("test")
        .status()
        .unwrap();

    assert!(status.success());
}

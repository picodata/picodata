fn main() {
    std::process::Command::new(env!("CARGO_BIN_EXE_picodata"))
        .arg("test")
        .spawn()
        .unwrap()
        .wait()
        .unwrap();
}

fn main() {
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_picodata"))
        .arg("test")
        .args(std::env::args().skip(1))
        .status()
        .unwrap();

    if !status.success() {
        std::process::exit(1);
    }
}

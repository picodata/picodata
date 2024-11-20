use std::os::unix::process::CommandExt;

fn main() {
    let err = std::process::Command::new(env!("CARGO_BIN_EXE_picodata"))
        .arg("test")
        .args(std::env::args().skip(1))
        .exec();

    panic!("failed to exec: {err}");
}

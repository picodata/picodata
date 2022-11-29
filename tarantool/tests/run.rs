use serde::{Deserialize, Serialize};
use std::env;
use std::process::Command;

#[derive(Serialize, Deserialize)]
struct Metadata {
    workspace_root: String,
}

fn main() {
    let filter = env::args().skip(1);
    let tarantool_exec =
        env::var("TARANTOOL_EXECUTABLE").unwrap_or_else(|_| "tarantool".to_owned());
    let metadata = Command::new("cargo")
        .arg("metadata")
        .arg("--format-version=1")
        .output()
        .expect("failed to get cargo metadata output");
    let metadata: Metadata =
        serde_json::from_slice(&metadata.stdout).expect("failed to parse cargo metadata output");
    let status = Command::new(tarantool_exec)
        .arg(format!("{}/tests/run_tests.lua", metadata.workspace_root))
        .args(filter)
        .status()
        .expect("failed to run tarantool child process");
    assert!(status.success())
}

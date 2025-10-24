#[tarantool::proc]
fn proc_names() -> Vec<&'static str> {
    tarantool::proc::all_procs()
        .iter()
        .map(|p| p.name())
        .collect()
}

#[tarantool::proc]
fn hello() -> &'static str {
    "hello"
}

#[tarantool::proc]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

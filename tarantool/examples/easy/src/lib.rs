use tarantool::proc;

#[proc]
fn easy() {
    println!("hello world");
}

#[proc]
fn easy2() {
    println!("hello world -- easy2");
}

#[tarantool::proc]
fn harder(fields: Vec<i32>) {
    println!("field_count = {}", fields.len());

    for val in fields {
        println!("val={}", val);
    }
}

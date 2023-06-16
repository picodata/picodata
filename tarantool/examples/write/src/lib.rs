use tarantool::{error::Error, fiber::sleep, proc, space::Space, transaction::transaction};

#[proc]
fn write() -> Result<(i32, String), String> {
    let space = Space::find("capi_test").ok_or_else(|| "Can't find space capi_test".to_string())?;

    let row = (1, "22".to_string());

    transaction(|| -> Result<(), Error> {
        space.replace(&row)?;
        Ok(())
    })
    .unwrap();

    sleep(std::time::Duration::from_millis(1));
    Ok(row)
}

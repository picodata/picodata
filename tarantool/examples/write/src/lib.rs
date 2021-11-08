use std::os::raw::c_int;

use tarantool::error::{Error, TarantoolErrorCode};
use tarantool::fiber::sleep;
use tarantool::space::Space;
use tarantool::transaction::start_transaction;
use tarantool::tuple::{FunctionArgs, FunctionCtx};

#[no_mangle]
pub extern "C" fn hardest(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    let mut space = match Space::find("capi_test") {
        None => {
            return tarantool::set_error!(TarantoolErrorCode::ProcC, "Can't find space capi_test")
        }
        Some(space) => space,
    };

    let row = (1, 22);

    start_transaction(|| -> Result<(), Error> {
        space.replace(&row)?;
        Ok(())
    })
    .unwrap();

    sleep(std::time::Duration::from_millis(1));
    ctx.return_mp(&row).unwrap()
}

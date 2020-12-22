use std::os::raw::c_int;

use tarantool::error::{set_error, Error, TarantoolErrorCode};
use tarantool::fiber::sleep;
use tarantool::space::Space;
use tarantool::transaction::start_transaction;
use tarantool::tuple::{FunctionArgs, FunctionCtx};

#[no_mangle]
pub extern "C" fn hardest(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    let mut space = match Space::find("capi_test") {
        None => {
            return set_error(
                file!(),
                line!(),
                &TarantoolErrorCode::ProcC,
                "Can't find space capi_test",
            )
        }
        Some(space) => space,
    };

    let row = (1, 22);

    start_transaction(|| -> Result<(), Error> {
        space.replace(&row)?;
        Ok(())
    })
    .unwrap();

    sleep(0.001);
    ctx.return_mp(&row).unwrap()
}

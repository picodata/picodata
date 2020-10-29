use std::os::raw::c_int;

use tarantool_module::error::{set_error, Error, TarantoolErrorCode};
use tarantool_module::fiber::sleep;
use tarantool_module::space::Space;
use tarantool_module::transaction::start_transaction;
use tarantool_module::tuple::{FunctionArgs, FunctionCtx};

#[no_mangle]
pub extern "C" fn hardest(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    let mut space = match Space::find_by_name("capi_test").unwrap() {
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
        space.replace(&row, false)?;
        Ok(())
    })
    .unwrap();

    sleep(0.001);
    ctx.return_mp(&row).unwrap()
}

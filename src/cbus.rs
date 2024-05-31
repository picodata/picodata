use crate::tlog;
use tarantool::fiber;

pub const ENDPOINT_NAME: &'static str = "picodata-channels";

/// Initializes the special fiber which will be responsible for the internal cbus machinery.
/// NOTE: the endpoint must be created before any channels which will attempt
/// to connect to it
pub fn init_cbus_endpoint() {
    fiber::Builder::new()
        .name(ENDPOINT_NAME)
        .func(|| {
            let e = tarantool::cbus::Endpoint::new(ENDPOINT_NAME)
                .expect("nobody should've created this endpoint");

            #[rustfmt::skip]
            tlog!(Debug, "cbus endpoint has been created, starting the cbus loop");

            e.cbus_loop();
        })
        .start_non_joinable()
        .expect("starting a fiber shouldn't fail");
}

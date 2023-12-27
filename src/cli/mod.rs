pub mod admin;
pub mod args;
pub mod connect;
pub mod console;
pub mod expel;
pub mod init_cfg;
pub mod run;
pub mod tarantool;
pub mod test;

#[macro_export]
macro_rules! tarantool_main {
    (
        $tt_args:expr,
         callback_data: $cb_data:tt,
         callback_data_type: $cb_data_ty:ty,
         callback_body: $cb_body:expr
    ) => {{
        let tt_args: Vec<_> = $tt_args;
        // `argv` is a vec of pointers to data owned by `tt_args`, so
        // make sure `tt_args` outlives `argv`, because the compiler is not
        // gonna do that for you
        let argv = tt_args.iter().map(|a| a.as_ptr()).collect::<Vec<_>>();
        extern "C" fn trampoline(data: *mut libc::c_void) {
            let args = unsafe { Box::from_raw(data as _) };
            let cb = |$cb_data: $cb_data_ty| $cb_body;
            cb(*args)
        }
        let cb_data: $cb_data_ty = $cb_data;
        unsafe {
            $crate::tarantool::main(
                argv.len() as _,
                argv.as_ptr() as _,
                Some(trampoline),
                Box::into_raw(Box::new(cb_data)) as _,
            )
        }
    }};
}

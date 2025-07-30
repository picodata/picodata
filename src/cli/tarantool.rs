use super::args;
use crate::tlog;
use std::ffi::CStr;

/// Run tarantool's main entry point with cmdline args,
/// executing callback `f` as a part of the lua run script.
pub fn main_cb<E, F>(args: &[impl AsRef<CStr>], f: F) -> !
where
    E: std::fmt::Display,
    F: FnOnce() -> Result<(), E>,
{
    let rc = main_cb_no_exit(args, f);
    std::process::exit(rc);
}

/// Run tarantool's main entry point with cmdline args,
/// executing callback `f` as a part of the lua run script.
///
/// Doesn't exit at the end.
pub fn main_cb_no_exit<E, F>(args: &[impl AsRef<CStr>], f: F) -> i32
where
    E: std::fmt::Display,
    F: FnOnce() -> Result<(), E>,
{
    extern "C" fn trampoline<E, F>(data: *mut libc::c_void)
    where
        E: std::fmt::Display,
        F: FnOnce() -> Result<(), E>,
    {
        // SAFETY: the caller is responsible for providing correct E & F.
        let closure = unsafe { Box::<F>::from_raw(data as _) };
        if let Err(e) = closure() {
            // Print the error message to stderr as well to the logging destination for user friendliness
            eprintln!("\x1b[31mCRITICAL: {e}\x1b[0m");
            tlog!(Critical, "{e}");
            std::process::exit(1);
        }
    }

    // XXX: Careful! We must specify exactly E & F here.
    let trampoline = trampoline::<E, F>;
    let trampoline_arg = Box::<F>::into_raw(Box::new(f)).cast();

    // XXX: `argv` is a vec of pointers to data owned by `args`, so
    // make sure `args` outlives `argv`, because the compiler is not
    // gonna do that for you
    let argv: Vec<_> = args.iter().map(|a| a.as_ref().as_ptr()).collect();

    crate::tarantool::set_use_system_alloc();
    unsafe {
        crate::tarantool::main(
            argv.len() as _,
            argv.as_ptr() as _,
            Some(trampoline),
            trampoline_arg,
        )
    }
}

/// Run tarantool's main entry point with cmdline args.
/// See `tarantool_main` and [`crate::tarantool::main`].
pub fn main(args: args::Tarantool) -> ! {
    let tt_args = args.tt_args().unwrap();

    // XXX: `argv` is a vec of pointers to data owned by `tt_args`, so
    // make sure `tt_args` outlives `argv`, because the compiler is not
    // gonna do that for you
    let argv: Vec<_> = tt_args.iter().map(|a| a.as_ref().as_ptr()).collect();

    let rc = unsafe {
        crate::tarantool::main(
            argv.len() as _,
            argv.as_ptr() as _,
            // This is needed because of a special hack in our tarantool fork.
            // Without it executing a lua file would not work.
            None,
            std::ptr::null_mut(),
        )
    };

    std::process::exit(rc);
}

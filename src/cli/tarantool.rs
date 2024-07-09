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
    extern "C" fn trampoline<E, F>(data: *mut libc::c_void)
    where
        E: std::fmt::Display,
        F: FnOnce() -> Result<(), E>,
    {
        // SAFETY: the caller is responsible for providing correct E & F.
        let closure = unsafe { Box::<F>::from_raw(data as _) };
        if let Err(e) = closure() {
            tlog!(Critical, "{e}");
            std::process::exit(1);
        }
    }

    // XXX: Careful! We must specify exactly E & F here.
    let trampoline = trampoline::<E, F>;
    let trampoline_arg = Box::<F>::into_raw(Box::new(f)).cast();

    // XXX: `argv` is a vec of pointers to data owned by `tt_args`, so
    // make sure `tt_args` outlives `argv`, because the compiler is not
    // gonna do that for you
    let argv: Vec<_> = args.iter().map(|a| a.as_ref().as_ptr()).collect();

    let rc = unsafe {
        crate::tarantool::main(
            argv.len() as _,
            argv.as_ptr() as _,
            Some(trampoline),
            trampoline_arg,
        )
    };

    std::process::exit(rc);
}

/// Run tarantool's main entry point with cmdline args.
/// See `tarantool_main` and [`crate::tarantool::main`].
pub fn main(args: args::Tarantool) -> ! {
    main_cb(&args.tt_args().unwrap(), || {
        Ok::<_, std::convert::Infallible>(())
    })
}

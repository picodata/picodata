use crate::tlog;
use std::collections::HashSet;

static mut INJECTED_ERRORS: Option<HashSet<String>> = None;

#[inline(always)]
pub fn enable(error: &str, enable: bool) {
    // SAFETY: safe as long as only called from tx thread
    let injected_errors = unsafe { &mut INJECTED_ERRORS };
    let injected_errors = injected_errors.get_or_insert_with(Default::default);
    if enable {
        injected_errors.insert(error.into());
        tlog!(Info, "ERROR INJECTION '{error}': fused");
    } else {
        tlog!(Info, "ERROR INJECTION '{error}': defused");
        injected_errors.remove(error);
    }
}

#[inline(always)]
pub fn is_enabled(error: &str) -> bool {
    // SAFETY: safe as long as only called from tx thread
    let Some(injected_errors) = (unsafe { &INJECTED_ERRORS }) else {
        return false;
    };
    injected_errors.contains(error)
}

#[cfg(not(feature = "error_injection"))]
#[macro_export]
macro_rules! error_injection {
    ($($t:tt)*) => {
        // Error injection is disabled
    };
}

#[cfg(feature = "error_injection")]
#[macro_export]
macro_rules! error_injection {
    (exit $error:expr) => {
        #[rustfmt::skip]
        if $crate::error_injection::is_enabled($error) {
            $crate::tlog!(Info, "################################################################");
            $crate::tlog!(Info, "ERROR INJECTION '{}': EXITING", $error);
            $crate::tlog!(Info, "################################################################");
            $crate::tarantool::exit(69);
        };
    };
    (block $error:expr) => {{
        let error = $error;
        #[rustfmt::skip]
        if $crate::error_injection::is_enabled(error) {
            $crate::tlog!(Info, "################################################################");
            $crate::tlog!(Info, "ERROR INJECTION '{}': BLOCKING", error);
            $crate::tlog!(Info, "################################################################");

            while $crate::error_injection::is_enabled(error) {
                ::tarantool::fiber::sleep(::std::time::Duration::from_millis(100));
            }

            $crate::tlog!(Info, "################################################################");
            $crate::tlog!(Info, "ERROR INJECTION '{}': UNBLOCKING", error);
            $crate::tlog!(Info, "################################################################");
        };
    }};
}

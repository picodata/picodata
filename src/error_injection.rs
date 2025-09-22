#[allow(unused_imports)]
use std::{collections::HashMap, ptr};

#[cfg(feature = "error_injection")]
static mut INJECTED_ERRORS: Option<HashMap<String, String>> = None;

#[cfg(feature = "error_injection")]
#[inline(always)]
pub fn enable(error: &str, value: impl Into<String>) {
    // SAFETY: safe as long as only called from tx thread
    let injected_errors =
        unsafe { crate::static_ref!(mut INJECTED_ERRORS).get_or_insert_default() };

    // DO NOT LOG "ERROR INJECTION '{error}'" HERE!!!
    // We check this error message in tests to catch the moment the injected error happens
    injected_errors.insert(error.into(), value.into());
}

#[cfg(feature = "error_injection")]
pub fn disable(error: &str) {
    // SAFETY: safe as long as only called from tx thread
    let injected_errors =
        unsafe { crate::static_ref!(mut INJECTED_ERRORS).get_or_insert_default() };

    injected_errors.remove(error);
}

#[inline(always)]
pub fn is_enabled(error: &str) -> bool {
    injected_value(error).is_some()
}

#[inline(always)]
pub fn injected_value(error: &str) -> Option<&str> {
    // SAFETY: safe as long as only called from tx thread
    #[cfg(feature = "error_injection")]
    if let Some(injected_errors) = unsafe { crate::static_ref!(const INJECTED_ERRORS) } {
        return injected_errors.get(error).map(String::as_ref);
    }

    _ = error;
    None
}

// Scan environment variables for `PICODATA_ERROR_INJECTION_<NAME>`-like
// patterns. Arm corresponding injection for each one.
#[cfg(feature = "error_injection")]
pub fn set_from_env() {
    for (key, value) in std::env::vars() {
        let inject_name = match key.strip_prefix("PICODATA_ERROR_INJECTION_") {
            Some(name) => name,
            None => continue,
        };

        enable(inject_name, value)
    }
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
            ::std::process::exit(69);
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
    ($error:expr => return $result:expr) => {{
        let error = $error;
        if $crate::error_injection::is_enabled(error) {
            let result = $result;
            $crate::tlog!(Info, "################################################################");
            $crate::tlog!(Info, "ERROR INJECTION '{error}': RETURNING {result:?}");
            $crate::tlog!(Info, "################################################################");

            return result;
        }
    }};
    ($error:expr => $(| $value:ident |)? $block:block) => {{
        let error = $error;
        if let Some(_value) = $crate::error_injection::injected_value(error) {
            $crate::tlog!(Info, "################################################################");
            $crate::tlog!(Info, "ERROR INJECTION '{error}': EXECUTING");
            $crate::tlog!(Info, "################################################################");
            $( let $value = _value; )?
            $block;
        }
    }}
}

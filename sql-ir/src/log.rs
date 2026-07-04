//! Logging module.

use std::os::raw::c_int;

#[macro_export]
#[allow(unused_variables)]
macro_rules! log {
    ($level:ident, $error:expr, $($message:tt)*) => {{
        let _line: i32 = line!().try_into().unwrap_or(0);
        #[cfg(not(feature = "mock"))]
        {
            let tnt_level: i32 = unsafe { tarantool::ffi::tarantool::LOG_LEVEL };
            if tnt_level >= tarantool::log::SayLevel::$level as i32 {
                tarantool::log::say(
                    tarantool::log::SayLevel::$level,
                    file!(),
                    _line,
                    $error,
                    $($message)*
                );
            }
        }
    }};
}

#[macro_export]
macro_rules! fatal {
    ($error:expr, $($message:tt)*) => {
        $crate::log!(Fatal, $error, $($message)*);
    };
}

#[macro_export]
macro_rules! system {
    ($error:expr, $($message:tt)*) => {
        $crate::log!(System, $error, $($message)*)
    };
}

#[macro_export]
macro_rules! error {
    ($error:expr, $($message:tt)*) => {
        $crate::log!(Error, $error, $($message)*)
    };
}

#[macro_export]
macro_rules! crit {
    ($error:expr, $($message:tt)*) => {
        $crate::log!(Crit, $error, $($message)*)
    };
}

#[macro_export]
macro_rules! warn {
    ($error:expr, $($message:tt)*) => {
        $crate::log!(Warn, $error, $($message)*)
    };
}

#[macro_export]
macro_rules! info {
    ($error:expr, $($message:tt)*) => {
        $crate::log!(Info, $error, $($message)*)
    };
}

#[macro_export]
macro_rules! verbose {
    ($error:expr, $($message:tt)*) => {
        $crate::log!(Verbose, $error, $($message)*)
    };
}

#[macro_export]
macro_rules! debug {
    ($error:expr, $($message:tt)*) => {
        $crate::log!(Debug, $error, $($message)*)
    };
}

#[inline]
#[allow(unreachable_code)]
#[allow(unused_variables)]
#[must_use]
pub fn tarantool_error(message: &str) -> c_int {
    #[cfg(not(feature = "mock"))]
    {
        use tarantool::error::TarantoolErrorCode;
        use tarantool::set_error;

        set_error!(TarantoolErrorCode::ProcC, "Sbroad Error: {}", message);
    }
    -1
}

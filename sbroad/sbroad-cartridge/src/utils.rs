use std::{fmt::Display, os::raw::c_int};

use tarantool::{
    error::IntoBoxError,
    proc::Return,
    tuple::{FunctionCtx, Tuple},
};

/// Adds necessary error prefix and logs error with sbroad's macroses.
#[allow(unused_variables)]
pub fn wrap_proc_result<T, E: Display>(
    target: Option<&str>,
    result: Result<T, E>,
) -> ProcResult<T> {
    result.map_err(|err| {
        let error = format!("Sbroad Error: {err:#}");
        sbroad::error!(target, &error.to_string());
        error
    })
}

pub type ProcResult<T> = Result<T, String>;

#[derive(Debug, Clone)]
pub enum RawProcResult {
    Tuple(Tuple),
    Msgpack(Vec<u8>),
}

impl Return for RawProcResult {
    fn ret(self, ctx: FunctionCtx) -> c_int {
        match self {
            Self::Tuple(tuple) => tuple.ret(ctx),
            Self::Msgpack(msgpack) => ctx
                .return_mp(&msgpack)
                .expect("Failed to return msgpack from proc"),
        }
    }
}

/// [Return] wrapper around result for unsupported variant.
/// Needed because Result<T, E> where T: Return is not supported by tarantool module.
#[derive(Debug, Clone)]
pub struct RetResult<T, E>(pub Result<T, E>);

impl<T: Return, E: Display + IntoBoxError> Return for RetResult<T, E> {
    fn ret(self, ctx: FunctionCtx) -> c_int {
        match self.0 {
            Ok(ok) => ok.ret(ctx),
            Err(err) => Result::<(), E>::Err(err).ret(ctx),
        }
    }
}

use crate::{
    error::TarantoolErrorCode::ProcC,
    ffi::tarantool as ffi,
    set_error,
    tuple::{FunctionCtx, RawByteBuf, RawBytes, Tuple, TupleBuffer},
};
use serde::Serialize;
use std::{fmt::Display, os::raw::c_int};

macro_rules! unwrap_or_report_err {
    ($res:expr) => {
        match $res {
            Ok(o) => o,
            Err(e) => {
                set_error!(ProcC, "{}", e);
                -1
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////
// Proc
////////////////////////////////////////////////////////////////////////////////

/// Description of a tarantool stored procedure defined using the
/// `#[`[`tarantool::proc`]`]` macro attribute.
///
/// See also [`all_procs`].
///
/// [`tarantool::proc`]: macro@crate::proc
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proc {
    name: &'static str,
    proc: ffi::Proc,
}

impl Proc {
    /// Create a new stored proc description.
    ///
    /// This function is called when `#[`[`tarantool::proc`]`]` attribute is
    /// used, so users don't usually use it directly.
    ///
    /// [`tarantool::proc`]: macro@crate::proc
    pub const fn new(name: &'static str, proc: ffi::Proc) -> Self {
        Self { name, proc }
    }

    /// Get the name of the stored procedure NOT including the module name.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Get the proc's function pointer.
    ///
    /// This function is usually not necessary for defining tarantool's stored
    /// procedures, the name is enough. But it is there if you need it for some
    /// reason.
    pub const fn proc(&self) -> ffi::Proc {
        self.proc
    }
}

// Linkme distributed_slice exports a symbol with the given name, so we must
// make sure the name is unique, so as not to conflict with distributed slices
// from other crates or any other global symbols.
#[doc(hidden)]
#[::linkme::distributed_slice]
pub static TARANTOOL_MODULE_STORED_PROCS: [Proc] = [..];

/// Returns a slice of all stored procedures defined using the
/// `#[`[`tarantool::proc`]`]` macro attribute.
///
/// [`tarantool::proc`]: macro@crate::proc
pub fn all_procs() -> &'static [Proc] {
    &*TARANTOOL_MODULE_STORED_PROCS
}

////////////////////////////////////////////////////////////////////////////////
// ReturnMsgpack
////////////////////////////////////////////////////////////////////////////////

/// A wrapper type for returning custom types from stored procedures. Consider
/// using the `custom_ret` attribute parameter instead (see [`tarantool::proc`]
/// docs for examples).
///
/// # using `ReturnMsgpack` directly
///
/// You can either return `ReturnMsgpack` directly:
///
/// ```
/// use tarantool::proc::ReturnMsgpack;
///
/// #[tarantool::proc]
/// fn foo(x: i32) -> ReturnMsgpack<MyStruct> {
///     ReturnMsgpack(MyStruct { x, y: x * 2 })
/// }
///
/// #[derive(serde::Serialize)]
/// struct MyStruct { x: i32, y: i32 }
/// ```
///
/// # implementing `Return` for custom type
///
/// Or you can use it to implement `Return` for your custom type:
///
/// ```
/// use std::os::raw::c_int;
/// use tarantool::{proc::{Return, ReturnMsgpack}, tuple::FunctionCtx};
///
/// #[tarantool::proc]
/// fn foo(x: i32) -> MyStruct {
///     MyStruct { x, y: x * 2 }
/// }
///
/// #[derive(serde::Serialize)]
/// struct MyStruct { x: i32, y: i32 }
///
/// impl Return for MyStruct {
///     fn ret(self, ctx: FunctionCtx) -> c_int {
///         ReturnMsgpack(self).ret(ctx)
///     }
/// }
/// ```
///
/// [`tarantool::proc`]: macro@crate::proc
pub struct ReturnMsgpack<T>(pub T);

impl<T: Serialize> Return for ReturnMsgpack<T> {
    #[inline(always)]
    fn ret(self, ctx: FunctionCtx) -> c_int {
        unwrap_or_report_err!(ctx.return_mp(&self.0))
    }
}

////////////////////////////////////////////////////////////////////////////////
// Return
////////////////////////////////////////////////////////////////////////////////

pub trait Return: Sized {
    fn ret(self, ctx: FunctionCtx) -> c_int;
}

impl Return for Tuple {
    #[inline]
    fn ret(self, ctx: FunctionCtx) -> c_int {
        let res = ctx.return_tuple(&self);
        unwrap_or_report_err!(res)
    }
}

impl<E> Return for Result<Tuple, E>
where
    E: Display,
{
    #[inline(always)]
    fn ret(self, ctx: FunctionCtx) -> c_int {
        unwrap_or_report_err!(self.map(|t| t.ret(ctx)))
    }
}

impl Return for TupleBuffer {
    #[inline]
    fn ret(self, ctx: FunctionCtx) -> c_int {
        let res = ctx.return_bytes(self.as_ref());
        unwrap_or_report_err!(res)
    }
}

impl<E> Return for Result<TupleBuffer, E>
where
    E: Display,
{
    #[inline(always)]
    fn ret(self, ctx: FunctionCtx) -> c_int {
        unwrap_or_report_err!(self.map(|t| t.ret(ctx)))
    }
}

impl Return for &RawBytes {
    #[inline]
    fn ret(self, ctx: FunctionCtx) -> c_int {
        let res = ctx.return_bytes(self);
        unwrap_or_report_err!(res)
    }
}

impl<E> Return for Result<&RawBytes, E>
where
    E: Display,
{
    #[inline(always)]
    fn ret(self, ctx: FunctionCtx) -> c_int {
        unwrap_or_report_err!(self.map(|t| t.ret(ctx)))
    }
}

impl Return for RawByteBuf {
    #[inline]
    fn ret(self, ctx: FunctionCtx) -> c_int {
        let res = ctx.return_bytes(&self);
        unwrap_or_report_err!(res)
    }
}

impl<E> Return for Result<RawByteBuf, E>
where
    E: Display,
{
    #[inline(always)]
    fn ret(self, ctx: FunctionCtx) -> c_int {
        unwrap_or_report_err!(self.map(|t| t.ret(ctx)))
    }
}

impl Return for () {
    #[inline(always)]
    fn ret(self, _: FunctionCtx) -> c_int {
        0
    }
}

impl<O, E> Return for Result<O, E>
where
    O: Serialize,
    E: Display,
{
    #[inline(always)]
    fn ret(self, ctx: FunctionCtx) -> c_int {
        match self {
            Ok(o) => match ctx.return_mp(&o) {
                Ok(_) => 0,
                Err(e) => {
                    set_error!(ProcC, "{}", e);
                    -1
                }
            },
            Err(e) => {
                set_error!(ProcC, "{}", e);
                -1
            }
        }
    }
}

macro_rules! impl_return {
    (impl $([ $( $tp:tt )* ])? for $t:ty) => {
        impl $(< $($tp)* >)? Return for $t
        where
            Self: Serialize,
        {
            #[inline(always)]
            fn ret(self, ctx: FunctionCtx) -> c_int {
                unwrap_or_report_err!(ctx.return_mp(&self))
            }
        }
    };
    ($( $t:ty )+) => {
        $( impl_return!{ impl for $t } )+
    }
}

impl_return! { impl[V]                 for Option<V> }
impl_return! { impl[V]                 for Vec<V> }
impl_return! { impl[V]                 for &'_ [V] }
impl_return! { impl[V, const N: usize] for [V; N] }
impl_return! { impl[K, V]              for std::collections::HashMap<K, V> }
impl_return! { impl[K]                 for std::collections::HashSet<K> }
impl_return! { impl[K, V]              for std::collections::BTreeMap<K, V> }
impl_return! { impl[K]                 for std::collections::BTreeSet<K> }
impl_return! {
    bool
    i8 u8 i16 u16 i32 u32 i64 u64 i128 u128 isize usize
    f32 f64
    String &'_ str
    std::ffi::CString &'_ std::ffi::CStr
}

macro_rules! impl_return_for_tuple {
    () => {};
    ($h:ident $($t:ident)*) => {
        impl<$h, $($t),*> Return for ($h, $($t,)*)
        where
            Self: Serialize,
        {
            #[inline(always)]
            fn ret(self, ctx: FunctionCtx) -> c_int {
                unwrap_or_report_err!(ctx.return_mp(&self))
            }
        }

        impl_return_for_tuple!{$($t)*}
    }
}
impl_return_for_tuple! {A B C D E F G H I J K L M N O P Q}

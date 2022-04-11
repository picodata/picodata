use crate::{
    error::TarantoolErrorCode::ProcC,
    set_error,
    tuple::FunctionCtx,
};
use serde::Serialize;
use std::{
    fmt::Display,
    os::raw::c_int,
};

////////////////////////////////////////////////////////////////////////////////
// ReturnMsgpack
////////////////////////////////////////////////////////////////////////////////

/// A wrapper type for returning custom types form stored procedures.
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
pub struct ReturnMsgpack<T>(pub T);

impl<T: Serialize> Return for ReturnMsgpack<T> {
    #[inline(always)]
    fn ret(self, ctx: FunctionCtx) -> c_int {
        return_mp(self.0, ctx)
    }
}

#[inline(always)]
fn return_mp(v: impl Serialize, ctx: FunctionCtx) -> c_int {
    match ctx.return_mp(&v) {
        Ok(_) => 0,
        Err(e) => {
            set_error!(ProcC, "{}", e);
            -1
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Return
////////////////////////////////////////////////////////////////////////////////

pub trait Return: Sized {
    fn ret(self, ctx: FunctionCtx) -> c_int;
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
            }
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
                return_mp(self, ctx)
            }
        }
    };
    ($( $t:ty )+) => {
        $( impl_return!{ impl for $t } )+
    }
}

impl_return!{ impl[V]                 for Option<V> }
impl_return!{ impl[V]                 for Vec<V> }
impl_return!{ impl[V]                 for &'_ [V] }
impl_return!{ impl[V, const N: usize] for [V; N] }
impl_return!{ impl[K, V]              for std::collections::HashMap<K, V> }
impl_return!{ impl[K]                 for std::collections::HashSet<K> }
impl_return!{ impl[K, V]              for std::collections::BTreeMap<K, V> }
impl_return!{ impl[K]                 for std::collections::BTreeSet<K> }
impl_return!{
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
                return_mp(self, ctx)
            }
        }

        impl_return_for_tuple!{$($t)*}
    }
}
impl_return_for_tuple!{A B C D E F G H I J K L M N O P Q}

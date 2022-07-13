use std::num::NonZeroI32;
use std::fmt::{self, Debug};

use crate::{
    ffi,
    AsLua,
    Push,
    PushInto,
    PushOne,
    PushOneInto,
    PushGuard,
    LuaRead,
    LuaState,
    Void,
    object::{Object, Indexable, Index},
    rust_tables::{push_iter, PushIterError},
};

macro_rules! tuple_impl {
    ////////////////////////////////////////////////////////////////////////////
    // single element
    ////////////////////////////////////////////////////////////////////////////
    ($ty:ident) => {

        ////////////////////////////////////////////////////////////////////////
        // Push
        impl<LU, $ty> Push<LU> for ($ty,)
        where
            LU: AsLua,
            $ty: Push<LU>,
        {
            type Err = TuplePushError<
                <$ty as Push<LU>>::Err,
                Void,
            >;

            #[inline]
            fn push_to_lua(&self, lua: LU) -> Result<PushGuard<LU>, (Self::Err, LU)> {
                self.0.push_to_lua(lua)
                    .map_err(|(e, l)| (TuplePushError::First(e), l))
            }
        }

        impl<LU, $ty> PushOne<LU> for ($ty,)
        where
            LU: AsLua,
            $ty: PushOne<LU>,
        {
        }

        ////////////////////////////////////////////////////////////////////////
        // PushInto
        impl<LU, $ty> PushInto<LU> for ($ty,)
        where
            LU: AsLua,
            $ty: PushInto<LU>,
        {
            type Err = TuplePushError<
                <$ty as PushInto<LU>>::Err,
                Void,
            >;

            #[inline]
            fn push_into_lua(self, lua: LU) -> Result<PushGuard<LU>, (Self::Err, LU)> {
                self.0.push_into_lua(lua)
                    .map_err(|(e, l)| (TuplePushError::First(e), l))
            }
        }

        impl<LU, $ty> PushOneInto<LU> for ($ty,)
        where
            LU: AsLua,
            $ty: PushOneInto<LU>,
        {
        }

        ////////////////////////////////////////////////////////////////////////
        // LuaRead
        impl<LU, $ty> LuaRead<LU> for ($ty,)
        where
            LU: AsLua,
            $ty: LuaRead<LU>,
        {
            fn n_values_expected() -> i32 {
                $ty::n_values_expected()
            }

            #[inline]
            fn lua_read_at_position(lua: LU, index: NonZeroI32) -> Result<($ty,), LU> {
                LuaRead::lua_read_at_position(lua, index).map(|v| (v,))
            }

            #[inline]
            fn lua_read_at_maybe_zero_position(lua: LU, index: i32) -> Result<($ty,), LU> {
                LuaRead::lua_read_at_maybe_zero_position(lua, index).map(|v| (v,))
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // AsTable
        ////////////////////////////////////////////////////////////////////////

        ////////////////////////////////////////////////////////////////////////
        // Push
        impl<LU, $ty> Push<LU> for AsTable<($ty,)>
        where
            LU: AsLua,
            $ty: Push<LuaState>,
        {
            type Err = PushIterError<TuplePushError<$ty::Err, Void>>;

            #[inline]
            fn push_to_lua(&self, lua: LU) -> Result<PushGuard<LU>, (Self::Err, LU)> {
                push_iter(lua, std::iter::once(&self.0.0))
                    .map_err(|(e, l)| (e.map(TuplePushError::First), l))
            }
        }

        impl<LU, $ty> PushOne<LU> for AsTable<($ty,)>
        where
            LU: AsLua,
            $ty: PushOne<LuaState>,
        {
        }

        ////////////////////////////////////////////////////////////////////////
        // PushInto
        impl<LU, $ty> PushInto<LU> for AsTable<($ty,)>
        where
            LU: AsLua,
            $ty: PushInto<LuaState>,
        {
            type Err = PushIterError<TuplePushError<$ty::Err, Void>>;

            #[inline]
            fn push_into_lua(self, lua: LU) -> Result<PushGuard<LU>, (Self::Err, LU)> {
                push_iter(lua, std::iter::once(self.0.0))
                    .map_err(|(e, l)| (e.map(TuplePushError::First), l))
            }
        }

        impl<LU, $ty> PushOneInto<LU> for AsTable<($ty,)>
        where
            LU: AsLua,
            $ty: PushOneInto<LuaState>,
        {
        }

        ////////////////////////////////////////////////////////////////////////
        // LuaRead
        impl<LU, $ty> LuaRead<LU> for AsTable<($ty,)>
        where
            LU: AsLua,
            $ty: for<'a> LuaRead<PushGuard<&'a LU>>,
        {
            #[inline]
            fn lua_read_at_position(lua: LU, index: NonZeroI32) -> Result<Self, LU> {
                let table = Indexable::lua_read_at_position(lua, index)?;
                match table.get(1) {
                    Some(res) => Ok(AsTable(res)),
                    None => Err(Object::from(table).into_guard()),
                }
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // multiple elements
    ////////////////////////////////////////////////////////////////////////////
    ($first:ident, $($other:ident),+) => {
        #[allow(non_snake_case)]
        impl<LU, $first, $($other),+> Push<LU> for ($first, $($other),+)
        where
            LU: AsLua,
            $first: Push<LuaState>,
            $( $other: Push<LuaState>, )+
        {
            type Err = TuplePushError<
                <$first as Push<LuaState>>::Err,
                <($($other,)+) as Push<LuaState>>::Err,
            >;

            #[inline]
            fn push_to_lua(&self, lua: LU) -> Result<PushGuard<LU>, (Self::Err, LU)> {
                use TuplePushError::{First, Other};
                match self {
                    ($first, $($other),+) => {
                        let error = |e| e;
                        let pushed = match lua.as_lua().try_push($first) {
                            Ok(pushed) => pushed,
                            Err((err, _)) => return Err((error(First(err)), lua)),
                        };
                        let total = move || pushed.forget_internal();

                        $(
                            let error = |e| error(Other(e));
                            let pushed = match lua.as_lua().try_push($other) {
                                Ok(pushed) => pushed,
                                // TODO(gmoshkin): return an error capturing the
                                // previously created pushguards so that the
                                // caller can handle partially pushed tuples
                                Err((err, _)) => return Err((error(First(err)), lua)),
                            };
                            let total = move || pushed.forget_internal() + total();
                        )+

                        unsafe {
                            Ok(PushGuard::new(lua, total()))
                        }
                    }
                }
            }
        }

        #[allow(non_snake_case)]
        impl<LU, $first, $($other),+> PushInto<LU> for ($first, $($other),+)
        where
            LU: AsLua,
            $first: PushInto<LuaState>,
            $( $other: PushInto<LuaState>, )+
        {
            type Err = TuplePushError<
                <$first as PushInto<LuaState>>::Err,
                <($($other,)+) as PushInto<LuaState>>::Err,
            >;

            #[inline]
            fn push_into_lua(self, lua: LU) -> Result<PushGuard<LU>, (Self::Err, LU)> {
                use TuplePushError::{First, Other};
                match self {
                    ($first, $($other),+) => {
                        let first_pushed = match lua.as_lua().try_push($first) {
                            Ok(pushed) => pushed,
                            Err((err, _)) => return Err((First(err), lua)),
                        };

                        let other_pushed = match lua.as_lua().try_push(($($other,)+)) {
                            Ok(pushed) => pushed,
                            // TODO(gmoshkin): return an error capturing the
                            // first_pushed pushguard so that the caller can
                            // handle partially pushed tuples
                            Err((err, _)) => return Err((Other(err), lua)),
                        };

                        let total = first_pushed.forget_internal()
                            + other_pushed.forget_internal();

                        unsafe {
                            Ok(PushGuard::new(lua, total))
                        }
                    }
                }
            }
        }

        // TODO: what if T or U are also tuples? indices won't match
        #[allow(unused_assignments)]
        #[allow(non_snake_case)]
        impl<LU, $first, $($other),+> LuaRead<LU> for ($first, $($other),+)
        where
            LU: AsLua,
            $first: for<'a> LuaRead<&'a LU>,
            $($other: for<'a> LuaRead<&'a LU>),+
        {
            #[inline(always)]
            fn n_values_expected() -> i32 {
                $first::n_values_expected() $( + $other::n_values_expected() )+
            }

            #[inline]
            fn lua_read_at_position(lua: LU, index: NonZeroI32) -> Result<($first, $($other),+), LU> {
                LuaRead::lua_read_at_maybe_zero_position(lua, index.into())
            }

            #[inline]
            fn lua_read_at_maybe_zero_position(lua: LU, index: i32) -> Result<($first, $($other),+), LU> {
                let $first: $first = match LuaRead::lua_read_at_maybe_zero_position(&lua, index) {
                    Ok(v) => v,
                    Err(_) => return Err(lua)
                };

                let mut i = index;
                // TODO(gmoshkin): + n_values_expected
                // see comment below
                i = if i == 0 { 0 } else { i + 1 };

                $(
                    let $other: $other = match LuaRead::lua_read_at_maybe_zero_position(&lua, i) {
                        Ok(v) => v,
                        Err(_) => return Err(lua)
                    };
                    // The 0 index is the special case that should not be walked
                    // over. Either we return Err on it or we handle the
                    // situation correctly (e.g. Option<T>, (), ...)
                    // TODO(gmoshkin): + n_values_expected but make sure not to
                    // ignore going over 0
                    i = if i == 0 { 0 } else { i + 1 };
                )+

                Ok(($first, $($other),+))
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // AsTable
        ////////////////////////////////////////////////////////////////////////

        ////////////////////////////////////////////////////////////////////////
        // Push
        #[allow(non_snake_case)]
        impl<LU, $first, $($other),+> Push<LU> for AsTable<($first, $($other),+)>
        where
            LU: AsLua,
            $first: Push<LuaState>,
            $( $other: Push<LuaState>, )+
        {
            type Err = PushIterError<
                TuplePushError<
                    $first::Err,
                    <($($other,)+) as Push<LuaState>>::Err,
                >
            >;

            #[inline]
            fn push_to_lua(&self, lua: LU) -> Result<PushGuard<LU>, (Self::Err, LU)> {
                use TuplePushError::{First, Other};

                let raw_lua = lua.as_lua();
                let table = unsafe {
                    ffi::lua_newtable(raw_lua);
                    PushGuard::new(lua, 1)
                };

                let Self(($first, $($other),+)) = self;
                let i = 1;
                let tuple_error = |e| e;
                if let Err(e) = push_table_entry(raw_lua, i, $first) {
                    return Err((e.map(First).map(tuple_error), table.into_inner()))
                }

                $(
                    let i = i + 1;
                    let tuple_error = |e| tuple_error(Other(e));
                    if let Err(e) = push_table_entry(raw_lua, i, $other) {
                        return Err((e.map(First).map(tuple_error), table.into_inner()))
                    }
                )+

                Ok(table)
            }
        }

        #[allow(non_snake_case)]
        impl<LU, $first, $($other),+> PushOne<LU> for AsTable<($first, $($other),+)>
        where
            LU: AsLua,
            $first: Push<LuaState>,
            $( $other: Push<LuaState>, )+
        {
        }

        ////////////////////////////////////////////////////////////////////////
        // PushInto
        #[allow(non_snake_case)]
        impl<LU, $first, $($other),+> PushInto<LU> for AsTable<($first, $($other),+)>
        where
            LU: AsLua,
            $first: PushInto<LuaState>,
            $( $other: PushInto<LuaState>, )+
        {
            type Err = PushIterError<
                TuplePushError<
                    $first::Err,
                    <($($other,)+) as PushInto<LuaState>>::Err,
                >
            >;

            #[inline]
            fn push_into_lua(self, lua: LU) -> Result<PushGuard<LU>, (Self::Err, LU)> {
                use TuplePushError::{First, Other};

                let raw_lua = lua.as_lua();
                let table = unsafe {
                    ffi::lua_newtable(raw_lua);
                    PushGuard::new(lua, 1)
                };

                let Self(($first, $($other),+)) = self;
                let i = 1;
                let tuple_error = |e| e;
                if let Err(e) = push_table_entry(raw_lua, i, $first) {
                    return Err((e.map(First).map(tuple_error), table.into_inner()))
                }

                $(
                    let i = i + 1;
                    let tuple_error = |e| tuple_error(Other(e));
                    if let Err(e) = push_table_entry(raw_lua, i, $other) {
                        return Err((e.map(First).map(tuple_error), table.into_inner()))
                    }
                )+

                Ok(table)
            }
        }

        #[allow(non_snake_case)]
        impl<LU, $first, $($other),+> PushOneInto<LU> for AsTable<($first, $($other),+)>
        where
            LU: AsLua,
            $first: PushInto<LuaState>,
            $( $other: PushInto<LuaState>, )+
        {
        }

        ////////////////////////////////////////////////////////////////////////
        // LuaRead
        #[allow(non_snake_case)]
        impl<LU, $first, $($other),+> LuaRead<LU> for AsTable<($first, $($other),+)>
        where
            LU: AsLua,
            $first: for<'a> LuaRead<PushGuard<&'a LU>>,
            $($other: for<'a> LuaRead<PushGuard<&'a LU>>),+
        {
            #[inline]
            fn lua_read_at_position(lua: LU, index: NonZeroI32) -> Result<Self, LU> {
                let table = Indexable::lua_read_at_position(lua, index)?;
                let i = 1;
                let $first = match table.get(i) {
                    Some(res) => res,
                    None => return Err(Object::from(table).into_guard()),
                };

                $(
                    let i = i + 1;
                    let $other = match table.get(i) {
                        Some(res) => res,
                        None => return Err(Object::from(table).into_guard()),
                    };
                )+

                Ok(AsTable(($first, $($other),+)))
            }
        }

        tuple_impl!{ $($other),+ }
    };
}

tuple_impl!(A, B, C, D, E, F, G, H, I, J, K, L, M);

/// Error that can happen when pushing multiple values at once.
// TODO: implement Error on that thing
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum TuplePushError<C, O> {
    First(C),
    Other(O),
}

impl<F, O> TuplePushError<F, O> {
    pub fn first(self) -> F
    where
        O: Into<Void>,
    {
        match self {
            Self::First(f) => f,
            Self::Other(_) => unreachable!("no way to construct an instance of Void"),
        }
    }

    pub fn other(self) -> O
    where
        F: Into<Void>,
    {
        match self {
            Self::First(_) => unreachable!("no way to construct an instance of Void"),
            Self::Other(o) => o,
        }
    }
}

impl<H, T> fmt::Display for TuplePushError<H, T>
where
    H: fmt::Display,
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
            "Error during attempt to push multiple values: ({}, ...)",
            TuplePushErrorDisplayHelper(self),
        )
    }
}

struct TuplePushErrorDisplayHelper<'a, H, T>(&'a TuplePushError<H, T>);

impl<H, T> fmt::Display for TuplePushErrorDisplayHelper<'_, H, T>
where
    H: fmt::Display,
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            TuplePushError::First(head) => write!(f, "{}", head),
            TuplePushError::Other(tail) => write!(f, "ok, {}", tail),
        }
    }
}

macro_rules! impl_tuple_push_error {
    [@t] => { Void };
    [@t $h:tt $($t:tt)*] => { TuplePushError<$h, impl_tuple_push_error![@t $($t)*]> };
    () => {};
    ($h:tt $($t:tt)*) => {
        impl<$h, $($t,)*> From<impl_tuple_push_error![@t $h $($t)*]> for Void
        where
            $h: Into<Void>,
            $( $t: Into<Void>, )*
        {
            #[inline]
            fn from(_: impl_tuple_push_error![@t $h $($t)*]) -> Void {
                unreachable!("There's no way to create an instance of Void")
            }
        }
        impl_tuple_push_error!{ $($t)* }
    };
}

impl_tuple_push_error!{A B C D E F G H I J K L M}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
/// A wrapper type for pushing and reading rust tuples as lua tables.
///
/// Useful when working heterogeneous lua tables.
/// ```no_run
/// use tlua::{Lua, AsTable, AnyLuaValue::{LuaNumber, LuaString, LuaBoolean}};
///
/// let lua = Lua::new();
/// lua.checked_set("x", AsTable((true, "two", 3))).unwrap();
///
/// assert_eq!(
///     lua.get("x"),
///     Some([LuaBoolean(true), LuaString("two".into()), LuaNumber(3.0)]),
/// );
/// assert_eq!(lua.get("x"), Some(AsTable((true, "two".to_string(), 3))));
/// ```
pub struct AsTable<T>(pub T);

fn push_table_entry<T>(
    raw_lua: LuaState,
    i: i32,
    v: T,
) -> Result<(), PushIterError<T::Err>>
where
    T: PushInto<LuaState>,
{
    let n_pushed = match raw_lua.try_push(v) {
        Ok(pushed) => pushed.forget_internal(),
        Err((e, _)) => return Err(PushIterError::ValuePushError(e)),
    };
    match n_pushed {
        0 => {}
        1 => unsafe {
            raw_lua.push_one(i).forget();
            // swap index and value
            ffi::lua_insert(raw_lua, -2);
            ffi::lua_settable(raw_lua, -3);
        }
        2 => unsafe {
            ffi::lua_settable(raw_lua, -3);
        }
        n => unsafe {
            drop(PushGuard::new(raw_lua, n));
            return Err(PushIterError::TooManyValues);
        }
    }
    Ok(())
}

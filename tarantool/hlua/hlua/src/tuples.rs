use std::num::NonZeroI32;

use crate::{
    AsLua,
    Push,
    PushOne,
    PushGuard,
    LuaRead,
    LuaState,
    Void,
};

macro_rules! tuple_impl {
    ($ty:ident) => {
        impl<LU, $ty> Push<LU> for ($ty,)
        where
            LU: AsLua,
            $ty: Push<LU>,
        {
            type Err = <$ty as Push<LU>>::Err;

            #[inline]
            fn push_to_lua(self, lua: LU) -> Result<PushGuard<LU>, (Self::Err, LU)> {
                self.0.push_to_lua(lua)
            }
        }

        impl<LU, $ty> PushOne<LU> for ($ty,)
        where
            LU: AsLua,
            $ty: PushOne<LU>,
        {
        }

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
        }
    };

    ($first:ident, $($other:ident),+) => {
        #[allow(non_snake_case)]
        impl<LU, $first, $($other),+> Push<LU> for ($first, $($other),+)
        where
            LU: AsLua,
            Self: std::fmt::Debug,
            $first: Push<LuaState>,
            ($($other,)+): Push<LuaState>,
        {
            type Err = TuplePushError<
                <$first as Push<LuaState>>::Err,
                <($($other,)+) as Push<LuaState>>::Err,
            >;

            #[inline]
            fn push_to_lua(self, lua: LU) -> Result<PushGuard<LU>, (Self::Err, LU)> {
                match self {
                    ($first, $($other),+) => {
                        let mut total = 0;

                        let first_err = match $first.push_to_lua(lua.as_lua()) {
                            Ok(pushed) => { total += pushed.forget_internal(); None },
                            Err((err, _)) => Some(err),
                        };

                        if let Some(err) = first_err {
                            return Err((TuplePushError::First(err), lua));
                        }

                        let rest = ($($other,)+);
                        let other_err = match rest.push_to_lua(lua.as_lua()) {
                            Ok(pushed) => { total += pushed.forget_internal(); None },
                            Err((err, _)) => Some(err),
                        };

                        if let Some(err) = other_err {
                            return Err((TuplePushError::Other(err), lua));
                        }

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
                let $first: $first = match LuaRead::lua_read_at_position(&lua, index) {
                    Ok(v) => v,
                    Err(_) => return Err(lua)
                };

                let mut i: i32 = index.into();
                i += 1;

                $(
                    let $other: $other = match LuaRead::lua_read_at_maybe_zero_position(&lua, i) {
                        Ok(v) => v,
                        Err(_) => return Err(lua)
                    };
                    // The 0 index is the special case that should not be walked
                    // over. Either we return Err on it or we handle the
                    // situation correctly (e.g. Option<T>, (), ...)
                    i = if i == 0 { 0 } else { i + 1 };
                )+

                Ok(($first, $($other),+))

            }
        }

        tuple_impl!{ $($other),+ }
    };
}

tuple_impl!(A, B, C, D, E, F, G, H, I, J, K, L, M);

/// Error that can happen when pushing multiple values at once.
// TODO: implement Error on that thing
#[derive(Debug, Copy, Clone)]
pub enum TuplePushError<C, O> {
    First(C),
    Other(O),
}

impl From<TuplePushError<Void, Void>> for Void {
    #[inline]
    fn from(_: TuplePushError<Void, Void>) -> Void {
        unreachable!()
    }
}

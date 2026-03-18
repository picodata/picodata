use std::num::NonZeroI32;

use crate::{
    AsLua, LuaRead, LuaTable, Nil, Push, PushGuard, PushInto, PushOne, PushOneInto, ReadResult,
    Void,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AnyLuaString(pub Vec<u8>);

impl AnyLuaString {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

/// Represents any value that can be stored by Lua
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AnyHashableLuaValue {
    // TODO(gmoshkin): remove Lua prefix
    LuaString(String),
    LuaAnyString(AnyLuaString),
    LuaNumber(i32),
    // TODO(gmoshkin): True, False
    LuaBoolean(bool),
    LuaArray(Vec<(AnyHashableLuaValue, AnyHashableLuaValue)>),
    LuaNil,

    /// The "Other" element is (hopefully) temporary and will be replaced by "Function" and "Userdata".
    /// A panic! will trigger if you try to push a Other.
    LuaOther,
}

/// Represents any value that can be stored by Lua
#[derive(Clone, Debug, PartialEq)]
pub enum AnyLuaValue {
    // TODO(gmoshkin): remove Lua prefix
    LuaString(String),
    LuaAnyString(AnyLuaString),
    LuaNumber(f64),
    // TODO(gmoshkin): True, False
    LuaBoolean(bool),
    LuaArray(Vec<(AnyLuaValue, AnyLuaValue)>),
    LuaNil,

    /// The "Other" element is (hopefully) temporary and will be replaced by "Function" and "Userdata".
    /// A panic! will trigger if you try to push a Other.
    LuaOther,
}

macro_rules! impl_any_lua_value {
    (@push $self:expr, $lua:expr, $push:ident) => {
        Ok(match $self {
            Self::LuaString(val) => val.$push($lua),
            Self::LuaAnyString(val) => val.$push($lua),
            Self::LuaNumber(val) => val.$push($lua),
            Self::LuaBoolean(val) => val.$push($lua),
            Self::LuaArray(val) => val.$push($lua),
            Self::LuaNil => Nil.$push($lua),
            Self::LuaOther => panic!("can't push a AnyLuaValue of type Other"),
        })
    };
    ($t:ty) => {
        impl<L: AsLua> Push<L> for $t {
            type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

            #[inline]
            fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
                impl_any_lua_value!(@push self, lua, push_no_err)
            }
        }

        impl<L: AsLua> PushOne<L> for $t {}

        impl<L: AsLua> PushInto<L> for $t {
            type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

            #[inline]
            fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
                impl_any_lua_value!(@push self, lua, push_into_no_err)
            }
        }

        impl<L: AsLua> PushOneInto<L> for $t {}

        impl<L: AsLua> LuaRead<L> for $t {
            #[inline]
            fn lua_read_at_position(lua: L, index: NonZeroI32) -> ReadResult<Self, L> {
                let lua = match LuaRead::lua_read_at_position(lua, index) {
                    Ok(v) => return Ok(Self::LuaString(v)),
                    Err((lua, _)) => lua,
                };

                let lua = match LuaRead::lua_read_at_position(lua, index) {
                    Ok(v) => return Ok(Self::LuaAnyString(v)),
                    Err((lua, _)) => lua,
                };

                let lua = match LuaRead::lua_read_at_position(lua, index) {
                    Ok(v) => return Ok(Self::LuaNumber(v)),
                    Err((lua, _)) => lua,
                };

                let lua = match LuaRead::lua_read_at_position(lua, index) {
                    Ok(v) => return Ok(Self::LuaBoolean(v)),
                    Err((lua, _)) => lua,
                };

                let lua = match LuaRead::lua_read_at_position(lua, index) {
                    Ok(v) => return Ok(Self::LuaString(v)),
                    Err((lua, _)) => lua,
                };

                let lua = match LuaRead::lua_read_at_position(lua, index) {
                    Ok(v) => return Ok(Self::LuaAnyString(v)),
                    Err((lua, _)) => lua,
                };

                let lua = match Nil::lua_read_at_position(lua, index) {
                    Ok(Nil) => return Ok(Self::LuaNil),
                    Err((lua, _)) => lua,
                };

                let _ = match LuaTable::lua_read_at_position(lua.as_lua(), index) {
                    Ok(v) => return Ok(
                        Self::LuaArray(v.iter::<Self, Self>().flatten().collect())
                    ),
                    Err((lua, _)) => lua,
                };

                Ok(Self::LuaOther)
            }
        }
    }
}

impl_any_lua_value! {AnyLuaValue}
impl_any_lua_value! {AnyHashableLuaValue}

/// A helper struct which is useful when used as one of the values in multiple
/// return values from lua, if you don't care about the actual value, but need
/// to put something in there so that the code compiles and runs correctly.
///
/// This struct's [`LuaRead`] implementation always returns `Ok(Ignore)` no
/// matter what the type of the value on the lua stack, or even if there is a
/// value or not.
///
/// This struct also implements the [`Push`] family of traits for your
/// convenience. The implementation simply pushes a `nil` value.
///
/// You can also use `Option<Ignore>` to see if there was a value, but don't
/// care to check the value's type.
///
/// See also [`crate::Typename`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ignore;

impl<L> LuaRead<L> for Ignore
where
    L: AsLua,
{
    fn lua_read_at_maybe_zero_position(_lua: L, _index: i32) -> ReadResult<Ignore, L> {
        Ok(Ignore)
    }

    fn lua_read_at_position(_lua: L, _index: NonZeroI32) -> ReadResult<Ignore, L> {
        Ok(Ignore)
    }
}

impl<L> Push<L> for Ignore
where
    L: AsLua,
{
    type Err = Void;

    #[inline(always)]
    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
        crate::Nil.push_to_lua(lua)
    }
}

impl<L> PushOne<L> for Ignore where L: AsLua {}

impl<L> PushInto<L> for Ignore
where
    L: AsLua,
{
    type Err = Void;

    #[inline(always)]
    fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
        crate::Nil.push_to_lua(lua)
    }
}

impl<L> PushOneInto<L> for Ignore where L: AsLua {}

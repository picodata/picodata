use std::num::NonZeroI32;

use crate::{AsLua, LuaRead, LuaTable, Nil, Push, PushGuard, PushInto, PushOne, PushOneInto, Void};

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
            fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Self, L> {
                let lua = match LuaRead::lua_read_at_position(lua, index) {
                    Ok(v) => return Ok(Self::LuaString(v)),
                    Err(lua) => lua,
                };

                let lua = match LuaRead::lua_read_at_position(lua, index) {
                    Ok(v) => return Ok(Self::LuaAnyString(v)),
                    Err(lua) => lua,
                };

                let lua = match LuaRead::lua_read_at_position(lua, index) {
                    Ok(v) => return Ok(Self::LuaNumber(v)),
                    Err(lua) => lua,
                };

                let lua = match LuaRead::lua_read_at_position(lua, index) {
                    Ok(v) => return Ok(Self::LuaBoolean(v)),
                    Err(lua) => lua,
                };

                let lua = match LuaRead::lua_read_at_position(lua, index) {
                    Ok(v) => return Ok(Self::LuaString(v)),
                    Err(lua) => lua,
                };

                let lua = match LuaRead::lua_read_at_position(lua, index) {
                    Ok(v) => return Ok(Self::LuaAnyString(v)),
                    Err(lua) => lua,
                };

                let lua = match Nil::lua_read_at_position(lua, index) {
                    Ok(Nil) => return Ok(Self::LuaNil),
                    Err(lua) => lua,
                };

                let _ = match LuaTable::lua_read_at_position(lua.as_lua(), index) {
                    Ok(v) => return Ok(
                        Self::LuaArray(v.iter::<Self, Self>().flatten().collect())
                    ),
                    Err(lua) => lua,
                };

                Ok(Self::LuaOther)
            }
        }
    }
}

impl_any_lua_value! {AnyLuaValue}
impl_any_lua_value! {AnyHashableLuaValue}

use std::mem::transmute;
use tarantool::tlua;
use tlua::ffi;
use tlua::AsLua;
use tlua::Void;
use tlua::WrongType;

////////////////////////////////////////////////////////////////////////////////
// YamlValue
////////////////////////////////////////////////////////////////////////////////

// TODO: just implement tlua for serde_yaml::Value in tlua
/// Represents any valid YAML value.
#[derive(Debug, Clone, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
#[repr(transparent)]
pub struct YamlValue(pub serde_yaml::Value);

impl Default for YamlValue {
    #[inline(always)]
    fn default() -> Self {
        Self(serde_yaml::Value::Null)
    }
}

impl<L: AsLua> tlua::Push<L> for YamlValue {
    type Err = Void;

    #[inline]
    fn push_to_lua(&self, lua: L) -> Result<tlua::PushGuard<L>, (Void, L)> {
        use serde_yaml::Value;
        Ok(match &self.0 {
            Value::String(val) => val.push_no_err(lua),
            Value::Number(val) => val.as_f64().push_no_err(lua),
            Value::Bool(val) => val.push_no_err(lua),
            Value::Sequence(val) => {
                let iter = val.iter().map(|v| {
                    // Safety: this is safe, because YamlValue is marked
                    // with #[repr(transparent)].
                    unsafe { transmute::<&Value, &YamlValue>(v) }
                });

                match lua.push_iter(iter) {
                    Ok(g) => g,
                    Err(_) => {
                        unreachable!("pushing a yaml value cannot fail");
                    }
                }
            }
            Value::Mapping(val) => {
                let iter = val.into_iter().map(|(k, v)| {
                    // Safety: this is safe, because YamlValue is marked
                    // with #[repr(transparent)].
                    unsafe { transmute::<(&Value, &Value), (&YamlValue, &YamlValue)>((k, v)) }
                });

                match lua.push_iter(iter) {
                    Ok(g) => g,
                    Err(_) => {
                        unreachable!("pushing a yaml value cannot fail");
                    }
                }
            }
            Value::Tagged(val) => {
                // Note: ignoring the tag for now
                // Safety: this is safe, because YamlValue is marked
                // with #[repr(transparent)].
                let val = unsafe { transmute::<&Value, &YamlValue>(&val.value) };
                val.push_no_err(lua)
            }
            Value::Null => tlua::Nil.push_no_err(lua),
        })
    }
}
impl<L: AsLua> tlua::PushOne<L> for YamlValue {}

impl<L: AsLua> tlua::PushInto<L> for YamlValue {
    type Err = Void;

    #[inline]
    fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (Void, L)> {
        use serde_yaml::Value;
        Ok(match self.0 {
            Value::String(val) => val.push_into_no_err(lua),
            Value::Number(val) => val.as_f64().push_into_no_err(lua),
            Value::Bool(val) => val.push_into_no_err(lua),
            Value::Sequence(val) => {
                let iter = val.into_iter().map(|v| {
                    // Safety: this is safe, because YamlValue is marked
                    // with #[repr(transparent)] and we don't have a custom
                    // Drop implementation.
                    unsafe { transmute::<Value, YamlValue>(v) }
                });

                match lua.push_iter(iter) {
                    Ok(g) => g,
                    Err(_) => {
                        unreachable!("pushing a yaml value cannot fail");
                    }
                }
            }
            Value::Mapping(val) => {
                let iter = val.into_iter().map(|(k, v)| {
                    // Safety: this is safe, because YamlValue is marked
                    // with #[repr(transparent)] and we don't have a custom
                    // Drop implementation.
                    unsafe { transmute::<(Value, Value), (YamlValue, YamlValue)>((k, v)) }
                });

                match lua.push_iter(iter) {
                    Ok(g) => g,
                    Err(_) => {
                        unreachable!("pushing a yaml value cannot fail");
                    }
                }
            }
            Value::Tagged(val) => {
                // Note: ignoring the tag for now
                // Safety: this is safe, because YamlValue is marked
                // with #[repr(transparent)] and we don't have a custom
                // Drop implementation.
                let val = unsafe { transmute::<Value, YamlValue>(val.value) };
                val.push_into_no_err(lua)
            }
            Value::Null => tlua::Nil.push_into_no_err(lua),
        })
    }
}
impl<L: AsLua> tlua::PushOneInto<L> for YamlValue {}

impl<L: AsLua> tlua::LuaRead<L> for YamlValue {
    #[inline]
    fn lua_read_at_position(lua: L, index: std::num::NonZeroI32) -> tlua::ReadResult<Self, L> {
        use serde_yaml::Value;
        // Safety: safe as long `L::as_lua` implementation is valid
        let t = unsafe { ffi::lua_type(lua.as_lua(), index.get()) };
        match t {
            ffi::LUA_TNIL => {
                return Ok(Self(Value::Null));
            }
            ffi::LUA_TBOOLEAN => {
                let Ok(v) = bool::lua_read_at_position(lua, index) else {
                    unreachable!("already checked the type");
                };
                return Ok(Self(Value::Bool(v)));
            }
            ffi::LUA_TNUMBER => {
                let Ok(v) = f64::lua_read_at_position(lua, index) else {
                    unreachable!("already checked the type");
                };
                return Ok(Self(Value::from(v)));
            }
            ffi::LUA_TCDATA => {
                // We only support numeric cdata currently
                let res = f64::lua_read_at_position(lua, index);
                let v = tlua::unwrap_ok_or!(res,
                    Err((lua, err)) => {
                        let e = err
                            .when("reading a json value")
                            .expected_type::<Value>();
                        return Err((lua, e));
                    }
                );
                return Ok(Self(Value::from(v)));
            }
            ffi::LUA_TSTRING => {
                let Ok(v) = String::lua_read_at_position(lua, index) else {
                    unreachable!("already checked the type");
                };
                return Ok(Self(Value::String(v)));
            }
            ffi::LUA_TTABLE => {
                // XXX: we erase the current type `L` using the raw pointer to
                // the lua context instead, because otherwise there's an
                // infinite recursion in the compiler when it's trying to
                // resolve the stupid trait bounds
                let type_erased_lua = lua.as_lua();
                if let Ok(vals) = Vec::<Self>::lua_read_at_position(type_erased_lua, index) {
                    let vals = vals.into_iter().map(|v| v.0).collect();
                    return Ok(Self(Value::Sequence(vals)));
                }

                let Ok(table) = tlua::LuaTable::lua_read_at_position(type_erased_lua, index) else {
                    unreachable!("already checked the type");
                };

                let res: Result<_, _> = table
                    .iter::<Self, Self>()
                    .map(|r| r.map(|(k, v)| (k.0, v.0)))
                    .collect();
                let v: serde_yaml::value::Mapping = tlua::unwrap_ok_or!(res,
                    Err(err) => {
                        let e = err
                            .when("converting Lua table to a yaml mapping")
                            .expected("a lua table with valid keys");
                        return Err((lua, e));
                    }
                );
                return Ok(Self(Value::Mapping(v)));
            }
            ffi::LUA_TFUNCTION
            | ffi::LUA_TLIGHTUSERDATA
            | ffi::LUA_TUSERDATA
            | ffi::LUA_TTHREAD => {
                let e = WrongType::default()
                    .expected("a json-compatible value")
                    .actual_single_lua(&lua, index);
                return Err((lua, e));
            }
            ffi::LUA_TNONE => {
                unreachable!("index is not zero, there must be value there")
            }
            _ => {
                unreachable!("all possible values are covered")
            }
        }
    }
}

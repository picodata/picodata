use std::num::NonZeroI32;

use crate::{
    AsLua,
    Push,
    PushGuard,
    PushOne,
    LuaRead,
    LuaTable,
    Nil,
    Void,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AnyLuaString(pub Vec<u8>);

/// Represents any value that can be stored by Lua
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AnyHashableLuaValue {
    LuaString(String),
    LuaAnyString(AnyLuaString),
    LuaNumber(i32),
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
    LuaString(String),
    LuaAnyString(AnyLuaString),
    LuaNumber(f64),
    LuaBoolean(bool),
    LuaArray(Vec<(AnyLuaValue, AnyLuaValue)>),
    LuaNil,

    /// The "Other" element is (hopefully) temporary and will be replaced by "Function" and "Userdata".
    /// A panic! will trigger if you try to push a Other.
    LuaOther,
}

impl<'lua, L> Push<L> for AnyLuaValue
where
    L: AsLua,
{
    type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

    #[inline]
    fn push_to_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
        match self {
            AnyLuaValue::LuaString(val) => val.push_to_lua(lua),
            AnyLuaValue::LuaAnyString(val) => val.push_to_lua(lua),
            AnyLuaValue::LuaNumber(val) => val.push_to_lua(lua),
            AnyLuaValue::LuaBoolean(val) => val.push_to_lua(lua),
            AnyLuaValue::LuaArray(val) => Ok(val.push_no_err(lua)),
            AnyLuaValue::LuaNil => Nil.push_to_lua(lua),
            AnyLuaValue::LuaOther => panic!("can't push a AnyLuaValue of type Other"),
        }
    }
}

impl<'lua, L> PushOne<L> for AnyLuaValue
where
    L: AsLua,
{
}

impl<L: AsLua> LuaRead<L> for AnyLuaValue {
    #[inline]
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<AnyLuaValue, L> {

        // If we know that the value on the stack is a string, we should try
        // to parse it as a string instead of a number or boolean, so that
        // values such as '1.10' don't become `AnyLuaValue::LuaNumber(1.1)`.
        let data_type = unsafe { ffi::lua_type(lua.as_lua(), index.into()) };
        if data_type == ffi::LUA_TSTRING {

            let lua = match LuaRead::lua_read_at_position(lua, index) {
                Ok(v) => return Ok(AnyLuaValue::LuaString(v)),
                Err(lua) => lua,
            };

            let _lua = match LuaRead::lua_read_at_position(lua, index) {
                Ok(v) => return Ok(AnyLuaValue::LuaAnyString(v)),
                Err(lua) => lua,
            };

            Ok(AnyLuaValue::LuaOther)

        } else {

            let lua = match LuaRead::lua_read_at_position(lua, index) {
                Ok(v) => return Ok(AnyLuaValue::LuaNumber(v)),
                Err(lua) => lua,
            };

            let lua = match LuaRead::lua_read_at_position(lua, index) {
                Ok(v) => return Ok(AnyLuaValue::LuaBoolean(v)),
                Err(lua) => lua,
            };

            let lua = match LuaRead::lua_read_at_position(lua, index) {
                Ok(v) => return Ok(AnyLuaValue::LuaString(v)),
                Err(lua) => lua,
            };

            let lua = match LuaRead::lua_read_at_position(lua, index) {
                Ok(v) => return Ok(AnyLuaValue::LuaAnyString(v)),
                Err(lua) => lua,
            };

            let lua = match Nil::lua_read_at_position(lua, index) {
                Ok(Nil) => return Ok(AnyLuaValue::LuaNil),
                Err(lua) => lua,
            };

            let table = LuaTable::lua_read_at_position(lua.as_lua(), index);
            let _lua = match table {
                Ok(v) => return Ok(
                    AnyLuaValue::LuaArray(
                        v.iter::<AnyLuaValue, AnyLuaValue>().flatten().collect()
                    )
                ),
                Err(lua) => lua,
            };

            Ok(AnyLuaValue::LuaOther)
        }
    }
}

impl<'lua, L> Push<L> for AnyHashableLuaValue
where
    L: AsLua,
{
    type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

    #[inline]
    fn push_to_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
        match self {
            AnyHashableLuaValue::LuaString(val) => val.push_to_lua(lua),
            AnyHashableLuaValue::LuaAnyString(val) => val.push_to_lua(lua),
            AnyHashableLuaValue::LuaNumber(val) => val.push_to_lua(lua),
            AnyHashableLuaValue::LuaBoolean(val) => val.push_to_lua(lua),
            AnyHashableLuaValue::LuaArray(val) => Ok(val.push_no_err(lua)),
            AnyHashableLuaValue::LuaNil => Nil.push_to_lua(lua),
            AnyHashableLuaValue::LuaOther => panic!("can't push a AnyHashableLuaValue of type Other"),
        }
    }
}

impl<'lua, L> PushOne<L> for AnyHashableLuaValue
where
    L: AsLua,
{
}

impl<'lua, L> LuaRead<L> for AnyHashableLuaValue
where
    L: AsLua,
{
    #[inline]
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<AnyHashableLuaValue, L> {
        let data_type = unsafe { ffi::lua_type(lua.as_lua(), index.into()) };
        if data_type == ffi::LUA_TSTRING {

            let lua = match LuaRead::lua_read_at_position(lua, index) {
                Ok(v) => return Ok(AnyHashableLuaValue::LuaString(v)),
                Err(lua) => lua,
            };

            let _lua = match LuaRead::lua_read_at_position(lua, index) {
                Ok(v) => return Ok(AnyHashableLuaValue::LuaAnyString(v)),
                Err(lua) => lua,
            };

            Ok(AnyHashableLuaValue::LuaOther)

        } else {

            let lua = match LuaRead::lua_read_at_position(lua, index) {
                Ok(v) => return Ok(AnyHashableLuaValue::LuaNumber(v)),
                Err(lua) => lua,
            };

            let lua = match LuaRead::lua_read_at_position(lua, index) {
                Ok(v) => return Ok(AnyHashableLuaValue::LuaBoolean(v)),
                Err(lua) => lua,
            };

            let lua = match LuaRead::lua_read_at_position(lua, index) {
                Ok(v) => return Ok(AnyHashableLuaValue::LuaString(v)),
                Err(lua) => lua,
            };

            let lua = match LuaRead::lua_read_at_position(lua, index) {
                Ok(v) => return Ok(AnyHashableLuaValue::LuaAnyString(v)),
                Err(lua) => lua,
            };

            let lua = match Nil::lua_read_at_position(lua, index) {
                Ok(Nil) => return Ok(AnyHashableLuaValue::LuaNil),
                Err(lua) => lua,
            };

            let table = LuaTable::lua_read_at_position(lua.as_lua(), index);
            let _lua = match table {
                Ok(v) => return Ok(AnyHashableLuaValue::LuaArray(
                    v.iter::<AnyHashableLuaValue, AnyHashableLuaValue>()
                        .flatten().collect()
                )),
                Err(lua) => lua,
            };

            Ok(AnyHashableLuaValue::LuaOther)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Lua,
        AnyLuaValue,
        AnyHashableLuaValue,
        AnyLuaString,
    };

    #[test]
    fn read_numbers() {
        let mut lua = Lua::new();

        lua.set("a", "-2");
        lua.set("b", 3.5f32);
        lua.set("c", -2.0f32);

        let x: AnyLuaValue = lua.get("a").unwrap();
        assert_eq!(x, AnyLuaValue::LuaString("-2".to_owned()));

        let y: AnyLuaValue = lua.get("b").unwrap();
        assert_eq!(y, AnyLuaValue::LuaNumber(3.5));

        let z: AnyLuaValue = lua.get("c").unwrap();
        assert_eq!(z, AnyLuaValue::LuaNumber(-2.0));
    }

    #[test]
    fn read_hashable_numbers() {
        let mut lua = Lua::new();

        lua.set("a", -2.0f32);
        lua.set("b", 4.0f32);
        lua.set("c", "4");

        let x: AnyHashableLuaValue = lua.get("a").unwrap();
        assert_eq!(x, AnyHashableLuaValue::LuaNumber(-2));

        let y: AnyHashableLuaValue = lua.get("b").unwrap();
        assert_eq!(y, AnyHashableLuaValue::LuaNumber(4));

        let z: AnyHashableLuaValue = lua.get("c").unwrap();
        assert_eq!(z, AnyHashableLuaValue::LuaString("4".to_owned()));
    }

    #[test]
    fn read_strings() {
        let mut lua = Lua::new();

        lua.set("a", "hello");
        lua.set("b", "3x");
        lua.set("c", "false");

        let x: AnyLuaValue = lua.get("a").unwrap();
        assert_eq!(x, AnyLuaValue::LuaString("hello".to_string()));

        let y: AnyLuaValue = lua.get("b").unwrap();
        assert_eq!(y, AnyLuaValue::LuaString("3x".to_string()));

        let z: AnyLuaValue = lua.get("c").unwrap();
        assert_eq!(z, AnyLuaValue::LuaString("false".to_string()));
    }

    #[test]
    fn read_hashable_strings() {
        let mut lua = Lua::new();

        lua.set("a", "hello");
        lua.set("b", "3x");
        lua.set("c", "false");

        let x: AnyHashableLuaValue = lua.get("a").unwrap();
        assert_eq!(x, AnyHashableLuaValue::LuaString("hello".to_string()));

        let y: AnyHashableLuaValue = lua.get("b").unwrap();
        assert_eq!(y, AnyHashableLuaValue::LuaString("3x".to_string()));

        let z: AnyHashableLuaValue = lua.get("c").unwrap();
        assert_eq!(z, AnyHashableLuaValue::LuaString("false".to_string()));
    }

    #[test]
    fn read_booleans() {
        let mut lua = Lua::new();

        lua.set("a", true);
        lua.set("b", false);

        let x: AnyLuaValue = lua.get("a").unwrap();
        assert_eq!(x, AnyLuaValue::LuaBoolean(true));

        let y: AnyLuaValue = lua.get("b").unwrap();
        assert_eq!(y, AnyLuaValue::LuaBoolean(false));
    }

    #[test]
    fn read_hashable_booleans() {
        let mut lua = Lua::new();

        lua.set("a", true);
        lua.set("b", false);

        let x: AnyHashableLuaValue = lua.get("a").unwrap();
        assert_eq!(x, AnyHashableLuaValue::LuaBoolean(true));

        let y: AnyHashableLuaValue = lua.get("b").unwrap();
        assert_eq!(y, AnyHashableLuaValue::LuaBoolean(false));
    }

    #[test]
    fn read_tables() {
        let mut lua = Lua::new();
        lua.execute::<()>("
        a = {x = 12, y = 19}
        b = {z = a, w = 'test string'}
        c = {'first', 'second'}
        ").unwrap();

        fn get<'a>(table: &'a AnyLuaValue, key: &str) -> &'a AnyLuaValue {
            let test_key = AnyLuaValue::LuaString(key.to_owned());
            match table {
                &AnyLuaValue::LuaArray(ref vec) => {
                    let &(_, ref value) = vec.iter().find(|&&(ref key, _)| key == &test_key).expect("key not found");
                    value
                },
                _ => panic!("not a table")
            }
        }

        fn get_numeric<'a>(table: &'a AnyLuaValue, key: usize) -> &'a AnyLuaValue {
            let test_key = AnyLuaValue::LuaNumber(key as f64);
            match table {
                &AnyLuaValue::LuaArray(ref vec) => {
                    let &(_, ref value) = vec.iter().find(|&&(ref key, _)| key == &test_key).expect("key not found");
                    value
                },
                _ => panic!("not a table")
            }
        }

        let a: AnyLuaValue = lua.get("a").unwrap();
        assert_eq!(get(&a, "x"), &AnyLuaValue::LuaNumber(12.0));
        assert_eq!(get(&a, "y"), &AnyLuaValue::LuaNumber(19.0));

        let b: AnyLuaValue = lua.get("b").unwrap();
        assert_eq!(get(&get(&b, "z"), "x"), get(&a, "x"));
        assert_eq!(get(&get(&b, "z"), "y"), get(&a, "y"));

        let c: AnyLuaValue = lua.get("c").unwrap();
        assert_eq!(get_numeric(&c, 1), &AnyLuaValue::LuaString("first".to_owned()));
        assert_eq!(get_numeric(&c, 2), &AnyLuaValue::LuaString("second".to_owned()));
    }

    #[test]
    fn read_hashable_tables() {
        let mut lua = Lua::new();
        lua.execute::<()>("
        a = {x = 12, y = 19}
        b = {z = a, w = 'test string'}
        c = {'first', 'second'}
        ").unwrap();

        fn get<'a>(table: &'a AnyHashableLuaValue, key: &str) -> &'a AnyHashableLuaValue {
            let test_key = AnyHashableLuaValue::LuaString(key.to_owned());
            match table {
                &AnyHashableLuaValue::LuaArray(ref vec) => {
                    let &(_, ref value) = vec.iter().find(|&&(ref key, _)| key == &test_key).expect("key not found");
                    value
                },
                _ => panic!("not a table")
            }
        }

        fn get_numeric<'a>(table: &'a AnyHashableLuaValue, key: usize) -> &'a AnyHashableLuaValue {
            let test_key = AnyHashableLuaValue::LuaNumber(key as i32);
            match table {
                &AnyHashableLuaValue::LuaArray(ref vec) => {
                    let &(_, ref value) = vec.iter().find(|&&(ref key, _)| key == &test_key).expect("key not found");
                    value
                },
                _ => panic!("not a table")
            }
        }

        let a: AnyHashableLuaValue = lua.get("a").unwrap();
        assert_eq!(get(&a, "x"), &AnyHashableLuaValue::LuaNumber(12));
        assert_eq!(get(&a, "y"), &AnyHashableLuaValue::LuaNumber(19));

        let b: AnyHashableLuaValue = lua.get("b").unwrap();
        assert_eq!(get(&get(&b, "z"), "x"), get(&a, "x"));
        assert_eq!(get(&get(&b, "z"), "y"), get(&a, "y"));

        let c: AnyHashableLuaValue = lua.get("c").unwrap();
        assert_eq!(get_numeric(&c, 1), &AnyHashableLuaValue::LuaString("first".to_owned()));
        assert_eq!(get_numeric(&c, 2), &AnyHashableLuaValue::LuaString("second".to_owned()));
    }

    #[test]
    fn push_numbers() {
        let mut lua = Lua::new();

        lua.set("a", AnyLuaValue::LuaNumber(3.0));

        let x: i32 = lua.get("a").unwrap();
        assert_eq!(x, 3);
    }

    #[test]
    fn push_hashable_numbers() {
        let mut lua = Lua::new();

        lua.set("a", AnyHashableLuaValue::LuaNumber(3));

        let x: i32 = lua.get("a").unwrap();
        assert_eq!(x, 3);
    }

    #[test]
    fn push_strings() {
        let mut lua = Lua::new();

        lua.set("a", AnyLuaValue::LuaString("hello".to_string()));

        let x: String = lua.get("a").unwrap();
        assert_eq!(x, "hello");
    }

    #[test]
    fn push_hashable_strings() {
        let mut lua = Lua::new();

        lua.set("a", AnyHashableLuaValue::LuaString("hello".to_string()));

        let x: String = lua.get("a").unwrap();
        assert_eq!(x, "hello");
    }

    #[test]
    fn push_booleans() {
        let mut lua = Lua::new();

        lua.set("a", AnyLuaValue::LuaBoolean(true));

        let x: bool = lua.get("a").unwrap();
        assert_eq!(x, true);
    }

    #[test]
    fn push_hashable_booleans() {
        let mut lua = Lua::new();

        lua.set("a", AnyHashableLuaValue::LuaBoolean(true));

        let x: bool = lua.get("a").unwrap();
        assert_eq!(x, true);
    }

    #[test]
    fn push_nil() {
        let mut lua = Lua::new();

        lua.set("a", AnyLuaValue::LuaNil);

        let x: Option<i32> = lua.get("a");
        assert!(x.is_none(),
                "x is a Some value when it should be a None value. X: {:?}",
                x);
    }

    #[test]
    fn push_hashable_nil() {
        let mut lua = Lua::new();

        lua.set("a", AnyHashableLuaValue::LuaNil);

        let x: Option<i32> = lua.get("a");
        assert!(x.is_none(),
                "x is a Some value when it should be a None value. X: {:?}",
                x);
    }

    #[test]
    fn non_utf_8_string() {
        let mut lua = Lua::new();
        let a = lua.execute::<AnyLuaValue>(r"return '\xff\xfe\xff\xfe'").unwrap();
        match a {
            AnyLuaValue::LuaAnyString(AnyLuaString(v)) => {
                assert_eq!(Vec::from(&b"\xff\xfe\xff\xfe"[..]), v);
            },
            _ => panic!("Decoded to wrong variant"),
        }
    }
}

use tarantool::define_str_enum;
use tarantool::tlua;

/// Ensure the macro supports:
/// - docstrings,
/// - all claimed traits,
/// - deriving other traits (Default in this case).
pub fn basic() {
    define_str_enum! {
        enum Color {
            /// Black as night
            Black = "#000000",
            /// White as snow
            White = "#ffffff",
        }
    }

    impl Default for Color {
        fn default() -> Self {
            Self::Black
        }
    }

    assert_eq!(Color::default(), Color::Black);
    assert_eq!(Color::Black.as_ref(), "#000000");
    assert_eq!(Color::White.as_str(), "#ffffff");

    // PartialEq, PartialOrd
    assert_eq!(Color::Black == Color::White, false);
    assert_eq!(Color::Black <= Color::White, true);

    // Debug, Display
    assert_eq!(format!("{:?}", Color::Black), "Black");
    assert_eq!(format!("{}", Color::White), "#ffffff");
    assert_eq!(String::from(Color::White), "#ffffff");

    // std::str::FromStr
    use std::str::FromStr;
    assert_eq!(Color::from_str(" #FFFFFF "), Ok(Color::White));
    assert_eq!(Color::from_str("#000000"), Ok(Color::Black));
    assert_eq!(
        Color::from_str("#ff0000").unwrap_err().to_string(),
        "unknown Color \"#ff0000\""
    );

    // serde: ser
    assert_eq!(serde_json::to_string(&Color::White).unwrap(), "\"#ffffff\"");

    // serde: de
    let de = |v| -> Result<Color, _> { serde_json::from_str(v) };
    assert_eq!(de("\"#ffffff\"").unwrap(), Color::White);
    assert_eq!(
        de("\"#00ff00\"").unwrap_err().to_string(),
        "unknown variant `#00ff00`, expected `#000000` or `#ffffff`"
    );

    // Lua-related traits
    let lua = tarantool::lua_state();
    assert_eq!(lua.eval::<Color>("return '#ffffff'").unwrap(), Color::White);
    assert_eq!(
        lua.eval_with::<_, String>("return ...", Color::Black)
            .unwrap(),
        "#000000"
    );
    assert_eq!(
        lua.eval::<Color>("return '#808080'")
            .unwrap_err()
            .to_string(),
        format!(
            "Wrong type returned by Lua: {} expected, got string",
            std::any::type_name::<Color>()
        )
    );

    // other claimed traits
    impl<'de, L: tlua::AsLua> AssertImpl<'de, L> for Color {}
    trait AssertImpl<'de, L: tlua::AsLua>:
        AsRef<str>
        + Into<String>
        + Default
        + Clone
        + Copy
        + Eq
        + Ord
        + PartialEq
        + PartialOrd
        + std::fmt::Debug
        + std::fmt::Display
        + std::hash::Hash
        + serde::Deserialize<'de>
        + serde::Serialize
        + tlua::LuaRead<L>
        + tlua::Push<L>
        + tlua::PushInto<L>
        + tlua::PushOne<L>
        + tlua::PushOneInto<L>
    {
    }
}

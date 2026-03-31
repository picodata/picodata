use crate::AsLua;
use crate::Call;
use crate::LuaError;
use std::borrow::Cow;

pub type Module<L> = crate::Indexable<crate::PushGuard<crate::Callable<crate::PushGuard<L>>>>;

pub fn require<L>(lua: L, module: &str) -> Result<Module<L>, LuaError>
where
    L: AsLua,
{
    let Ok(v) = lua.get_global("require") else {
        return Err(LuaError::ExecutionError(Cow::Borrowed(
            "global function 'require' not found",
        )));
    };
    let require: crate::Callable<_> = v;

    let module = match require.into_call_with(module) {
        Ok(v) => v,
        Err(e) => {
            return Err(LuaError::ExecutionError(Cow::Owned(format!(
                "failed to call require('{module}'): {e}"
            ))));
        }
    };

    Ok(module)
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use crate::Index;

    #[crate::test]
    fn test_require() {
        let lua = crate::Lua::new();

        // Needed for the magic `package` thing
        lua.openlibs();

        // Preload the 'my_module' module with a hard-coded lua table
        lua.exec(
            "package.preload['my_module'] = function()
                return {foo={1,2,3}, bar='baz'}
            end",
        )
        .unwrap();

        let my_module = require(&lua, "my_module").unwrap();

        let foo: Vec<i32> = my_module.get("foo").unwrap();
        assert_eq!(foo, &[1, 2, 3]);

        let bar: String = my_module.get("bar").unwrap();
        assert_eq!(bar, "baz");
    }
}

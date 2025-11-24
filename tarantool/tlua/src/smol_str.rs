use crate::values::push_lua_string_and_guard;
use crate::values::read_lua_string_or_err;
use crate::AsLua;
use crate::Push;
use crate::PushGuard;
use crate::Void;
use smol_str::SmolStr;

impl<L> crate::Push<L> for SmolStr
where
    L: AsLua,
{
    type Err = Void;

    #[inline]
    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
        Ok(push_lua_string_and_guard(lua, self.as_bytes()))
    }
}

impl<L> crate::PushOne<L> for SmolStr where L: AsLua {}

impl<L> crate::PushInto<L> for SmolStr
where
    L: AsLua,
{
    type Err = Void;

    #[inline(always)]
    fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
        self.push_to_lua(lua)
    }
}

impl<L> crate::PushOneInto<L> for SmolStr where L: AsLua {}

impl<L> crate::LuaRead<L> for SmolStr
where
    L: AsLua,
{
    fn lua_read_at_position(lua: L, index: std::num::NonZeroI32) -> crate::ReadResult<Self, L> {
        let (slice, lua) = read_lua_string_or_err::<Self, _>(lua, index)?;

        let res = std::str::from_utf8(slice);
        let s = match res {
            Ok(v) => v,
            Err(e) => {
                let e = crate::WrongType::default()
                    .expected_type::<Self>()
                    .actual(format!("non-utf8 string: {e}"));
                return Err((lua, e));
            }
        };

        Ok(Self::new(s))
    }
}

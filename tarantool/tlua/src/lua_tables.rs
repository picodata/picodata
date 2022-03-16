use std::marker::PhantomData;
use std::num::NonZeroI32;

use crate::{
    ffi,
    AsLua,
    impl_object,
    PushInto,
    PushGuard,
    PushOne,
    PushOneInto,
    LuaRead,
    LuaState,
    nzi32,
    object::{Callable, CheckedSetError, FromObject, Index, Object, MethodCallError, NewIndex},
    Void,
};

/// Represents a table stored in the Lua context.
///
/// Just like you can read variables as integers and strings, you can also read Lua table by
/// requesting a `LuaTable` object. Doing so will mutably borrow the object which you got the table
/// from.
///
/// # Example: reading a global variable
///
/// ```
/// let mut lua = tlua::Lua::new();
/// lua.exec("a = {28, 92, 17};").unwrap();
///
/// let mut table: tlua::LuaTable<_> = lua.get("a").unwrap();
/// for (k, v) in table.iter::<i32, i32>().filter_map(|e| e) {
///     println!("{} => {}", k, v);
/// }
/// ```
///
#[derive(Debug)]
pub struct LuaTable<L> {
    inner: Object<L>,
}

impl<L> LuaTable<L>
where
    L: AsLua,
{
    unsafe fn new(lua: L, index: NonZeroI32) -> Self {
        Self::from_obj(Object::new(lua, index))
    }

    pub fn empty(lua: L) -> Self {
        unsafe {
            ffi::lua_newtable(lua.as_lua());
            Self::new(lua, nzi32!(-1))
        }
    }
}

impl_object!{ LuaTable,
    check(lua, index) {
        ffi::lua_istable(lua.as_lua(), index.into())
    }
    impl Index,
    impl NewIndex,
}

impl<'lua, L> LuaTable<L>
where
    L: 'lua,
    L: AsLua,
{
    /// Destroys the `LuaTable` and returns its inner Lua context. Useful when it takes Lua by
    /// value.
    // TODO: find an example where it is useful
    #[inline]
    pub fn into_inner(self) -> L {
        self.inner.into_guard()
    }

    /// Iterates over the elements inside the table.
    // TODO: doc
    #[inline]
    pub fn iter<K, V>(&self) -> LuaTableIterator<L, K, V> {
        unsafe {
            ffi::lua_pushnil(self.as_lua());

            LuaTableIterator {
                table: self,
                finished: false,
                last_top: ffi::lua_gettop(self.as_lua()),
                marker: PhantomData,
            }
        }
    }

    /// Loads a value in the table given its index.
    ///
    /// The index must implement the [`PushOneInto`] trait and the return type
    /// must implement the [`LuaRead`] trait. See [the documentation at the
    /// crate root](index.html#pushing-and-loading-values) for more information.
    ///
    /// # Example: reading a table inside of a table.
    ///
    /// ```
    /// let lua = tlua::Lua::new();
    /// lua.exec("a = { 9, { 8, 7 }, 6 }").unwrap();
    ///
    /// let mut table = lua.get::<tlua::LuaTable<_>, _>("a").unwrap();
    ///
    /// assert_eq!(table.get::<i32, _, _>(1).unwrap(), 9);
    /// assert_eq!(table.get::<i32, _, _>(3).unwrap(), 6);
    ///
    /// {
    ///     let mut subtable: tlua::LuaTable<_> = table.get(2).unwrap();
    ///     assert_eq!(subtable.get::<i32, _, _>(1).unwrap(), 8);
    ///     assert_eq!(subtable.get::<i32, _, _>(2).unwrap(), 7);
    /// }
    /// ```
    ///
    #[inline]
    pub fn get<R, I>(&'lua self, index: I) -> Option<R>
    where
        I: PushOneInto<LuaState, Err = Void>,
        R: LuaRead<PushGuard<&'lua L>>,
    {
        Index::get(self, index)
    }

    /// Loads a value in the table, with the result capturing the table by value.
    ///
    /// See also [`LuaTable::get`]
    #[inline]
    pub fn into_get<R, I>(self, index: I) -> Result<R, Self>
    where
        I: PushOneInto<LuaState, Err = Void>,
        R: LuaRead<PushGuard<Self>>,
    {
        Index::into_get(self, index)
    }

    /// Inserts or modifies an elements of the table.
    ///
    /// Contrary to `checked_set`, can only be called when writing the key and value cannot fail
    /// (which is the case for most types).
    ///
    /// The index and the value must both implement the `PushOne` trait. See
    /// [the documentation at the crate root](index.html#pushing-and-loading-values) for more
    /// information.
    // TODO: doc
    #[inline(always)]
    pub fn set<I, V>(&self, index: I, value: V)
    where
        I: PushOneInto<LuaState>,
        V: PushOneInto<LuaState>,
        // Cannot be just `Void`, because we want to support setting values to
        // `Vec` and others
        I::Err: Into<Void>,
        V::Err: Into<Void>,
    {
        NewIndex::set(self, index, value)
    }

    /// Inserts or modifies an elements of the table.
    ///
    /// Returns an error if we failed to write the key and the value. This can only happen for a
    /// limited set of types. You are encouraged to use the `set` method if writing cannot fail.
    // TODO: doc
    #[inline]
    pub fn checked_set<I, V>(
        &self,
        index: I,
        value: V,
    ) -> Result<(), CheckedSetError<I::Err, V::Err>>
    where
        I: PushOneInto<LuaState>,
        V: PushOneInto<LuaState>,
    {
        NewIndex::checked_set(self, index, value)
    }

    pub fn call_method<R, A>(&'lua self, name: &str, args: A)
        -> Result<R, MethodCallError<A::Err>>
    where
        L: std::fmt::Debug,
        A: PushInto<LuaState>,
        A: std::fmt::Debug,
        R: LuaRead<PushGuard<Callable<PushGuard<&'lua L>>>>,
    {
        Index::call_method(self, name, args)
    }

    /// Inserts an empty array, then loads it.
    #[inline]
    pub fn empty_array<I>(&'lua self, index: I) -> LuaTable<PushGuard<&'lua L>>
    where
        I: PushOne<LuaState, Err = Void>,
    {
        unsafe {
            self.as_lua().push(&index).assert_one_and_forget();
            ffi::lua_newtable(self.as_lua());
            ffi::lua_settable(self.as_lua(), self.as_ref().index().into());
            self.get(&index).unwrap()
        }
    }

    /// Obtains or creates the metatable of the table.
    ///
    /// A metatable is an additional table that can be attached to a table or a userdata. It can
    /// contain anything, but its most interesting usage are the following special methods:
    ///
    /// - If non-nil, the `__index` entry of the metatable is used as a function whenever the user
    ///   tries to read a non-existing entry in the table or userdata. Its signature is
    ///   `(object, index) -> value`.
    /// - If non-nil, the `__newindex` entry of the metatable is used as a function whenever the
    ///   user tries to write a non-existing entry in the table or userdata. Its signature is
    ///   `(object, index, value)`.
    /// - If non-nil, the `__lt`, `__le` and `__eq` entries correspond respectively to operators
    ///    `<`, `<=` and `==`. Their signature is `(a, b) -> bool`. Other operators are
    ///   automatically derived from these three functions.
    /// - If non-nil, the `__add`, `__mul`, `__sub`, `__div`, `__unm`, `__pow` and `__concat`
    ///   entries correspond to operators `+`, `*`, `-`, `/`, `-` (unary), `^` and `..`. Their
    ///   signature is `(a, b) -> result`.
    /// - If non-nil, the `__gc` entry is called whenever the garbage collector is about to drop
    ///   the object. Its signature is simply `(obj)`. Remember that usercode is able to modify
    ///   the metatable as well, so there is no strong guarantee that this is actually going to be
    ///   called.
    ///
    /// Interestingly enough, a metatable can also have a metatable. For example if you try to
    /// access a non-existing field in a table, Lua will look for the `__index` function in its
    /// metatable. If that function doesn't exist, it will try to use the `__index` function of the
    /// metatable's metatable in order to get the `__index` function of the metatable. This can
    /// go on infinitely.
    ///
    /// # Example
    ///
    /// ```
    /// use tlua::Lua;
    /// use tlua::LuaTable;
    /// use tlua::AnyLuaValue;
    ///
    /// let mut lua = Lua::new();
    /// lua.exec("a = {}").unwrap();
    ///
    /// {
    ///     let mut table: LuaTable<_> = lua.get("a").unwrap();
    ///     let mut metatable = table.get_or_create_metatable();
    ///     metatable.set("__index", tlua::function2(|_: AnyLuaValue, var: String| -> AnyLuaValue {
    ///         println!("The user tried to access non-existing index {:?}", var);
    ///         AnyLuaValue::LuaNil
    ///     }));
    /// }
    /// ```
    #[inline]
    pub fn get_or_create_metatable(self) -> LuaTable<PushGuard<L>> {
        unsafe {
            let index = self.as_ref().index().into();
            // We put the metatable at the top of the stack.
            if ffi::lua_getmetatable(self.as_lua(), index) == 0 {
                // No existing metatable ; create one then set it and reload it.
                ffi::lua_newtable(self.as_lua());
                ffi::lua_setmetatable(self.as_lua(), index);
                let r = ffi::lua_getmetatable(self.as_lua(), index);
                debug_assert!(r != 0);
            }

            LuaTable::new(PushGuard::new(self.into_inner(), 1), crate::NEGATIVE_ONE)
        }
    }

    /// Builds the `LuaTable` that yields access to the registry.
    ///
    /// The registry is a special table available from anywhere and that is not directly
    /// accessible from Lua code. It can be used to store whatever you want to keep in memory.
    ///
    /// # Example
    ///
    /// ```
    /// use tlua::Lua;
    /// use tlua::LuaTable;
    ///
    /// let mut lua = Lua::new();
    ///
    /// let mut table = LuaTable::registry(&mut lua);
    /// table.set(3, "hello");
    /// ```
    #[inline]
    pub fn registry(lua: L) -> LuaTable<L> {
        unsafe {
            LuaTable::new(lua, nzi32!(ffi::LUA_REGISTRYINDEX))
        }
    }
}

/// Iterator that enumerates the content of a Lua table.
///
/// See `LuaTable::iter` for more info.
// Implementation note: While the LuaTableIterator is active, the current key is constantly
// pushed over the table. The destructor takes care of removing it.
#[derive(Debug)]
pub struct LuaTableIterator<'t, L: 't, K, V>
where
    L: AsLua,
{
    table: &'t LuaTable<L>,
    finished: bool, // if true, the key is not on the stack anymore
    last_top: i32,
    marker: PhantomData<(K, V)>,
}

impl<'t, L, K, V> Iterator for LuaTableIterator<'t, L, K, V>
where
    L: AsLua + 't,
    K: LuaRead<&'t LuaTable<L>>,
    V: LuaRead<PushGuard<&'t LuaTable<L>>>,
{
    type Item = Option<(K, V)>;

    #[inline]
    fn next(&mut self) -> Option<Option<(K, V)>> {
        unsafe {
            if self.finished {
                return None;
            }

            // The key must always be at the top of the stack unless
            // `finished` is true. Because the `value` may capture the pushguard
            // by value and the caller will be responsibe for dropping the stack
            // values, we need to make sure the stack is in the correct
            // configuration before invoking `lua_next`.
            assert_eq!(self.last_top, ffi::lua_gettop(self.table.as_lua()),
                "lua stack is corrupt"
            );
            // This call pops the current key and pushes the next key and value at the top.
            if ffi::lua_next(self.table.as_lua(), self.table.as_ref().index().into()) == 0 {
                self.finished = true;
                return None;
            }

            // The key must remain on the stack, but the value must be dropped
            // before next iteration. If `V` captures the `guard`, the user
            // must make sure it is dropped before calling `next` on this
            // iterator, otherwise it will result in a panic
            let guard = PushGuard::new(self.table, 1);

            // Reading the key and value.
            let key = K::lua_read_at_position(self.table, crate::NEGATIVE_TWO).ok();
            let value = V::lua_read_at_position(guard, crate::NEGATIVE_ONE).ok();

            Some(
                match (key, value) {
                    (Some(key), Some(value)) => Some((key, value)),
                    _ => None
                }
            )
        }
    }
}

impl<'t, L, K, V> Drop for LuaTableIterator<'t, L, K, V>
where
    L: AsLua,
{
    #[inline]
    fn drop(&mut self) {
        unsafe {
            if !self.finished {
                ffi::lua_pop(self.table.as_lua(), 1);
            }
        }
    }
}


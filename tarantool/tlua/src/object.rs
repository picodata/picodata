use crate::{
    impl_object, AbsoluteIndex, AsLua, LuaError, LuaRead, LuaState, Push, PushGuard, PushInto,
    PushOneInto, ReadResult, Void,
};
use std::{error::Error, fmt, num::NonZeroI32};

////////////////////////////////////////////////////////////////////////////////
// Object
////////////////////////////////////////////////////////////////////////////////

/// A single value stored on the lua stack. Type parameter `L` represents a
/// value guarding the state of the lua stack (see [`PushGuard`]).
///
/// Use this type to convert between different lua values, e.g. [`LuaTable`] <->
/// [`Indexable`], etc.
///
/// [`LuaTable`]: crate::lua_tables::LuaTable
#[derive(Debug)]
pub struct Object<L> {
    guard: L,
    index: AbsoluteIndex,
}

impl<L: AsLua> Object<L> {
    #[inline(always)]
    pub(crate) fn new(guard: L, index: NonZeroI32) -> Self
    where
        L: AsLua,
    {
        Self {
            index: AbsoluteIndex::new(index, guard.as_lua()),
            guard,
        }
    }

    #[inline(always)]
    pub fn guard(&self) -> &L {
        &self.guard
    }

    #[inline(always)]
    pub fn into_guard(self) -> L {
        self.guard
    }

    #[inline(always)]
    pub fn index(&self) -> AbsoluteIndex {
        self.index
    }

    /// Try converting to a value implementing [`LuaRead`].
    ///
    /// # Safety
    ///
    /// In some cases this function will result in a drop of `self.guard` which
    /// is invalid in case `self.index` is not the top of the lua stack.
    #[inline(always)]
    pub unsafe fn try_downcast<T>(self) -> Result<T, Self>
    where
        T: LuaRead<L>,
    {
        let Self { guard, index } = self;
        T::lua_read_at_position(guard, index.0).map_err(|(guard, _)| Self { guard, index })
    }
}

impl<L> AsLua for Object<L>
where
    L: AsLua,
{
    fn as_lua(&self) -> LuaState {
        self.guard.as_lua()
    }
}

impl<L> LuaRead<L> for Object<L>
where
    L: AsLua,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> ReadResult<Self, L> {
        Ok(Self::new(lua, index))
    }
}

impl<L, K> Push<L> for Object<K>
where
    L: AsLua,
    K: AsLua,
{
    type Err = Void;
    fn push_to_lua(&self, lua: L) -> crate::PushResult<L, Self> {
        unsafe {
            crate::ffi::lua_pushvalue(lua.as_lua(), self.index.into());
            Ok(PushGuard::new(lua, 1))
        }
    }
}
impl<L> crate::PushOne<L> for Object<L> where L: AsLua {}

impl<L, K> PushInto<L> for Object<K>
where
    L: AsLua,
    K: AsLua,
{
    type Err = Void;
    fn push_into_lua(self, lua: L) -> crate::PushIntoResult<L, Self> {
        unsafe {
            crate::ffi::lua_pushvalue(lua.as_lua(), self.index.into());
            Ok(PushGuard::new(lua, 1))
        }
    }
}
impl<L> crate::PushOneInto<L> for Object<L> where L: AsLua {}

/// Types implementing this trait represent a single value stored on the lua
/// stack. Type parameter `L` represents a value guarding the state of the lua
/// stack (see [`PushGuard`]).
pub trait FromObject<L: AsLua> {
    /// Check if a value at `index` satisfies the given type's invariants
    unsafe fn check(lua: impl AsLua, index: NonZeroI32) -> bool;

    /// Duh
    unsafe fn from_obj(inner: Object<L>) -> Self;

    fn try_from_obj(inner: Object<L>) -> Result<Self, Object<L>>
    where
        Self: Sized,
        L: AsLua,
    {
        if unsafe { Self::check(inner.guard(), inner.index().0) } {
            Ok(unsafe { Self::from_obj(inner) })
        } else {
            Err(inner)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Index
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait represent a single lua value that can be
/// indexed, i.e. a regular lua table or other value implementing `__index`
/// metamethod.
pub trait Index<L>: AsRef<Object<L>>
where
    L: AsLua,
{
    /// Loads a value from the table (or other object using the `__index`
    /// metamethod) given its `index`.
    ///
    /// The index must implement the [`PushOneInto`] trait and the return type
    /// must implement the [`LuaRead`] trait. See [the documentation at the
    /// crate root](index.html#pushing-and-loading-values) for more information.
    #[track_caller]
    #[inline(always)]
    fn get<'lua, K, R>(&'lua self, key: K) -> Option<R>
    where
        L: 'lua,
        K: PushOneInto<LuaState>,
        K::Err: Into<Void>,
        R: LuaRead<PushGuard<&'lua L>>,
    {
        self.try_get(key).ok()
    }

    /// Loads a value from the table (or other object using the `__index`
    /// metamethod) given its `index`.
    ///
    /// # Possible errors:
    /// - `LuaError::ExecutionError` if an error happened during the check that
    ///     `index` is valid in `self`
    /// - `LuaError::WrongType` if the result lua value couldn't be read as the
    ///     expected rust type
    ///
    /// The index must implement the [`PushOneInto`] trait and the return type
    /// must implement the [`LuaRead`] trait. See [the documentation at the
    /// crate root](index.html#pushing-and-loading-values) for more information.
    #[track_caller]
    #[inline]
    fn try_get<'lua, K, R>(&'lua self, key: K) -> Result<R, LuaError>
    where
        L: 'lua,
        K: PushOneInto<LuaState>,
        K::Err: Into<Void>,
        R: LuaRead<PushGuard<&'lua L>>,
    {
        let Object { guard, index } = self.as_ref();
        unsafe { imp::try_get(guard, *index, key).map_err(|(_, e)| e) }
    }

    /// Loads a value in the table (or other object using the `__index`
    /// metamethod) given its `index`, with the result capturing the table by
    /// value.
    ///
    /// See also [`Index::get`]
    #[track_caller]
    #[inline(always)]
    fn into_get<K, R>(self, key: K) -> Result<R, Self>
    where
        Self: AsLua + Sized,
        K: PushOneInto<LuaState>,
        K::Err: Into<Void>,
        R: LuaRead<PushGuard<Self>>,
    {
        self.try_into_get(key).map_err(|(this, _)| this)
    }

    /// Loads a value in the table (or other object using the `__index`
    /// metamethod) given its `index`, with the result capturing the table by
    /// value.
    ///
    /// # Possible errors:
    /// - `LuaError::ExecutionError` if an error happened during the check that
    ///     `index` is valid in `self`
    /// - `LuaError::WrongType` if the result lua value couldn't be read as the
    ///     expected rust type
    ///
    /// See also [`Index::get`]
    #[track_caller]
    #[inline]
    fn try_into_get<K, R>(self, key: K) -> Result<R, (Self, LuaError)>
    where
        Self: AsLua + Sized,
        K: PushOneInto<LuaState>,
        K::Err: Into<Void>,
        R: LuaRead<PushGuard<Self>>,
    {
        let this_index = self.as_ref().index;
        unsafe { imp::try_get(self, this_index, key) }
    }

    /// Calls the method called `name` of the table (or other indexable object)
    /// with the provided `args`.
    ///
    /// Possible errors:
    /// - `MethodCallError::NoSuchMethod` in case `self[name]` is `nil`
    /// - `MethodCallError::PushError` if pushing `args` failed
    /// - `MethodCallError::LuaError` if error happened during the function call
    #[track_caller]
    #[inline]
    fn call_method<'lua, A, R>(
        &'lua self,
        name: &str,
        args: A,
    ) -> Result<R, MethodCallError<A::Err>>
    where
        L: 'lua,
        Self: Push<LuaState>,
        Self::Err: Into<Void>,
        A: PushInto<LuaState>,
        R: LuaRead<PushGuard<Callable<PushGuard<&'lua L>>>>,
    {
        use MethodCallError::{LuaError, NoSuchMethod, PushError};

        self.get::<_, Callable<_>>(name)
            .ok_or(NoSuchMethod)?
            .into_call_with((self, args))
            .map_err(|e| match e {
                CallError::LuaError(e) => LuaError(e),
                CallError::PushError(e) => PushError(e.other().first()),
            })
    }
}

#[derive(Debug)]
pub enum MethodCallError<E> {
    /// The corresponding method was not found (t\[k] == nil)
    NoSuchMethod,
    /// Error during function call
    LuaError(LuaError),
    /// Pushing arguments failed
    PushError(E),
}

impl<E> From<CallError<E>> for MethodCallError<E> {
    fn from(e: CallError<E>) -> Self {
        match e {
            CallError::PushError(e) => Self::PushError(e),
            CallError::LuaError(e) => Self::LuaError(e),
        }
    }
}

impl<E> fmt::Display for MethodCallError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NoSuchMethod => f.write_str("Method not found"),
            Self::LuaError(lua_error) => write!(f, "Lua error: {}", lua_error),
            Self::PushError(err) => {
                write!(f, "Error while pushing arguments: {}", err)
            }
        }
    }
}

impl<E> Error for MethodCallError<E>
where
    E: Error,
{
    fn description(&self) -> &str {
        match self {
            Self::NoSuchMethod => "Method not found",
            Self::LuaError(_) => "Lua error",
            Self::PushError(_) => "Error while pushing arguments",
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            Self::NoSuchMethod => None,
            Self::LuaError(lua_error) => Some(lua_error),
            Self::PushError(err) => Some(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Indexable
////////////////////////////////////////////////////////////////////////////////

/// An opaque value on lua stack that can be indexed. Can represent a lua
/// table, a lua table with a `__index` metamethod or other indexable lua
/// value.
///
/// Use this type when reading return values from lua functions or getting lua
/// function from tables.
#[derive(Debug)]
pub struct Indexable<L> {
    inner: Object<L>,
}

impl_object! { Indexable,
    check(lua, index) {
        imp::is_indexable(&lua, index)
    }
    impl Index,
}

////////////////////////////////////////////////////////////////////////////////
// NewIndex
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait represent a single lua value that can be
/// changed by indexed, i.e. a regular lua table or other value implementing
/// `__newindex` metamethod.
pub trait NewIndex<L>: AsRef<Object<L>>
where
    L: AsLua,
{
    /// Inserts or modifies a `value` of the table (or other object using the
    /// `__index` or `__newindex` metamethod) given its `index`.
    ///
    /// Contrary to [`NewIndex::checked_set`], can only be called when writing
    /// the key and value cannot fail (which is the case for most types).
    ///
    /// # Panic
    ///
    /// Will panic if an error happens during attempt to set value. Can happen
    /// if `__index` or `__newindex` throws an error. Use [`NewIndex::try_set`]
    /// if this is a possibility in your case.
    ///
    /// The index must implement the [`PushOneInto`] trait and the return type
    /// must implement the [`LuaRead`] trait. See [the documentation at the
    /// crate root](index.html#pushing-and-loading-values) for more information.
    #[track_caller]
    #[inline(always)]
    fn set<K, V>(&self, key: K, value: V)
    where
        K: PushOneInto<LuaState>,
        K::Err: Into<Void>,
        V: PushOneInto<LuaState>,
        V::Err: Into<Void>,
    {
        if let Err(e) = self.try_set(key, value) {
            panic!("Setting value failed: {}", e)
        }
    }

    /// Inserts or modifies a `value` of the table (or other object using the
    /// `__index` or `__newindex` metamethod) given its `index`.
    ///
    /// Contrary to [`NewIndex::try_checked_set`], can only be called when
    /// writing the key and value cannot fail (which is the case for most
    /// types).
    ///
    /// Returns a `LuaError::ExecutionError` in case an error happened during an
    /// attempt to set value.
    ///
    /// The index must implement the [`PushOneInto`] trait and the return type
    /// must implement the [`LuaRead`] trait. See [the documentation at the
    /// crate root](index.html#pushing-and-loading-values) for more information.
    #[track_caller]
    #[inline]
    fn try_set<K, V>(&self, key: K, value: V) -> Result<(), LuaError>
    where
        K: PushOneInto<LuaState>,
        K::Err: Into<Void>,
        V: PushOneInto<LuaState>,
        V::Err: Into<Void>,
    {
        let Object { guard, index } = self.as_ref();
        unsafe { imp::try_checked_set(guard, *index, key, value) }.map_err(|e| match e {
            Ok(_) => unreachable!("Void is uninstantiatable"),
            Err(e) => e,
        })
    }

    /// Inserts or modifies a `value` of the table (or other object using the
    /// `__newindex` metamethod) given its `index`.
    ///
    /// Returns an error if pushing `index` or `value` failed. This can only
    /// happen for a limited set of types. You are encouraged to use the
    /// [`NewIndex::set`]
    /// method if pushing cannot fail.
    ///
    /// # Panic
    ///
    /// Will panic if an error happens during attempt to set value. Can happen
    /// if `__index` or `__newindex` throws an error. Use
    /// [`NewIndex::try_checked_set`] if this is a possibility in your case.
    #[track_caller]
    #[inline(always)]
    fn checked_set<K, V>(&self, key: K, value: V) -> Result<(), CheckedSetError<K::Err, V::Err>>
    where
        K: PushOneInto<LuaState>,
        V: PushOneInto<LuaState>,
    {
        self.try_checked_set(key, value)
            .map_err(|e| e.unwrap_or_else(|e| panic!("Setting value failed: {}", e)))
    }

    /// Inserts or modifies a `value` of the table (or other object using the
    /// `__newindex` metamethod) given its `index`.
    ///
    /// # Possible errors
    /// - Returns an error if pushing `index` or `value` failed. This can only
    /// happen for a limited set of types. You are encouraged to use the
    /// [`NewIndex::set`] method if pushing cannot fail.
    /// - Returns a `LuaError::ExecutionError` in case an error happened during
    /// an attempt to set value.
    #[track_caller]
    #[inline(always)]
    fn try_checked_set<K, V>(
        &self,
        key: K,
        value: V,
    ) -> Result<(), TryCheckedSetError<K::Err, V::Err>>
    where
        K: PushOneInto<LuaState>,
        V: PushOneInto<LuaState>,
    {
        let Object { guard, index } = self.as_ref();
        unsafe { imp::try_checked_set(guard, *index, key, value) }
    }
}

pub type TryCheckedSetError<K, V> = Result<CheckedSetError<K, V>, LuaError>;

/// Error returned by the [`NewIndex::checked_set`] function.
#[derive(Debug, Copy, Clone)]
pub enum CheckedSetError<K, V> {
    /// Error while pushing the key.
    KeyPushError(K),
    /// Error while pushing the value.
    ValuePushError(V),
}

////////////////////////////////////////////////////////////////////////////////
// IndexableRW
////////////////////////////////////////////////////////////////////////////////

/// An opaque value on lua stack that can be indexed immutably as well as
/// mutably. Can represent a lua table, a lua table with a `__index` and
/// `__newindex` metamethods or other indexable lua value.
///
/// Use this type when reading return values from lua functions or getting lua
/// function from tables.
#[derive(Debug)]
pub struct IndexableRW<L> {
    inner: Object<L>,
}

impl_object! { IndexableRW,
    check(lua, index) {
        imp::is_rw_indexable(&lua, index)
    }
    impl Index,
    impl NewIndex,
}

////////////////////////////////////////////////////////////////////////////////
// Call
////////////////////////////////////////////////////////////////////////////////

pub trait Call<L>: AsRef<Object<L>>
where
    L: AsLua,
{
    #[track_caller]
    #[inline]
    fn call<'lua, R>(&'lua self) -> Result<R, LuaError>
    where
        L: 'lua,
        R: LuaRead<PushGuard<&'lua L>>,
    {
        Ok(self.call_with(())?)
    }

    #[track_caller]
    #[inline]
    fn call_with<'lua, A, R>(&'lua self, args: A) -> Result<R, CallError<A::Err>>
    where
        L: 'lua,
        A: PushInto<LuaState>,
        R: LuaRead<PushGuard<&'lua L>>,
    {
        let Object { guard, index } = self.as_ref();
        imp::call(guard, *index, args)
    }

    #[track_caller]
    #[inline]
    fn into_call<R>(self) -> Result<R, LuaError>
    where
        Self: AsLua + Sized,
        R: LuaRead<PushGuard<Self>>,
    {
        Ok(self.into_call_with(())?)
    }

    #[track_caller]
    #[inline]
    fn into_call_with<A, R>(self, args: A) -> Result<R, CallError<A::Err>>
    where
        Self: AsLua + Sized,
        A: PushInto<LuaState>,
        R: LuaRead<PushGuard<Self>>,
    {
        let index = self.as_ref().index;
        imp::call(self, index, args)
    }
}

/// Error that can happen when calling a type implementing [`Call`].
#[derive(Debug)]
pub enum CallError<E> {
    /// Error while executing the function.
    LuaError(LuaError),
    /// Error while pushing one of the parameters.
    PushError(E),
}

impl<E> CallError<E> {
    pub fn map<F, R>(self, f: F) -> CallError<R>
    where
        F: FnOnce(E) -> R,
    {
        match self {
            CallError::LuaError(e) => CallError::LuaError(e),
            CallError::PushError(e) => CallError::PushError(f(e)),
        }
    }
}

impl<E> From<LuaError> for CallError<E> {
    fn from(e: LuaError) -> Self {
        Self::LuaError(e)
    }
}

impl<E> From<CallError<E>> for LuaError
where
    E: Into<Void>,
{
    fn from(e: CallError<E>) -> Self {
        match e {
            CallError::LuaError(le) => le,
            CallError::PushError(_) => {
                unreachable!("no way to create instance of Void")
            }
        }
    }
}

impl<E> fmt::Display for CallError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::LuaError(lua_error) => write!(f, "Lua error: {}", lua_error),
            Self::PushError(err) => {
                write!(f, "Error while pushing arguments: {}", err)
            }
        }
    }
}

impl<E> Error for CallError<E>
where
    E: Error,
{
    fn description(&self) -> &str {
        match self {
            Self::LuaError(_) => "Lua error",
            Self::PushError(_) => "error while pushing arguments",
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            Self::LuaError(lua_error) => Some(lua_error),
            Self::PushError(err) => Some(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Callable
////////////////////////////////////////////////////////////////////////////////

/// An opaque value on lua stack that can be called. Can represent a lua
/// function, a lua table with a `__call` metamethod or other callable lua
/// value.
///
/// Use this type when reading return values from lua functions or getting lua
/// function from tables.
#[derive(Debug)]
pub struct Callable<L> {
    inner: Object<L>,
}

impl_object! { Callable,
    check(lua, index) {
        imp::is_callable(&lua, index)
    }
    impl Call,
}

////////////////////////////////////////////////////////////////////////////////
// imp
////////////////////////////////////////////////////////////////////////////////

mod imp {
    use super::{CallError, CheckedSetError, TryCheckedSetError};
    use crate::{
        c_ptr, ffi, nzi32, AbsoluteIndex, AsLua, LuaError, LuaRead, LuaState, PushGuard, PushInto,
        PushOneInto, ToString, Void, WrongType,
    };
    use std::num::NonZeroI32;

    ////////////////////////////////////////////////////////////////////////////
    // try_get
    ////////////////////////////////////////////////////////////////////////////

    #[track_caller]
    pub(super) unsafe fn try_get<T, K, R>(
        this: T,
        this_index: AbsoluteIndex,
        key: K,
    ) -> Result<R, (T, LuaError)>
    where
        T: AsLua,
        K: PushOneInto<LuaState>,
        K::Err: Into<Void>,
        R: LuaRead<PushGuard<T>>,
    {
        let raw_lua = this.as_lua();
        let this_index = this_index.into();

        if ffi::lua_istable(raw_lua, this_index)
            && !ffi::luaL_hasmetafield(raw_lua, this_index, c_ptr!("__index"))
        {
            // push key
            raw_lua.push_one(key).assert_one_and_forget();
            // replace key with value
            ffi::lua_rawget(raw_lua, this_index);
        } else {
            // push index onto the stack
            raw_lua.push_one(key).assert_one_and_forget();
            // move index into registry
            let index_ref = ffi::luaL_ref(raw_lua, ffi::LUA_REGISTRYINDEX);
            // push indexable onto the stack
            ffi::lua_pushvalue(raw_lua, this_index);
            // move indexable into registry
            let table_ref = ffi::luaL_ref(raw_lua, ffi::LUA_REGISTRYINDEX);

            let res = raw_lua.pcall(|l| {
                let raw_lua = l.as_lua();
                // push indexable
                ffi::lua_rawgeti(raw_lua, ffi::LUA_REGISTRYINDEX, table_ref);
                // push index
                ffi::lua_rawgeti(raw_lua, ffi::LUA_REGISTRYINDEX, index_ref);
                // pop index, push value
                ffi::lua_gettable(raw_lua, -2);
                // save value
                ffi::luaL_ref(raw_lua, ffi::LUA_REGISTRYINDEX)
                // stack is temporary so indexable is discarded after return
            });
            let value_ref = match res {
                Ok(value_ref) => value_ref,
                Err(e) => return Err((this, e)),
            };

            // move value from registry to stack
            ffi::lua_rawgeti(raw_lua, ffi::LUA_REGISTRYINDEX, value_ref);

            // unref temporaries
            ffi::luaL_unref(raw_lua, ffi::LUA_REGISTRYINDEX, value_ref);
            ffi::luaL_unref(raw_lua, ffi::LUA_REGISTRYINDEX, index_ref);
            ffi::luaL_unref(raw_lua, ffi::LUA_REGISTRYINDEX, table_ref);
        }

        R::lua_read_at_position(PushGuard::new(this, 1), nzi32!(-1)).map_err(|(g, e)| {
            let e = WrongType::info("reading value from Lua table")
                .expected_type::<R>()
                .actual_single_lua(raw_lua, nzi32!(-1))
                .subtype(e);
            (g.into_inner(), e.into())
        })
    }

    ////////////////////////////////////////////////////////////////////////////
    // try_checked_set
    ////////////////////////////////////////////////////////////////////////////

    #[track_caller]
    pub(super) unsafe fn try_checked_set<T, K, V>(
        this: T,
        this_index: AbsoluteIndex,
        key: K,
        value: V,
    ) -> Result<(), TryCheckedSetError<K::Err, V::Err>>
    where
        T: AsLua,
        K: PushOneInto<LuaState>,
        V: PushOneInto<LuaState>,
    {
        let raw_lua = this.as_lua();
        let this_index = this_index.into();
        if ffi::lua_istable(raw_lua, this_index)
            && !ffi::luaL_hasmetafield(raw_lua, this_index, c_ptr!("__index"))
            && !ffi::luaL_hasmetafield(raw_lua, this_index, c_ptr!("__newindex"))
        {
            // push key
            raw_lua
                .try_push_one(key)
                .map_err(|(e, _)| Ok(CheckedSetError::KeyPushError(e)))?
                .assert_one_and_forget();
            // push value
            raw_lua
                .try_push_one(value)
                .map_err(|(e, _)| Ok(CheckedSetError::ValuePushError(e)))?
                .assert_one_and_forget();
            // remove key and value
            ffi::lua_rawset(raw_lua, this_index);
        } else {
            // push value onto the stack
            raw_lua
                .try_push_one(value)
                .map_err(|(e, _)| Ok(CheckedSetError::ValuePushError(e)))?
                .assert_one_and_forget();
            // move value into registry
            let value_ref = ffi::luaL_ref(raw_lua, ffi::LUA_REGISTRYINDEX);

            // push index onto the stack
            raw_lua
                .try_push_one(key)
                .map_err(|(e, _)| Ok(CheckedSetError::KeyPushError(e)))?
                .assert_one_and_forget();
            // move index into registry
            let index_ref = ffi::luaL_ref(raw_lua, ffi::LUA_REGISTRYINDEX);

            // push indexable onto the stack
            ffi::lua_pushvalue(raw_lua, this_index);
            // move indexable into registry
            let table_ref = ffi::luaL_ref(raw_lua, ffi::LUA_REGISTRYINDEX);

            raw_lua
                .pcall(|l| {
                    let raw_lua = l.as_lua();
                    // push indexable
                    ffi::lua_rawgeti(raw_lua, ffi::LUA_REGISTRYINDEX, table_ref);
                    // push index
                    ffi::lua_rawgeti(raw_lua, ffi::LUA_REGISTRYINDEX, index_ref);
                    // push value
                    ffi::lua_rawgeti(raw_lua, ffi::LUA_REGISTRYINDEX, value_ref);
                    // pop index, push value
                    ffi::lua_settable(raw_lua, -3);
                    // stack is temporary so indexable is discarded after return
                })
                .map_err(Err)?;

            // unref temporaries
            ffi::luaL_unref(raw_lua, ffi::LUA_REGISTRYINDEX, value_ref);
            ffi::luaL_unref(raw_lua, ffi::LUA_REGISTRYINDEX, index_ref);
            ffi::luaL_unref(raw_lua, ffi::LUA_REGISTRYINDEX, table_ref);
        }
        Ok(())
    }

    ////////////////////////////////////////////////////////////////////////////
    // call
    ////////////////////////////////////////////////////////////////////////////

    #[track_caller]
    #[inline]
    pub(super) fn call<T, A, R>(
        this: T,
        index: AbsoluteIndex,
        args: A,
    ) -> Result<R, CallError<A::Err>>
    where
        T: AsLua,
        A: PushInto<LuaState>,
        R: LuaRead<PushGuard<T>>,
    {
        let raw_lua = this.as_lua();
        // calling pcall pops the parameters and pushes output
        let (pcall_return_value, pushed_value) = unsafe {
            let old_top = ffi::lua_gettop(raw_lua);
            // lua_pcall pops the function, so we have to make a copy of it
            ffi::lua_pushvalue(raw_lua, index.into());
            let num_pushed = match this.as_lua().try_push(args) {
                Ok(g) => g.forget_internal(),
                Err((err, _)) => return Err(CallError::PushError(err)),
            };
            let pcall_return_value = ffi::lua_pcall(raw_lua, num_pushed, ffi::LUA_MULTRET, 0);
            let n_results = ffi::lua_gettop(raw_lua) - old_top;
            (pcall_return_value, PushGuard::new(this, n_results))
        };

        match pcall_return_value {
            ffi::LUA_ERRMEM => panic!("lua_pcall returned LUA_ERRMEM"),
            ffi::LUA_ERRRUN => {
                let error_msg = ToString::lua_read(pushed_value)
                    .ok()
                    .expect("can't find error message at the top of the Lua stack");
                return Err(LuaError::ExecutionError(error_msg.into()).into());
            }
            0 => {}
            _ => panic!(
                "Unknown error code returned by lua_pcall: {}",
                pcall_return_value
            ),
        }

        let n_results = pushed_value.size;
        LuaRead::lua_read_at_maybe_zero_position(pushed_value, -n_results).map_err(|(lua, e)| {
            WrongType::info("reading value(s) returned by Lua")
                .expected_type::<R>()
                .actual_multiple_lua(lua, n_results)
                .subtype(e)
                .into()
        })
    }

    ////////////////////////////////////////////////////////////////////////////
    // checks
    ////////////////////////////////////////////////////////////////////////////

    #[inline(always)]
    pub(super) fn is_callable(lua: impl AsLua, index: NonZeroI32) -> bool {
        let raw_lua = lua.as_lua();
        let i = index.into();
        unsafe {
            // luaL_iscallable doesn't work for `ffi`
            ffi::lua_isfunction(raw_lua, i) || ffi::luaL_hasmetafield(raw_lua, i, c_ptr!("__call"))
        }
    }

    #[inline(always)]
    pub(super) fn is_indexable(lua: impl AsLua, index: NonZeroI32) -> bool {
        let raw_lua = lua.as_lua();
        let i = index.into();
        unsafe {
            ffi::lua_istable(raw_lua, i) || ffi::luaL_hasmetafield(raw_lua, i, c_ptr!("__index"))
        }
    }

    #[inline(always)]
    pub(super) fn is_rw_indexable(lua: impl AsLua, index: NonZeroI32) -> bool {
        let raw_lua = lua.as_lua();
        let i = index.into();
        unsafe {
            ffi::lua_istable(raw_lua, i)
                || ffi::luaL_hasmetafield(raw_lua, i, c_ptr!("__index"))
                    && ffi::luaL_hasmetafield(raw_lua, i, c_ptr!("__newindex"))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// impl_object
////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! impl_object {
    (
        $this:ident,
        check($lua:ident, $index:ident) { $($check:tt)* }
        $( impl $trait:ident, )*
    ) => {
        impl<L> $crate::object::FromObject<L> for $this<L>
        where
            L: $crate::AsLua,
        {
            /// # Safety
            /// `index` must correspond to a valid value in `lua`
            #[inline(always)]
            unsafe fn check($lua: impl $crate::AsLua, $index: ::std::num::NonZeroI32) -> bool {
                $($check)*
            }

            /// # Safety
            /// `inner` must satisfy the neccessary invariants of `Self`. See
            /// [`check`]
            #[inline(always)]
            unsafe fn from_obj(inner: $crate::object::Object<L>) -> Self {
                Self { inner }
            }
        }

        impl<L> $crate::AsLua for $this<L>
        where
            L: $crate::AsLua,
        {
            #[inline(always)]
            fn as_lua(&self) -> $crate::LuaState {
                self.inner.as_lua()
            }
        }

        impl<L> ::std::convert::AsRef<$crate::object::Object<L>> for $this<L>
        where
            L: $crate::AsLua,
        {
            #[inline(always)]
            fn as_ref(&self) -> &$crate::object::Object<L> {
                &self.inner
            }
        }

        impl<L> ::std::convert::From<$this<L>> for $crate::object::Object<L>
        where
            L: $crate::AsLua,
        {
            #[inline(always)]
            fn from(o: $this<L>) -> Self {
                o.inner
            }
        }

        impl<L> ::std::convert::TryFrom<$crate::object::Object<L>> for $this<L>
        where
            L: $crate::AsLua,
        {
            type Error = $crate::object::Object<L>;

            #[inline(always)]
            fn try_from(o: $crate::object::Object<L>) -> ::std::result::Result<Self, Self::Error> {
                Self::try_from_obj(o)
            }
        }

        $(
            impl<L> $trait<L> for $this<L>
            where
                L: $crate::AsLua,
            {}
        )*

        impl<L> $crate::LuaRead<L> for $this<L>
        where
            L: $crate::AsLua,
        {
            #[inline(always)]
            fn lua_read_at_position(
                lua: L,
                index: ::std::num::NonZeroI32,
            ) -> $crate::ReadResult<Self, L> {
                ::std::convert::TryFrom::try_from($crate::object::Object::new(lua, index))
                    .map_err(|l| {
                        let g = $crate::object::Object::into_guard(l);
                        let e = $crate::WrongType::info("reading lua value from stack")
                            .expected_type::<Self>()
                            .actual_single_lua(&g, index);
                        (g, e)
                    })
            }
        }

        impl<L, T> $crate::Push<L> for $this<T>
        where
            L: $crate::AsLua,
            T: $crate::AsLua,
        {
            type Err = $crate::Void;

            #[inline(always)]
            fn push_to_lua(&self, lua: L) -> $crate::PushResult<L, Self> {
                unsafe {
                    $crate::ffi::lua_pushvalue(lua.as_lua(), self.as_ref().index().into());
                    Ok(PushGuard::new(lua, 1))
                }
            }
        }

        impl<L, T> $crate::PushOne<L> for $this<T>
        where
            L: $crate::AsLua,
            T: $crate::AsLua,
        {}
    }
}

use std::error::Error;
use std::fmt;
use std::io::Cursor;
use std::io::Read;
use std::io::Error as IoError;
use std::num::NonZeroI32;

use crate::{
    ffi,
    c_ptr,
    AbsoluteIndex,
    AsLua,
    LuaState,
    LuaRead,
    LuaError,
    Push,
    PushInto,
    PushGuard,
    PushOne,
    PushOneInto,
    ToString,
    Void,
};

/// Wrapper around a `&str`. When pushed, the content will be parsed as Lua code and turned into a
/// function.
///
/// Since pushing this value can fail in case of a parsing error, you must use the `checked_set`
/// method instead of `set`.
///
/// > **Note**: This struct is a wrapper around `LuaCodeFromReader`. There's no advantage in using
/// > it except that it is more convenient. More advanced usages (such as returning a Lua function
/// > from a Rust function) can be done with `LuaCodeFromReader`.
///
/// # Example
///
/// ```
/// let mut lua = hlua::Lua::new();
/// lua.checked_set("hello", hlua::LuaCode("return 5")).unwrap();
///
/// let r: i32 = lua.eval("return hello();").unwrap();
/// assert_eq!(r, 5);
/// ```
#[derive(Debug)]
pub struct LuaCode<'a>(pub &'a str);

impl<'c, L> Push<L> for LuaCode<'c>
where
    L: AsLua,
{
    type Err = LuaError;

    #[inline]
    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (LuaError, L)> {
        LuaCodeFromReader(Cursor::new(self.0.as_bytes())).push_into_lua(lua)
    }
}

impl<'c, L> PushOne<L> for LuaCode<'c>
where
    L: AsLua,
{
}

/// Wrapper around a `Read` object. When pushed, the content will be parsed as Lua code and turned
/// into a function.
///
/// Since pushing this value can fail in case of a reading error or a parsing error, you must use
/// the `checked_set` method instead of `set`.
///
/// # Example: returning a Lua function from a Rust function
///
/// ```
/// use std::io::Cursor;
///
/// let mut lua = hlua::Lua::new();
///
/// lua.set("call_rust", hlua::function0(|| -> hlua::LuaCodeFromReader<Cursor<String>> {
///     let lua_code = "return 18;";
///     return hlua::LuaCodeFromReader(Cursor::new(lua_code.to_owned()));
/// }));
///
/// let r: i32 = lua.eval("local lua_func = call_rust(); return lua_func();").unwrap();
/// assert_eq!(r, 18);
/// ```
#[derive(Debug)]
pub struct LuaCodeFromReader<R>(pub R);

impl<L, R> PushInto<L> for LuaCodeFromReader<R>
where
    L: AsLua,
    R: Read,
{
    type Err = LuaError;

    #[inline]
    fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (LuaError, L)> {
        unsafe {
            struct ReadData<R> {
                reader: R,
                buffer: [u8; 128],
                triggered_error: Option<IoError>,
            }

            let mut read_data = ReadData {
                reader: self.0,
                buffer: [0; 128],
                triggered_error: None,
            };

            extern "C" fn reader<R>(_: LuaState,
                                    data: *mut libc::c_void,
                                    size: *mut libc::size_t)
                                    -> *const libc::c_char
                where R: Read
            {
                unsafe {
                    let data: *mut ReadData<R> = data as *mut _;
                    let data: &mut ReadData<R> = &mut *data;

                    if data.triggered_error.is_some() {
                        (*size) = 0;
                        return data.buffer.as_ptr() as *const libc::c_char;
                    }

                    match data.reader.read(&mut data.buffer) {
                        Ok(len) => (*size) = len as libc::size_t,
                        Err(e) => {
                            (*size) = 0;
                            data.triggered_error = Some(e);
                        }
                    };

                    data.buffer.as_ptr() as *const libc::c_char
                }
            }

            let (load_return_value, pushed_value) = {
                let code = ffi::lua_load(
                    lua.as_lua(),
                    reader::<R>,
                    &mut read_data as *mut ReadData<_> as *mut _,
                    c_ptr!("chunk"),
                );
                (code, PushGuard::new(lua, 1))
            };

            if read_data.triggered_error.is_some() {
                let error = read_data.triggered_error.unwrap();
                return Err((LuaError::ReadError(error), pushed_value.into_inner()));
            }

            if load_return_value == 0 {
                return Ok(pushed_value);
            }

            let error_msg: String = LuaRead::lua_read(pushed_value.as_lua())
                .ok()
                .expect("can't find error message at the top of the Lua stack");

            if load_return_value == ffi::LUA_ERRMEM {
                panic!("LUA_ERRMEM");
            }

            if load_return_value == ffi::LUA_ERRSYNTAX {
                return Err((LuaError::SyntaxError(error_msg), pushed_value.into_inner()));
            }

            panic!("Unknown error while calling lua_load");
        }
    }
}

impl<L, R> PushOneInto<L> for LuaCodeFromReader<R>
where
    L: AsLua,
    R: Read,
{
}

/// Handle to a function in the Lua context.
///
/// Just like you can read variables as integers and strings, you can also read Lua functions by
/// requesting a `LuaFunction` object. Once you have a `LuaFunction` you can call it with `call()`.
///
/// > **Note**: Passing parameters when calling the function is not yet implemented.
///
/// # Example
///
/// ```
/// let mut lua = hlua::Lua::new();
/// lua.exec("function foo() return 12 end").unwrap();
///
/// let mut foo: hlua::LuaFunction<_> = lua.get("foo").unwrap();
/// let result: i32 = foo.call().unwrap();
/// assert_eq!(result, 12);
/// ```
// TODO: example for how to get a LuaFunction as a parameter of a Rust function
#[derive(Debug)]
pub struct LuaFunction<L> {
    lua: L,
    index: AbsoluteIndex,
}

impl<L> LuaFunction<L>
where
    L: AsLua,
{
    fn new(lua: L, index: NonZeroI32) -> Self {
        Self {
            index: AbsoluteIndex::new(index, lua.as_lua()),
            lua,
        }
    }
}

impl<L: AsLua> AsLua for LuaFunction<L> {
    #[inline]
    fn as_lua(&self) -> LuaState {
        self.lua.as_lua()
    }
}

impl<'lua, L> LuaFunction<L>
where
    L: 'lua,
    L: AsLua,
{
    /// Calls the function. Doesn't allow passing parameters.
    ///
    /// TODO: will eventually disappear and get replaced with `call_with_args`
    ///
    /// Returns an error if there is an error while executing the Lua code (eg. a function call
    /// returns an error), or if the requested return type doesn't match the actual return type.
    ///
    /// > **Note**: In order to pass parameters, see `call_with_args` instead.
    #[inline]
    pub fn call<V>(&'lua self) -> Result<V, LuaError>
    where
        V: LuaRead<PushGuard<&'lua L>>,
    {
        match Self::call_impl(&self.lua, self.index.into(), ()) {
            Ok(v) => Ok(v),
            Err(LuaFunctionCallError::LuaError(err)) => Err(err),
            Err(LuaFunctionCallError::PushError(_)) => unreachable!(),
        }
    }

    /// Calls the function taking ownership of the underlying push guard.
    /// Doesn't allow passing parameters.
    ///
    /// TODO: will eventually disappear and get replaced with `call_with_args`
    ///
    /// Returns an error if there is an error while executing the Lua code (eg. a function call
    /// returns an error), or if the requested return type doesn't match the actual return type.
    ///
    /// > **Note**: In order to pass parameters, see `into_call_with_args`
    /// instead.
    #[inline]
    pub fn into_call<V>(self) -> Result<V, LuaError>
    where
        V: LuaRead<PushGuard<Self>>,
    {
        let index = self.index.into();
        match Self::call_impl(self, index, ()) {
            Ok(v) => Ok(v),
            Err(LuaFunctionCallError::LuaError(err)) => Err(err),
            Err(LuaFunctionCallError::PushError(_)) => unreachable!(),
        }
    }

    /// Calls the function with parameters.
    ///
    /// TODO: should be eventually be renamed to `call`
    ///
    /// **Note:** this function can return multiple values if `V` is a tuple.
    /// * If the expected number of values is less than the actual, only the
    ///   first few values will be returned.
    /// * If the expected number of values is greater than the actual, the
    ///   function will return an error, unless the excess elements are
    ///   Option<T>.
    ///
    /// You can either pass a single value by passing a single value, or multiple parameters by
    /// passing a tuple.
    /// If you pass a tuple, the first element of the tuple will be the first argument, the second
    /// element of the tuple the second argument, and so on.
    ///
    /// Returns an error if there is an error while executing the Lua code (eg. a function call
    /// returns an error), if the requested return type doesn't match the actual return type, or
    /// if we failed to push an argument.
    ///
    /// # Example
    ///
    /// ```
    /// let lua = hlua::Lua::new();
    /// lua.exec("function sub(a, b) return a - b end").unwrap();
    ///
    /// let foo: hlua::LuaFunction<_> = lua.get("sub").unwrap();
    /// let result: i32 = foo.call_with_args((18, 4)).unwrap();
    /// assert_eq!(result, 14);
    /// ```
    ///
    /// # Multiple return values
    ///
    /// ```
    /// let lua = hlua::Lua::new();
    /// lua.exec("function divmod(a, b) return math.floor(a / b), a % b end").unwrap();
    ///
    /// let foo: hlua::LuaFunction<_> = lua.get("divmod").unwrap();
    ///
    /// let first_result: i32 = foo.call_with_args((18, 4)).unwrap();
    /// assert_eq!(first_result, 4);
    ///
    /// let all_result: (i32, i32) = foo.call_with_args((18, 4)).unwrap();
    /// assert_eq!(all_result, (4, 2));
    ///
    /// let excess_results: (i32, i32, Option<i32>) = foo.call_with_args((18, 4)).unwrap();
    /// assert_eq!(excess_results, (4, 2, None));
    /// ```
    #[inline]
    pub fn call_with_args<V, A>(&'lua self, args: A)
        -> Result<V, LuaFunctionCallError<<A as Push<LuaState>>::Err>>
    where
        A: Push<LuaState>,
        V: LuaRead<PushGuard<&'lua L>>,
    {
        Self::call_impl(&self.lua, self.index.into(), args)
    }

    /// Calls the function with parameters taking ownership of the underlying
    /// push guard.
    ///
    /// TODO: should be eventually be renamed to `call`
    ///
    /// **Note:** this function can return multiple values if `V` is a tuple.
    /// * If the expected number of values is less than the actual, only the
    ///   first few values will be returned.
    /// * If the expected number of values is greater than the actual, the
    ///   function will return an error, unless the excess elements are
    ///   Option<T>.
    ///
    /// You can either pass a single value by passing a single value, or multiple parameters by
    /// passing a tuple.
    /// If you pass a tuple, the first element of the tuple will be the first argument, the second
    /// element of the tuple the second argument, and so on.
    ///
    /// Returns an error if there is an error while executing the Lua code (eg. a function call
    /// returns an error), if the requested return type doesn't match the actual return type, or
    /// if we failed to push an argument.
    ///
    /// # Example
    ///
    /// ```
    /// let lua = hlua::Lua::new();
    /// lua.exec("function sub(a, b) return a - b end").unwrap();
    ///
    /// let foo: hlua::LuaFunction<_> = lua.get("sub").unwrap();
    /// let result: i32 = foo.into_call_with_args((18, 4)).unwrap();
    /// assert_eq!(result, 14);
    /// ```
    ///
    /// # Multiple return values
    ///
    /// ```
    /// let lua = hlua::Lua::new();
    /// lua.exec("function divmod(a, b) return math.floor(a / b), a % b end").unwrap();
    ///
    /// let foo: hlua::LuaFunction<_> = lua.get("divmod").unwrap();
    ///
    /// let all_result: (i32, i32) = foo.into_call_with_args((18, 4)).unwrap();
    /// assert_eq!(all_result, (4, 2));
    /// ```
    #[inline]
    pub fn into_call_with_args<V, A>(self, args: A)
        -> Result<V, LuaFunctionCallError<<A as Push<LuaState>>::Err>>
    where
        A: Push<LuaState>,
        V: LuaRead<PushGuard<Self>>,
    {
        let index = self.index.into();
        Self::call_impl(self, index, args)
    }

    #[inline]
    pub fn call_impl<T, V, A, E>(
        this: T,
        index: i32,
        args: A,
    ) -> Result<V, LuaFunctionCallError<E>>
    where
        T: AsLua,
        A: Push<LuaState, Err = E>,
        V: LuaRead<PushGuard<T>>,
    {
        let raw_lua = this.as_lua();
        // calling pcall pops the parameters and pushes output
        let (pcall_return_value, pushed_value) = unsafe {
            let old_top = ffi::lua_gettop(raw_lua);
            // lua_pcall pops the function, so we have to make a copy of it
            ffi::lua_pushvalue(raw_lua, index);
            let num_pushed = match args.push_to_lua(this.as_lua()) {
                Ok(g) => g.forget_internal(),
                Err((err, _)) => return Err(LuaFunctionCallError::PushError(err)),
            };
            let pcall_return_value = ffi::lua_pcall(
                raw_lua,
                num_pushed,
                ffi::LUA_MULTRET,
                0,
            );
            let n_results = ffi::lua_gettop(raw_lua) - old_top;
            (pcall_return_value, PushGuard::new(this, n_results))
        };

        match pcall_return_value {
            ffi::LUA_ERRMEM => panic!("lua_pcall returned LUA_ERRMEM"),
            ffi::LUA_ERRRUN => {
                let error_msg: ToString = LuaRead::lua_read(pushed_value)
                    .ok()
                    .expect("can't find error message at the top of the Lua stack");
                return Err(LuaError::ExecutionError(error_msg.into()).into())
            }
            0 => {}
            _ => panic!("Unknown error code returned by lua_pcall: {}", pcall_return_value),
        }

        let n_results = pushed_value.size;
        LuaRead::lua_read_at_maybe_zero_position(pushed_value, -n_results)
            .map_err(|lua| LuaError::wrong_type::<V, _>(lua, n_results).into())
    }

    /// Builds a new `LuaFunction` from the code of a reader.
    ///
    /// Returns an error if reading from the `Read` object fails or if there is a syntax error in
    /// the code.
    ///
    /// # Example
    ///
    /// ```
    /// use std::io::Cursor;
    ///
    /// let mut lua = hlua::Lua::new();
    ///
    /// let mut f = hlua::LuaFunction::load_from_reader(&mut lua, Cursor::new("return 8")).unwrap();
    /// let ret: i32 = f.call().unwrap();
    /// assert_eq!(ret, 8);
    /// ```
    #[inline]
    pub fn load_from_reader(lua: L, code: impl Read)
        -> Result<LuaFunction<PushGuard<L>>, LuaError>
    {
        match LuaCodeFromReader(code).push_into_lua(lua) {
            Ok(pushed) => Ok(LuaFunction::new(pushed, crate::NEGATIVE_ONE)),
            Err((err, _)) => Err(err),
        }
    }

    /// Builds a new `LuaFunction` from a raw string.
    ///
    /// > **Note**: This is just a wrapper around `load_from_reader`. There is no advantage in
    /// > using `load` except that it is more convenient.
    // TODO: remove this function? it's only a thin wrapper and it's for a very niche situation
    #[inline]
    pub fn load(lua: L, code: &str) -> Result<LuaFunction<PushGuard<L>>, LuaError> {
        let reader = Cursor::new(code.as_bytes());
        LuaFunction::load_from_reader(lua, reader)
    }
}

/// Error that can happen when calling a `LuaFunction`.
// TODO: implement Error on this
#[derive(Debug)]
pub enum LuaFunctionCallError<E> {
    /// Error while executing the function.
    LuaError(LuaError),
    /// Error while pushing one of the parameters.
    PushError(E),
}

impl<E> fmt::Display for LuaFunctionCallError<E>
    where E: fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            LuaFunctionCallError::LuaError(ref lua_error) => write!(f, "Lua error: {}", lua_error),
            LuaFunctionCallError::PushError(ref err) => {
                write!(f, "Error while pushing arguments: {}", err)
            }
        }
    }
}

impl<E> From<LuaError> for LuaFunctionCallError<E> {
    #[inline]
    fn from(err: LuaError) -> LuaFunctionCallError<E> {
        LuaFunctionCallError::LuaError(err)
    }
}

impl<E> From<LuaFunctionCallError<E>> for LuaError
where
    E: Into<Void>,
{
    fn from(err: LuaFunctionCallError<E>) -> LuaError {
        match err {
            LuaFunctionCallError::LuaError(lua_error) => lua_error,
            LuaFunctionCallError::PushError(_) => unreachable!("Void cannot be instantiated"),
        }
    }
}

impl<E> Error for LuaFunctionCallError<E>
    where E: Error
{
    fn description(&self) -> &str {
        match *self {
            LuaFunctionCallError::LuaError(_) => "Lua error",
            LuaFunctionCallError::PushError(_) => "error while pushing arguments",
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            LuaFunctionCallError::LuaError(ref lua_error) => Some(lua_error),
            LuaFunctionCallError::PushError(ref err) => Some(err),
        }
    }
}

impl Error for LuaFunctionCallError<Void> {
    fn description(&self) -> &str {
        match *self {
            LuaFunctionCallError::LuaError(_) => "Lua error",
            _ => unreachable!("Void cannot be instantiated"),
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            LuaFunctionCallError::LuaError(ref lua_error) => Some(lua_error),
            _ => unreachable!("Void cannot be instantiated"),
        }
    }
}

// TODO: return Result<Ret, ExecutionError> instead
// impl<'a, 'lua, Ret: CopyRead> ::std::ops::FnMut<(), Ret> for LuaFunction<'a,'lua> {
// fn call_mut(&mut self, _: ()) -> Ret {
// self.call().unwrap()
// }
// }

impl<L> LuaRead<L> for LuaFunction<L>
where
    L: AsLua,
{
    #[inline]
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<LuaFunction<L>, L> {
        if unsafe { ffi::lua_isfunction(lua.as_lua(), index.get()) } {
            Ok(LuaFunction::new(lua, index))
        } else {
            Err(lua)
        }
    }
}


use crate::{
    ffi,
    AsLua,
    error,
    Nil,
    LuaError,
    LuaRead,
    LuaState,
    Push,
    PushInto,
    PushGuard,
    PushOne,
    PushOneInto,
    Void,
};

use std::marker::PhantomData;
use std::fmt::Display;
use std::mem;
use std::ptr;

#[macro_export]
macro_rules! function {
    (@ret) => { () };
    (@ret $t:ty) => { $t };
    (($($p:ty),*) $(-> $r:ty)?) => {
        $crate::Function<
            fn($($p),*) $(-> $r)?,
            ($($p,)*),
            function!(@ret $($r)?)
        >
    }
}

macro_rules! impl_function {
    ($name:ident, $($p:ident),*) => (
        /// Wraps a type that implements `FnMut` so that it can be used by tlua.
        ///
        /// This is needed because of a limitation in Rust's inferrence system. Even though in
        /// practice functions and closures always have a fixed number of parameters, the `FnMut`
        /// trait of Rust was designed so that it allows calling the same closure with a varying
        /// number of parameters. The consequence however is that there is no way of inferring
        /// with the trait alone many parameters a function or closure expects.
        #[inline]
        pub fn $name<Z, R $(, $p)*>(f: Z) -> Function<Z, ($($p,)*), R>
        where
            Z: FnMut($($p),*) -> R,
        {
            Function {
                function: f,
                marker: PhantomData,
            }
        }
    )
}

impl_function!(function0,);
impl_function!(function1, A);
impl_function!(function2, A, B);
impl_function!(function3, A, B, C);
impl_function!(function4, A, B, C, D);
impl_function!(function5, A, B, C, D, E);
impl_function!(function6, A, B, C, D, E, F);
impl_function!(function7, A, B, C, D, E, F, G);
impl_function!(function8, A, B, C, D, E, F, G, H);
impl_function!(function9, A, B, C, D, E, F, G, H, I);
impl_function!(function10, A, B, C, D, E, F, G, H, I, J);

/// Opaque type containing a Rust function or closure.
///
/// In order to build an instance of this struct, you need to use one of the `functionN` functions.
/// There is one function for each possible number of parameter. For example if you have a function
/// with two parameters, you must use [`function2`](fn.function2.html).
/// Example:
///
/// ```
/// let f: tlua::Function<_, _, _> = tlua::function2(move |a: i32, b: i32| { });
/// ```
///
/// > **Note**: In practice you will never need to build an object of type `Function` as an
/// > intermediary step. Instead you will most likely always immediately push the function, like
/// > in the code below.
///
/// You can push a `Function` object like any other value:
///
/// ```
/// use tlua::Lua;
/// let lua = Lua::new();
///
/// lua.set("foo", tlua::function1(move |a: i32| -> i32 {
///     a * 5
/// }));
/// ```
///
/// The function can then be called from Lua:
///
/// ```
/// # use tlua::Lua;
/// # let lua = Lua::new();
/// # lua.set("foo", tlua::function1(move |a: i32| -> i32 { a * 5 }));
/// lua.exec("a = foo(12)").unwrap();
///
/// assert_eq!(lua.get::<i32, _>("a").unwrap(), 60);
/// ```
///
/// Remember that in Lua functions are regular variables, so you can do something like this
/// for example:
///
/// ```
/// # use tlua::Lua;
/// # let lua = Lua::new();
/// # lua.set("foo", tlua::function1(move |a: i32| -> i32 { a * 5 }));
/// lua.exec("bar = foo; a = bar(12)").unwrap();
/// ```
///
/// # Multiple return values
///
/// The Lua language supports functions that return multiple values at once.
///
/// In order to return multiple values from a Rust function, you can return a tuple. The elements
/// of the tuple will be returned in order.
///
/// ```
/// use tlua::Lua;
/// let lua = Lua::new();
///
/// lua.set("values", tlua::function0(move || -> (i32, i32, i32) {
///     (12, 24, 48)
/// }));
///
/// lua.exec("a, b, c = values()").unwrap();
///
/// assert_eq!(lua.get::<i32, _>("a").unwrap(), 12);
/// assert_eq!(lua.get::<i32, _>("b").unwrap(), 24);
/// assert_eq!(lua.get::<i32, _>("c").unwrap(), 48);
/// ```
///
/// # Using `Result`
///
/// If you want to return an error to the Lua script, you can use a `Result` that contains an
/// `Err`. The error will be returned to Lua as two values: A `nil` value and the error message.
///
/// The error type of the `Result` must implement the `Display` trait, and will be turned into a
/// Lua string.
///
/// ```
/// use tlua::Lua;
/// let lua = Lua::new();
/// lua.openlibs();
///
/// lua.set("err", tlua::function0(move || -> Result<i32, &'static str> {
///     Err("something wrong happened")
/// }));
///
/// lua.exec(r#"
///     res, err = err();
///     assert(res == nil);
///     assert(err == "something wrong happened");
/// "#).unwrap();
/// ```
///
/// This also allows easy use of `assert` to act like `.unwrap()` in Rust:
///
/// ```
/// use tlua::Lua;
/// let lua = Lua::new();
/// lua.openlibs();
///
/// lua.set("err", tlua::function0(move || -> Result<i32, &'static str> {
///     Err("something wrong happened")
/// }));
///
/// let ret = lua.exec("res = assert(err())");
/// assert!(ret.is_err());
/// ```
#[derive(Clone, Copy)]
pub struct Function<F, P, R> {
    function: F,
    marker: PhantomData<(P, R)>,
}

impl<F, P, R> std::fmt::Debug for Function<F, P, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Function({})", std::any::type_name::<F>())
    }
}

impl<F, P, R> Function<F, P, R> {
    pub fn new(function: F) -> Self {
        Self { function, marker: PhantomData }
    }
}

/// Trait implemented on `Function` to mimic `FnMut`.
///
/// We could in theory use the `FnMut` trait instead of this one, but it is still unstable.
pub trait FnMutExt<P> {
    type Output;

    fn call_mut(&mut self, params: P) -> Self::Output;
}

macro_rules! impl_function_ext {
    (@recur) => {};
    (@recur $_head:ident $($tail:ident)*) => {
        impl_function_ext!{ $($tail)* }
    };
    ($($p:ident)*) => {
        impl<Z, R $(,$p)*> FnMutExt<($($p,)*)> for Function<Z, ($($p,)*), R>
        where
            Z: FnMut($($p),*) -> R,
        {
            type Output = R;

            #[allow(non_snake_case)]
            #[inline]
            fn call_mut(&mut self, params: ($($p,)*)) -> Self::Output {
                let ($($p,)*) = params;
                (self.function)($($p),*)
            }
        }

        impl<L, Z, R $(,$p: 'static)*> PushInto<L> for Function<Z, ($($p,)*), R>
        where
            L: AsLua,
            Z: FnMut($($p),*) -> R,
            Z: 'static,
            ($($p,)*): for<'p> LuaRead<&'p InsideCallback>,
            R: PushInto<InsideCallback> + 'static,
        {
            type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

            #[inline]
            fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
                unsafe {
                    // pushing the function pointer as a userdata
                    let ud = ffi::lua_newuserdata(lua.as_lua(), mem::size_of::<Self>() as _);
                    ptr::write(ud.cast(), self);

                    if std::mem::needs_drop::<Self>() {
                        // Creating a metatable.
                        ffi::lua_newtable(lua.as_lua());

                        // Index "__gc" in the metatable calls the object's destructor.
                        lua.as_lua().push("__gc").forget_internal();
                        ffi::lua_pushcfunction(lua.as_lua(), wrap_gc::<Self>);
                        ffi::lua_settable(lua.as_lua(), -3);

                        ffi::lua_setmetatable(lua.as_lua(), -2);
                    }

                    // pushing wrapper as a closure
                    ffi::lua_pushcclosure(lua.as_lua(), wrapper::<Self, _, R>, 1);
                    return Ok(PushGuard::new(lua, 1));

                    extern "C" fn wrap_gc<T>(lua: LuaState) -> i32 {
                        unsafe {
                            let obj = ffi::lua_touserdata(lua, -1);
                            ptr::drop_in_place(obj.cast::<T>());
                            0
                        }
                    }
                }
            }
        }

        impl<L, Z, R $(,$p: 'static)*> PushOneInto<L> for Function<Z, ($($p,)*), R>
        where
            L: AsLua,
            Z: FnMut($($p),*) -> R,
            Z: 'static,
            ($($p,)*): for<'p> LuaRead<&'p InsideCallback>,
            R: PushInto<InsideCallback> + 'static,
        {
        }

        impl<L, Z, R $(,$p: 'static)*> Push<L> for Function<Z, ($($p,)*), R>
        where
            L: AsLua,
            Z: FnMut($($p),*) -> R,
            Self: Copy + 'static,
            ($($p,)*): for<'p> LuaRead<&'p InsideCallback>,
            R: PushInto<InsideCallback> + 'static,
        {
            type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

            fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
                unsafe {
                    // pushing the function pointer as a userdata
                    let ud = ffi::lua_newuserdata(lua.as_lua(), mem::size_of::<Self>() as _);
                    ptr::write(ud.cast(), *self);

                    // pushing wrapper as a closure
                    ffi::lua_pushcclosure(lua.as_lua(), wrapper::<Self, _, R>, 1);
                    Ok(PushGuard::new(lua, 1))
                }
            }
        }

        impl<L, Z, R $(,$p: 'static)*> PushOne<L> for Function<Z, ($($p,)*), R>
        where
            L: AsLua,
            Z: FnMut($($p),*) -> R,
            Self: Copy + 'static,
            ($($p,)*): for<'p> LuaRead<&'p InsideCallback>,
            R: PushInto<InsideCallback> + 'static,
        {
        }

        impl_function_ext!{ @recur $($p)* }
    }
}

impl_function_ext!{A B C D E F G H I J K M N}

/// Opaque type that represents the Lua context when inside a callback.
///
/// Some types (like `Result`) can only be returned from a callback and not written inside a
/// Lua variable. This type is here to enforce this restriction.
#[derive(Debug)]
pub struct InsideCallback(LuaState);

impl AsLua for InsideCallback {
    #[inline]
    fn as_lua(&self) -> LuaState {
        self.0
    }
}

// This impl is the reason Push has a generic type parameter. But do we really
// need this impl at all?
impl<T, E> PushInto<InsideCallback> for Result<T, E>
where
    T: PushInto<InsideCallback>,
    E: Display,
{
    type Err = T::Err;

    #[inline]
    fn push_into_lua(self, lua: InsideCallback)
        -> Result<PushGuard<InsideCallback>, (T::Err, InsideCallback)>
    {
        match self {
            Ok(val) => val.push_into_lua(lua),
            Err(val) => Ok(lua.push(&(Nil, val.to_string()))),
        }
    }
}

impl<'a, T, E> PushOneInto<InsideCallback> for Result<T, E>
where
    T: PushOneInto<InsideCallback>,
    E: Display
{
}

// this function is called when Lua wants to call one of our functions
extern "C" fn wrapper<T, A, R>(lua: LuaState) -> libc::c_int
where
    T: FnMutExt<A, Output = R>,
    // TODO(gmoshkin): these bounds are too strict, how do we loosen them?
    A: for<'p> LuaRead<&'p InsideCallback> + 'static,
    R: PushInto<InsideCallback>,
{
    // loading the object that we want to call from the Lua context
    let data_raw = unsafe { ffi::lua_touserdata(lua, ffi::lua_upvalueindex(1)) };
    let data = unsafe { data_raw.cast::<T>().as_mut() }
        .expect("lua_touserdata returned NULL");

    // creating a temporary Lua context in order to pass it to push & read functions
    let tmp_lua = InsideCallback(lua.as_lua());

    // trying to read the arguments
    let arguments_count = unsafe { ffi::lua_gettop(lua) } as i32;
    // TODO: what if the user has the wrong params?
    let args = A::lua_read_at_maybe_zero_position(&tmp_lua, -arguments_count);
    let args = match args {
        Err(lua) => {
            error!(lua, "{}",
                LuaError::wrong_type_passed::<A, _>(lua, arguments_count),
            )
        }
        Ok(a) => a,
    };

    let ret_value = data.call_mut(args);

    // pushing back the result of the function on the stack
    let nb = match ret_value.push_into_lua(tmp_lua) {
        Ok(p) => p.forget_internal(),
        Err(_) => panic!(),      // TODO: wrong
    };
    nb as _
}


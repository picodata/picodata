//! Module provides FFI bindings for the following constants,
//! types and functions, realted to Lua C API:
//! 1. Plain lua C API
//! 2. lauxlib
//! 3. Lua utitlites, implemented in Tarantool

#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]
use std::os::raw::{c_double, c_int, c_char, c_void};
use std::ptr::null_mut;

/// Lua provides a registry, a pre-defined table that can be used by any C code
/// to store whatever Lua value it needs to store. This table is always located
/// at pseudo-index `LUA_REGISTRYINDEX`. Any C library can store data into this
/// table, but it should take care to choose keys different from those used by
/// other libraries, to avoid collisions. Typically, you should use as key a
/// string containing your library name or a light userdata with the address of
/// a C object in your code.
///
/// The integer keys in the registry are used by the reference mechanism,
/// implemented by the auxiliary library, and therefore should not be used for
/// other purposes.
pub const LUA_REGISTRYINDEX: c_int = -10000;
pub const LUA_ENVIRONINDEX: c_int = -10001;
pub const LUA_GLOBALSINDEX: c_int = -10002;

pub fn is_relative_index(index: c_int) -> bool {
    index < 0 && index > LUA_REGISTRYINDEX
}

pub const LUA_OK: c_int = 0;
pub const LUA_YIELD: c_int = 1;
pub const LUA_ERRRUN: c_int = 2;
pub const LUA_ERRSYNTAX: c_int = 3;
pub const LUA_ERRMEM: c_int = 4;
pub const LUA_ERRERR: c_int = 5;

pub const LUA_TNONE: c_int = -1;

pub const LUA_TNIL: c_int = 0;
pub const LUA_TBOOLEAN: c_int = 1;
pub const LUA_TLIGHTUSERDATA: c_int = 2;
pub const LUA_TNUMBER: c_int = 3;
pub const LUA_TSTRING: c_int = 4;
pub const LUA_TTABLE: c_int = 5;
pub const LUA_TFUNCTION: c_int = 6;
pub const LUA_TUSERDATA: c_int = 7;
pub const LUA_TTHREAD: c_int = 8;
pub const LUA_TCDATA: c_int = 10;

pub const LUA_MINSTACK: c_int = 20;

pub const LUA_NOREF: c_int = -2;
pub const LUA_REFNIL: c_int = -1;

pub const LUA_MULTRET: c_int = -1;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct lua_State {
    pub _unused: [u8; 0],
}

#[repr(C)]
pub struct luaL_Reg {
    pub name: *const c_char,
    pub func: lua_CFunction,
}

pub type lua_Number = libc::c_double;
pub type lua_Integer = libc::ptrdiff_t;

/// Type for C functions.
///
/// In order to communicate properly with Lua, a C function must use the
/// following protocol, which defines the way parameters and results are passed:
/// a C function receives its arguments from Lua in its stack in direct order
/// (the first argument is pushed first). So, when the function starts,
/// [`lua_gettop`]`(L)` returns the number of arguments received by the function.
/// The first argument (if any) is at index 1 and its last argument is at index
/// [`lua_gettop`]`(L)`. To return values to Lua, a C function just pushes them
/// onto the stack, in direct order (the first result is pushed first), and
/// returns the number of results. Any other value in the stack below the
/// results will be properly discarded by Lua. Like a Lua function, a C function
/// called by Lua can also return many results.
///
/// As an example, the following function receives a variable number of
/// numerical arguments and returns their average and sum:
///
/// ```
/// unsafe extern "C" fn foo(l: *mut lua_State) -> i32 {
///     let n = lua_gettop(l);    /* number of arguments */
///     let mut sum: lua_Number = 0;
///     let i: i32;
///     for i in 1..=n {
///         if !lua_isnumber(l, i) {
///             lua_pushstring(l, CString::new("incorrect argument").into_raw());
///             lua_error(l);
///         }
///         sum += lua_tonumber(l, i);
///     }
///     lua_pushnumber(l, sum / n); /* first result */
///     lua_pushnumber(l, sum);     /* second result */
///     return 2;                   /* number of results */
/// }
/// ```
pub type lua_CFunction = unsafe extern "C" fn(l: *mut lua_State) -> c_int;

pub type lua_Alloc = extern "C" fn(
    ud: *mut libc::c_void,
    ptr: *mut libc::c_void,
    osize: libc::size_t,
    nsize: libc::size_t,
) -> *mut libc::c_void;

/// The reader function used by [`lua_load`]. Every time it needs another piece
/// of the chunk, [`lua_load`] calls the reader, passing along its `data`
/// parameter. The reader must return a pointer to a block of memory with a new
/// piece of the chunk and set `size` to the block size. The block must exist
/// until the reader function is called again. To signal the end of the chunk,
/// the reader must return `NULL` or set `size` to zero. The reader function may
/// return pieces of any size greater than zero.
pub type lua_Reader = extern "C" fn(
    L: *mut lua_State,
    data: *mut libc::c_void,
    size: *mut libc::size_t,
) -> *const libc::c_char;

pub type lua_Writer = extern "C" fn(
    L: *mut lua_State,
    p: *const libc::c_void,
    sz: libc::size_t,
    ud: *mut libc::c_void,
) -> libc::c_int;

extern "C" {
    // Lua C API functions.
    pub fn lua_newstate(f: lua_Alloc, ud: *mut libc::c_void) -> *mut lua_State;
    pub fn lua_close(l: *mut lua_State);
    pub fn lua_newthread(l: *mut lua_State) -> *mut lua_State;

    pub fn lua_atpanic(l: *mut lua_State, panicf: lua_CFunction) -> lua_CFunction;

    pub fn lua_version(L: *mut lua_State) -> *const lua_Number;

    /// Returns the index of the top element in the stack. Because indices start
    /// at 1, this result is equal to the number of elements in the stack (and
    /// so 0 means an empty stack).
    /// *[-0, +0, -]*
    pub fn lua_gettop(l: *mut lua_State) -> c_int;
    pub fn lua_settop(l: *mut lua_State, index: c_int);
    pub fn lua_pushboolean(l: *mut lua_State, n: c_int);
    pub fn lua_pushlstring(l: *mut lua_State, s: *const libc::c_char, l: libc::size_t);

    /// Pushes the zero-terminated string pointed to by `s` onto the stack. Lua
    /// makes (or reuses) an internal copy of the given string, so the memory at
    /// s can be freed or reused immediately after the function returns. The
    /// string cannot contain embedded zeros; it is assumed to end at the first
    /// zero.
    /// *[-0, +1, m]*
    pub fn lua_pushstring(l: *mut lua_State, s: *const c_char) -> *const c_char;
    pub fn lua_pushinteger(l: *mut lua_State, n: isize);
    pub fn lua_pushnumber(l: *mut lua_State, n: c_double);

    /// Pushes a new C closure onto the stack.
    /// *[-n, +1, m]*
    ///
    /// When a C function is created, it is possible to associate some values
    /// with it, thus creating a C closure; these values are then accessible to
    /// the function whenever it is called. To associate values with a C
    /// function, first these values should be pushed onto the stack (when there
    /// are multiple values, the first value is pushed first). Then
    /// lua_pushcclosure is called to create and push the C function onto the
    /// stack, with the argument `n` telling how many values should be
    /// associated with the function. lua_pushcclosure also pops these values
    /// from the stack.
    ///
    /// The maximum value for `n` is 255.
    pub fn lua_pushcclosure(l: *mut lua_State, fun: lua_CFunction, n: c_int);
    pub fn lua_pushnil(l: *mut lua_State);

    /// Pushes a copy of the element at the given valid `index` onto the stack.
    /// *[-0, +1, -]*
    pub fn lua_pushvalue(l: *mut lua_State, index: c_int);
    pub fn lua_tointeger(l: *mut lua_State, index: c_int) -> isize;
    pub fn lua_toboolean(l: *mut lua_State, index: c_int) -> c_int;

    /// Converts the Lua value at the given acceptable `index` to a C string. If
    /// `len` is not NULL, it also sets `*len` with the string length. The Lua
    /// value must be a string or a number; otherwise, the function returns
    /// NULL. If the value is a number, then `lua_tolstring` also changes the
    /// actual value in the stack to a string. (This change confuses
    /// [`lua_next`] when `lua_tolstring` is applied to keys during a table
    /// traversal.)
    /// *[-0, +0, m]*
    ///
    /// `lua_tolstring` returns a fully aligned pointer to a string inside the
    /// Lua state. This string always has a zero ('\0') after its last character
    /// (as in C), but can contain other zeros in its body. Because Lua has
    /// garbage collection, there is no guarantee that the pointer returned by
    /// `lua_tolstring` will be valid after the corresponding value is removed
    /// from the stack.
    pub fn lua_tolstring(l: *mut lua_State, index: c_int, len: *mut usize) -> *const c_char;

    /// If the value at the given acceptable `index` is a full userdata, returns
    /// its block address. If the value is a light userdata, returns its
    /// pointer. Otherwise, returns `NULL`.
    /// *[-0, +0, -]*
    pub fn lua_touserdata(l: *mut lua_State, index: c_int) -> *mut libc::c_void;

    /// Does the equivalent to `t[k] = v`, where `t` is the value at the given
    /// valid index and `v` is the value at the top of the stack.
    /// *[-1, +0, e]*
    ///
    /// This function pops the value from the stack. As in Lua, this function
    /// may trigger a metamethod for the "newindex" event
    pub fn lua_setfield(l: *mut lua_State, index: c_int, k: *const c_char);

    /// Pushes onto the stack the value `t[k]`, where `t` is the value at the
    /// given valid `index`. As in Lua, this function may trigger a metamethod
    /// for the "index" event
    /// *[-0, +1, e]*
    pub fn lua_getfield(l: *mut lua_State, index: c_int, k: *const c_char);

    pub fn lua_createtable(l: *mut lua_State, narr: c_int, nrec: c_int);

    /// This function allocates a new block of memory with the given size,
    /// pushes onto the stack a new full userdata with the block address, and
    /// returns this address.
    /// *[-0, +1, m]*
    ///
    /// Userdata represent C values in Lua. A full userdata represents a block
    /// of memory. It is an object (like a table): you must create it, it can
    /// have its own metatable, and you can detect when it is being collected. A
    /// full userdata is only equal to itself (under raw equality).
    ///
    /// When Lua collects a full userdata with a gc metamethod, Lua calls the
    /// metamethod and marks the userdata as finalized. When this userdata is
    /// collected again then Lua frees its corresponding memory.
    pub fn lua_newuserdata(l: *mut lua_State, sz: libc::size_t) -> *mut libc::c_void;

    /// Pushes onto the stack the value `t[k]`, where `t` is the value at the
    /// given valid `index` and `k` is the value at the top of the stack.
    /// *[-1, +1, e]*
    ///
    /// This function pops the key from the stack (putting the resulting value
    /// in its place). As in Lua, this function may trigger a metamethod for the
    /// "index" event
    pub fn lua_gettable(l: *mut lua_State, index: c_int);

    /// Similar to [`lua_gettable`], but does a raw access (i.e., without
    /// metamethods).
    /// *[-1, +1, -]*
    pub fn lua_rawget(l: *mut lua_State, index: c_int);

    /// Pushes onto the stack the value `t[n]`, where `t` is the value at the
    /// given valid `index`. The access is *raw*; that is, it does not invoke
    /// metamethods.
    /// *[-0, +1, -]*
    pub fn lua_rawgeti(l: *mut lua_State, index: c_int, n: c_int);

    /// Does the equivalent to `t[k] = v`, where `t` is the value at the given
    /// valid `index`, `v` is the value at the top of the stack, and `k` is the
    /// value just below the top.
    /// *[-2, +0, e]*
    ///
    /// This function pops both the key and the value from the stack. As in Lua,
    /// this function may trigger a metamethod for the "newindex" event.
    pub fn lua_settable(l: *mut lua_State, index: c_int);

    /// Similar to [`lua_settable`], but does a raw assignment (i.e., without
    /// metamethods).
    /// *[-2, +0, m]*
    pub fn lua_rawset(l: *mut lua_State, index: c_int);

    /// Does the equivalent of `t[n] = v`, where `t` is the value at the given
    /// valid `index` and `v` is the value at the top of the stack.
    /// *[-1, +0, m]*
    ///
    /// This function pops the value from the stack. The assignment is raw; that
    /// is, it does not invoke metamethods.
    pub fn lua_rawseti(l: *mut lua_State, index: c_int, n: c_int);

    /// Returns the type of the value in the given acceptable `index`, or
    /// [`LUA_TNONE`] for a non-valid index (that is, an index to an "empty"
    /// stack position). The types returned by lua_type are coded by the
    /// following constants: [`LUA_TNIL`], [`LUA_TNUMBER`], [`LUA_TBOOLEAN`],
    /// [`LUA_TSTRING`], [`LUA_TTABLE`], [`LUA_TFUNCTION`], [`LUA_TUSERDATA`],
    /// [`LUA_TTHREAD`], and [`LUA_TLIGHTUSERDATA`].
    /// *[-0, +0, -]*
    pub fn lua_type(state: *mut lua_State, index: c_int) -> c_int;

    /// Returns the name of the type encoded by the value `tp`, which must be
    /// one the values returned by [`lua_type`].
    /// *[-0, +0, -]*
    pub fn lua_typename(state: *mut lua_State, tp: c_int) -> *mut c_char;

    /// Pops a table from the stack and sets it as the new metatable for the
    /// value at the given acceptable `index`.
    /// *[-1, +0, -]*
    pub fn lua_setmetatable(l: *mut lua_State, index: c_int) -> c_int;
    pub fn lua_getmetatable(l: *mut lua_State, index: c_int) -> c_int;

    pub fn lua_tonumber(l: *mut lua_State, index: c_int) -> lua_Number;
    pub fn lua_tonumberx(l: *mut lua_State, index: c_int, isnum: *mut c_int) -> lua_Number;
    pub fn lua_tointegerx(l: *mut lua_State, index: c_int, isnum: *mut c_int) -> lua_Integer;

    /// Calls a function in protected mode.
    /// *[-(nargs + 1), +(nresults|1), -]*
    ///
    /// Both `nargs` and `nresults` have the same meaning as in `lua_call`. If
    /// there are no errors during the call, `lua_pcall` behaves exactly like
    /// `lua_call`.  However, if there is any error, `lua_pcall` catches it,
    /// pushes a single value on the stack (the error message), and returns an
    /// error code. Like lua_call, `lua_pcall` always removes the function and
    /// its arguments from the stack.
    ///
    /// If `errfunc` is 0, then the error message returned on the stack is
    /// exactly the original error message. Otherwise, `errfunc` is the stack
    /// index of an error handler function. (In the current implementation, this
    /// index cannot be a pseudo-index.) In case of runtime errors, this
    /// function will be called with the error message and its return value will
    /// be the message returned on the stack by `lua_pcall`.
    ///
    /// Typically, the error handler function is used to add more debug
    /// information to the error message, such as a stack traceback. Such
    /// information cannot be gathered after the return of `lua_pcall`, since by
    /// then the stack has unwound.
    ///
    /// The `lua_pcall` function returns 0 in case of success or one of the
    /// following error codes:
    /// - [`LUA_ERRRUN`]: a runtime error.
    ///
    /// - [`LUA_ERRMEM`]: memory allocation error. For such errors, Lua does not
    ///                   call the error handler function.
    ///
    /// - [`LUA_ERRERR`]: error while running the error handler function.
    pub fn lua_pcall(l: *mut lua_State, nargs: c_int, nresults: c_int, errfunc: c_int) -> c_int;

    /// [-0, +1, -]
    /// Loads a Lua chunk. If there are no errors, `lua_load` pushes the
    /// compiled chunk as a Lua function on top of the stack. Otherwise, it
    /// pushes an error message. The return values of `lua_load` are:
    ///
    /// - `0`: no errors;
    /// - [`LUA_ERRSYNTAX`]: syntax error during pre-compilation;
    /// - [`LUA_ERRMEM`]: memory allocation error.
    /// This function only loads a chunk; it does not run it.
    ///
    /// `lua_load` automatically detects whether the chunk is text or binary,
    /// and loads it accordingly (see program `luac`).
    ///
    /// The `lua_load` function uses a user-supplied `reader` function to read
    /// the chunk (see [`lua_Reader`]). The `data` argument is an opaque value
    /// passed to the reader function.
    ///
    /// The `chunkname` argument gives a name to the chunk, which is used for
    /// error messages and in debug information
    pub fn lua_load(l: *mut lua_State, reader: lua_Reader, dt: *mut libc::c_void, chunkname: *const libc::c_char) -> c_int;
    pub fn lua_loadx(l: *mut lua_State, reader: lua_Reader, dt: *mut libc::c_void, chunkname: *const libc::c_char, mode: *const libc::c_char) -> c_int;
    pub fn lua_dump(l: *mut lua_State, writer: lua_Writer, data: *mut libc::c_void) -> c_int;

    /// Generates a Lua error. The error message (which can actually be a Lua
    /// value of any type) must be on the stack top. This function does a long
    /// jump, and therefore never returns. (see [`luaL_error`]).
    /// *[-1, +0, v]*
    pub fn lua_error(l: *mut lua_State) -> c_int;

    /// Pops a key from the stack, and pushes a key-value pair from the table at
    /// the given `index` (the "next" pair after the given key). If there are no
    /// more elements in the table, then `lua_next` returns 0 (and pushes
    /// nothing).
    /// *[-1, +(2|0), e]*
    ///
    /// A typical traversal looks like this:
    ///
    /// ```
    /// use std::ffi::CStr;
    /// unsafe {
    ///     // table is in the stack at index 't'
    ///     lua_pushnil(l);  // first key
    ///     while lua_next(l, t) != 0 {
    ///         // uses 'key' (at index -2) and 'value' (at index -1)
    ///         println!("{} - {}",
    ///             CStr::from_ptr(lua_typename(l, lua_type(l, -2))).to_str().unwrap(),
    ///             CStr::from_ptr(lua_typename(l, lua_type(l, -1))).to_str().unwrap(),
    ///         );
    ///         // removes 'value'; keeps 'key' for next iteration
    ///         lua_pop(l, 1);
    ///     }
    /// }
    /// ```
    /// While traversing a table, do not call [`lua_tolstring`] directly on a
    /// key, unless you know that the key is actually a string. Recall that
    /// `lua_tolstring` changes the value at the given index; this confuses the
    /// next call to lua_next.
    pub fn lua_next(l: *mut lua_State, index: c_int) -> c_int;
    pub fn lua_concat(l: *mut lua_State, n: c_int);
    pub fn lua_len(l: *mut lua_State, index: c_int);

    /// Moves the top element into the given valid `index`, shifting up the
    /// elements above this `index` to open space. Cannot be called with a
    /// pseudo-index, because a pseudo-index is not an actual stack position.
    /// **[-1, +1, -]**
    pub fn lua_insert(l: *mut lua_State, index: c_int);
    pub fn lua_remove(l: *mut lua_State, index: c_int);

    pub fn luaopen_base(l: *mut lua_State);
    pub fn luaopen_bit(l: *mut lua_State);
    pub fn luaopen_debug(l: *mut lua_State);
    pub fn luaopen_io(l: *mut lua_State);
    pub fn luaopen_math(l: *mut lua_State);
    pub fn luaopen_os(l: *mut lua_State);
    pub fn luaopen_package(l: *mut lua_State);
    pub fn luaopen_string(l: *mut lua_State);
    pub fn luaopen_table(l: *mut lua_State);

    // lauxlib functions.
    pub fn luaL_newstate() -> *mut lua_State;
    pub fn luaL_register(l: *mut lua_State, libname: *const c_char, lr: *const luaL_Reg);

    /// Raises an error. The error message format is given by `fmt` plus any
    /// extra arguments, following the same rules of `lua_pushfstring`. It also
    /// adds at the beginning of the message the file name and the line number
    /// where the error occurred, if this information is available.
    /// *[-0, +0, v]*
    ///
    /// This function never returns, but it is an idiom to use it in C functions
    /// as return `luaL_error(args)`.
    pub fn luaL_error(l: *mut lua_State, fmt: *const c_char, ...) -> c_int;
    pub fn luaL_openlibs(L: *mut lua_State);

    /// Creates and returns a reference, in the table at index `t`, for the
    /// object at the top of the stack (and pops the object).
    /// *[-1, +0, m]*
    ///
    /// A reference is a unique integer key. As long as you do not manually add
    /// integer keys into table t, `luaL_ref` ensures the uniqueness of the key
    /// it returns. You can retrieve an object referred by reference r by
    /// calling [`lua_rawgeti`]`(L, t, r)`. Function [`luaL_unref`] frees a
    /// reference and its associated object.
    ///
    /// If the object at the top of the stack is nil, `luaL_ref` returns the
    /// constant [`LUA_REFNIL`]. The constant [`LUA_NOREF`] is guaranteed to be
    /// different from any reference returned by `luaL_ref`.
    pub fn luaL_ref(l: *mut lua_State, t: c_int) -> c_int;

    /// Releases reference `r` from the table at index `t` (see [`luaL_ref`]).
    /// The entry is removed from the table, so that the referred object can be
    /// collected. The reference `r` is also freed to be used again.
    /// *[-0, +0, -]*
    ///
    /// If ref is [`LUA_NOREF`] or [`LUA_REFNIL`], `luaL_unref` does nothing.
    pub fn luaL_unref(l: *mut lua_State, t: c_int, r: c_int);
}

#[inline(always)]
/// Pushes onto the stack the value of the global `name`.
/// *[-0, +1, e]*
pub unsafe fn lua_getglobal(state: *mut lua_State, name: *const c_char) {
    lua_getfield(state, LUA_GLOBALSINDEX, name);
}

#[inline(always)]
/// Pops a value from the stack and sets it as the new value of global `name`.
/// *[-1, +0, e]*
pub unsafe fn lua_setglobal(state: *mut lua_State, name: *const c_char) {
    lua_setfield(state, LUA_GLOBALSINDEX, name);
}

#[inline(always)]
pub unsafe fn lua_pop(state: *mut lua_State, n: c_int) {
    lua_settop(state, -n - 1);
}

#[inline(always)]
/// Pushes a C function onto the stack. This function receives a pointer to a C
/// function and pushes onto the stack a Lua value of type function that, when
/// called, invokes the corresponding C function.
/// `[-0, +1, m]`
///
/// Any function to be registered in Lua must follow the correct protocol to
/// receive its parameters and return its results (see [`lua_CFunction`]).
pub unsafe fn lua_pushcfunction(state: *mut lua_State, f: lua_CFunction) {
    lua_pushcclosure(state, f, 0);
}

#[inline(always)]
pub unsafe fn lua_tostring(state: *mut lua_State, i: c_int) -> *const c_char {
    lua_tolstring(state, i, null_mut())
}

#[inline(always)]
pub unsafe fn lua_newtable(state: *mut lua_State) {
    lua_createtable(state, 0, 0);
}

#[inline(always)]
/// When a C function is created, it is possible to associate some values with
/// it, thus creating a C closure; these values are called upvalues and are
/// accessible to the function whenever it is called (see [`lua_pushcclosure`]).
///
/// Whenever a C function is called, its **upvalues** are located at specific
/// pseudo-indices. These pseudo-indices are produced by the function
/// `lua_upvalueindex`. The first value associated with a function is at
/// position `lua_upvalueindex(1)`, and so on. Any access to
/// `lua_upvalueindex(n)`, where n is greater than the number of upvalues of the
/// current function (but not greater than 256), produces an acceptable (but
/// invalid) index.
pub fn lua_upvalueindex(i: c_int) -> c_int {
    LUA_GLOBALSINDEX - i
}

#[inline(always)]
pub unsafe fn lua_isfunction(state: *mut lua_State, index: c_int) -> bool {
    lua_type(state, index) == LUA_TFUNCTION
}

#[inline(always)]
pub unsafe fn lua_istable(state: *mut lua_State, index: c_int) -> bool {
    lua_type(state, index) == LUA_TTABLE
}

#[inline(always)]
pub unsafe fn lua_islightuserdata(state: *mut lua_State, index: c_int) -> bool {
    lua_type(state, index) == LUA_TLIGHTUSERDATA
}

#[inline(always)]
pub unsafe fn lua_isnil(state: *mut lua_State, index: c_int) -> bool {
    lua_type(state, index) == LUA_TNIL
}

#[inline(always)]
pub unsafe fn lua_isboolean(state: *mut lua_State, index: c_int) -> bool {
    lua_type(state, index) == LUA_TBOOLEAN
}

#[inline(always)]
pub unsafe fn lua_isthread(state: *mut lua_State, index: c_int) -> bool {
    lua_type(state, index) == LUA_TTHREAD
}

#[allow(non_snake_case)]
#[inline(always)]
pub unsafe fn luaL_iscdata(state: *mut lua_State, index: c_int) -> bool {
    lua_type(state, index) == LUA_TCDATA
}

#[inline(always)]
pub unsafe fn lua_isnone(state: *mut lua_State, index: c_int) -> bool {
    lua_type(state, index) == LUA_TNONE
}

#[inline(always)]
pub unsafe fn lua_isnoneornil(state: *mut lua_State, index: c_int) -> bool {
    lua_type(state, index) <= 0
}

#[inline(always)]
pub unsafe fn lua_pushglobaltable(state: *mut lua_State) {
    lua_pushvalue(state, LUA_GLOBALSINDEX)
}

pub const CTID_NONE           : u32 = 0;
pub const CTID_VOID           : u32 = 1;
pub const CTID_CVOID          : u32 = 2;
pub const CTID_BOOL           : u32 = 3;
pub const CTID_CCHAR          : u32 = 4;
pub const CTID_INT8           : u32 = 5;
pub const CTID_UINT8          : u32 = 6;
pub const CTID_INT16          : u32 = 7;
pub const CTID_UINT16         : u32 = 8;
pub const CTID_INT32          : u32 = 9;
pub const CTID_UINT32         : u32 = 10;
pub const CTID_INT64          : u32 = 11;
pub const CTID_UINT64         : u32 = 12;
pub const CTID_FLOAT          : u32 = 13;
pub const CTID_DOUBLE         : u32 = 14;
pub const CTID_COMPLEX_FLOAT  : u32 = 15;
pub const CTID_COMPLEX_DOUBLE : u32 = 16;
pub const CTID_P_VOID         : u32 = 17;
pub const CTID_P_CVOID        : u32 = 18;
pub const CTID_P_CCHAR        : u32 = 19;
pub const CTID_A_CCHAR        : u32 = 20;
pub const CTID_CTYPEID        : u32 = 21;

extern "C" {
    /// Push `u64` onto the stack
    /// *[-0, +1, -]*
    pub fn luaL_pushuint64(l: *mut lua_State, val: u64);

    /// Push `i64` onto the stack
    /// *[-0, +1, -]*
    pub fn luaL_pushint64(l: *mut lua_State, val: i64);

    /// Checks whether the argument `idx` is a `u64` or a convertable string and
    /// returns this number.
    /// *[-0, +0, -]*
    ///
    /// **Return** the converted number or 0 of argument can't be converted.
    pub fn luaL_touint64(l: *mut lua_State, idx: c_int) -> u64;

    /// Checks whether the argument `idx` is a `i64` or a convertable string and
    /// returns this number.
    /// *[-0, +0, -]*
    ///
    /// **Return** the converted number or 0 of argument can't be converted.
    pub fn luaL_toint64(l: *mut lua_State, idx: c_int) -> i64;

    /// Push cdata of given `ctypeid` onto the stack.
    /// CTypeID must be used from FFI at least once. Allocated memory returned
    /// uninitialized. Only numbers and pointers are supported.
    /// - `l`:       Lua State
    /// - `ctypeid`: FFI's CTypeID of this cdata
    /// See also: [`luaL_checkcdata`]
    /// **Returns** memory associated with this cdata
    pub fn luaL_pushcdata(l: *mut lua_State, ctypeid: u32) -> *mut c_void;

    /// Checks whether the function argument `idx` is a cdata
    /// * `l`:       Lua State
    /// * `idx`:     stack index
    /// * `ctypeid`: FFI's CTypeID of this cdata
    /// See also: [`luaL_pushcdata`]
    /// **Returns** memory associated with this cdata
    pub fn luaL_checkcdata(l: *mut lua_State, idx: c_int, ctypeid: *mut u32) -> *mut c_void;

    /// Return CTypeID (FFI) of given CDATA type
    /// `ctypename` is a C type name as string (e.g. "struct request",
    /// "uint32_t", etc.).
    /// See also: [`luaL_pushcdata`], [`luaL_checkcdata`]
    pub fn luaL_ctypeid(l: *mut lua_State, ctypename: *const c_char) -> u32;
}

extern "C" {
    /// Convert the value at `idx` to string using `__tostring` metamethod if
    /// other measures didn't work and return it. Sets the `len` if it's not
    /// `NULL`. The newly created string is left on top of the stack.
    /// *[-0, +1, m]*
    pub fn luaT_tolstring(l: *mut lua_State, idx: c_int, len: *mut usize) -> *const c_char;
}


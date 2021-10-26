//! Сooperative multitasking module
//!
//! With the fiber module, you can:
//! - create, run and manage [fibers](struct.Fiber.html),
//! - use a synchronization mechanism for fibers, similar to “condition variables” and similar to operating-system
//! functions such as `pthread_cond_wait()` plus `pthread_cond_signal()`.
//!
//! See also:
//! - [Threads, fibers and yields](https://www.tarantool.io/en/doc/latest/book/box/atomic/#threads-fibers-and-yields)
//! - [Lua reference: Module fiber](https://www.tarantool.io/en/doc/latest/reference/reference_lua/fiber/)
//! - [C API reference: Module fiber](https://www.tarantool.io/en/doc/latest/dev_guide/reference_capi/fiber/)
use std::cell::UnsafeCell;
use std::ffi::CString;
use std::marker::PhantomData;
use std::os::raw::c_void;
use std::time::Duration;

use crate::hlua::{AsLua, c_ptr, lua_error};
use va_list::VaList;

use crate::error::TarantoolError;
use crate::ffi::{tarantool as ffi, lua};
use crate::Result;

/// *OBSOLETE*: This struct is being deprecated in favour of [`Immediate`],
/// [`Deferred`], etc. due to them being more efficient and idiomatic.
///
/// A fiber is a set of instructions which are executed with cooperative multitasking.
///
/// Fibers managed by the fiber module are associated with a user-supplied function called the fiber function.
///
/// A fiber has three possible states: **running**, **suspended** or **dead**.
/// When a fiber is started with [fiber.start()](struct.Fiber.html#method.start), it is **running**.
/// When a fiber is created with [Fiber::new()](struct.Fiber.html#method.new) (and has not been started yet) or yields control
/// with [sleep()](fn.sleep.html), it is **suspended**.
/// When a fiber ends (because the fiber function ends), it is **dead**.
///
/// A runaway fiber can be stopped with [fiber.cancel()](struct.Fiber.html#method.cancel).
/// However, [fiber.cancel()](struct.Fiber.html#method.cancel) is advisory — it works only if the runaway fiber calls
/// [is_cancelled()](fn.is_cancelled.html) occasionally. In practice, a runaway fiber can only become unresponsive if it
/// does many computations and does not check whether it has been cancelled.
///
/// The other potential problem comes from fibers which never get scheduled, because they are not subscribed to any events,
/// or because no relevant events occur. Such morphing fibers can be killed with [fiber.cancel()](struct.Fiber.html#method.cancel)
/// at any time, since [fiber.cancel()](struct.Fiber.html#method.cancel) sends an asynchronous wakeup event to the fiber,
/// and [is_cancelled()](fn.is_cancelled.html) is checked whenever such a wakeup event occurs.
///
/// Example:
/// ```rust
/// use tarantool::fiber::Fiber;
/// let mut fiber = Fiber::new("test_fiber", &mut |_| {
///     println!("I'm a fiber");
///     0
/// });
/// fiber.start(());
/// println!("Fiber started")
/// ```
///
/// ```text
/// I'm a fiber
/// Fiber started
/// ```
pub struct Fiber<'a, T: 'a> {
    inner: *mut ffi::Fiber,
    callback: *mut c_void,
    phantom: PhantomData<&'a T>,
}

impl<'a, T> Fiber<'a, T> {
    /// Create a new fiber.
    ///
    /// Takes a fiber from fiber cache, if it's not empty. Can fail only if there is not enough memory for
    /// the fiber structure or fiber stack.
    ///
    /// The created fiber automatically returns itself to the fiber cache when its `main` function
    /// completes. The initial fiber state is **suspended**.
    ///
    /// Ordinarily [Fiber::new()](#method.new) is used in conjunction with [fiber.set_joinable()](#method.set_joinable)
    /// and [fiber.join()](#method.join)
    ///
    /// - `name` - string with fiber name
    /// - `callback` - function for run inside fiber
    ///
    /// See also: [fiber.start()](#method.start)
    pub fn new<F>(name: &str, callback: &mut F) -> Self
    where
        F: FnMut(Box<T>) -> i32,
    {
        let (callback_ptr, trampoline) = unsafe { unpack_callback(callback) };
        Self {
            inner: unsafe { ffi::fiber_new(CString::new(name).unwrap().into_raw(), trampoline) },
            callback: callback_ptr,
            phantom: PhantomData,
        }
    }

    /// Create a new fiber with defined attributes.
    ///
    /// Can fail only if there is not enough memory for the fiber structure or fiber stack.
    ///
    /// The created fiber automatically returns itself to the fiber cache if has default stack size
    /// when its `main` function completes. The initial fiber state is **suspended**.
    ///
    /// - `name` - string with fiber name
    /// - `fiber_attr` - fiber attributes
    /// - `callback` - function for run inside fiber
    ///
    /// See also: [fiber.start()](#method.start)
    pub fn new_with_attr<F>(name: &str, attr: &FiberAttr, callback: &mut F) -> Self
    where
        F: FnMut(Box<T>) -> i32,
    {
        let (callback_ptr, trampoline) = unsafe { unpack_callback(callback) };
        Self {
            inner: unsafe {
                ffi::fiber_new_ex(
                    CString::new(name).unwrap().into_raw(),
                    attr.inner,
                    trampoline,
                )
            },
            callback: callback_ptr,
            phantom: PhantomData,
        }
    }

    /// Start execution of created fiber.
    ///
    /// - `arg` - argument to start the fiber with
    ///
    /// See also: [fiber.new()](#method.new)
    pub fn start(&mut self, arg: T) {
        unsafe {
            let boxed_arg = Box::into_raw(Box::<T>::new(arg));
            ffi::fiber_start(self.inner, self.callback, boxed_arg);
        }
    }

    /// Interrupt a synchronous wait of a fiber.
    pub fn wakeup(&self) {
        unsafe { ffi::fiber_wakeup(self.inner) }
    }

    /// Wait until the fiber is dead and then move its execution status to the caller.
    ///
    /// “Join” a joinable fiber. That is, let the fiber’s function run and wait until the fiber’s status is **dead**
    /// (normally a status becomes **dead** when the function execution finishes). Joining will cause a yield,
    /// therefore, if the fiber is currently in a **suspended** state, execution of its fiber function will resume.
    ///
    /// This kind of waiting is more convenient than going into a loop and periodically checking the status;
    /// however, it works only if the fiber was created with [fiber.new()](#method.new) and was made joinable with
    /// [fiber.set_joinable()](#method.set_joinable).
    ///
    /// The fiber must not be detached (See also: [fiber.set_joinable()](#method.set_joinable)).
    ///
    /// Return: fiber function return code
    pub fn join(&self) -> i32 {
        unsafe { ffi::fiber_join(self.inner) }
    }

    /// Set fiber to be joinable (false by default).
    ///
    /// - `is_joinable` - status to set
    pub fn set_joinable(&mut self, is_joinable: bool) {
        unsafe { ffi::fiber_set_joinable(self.inner, is_joinable) }
    }

    /// Cancel a fiber. (set `FIBER_IS_CANCELLED` flag)
    ///
    /// Running and suspended fibers can be cancelled. After a fiber has been cancelled, attempts to operate on it will
    /// cause error: the fiber is dead. But a dead fiber can still report its id and status.
    /// Possible errors: cancel is not permitted for the specified fiber object.
    ///
    /// If target fiber's flag `FIBER_IS_CANCELLABLE` set, then it would be woken up (maybe prematurely).
    /// Then current fiber yields until the target fiber is dead (or is woken up by
    /// [fiber.wakeup()](#method.wakeup)).
    pub fn cancel(&mut self) {
        unsafe { ffi::fiber_cancel(self.inner) }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Builder
////////////////////////////////////////////////////////////////////////////////

/// Fiber factory which can be used to configure the properties of the new
/// fiber.
///
/// Methods can be chained on it in order to configure it.
///
/// The currently supported configurations are:
///
/// * `name`:       specifies an associated name for the fiber
/// * `stack_size`: specifies the desired stack size for the fiber
/// * `func`/`proc`: specifies the fiber function (or procedure)
///
/// The [`start`](#method.start) and [`defer`](#method.defer) methods will
/// take ownership of the builder and create a [`Result`] to the fiber handle
/// with the given configuration.
///
/// The [`fiber::start`](start), [`fiber::start_proc`](start_proc),
/// [`fiber::defer`](defer) and [`fiber::defer_proc`](defer_proc) free functions
/// use a `Builder` with default configuration and unwraps its return value.
pub struct Builder<F> {
    name: Option<String>,
    attr: Option<FiberAttr>,
    f: F,
}

impl Builder<NoFunc> {
    /// Generates the base configuration for spawning a fiber, from which
    /// configuration methods can be chained.
    pub fn new() -> Self {
        Builder {
            name: None,
            attr: None,
            f: NoFunc,
        }
    }

    /// Sets the callee function for the new fiber.
    pub fn func<F, T>(self, f: F) -> Builder<FiberFunc<F, T>>
    where
        F: FnOnce() -> T,
    {
        Builder {
            name: self.name,
            attr: self.attr,
            f: FiberFunc {
                f: Box::new(f),
                result: Default::default(),
            },
        }
    }

    /// Sets the callee function for the new fiber.
    pub fn proc<F>(self, f: F) -> Builder<FiberProc<F>>
    where
        F: FnOnce(),
    {
        Builder {
            name: self.name,
            attr: self.attr,
            f: FiberProc { f: Box::new(f) },
        }
    }
}

impl<F> Builder<F> {
    /// Names the fiber-to-be.
    ///
    /// The name must not contain null bytes (`\0`).
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the size of the stack (in bytes) for the new fiber.
    ///
    /// This function performs some runtime tests to validate the given stack
    /// size. If `stack_size` is invalid then [`Error::Tarantool`] will be
    /// returned.
    ///
    /// [`Error::Tarantool`]: crate::error::Error::Tarantool
    pub fn stack_size(mut self, stack_size: usize) -> Result<Self> {
        let mut attr = FiberAttr::new();
        attr.set_stack_size(stack_size)?;
        self.attr = Some(attr);
        Ok(self)
    }
}

macro_rules! inner_spawn {
    ($self:expr, $invocation:tt) => {
        {
            let Self { name, attr, f } = $self;
            let name = name.unwrap_or_else(|| "<rust>".into());
            Ok(Fyber::$invocation(name, f, attr.as_ref())?.spawn())
        }
    };
}

impl<C> Builder<C>
where
    C: Callee,
{
    /// Spawns a new fiber by taking ownership of the `Builder`, and returns a
    /// [`Result`] to its [`JoinHandle`].
    ///
    /// The current fiber performs a **yield** and the execution is transfered
    /// to the new fiber immediately.
    ///
    /// See the [`start`] free function for more details.
    pub fn start(self) -> Result<C::JoinHandle> {
        inner_spawn!(self, immediate)
    }

    #[cfg(feature = "defer")]
    /// Spawns a new deferred fiber by taking ownership of the `Builder`, and
    /// returns a [`Result`] to its [`JoinHandle`].
    ///
    /// **NOTE:** In the current implementation the current fiber performs a
    /// **yield** to start the newly created fiber and then the new fiber
    /// performs another **yield**. This means that the deferred fiber is **not
    /// applicable for transactions** (which do not allow any context switches).
    /// In the future we are planning to add a correct implementation.
    ///
    /// See the [`defer`] free function for more details.
    pub fn defer(self) -> Result<C::JoinHandle> {
        inner_spawn!(self, deferred)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Fyber
////////////////////////////////////////////////////////////////////////////////

/// A handle to a fiber.
///
/// This is a (somewhat) high-level abstraction intended to facilitate a safe
/// and idiomatic way to work with fibers. It is configurable with different
/// types of behavior enabled by the type parameters which will be set by the
/// [`Builder`].
///
/// Currently there are 2 kinds of configuration supported:
/// - [`Callee`]: configures the kind of fiber function to be executed (one that
///               returns a value or one that does not)
/// - [`Invocation`]: configures the style of fibe invocation (immediately after
///                   creation or at some point in the future)
///
/// **TODO**: add support for cancelable fibers.
pub struct Fyber<C, I> {
    inner: *mut ffi::Fiber,
    callee: C,
    _invocation: PhantomData<I>,
}

impl<C, I> Fyber<C, I>
where
    C: Callee,
    I: Invocation,
{
    fn new(name: String, callee: C, attr: Option<&FiberAttr>) -> Result<Self> {
        let cname = CString::new(name)
            .expect("fiber name may not contain interior null bytes");

        let inner = unsafe {
            if let Some(attr) = attr {
                ffi::fiber_new_ex(
                    cname.as_ptr(), attr.inner, Some(Self::trampoline)
                )
            } else {
                ffi::fiber_new(cname.as_ptr(), Some(Self::trampoline))
            }
        };

        if inner.is_null() {
            return Err(TarantoolError::last().into())
        }

        Ok(
            Self {
                inner,
                callee,
                _invocation: PhantomData,
            }
        )
    }

    pub fn spawn(self) -> C::JoinHandle {
        unsafe {
            ffi::fiber_set_joinable(self.inner, true);
            let jh = self.callee.start_fiber(self.inner);
            I::after_start(self.inner);
            jh
        }
    }

    unsafe extern "C" fn trampoline(args: VaList) -> i32 {
        let a = C::parse_args(args);
        I::before_callee();
        C::invoke(a);
        0
    }
}

impl<C> Fyber<C, Immediate>
where
    C: Callee,
{
    pub fn immediate(
        name: String,
        callee: C,
        attr: Option<&FiberAttr>,
    ) -> Result<Self> {
        Self::new(name, callee, attr)
    }
}

impl<C> Fyber<C, Deferred>
where
    C: Callee,
{
    pub fn deferred(
        name: String,
        callee: C,
        attr: Option<&FiberAttr>,
    ) -> Result<Self> {
        Self::new(name, callee, attr)
    }
}

////////////////////////////////////////////////////////////////////////////////
/// LuaFiber
////////////////////////////////////////////////////////////////////////////////

pub struct LuaFiber<C> {
    callee: C,
}

impl<C> LuaFiber<C>
where
    C: LuaCallee,

{
    pub fn new(callee: C) -> Self {
        Self { callee }
    }

    pub fn spawn(self) -> Result<C::JoinHandle> {
        let Self { callee } = self;
        let fiber_ref = unsafe {
            let l = ffi::luaT_state();
            // TODO don't require("fiber") everytime
            lua::lua_getglobal(l, c_ptr!("require"));
            lua::lua_pushstring(l, c_ptr!("fiber"));
            if lua::lua_pcall(l, 1, 1, 0) == lua::LUA_ERRRUN {
                return Err(impl_details::lua_error_from_top(l).into())
            };
            lua::lua_getfield(l, -1, c_ptr!("new"));
            hlua::push_some_userdata(l, callee.into_inner());
            lua::lua_pushcclosure(l, Self::trampoline, 1);
            if lua::lua_pcall(l, 1, 1, 0) == lua::LUA_ERRRUN {
                return Err(impl_details::lua_error_from_top(l).into())
            };
            lua::lua_getfield(l, -1, c_ptr!("set_joinable"));
            lua::lua_pushvalue(l, -2);
            lua::lua_pushboolean(l, true as i32);
            if lua::lua_pcall(l, 2, 0, 0) == lua::LUA_ERRRUN {
                return Err(impl_details::lua_error_from_top(l).into())
            };
            let fiber_ref = lua::luaL_ref(l, lua::LUA_REGISTRYINDEX);

            // pop the fiber module from the stack
            lua::lua_pop(l, 1);

            fiber_ref
        };

        Ok(C::join_handle(fiber_ref))
    }

    unsafe extern "C" fn trampoline(l: *mut lua::lua_State) -> i32 {
        let ud_ptr = lua::lua_touserdata(l, lua::lua_upvalueindex(1));

        let f = (ud_ptr as *mut Option<C::Function>).as_mut()
            .unwrap_or_else(||
                // lua_touserdata returned NULL
                lua_error!(l, "failed to extract upvalue")
            )
            // put None back into userdata
            .take()
            .unwrap_or_else(||
                // userdata originally contained None
                lua_error!(l, "rust FnOnce callback was called more than once")
            );

        // call f and drop it afterwards
        let res = f();

        // return results to lua
        C::save_result(l, res)
    }
}

////////////////////////////////////////////////////////////////////////////////
/// LuaJoinHandle
////////////////////////////////////////////////////////////////////////////////

pub struct LuaJoinHandle<T> {
    fiber_ref: i32,
    marker: PhantomData<T>,
}

impl<T> LuaJoinHandle<T> {
    pub fn join(self) -> Result<T> {
        let Self { fiber_ref, .. } = self;
        unsafe {
            let guard = impl_details::lua_fiber_join(fiber_ref)?;
            let l = guard.as_lua().state_ptr();
            let ud_ptr = lua::lua_touserdata(l, -1);
            let res = (ud_ptr as *mut Option<T>).as_mut()
                .expect("fiber:join must return correct userdata")
                .take()
                .expect("data can only be taken once from the UDBox");
            Ok(res)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// LuaUnitJoinHandle
////////////////////////////////////////////////////////////////////////////////

pub struct LuaUnitJoinHandle {
    fiber_ref: i32,
}

impl LuaUnitJoinHandle {
    pub fn join(self) -> Result<()> {
        let Self { fiber_ref, .. } = self;
        unsafe { impl_details::lua_fiber_join(fiber_ref)? };
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////
/// impl_details
////////////////////////////////////////////////////////////////////////////////

mod impl_details {
    use super::*;
    use hlua::{AsMutLua, Lua, LuaError, PushGuard};

    pub unsafe fn lua_error_from_top(l: *mut lua::lua_State) -> LuaError {
        let mut len = std::mem::MaybeUninit::uninit();
        let data = lua::lua_tolstring(l, -1, len.as_mut_ptr());
        assert!(!data.is_null());
        let msg_bytes = std::slice::from_raw_parts(
            data as *mut u8, len.assume_init()
        );
        let msg = String::from_utf8_lossy(msg_bytes);
        hlua::LuaError::ExecutionError(msg.into()).into()
    }

    pub unsafe fn lua_fiber_join(f_ref: i32) -> Result<PushGuard<Lua<'static>>> {
        let mut l = Lua::from_existing_state(ffi::luaT_state(), false);
        let lptr = l.as_mut_lua().state_ptr();
        lua::lua_rawgeti(lptr, lua::LUA_REGISTRYINDEX, f_ref);
        lua::lua_getfield(lptr, -1, c_ptr!("join"));
        lua::lua_pushvalue(lptr, -2);

        if lua::lua_pcall(lptr, 1, 2, 0) == lua::LUA_ERRRUN {
            let err = lua_error_from_top(lptr).into();
            // 2 values on the stack:
            // 1) fiber; 2) error
            let _guard = PushGuard::new(l, 2);
            return Err(err)
        };
        // 3 values on the stack that need to be dropped:
        // 1) fiber; 2) join function; 3) fiber
        let guard = PushGuard::new(l, 3);

        // check fiber return code
        assert_ne!(lua::lua_toboolean(lptr, -2), 0);

        // fiber object and the 2 return values will need to be poped
        Ok(guard)
    }
}

////////////////////////////////////////////////////////////////////////////////
/// LuaCallee
////////////////////////////////////////////////////////////////////////////////

pub trait LuaCallee {
    /// Type of the callee
    type Function: FnOnce() -> Self::Output;

    /// Return type of the callee
    type Output;

    /// Type of the join handle returned by [`LuaFiber::spawn`] method
    type JoinHandle;

    /// Extract the inner function
    fn into_inner(self) -> Self::Function;

    /// Construct a `Self::JoinHandle` from a fiber reference
    fn join_handle(fiber_ref: i32) -> Self::JoinHandle;

    /// This function is called within `LuaFiber::trampoline` to save the
    /// return value after the callee's invocation
    ///
    /// This function is unsafe, because it is very easy to mess things up
    /// when preparing arugments.
    unsafe fn save_result(l: *mut lua::lua_State, res: Self::Output) -> i32;
}

////////////////////////////////////////////////////////////////////////////////
/// LuaFiberFunc
////////////////////////////////////////////////////////////////////////////////

pub struct LuaFiberFunc<F>(pub F);

impl<F, T> LuaCallee for LuaFiberFunc<F>
where
    F: FnOnce() -> T,
{
    type Function = F;
    type Output = T;
    type JoinHandle = LuaJoinHandle<T>;

    fn into_inner(self) -> F {
        self.0
    }

    fn join_handle(fiber_ref: i32) -> Self::JoinHandle {
        LuaJoinHandle { fiber_ref, marker: PhantomData }
    }

    unsafe fn save_result(l: *mut lua::lua_State, res: T) -> i32 {
        hlua::push_some_userdata(l, res);
        1
    }
}

////////////////////////////////////////////////////////////////////////////////
/// LuaFiberProc
////////////////////////////////////////////////////////////////////////////////

pub struct LuaFiberProc<F>(pub F);

impl<F> LuaCallee for LuaFiberProc<F>
where
    F: FnOnce(),
{
    type Function = F;
    type Output = ();
    type JoinHandle = LuaUnitJoinHandle;

    fn join_handle(fiber_ref: i32) -> Self::JoinHandle {
        LuaUnitJoinHandle { fiber_ref }
    }

    fn into_inner(self) -> F {
        self.0
    }

    unsafe fn save_result(_: *mut lua::lua_State, _: ()) -> i32 {
        0
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Callee
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait represent [`Fyber`] configurations relating to
/// the kind of the fiber function. Currently only 2 kinds of functions are
/// supported:
/// - [`FiberFunc`]: a no arguments function that returns a value
/// - [`FiberProc`]: a no arguments function that doesn't return a value
///
/// **TODO**: add support for functions which take arguments?
pub trait Callee {
    /// Arguments for the trampoline function which will be passed through the
    /// [`va_list::VaList`]
    type Args;

    /// JoinHandle type which will be returned from the [`Fyber::spawn`]
    /// function
    type JoinHandle;

    /// This function is called within [`Fyber::spawn`] to prepare the arguments
    /// for and invoke the [`ffi::fiber_start`] function.
    ///
    /// This function is unsafe, because it is very easy to mess things up
    /// when preparing arugments.
    unsafe fn start_fiber(self, inner: *mut ffi::Fiber) -> Self::JoinHandle;

    /// This function is called within `Fyber::trampoline` to extract the
    /// arguments from the [`va_list::VaList`].
    ///
    /// This function is unsafe, because it is very easy to mess things up
    /// when extracting arugments.
    unsafe fn parse_args(args: VaList) -> Self::Args;

    /// This function is called within `Fyber::trampoline` to invoke the
    /// underlying callee function and process it's results.
    ///
    /// This function is unsafe, because it is very easy to mess things up
    /// when storing the callee's result values.
    unsafe fn invoke(a: Self::Args);
}

/// This is a *typestate* helper type representing the state of a [`Builder`]
/// that hasn't been assigned a fiber function yet.
pub struct NoFunc;

/// This is a helper type used to configure [`Fyber`] with the appropriate
/// behavior for the fiber function that returns a value.
pub struct FiberFunc<F, T>
where
    F: FnOnce() -> T,
{
    f: Box<F>,
    result: Box<UnsafeCell<Option<T>>>,
}

impl<F, T> Callee for FiberFunc<F, T>
where
    F: FnOnce() -> T,
{
    type JoinHandle = JoinHandle<T>;
    type Args = (Box<F>, *mut Option<T>);

    unsafe fn start_fiber(self, inner: *mut ffi::Fiber) -> Self::JoinHandle {
        let (f, result): Self::Args = (self.f, self.result.get());
        ffi::fiber_start(inner, Box::into_raw(f), result);
        Self::JoinHandle { inner, result: self.result }
    }

    unsafe fn parse_args(mut args: VaList) -> Self::Args {
        let f = args.get_boxed::<F>();
        let result = args.get_ptr::<Option<T>>();
        (f, result)
    }

    unsafe fn invoke((f, result): Self::Args) {
        std::ptr::write(result, Some(f()))
    }
}

/// This is a helper type used to configure [`Fyber`] with the appropriate
/// behavior for the fiber procedure (function which doens't return a value).
pub struct FiberProc<F>
where
    F: FnOnce(),
{
    f: Box<F>,
}

impl<F> Callee for FiberProc<F>
where
    F: FnOnce(),
{
    type JoinHandle = UnitJoinHandle;
    type Args = Box<F>;

    unsafe fn start_fiber(self, inner: *mut ffi::Fiber) -> Self::JoinHandle {
        let f: Self::Args = self.f;
        ffi::fiber_start(inner, Box::into_raw(f));
        Self::JoinHandle { inner }
    }

    unsafe fn parse_args(mut args: VaList) -> Self::Args {
        args.get_boxed::<F>()
    }

    unsafe fn invoke(f: Self::Args) {
        f()
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Invocation
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait represent [`Fyber`] configurations relating to
/// kinds of fiber invocations. Currently there are 2 kinds of invocations
/// supported:
/// - [`Immediate`]: fiber that is started immediately after creation
/// - [`Deferred`]: fiber that is created and is scheduled for execution.
///                 **WARNING**: current implementation of deferred fibers
///                 doesn't support transactions due to tarantool API
///                 limitations
pub trait Invocation {
    /// This method is called from the `Fyber::trampoline` function right
    /// before calling the fiber function.
    ///
    /// This is an implementation detail and will most likely be removed in the
    /// future.
    unsafe fn before_callee();

    /// This method is called from the [`Fyber::spawn`] function right
    /// after starting the fiber.
    ///
    /// This is an implementation detail and will most likely be removed in the
    /// future.
    unsafe fn after_start(f: *mut ffi::Fiber);
}

pub struct Immediate;

impl Invocation for Immediate {
    unsafe fn before_callee() {}
    unsafe fn after_start(_: *mut ffi::Fiber) {}
}

pub struct Deferred;

impl Invocation for Deferred {
    unsafe fn before_callee() {
        ffi::fiber_yield()
    }

    unsafe fn after_start(f: *mut ffi::Fiber) {
        ffi::fiber_wakeup(f)
    }
}

////////////////////////////////////////////////////////////////////////////////
// JoinHandle
////////////////////////////////////////////////////////////////////////////////

/// An owned permission to join on an immediate fiber (block on its termination).
pub struct JoinHandle<T> {
    inner: *mut ffi::Fiber,
    result: Box<UnsafeCell<Option<T>>>,
}

impl<T> JoinHandle<T> {
    /// Block until the fiber's termination and return it's result value.
    pub fn join(self) -> T {
        // TODO: add error handling
        let _code = unsafe { ffi::fiber_join(self.inner) };
        self.result.into_inner().unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////
// UnitJoinHandle
////////////////////////////////////////////////////////////////////////////////

/// An owned permission to join on an immediate fiber (block on its termination).
///
/// This is an optimized case of [`JoinHandle`]`<()>`.
pub struct UnitJoinHandle {
    inner: *mut ffi::Fiber,
}

impl UnitJoinHandle {
    /// Block until the fiber's termination.
    pub fn join(self) {
        let _code = unsafe { ffi::fiber_join(self.inner) };
    }
}

////////////////////////////////////////////////////////////////////////////////
// TrampolineArgs
////////////////////////////////////////////////////////////////////////////////

/// A helper trait that implements some useful functions for working with
/// trampoline function arguments.
trait TrampolineArgs {
    unsafe fn get<T>(&mut self) -> T
    where
        T: va_list::VaPrimitive;

    unsafe fn get_boxed<T>(&mut self) -> Box<T> {
        Box::from_raw(self.get::<*const c_void>() as *mut T)
    }

    unsafe fn get_ptr<T>(&mut self) -> *mut T {
        self.get::<*const c_void>() as *mut T
    }

    unsafe fn get_str(&mut self) -> String {
        let buf = self.get::<*const u8>() as *mut u8;
        let length = self.get::<usize>();
        let capacity = self.get::<usize>();
        String::from_raw_parts(buf, length, capacity)
    }
}

impl TrampolineArgs for VaList {
    unsafe fn get<T>(&mut self) -> T
    where
        T: va_list::VaPrimitive,
    {
        self.get::<T>()
    }
}

////////////////////////////////////////////////////////////////////////////////
// Free functions
////////////////////////////////////////////////////////////////////////////////

/// Creates a new fiber and **yields** execution to it immediately, returning a
/// [`JoinHandle`] for the new fiber.
///
/// **NOTE**: The argument `f` is a function that returns `T`. In case when `T =
/// ()` (no return value) one should instead use [`start_proc`].
///
/// The join handle will implicitly *detach* the child fiber upon being
/// dropped. In this case, the child fiber may outlive the parent. Additionally,
/// the join handle provides a [`JoinHandle::join`] method that can be used to
/// join the child fiber and acquire the result value of the fiber function.
///
/// This will create a fiber using default parameters of [`Builder`], if you
/// want to specify the stack size or the name of the thread, use this API
/// instead.
pub fn start<F, T>(f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T,
{
    Builder::new().func(f).start().unwrap()
}

/// Creates a new proc fiber and **yields** execution to it immediately,
/// returning a [`UnitJoinHandle`] for the new fiber.
///
/// The *proc fiber* is a special case of a fiber whose function does not return
/// a value. In fact `UnitJoinHandle` is identical to `JoinHanble<()>` is all
/// aspects instead that it is implemented more efficiently and the former
/// should always be used instead of the latter.
///
/// For more details see: [`start`]
pub fn start_proc<F>(f: F) -> UnitJoinHandle
where
    F: FnOnce(),
{
    Builder::new().proc(f).start().unwrap()
}

/// Creates a new fiber and schedules it for execution, returning a
/// [`LuaJoinHandle`] for it.
///
/// **NOTE:** In the current implementation the fiber is constructed using the
/// lua api, so it's efficiency is far from perfect.
///
/// **NOTE**: The argument `f` is a function that returns `T`. In case when `T =
/// ()` (no return value) one should instead use [`defer_proc`].
///
/// The new fiber can be joined by calling [`JuaJoinHandle::join`] method on
/// it's join handle.
pub fn defer<F, T>(f: F) -> LuaJoinHandle<T>
where
    F: FnOnce() -> T,
{
    LuaFiber::new(LuaFiberFunc(f)).spawn().unwrap()
}

/// Creates a new proc fiber and schedules it for execution, returning a
/// [`LuaUnitJoinHandle`] for it.
///
/// **NOTE:** In the current implementation the fiber is constructed using the
/// lua api, so it's efficiency is far from perfect.
///
/// The new fiber can be joined by calling [`LuaUnitJoinHandle::join`] method on
/// it's join handle.
///
/// This is an optimized version [`defer`]`<F, ()>`.
pub fn defer_proc<F>(f: F) -> LuaUnitJoinHandle
where
    F: FnOnce(),
{
    LuaFiber::new(LuaFiberProc(f)).spawn().unwrap()
}

/// Make it possible or not possible to wakeup the current
/// fiber immediately when it's cancelled.
///
/// - `is_cancellable` - status to set
///
/// Returns previous state.
pub fn set_cancellable(is_cancellable: bool) -> bool {
    unsafe { ffi::fiber_set_cancellable(is_cancellable) }
}

/// Check current fiber for cancellation (it must be checked manually).
pub fn is_cancelled() -> bool {
    unsafe { ffi::fiber_is_cancelled() }
}

/// Put the current fiber to sleep for at least `time` seconds.
///
/// Yield control to the scheduler and sleep for the specified number of seconds.
/// Only the current fiber can be made to sleep.
///
/// - `time` - time to sleep
///
/// > **Note:** this is a cancellation point (See also: [is_cancelled()](fn.is_cancelled.html))
pub fn sleep(time: f64) {
    unsafe { ffi::fiber_sleep(time) }
}

/// Report loop begin time as double (cheap).
pub fn time() -> f64 {
    unsafe { ffi::fiber_time() }
}

/// Report loop begin time as 64-bit int.
pub fn time64() -> u64 {
    unsafe { ffi::fiber_time64() }
}

/// Report loop begin time as double (cheap). Uses monotonic clock.
pub fn clock() -> f64 {
    unsafe { ffi::fiber_clock() }
}

/// Report loop begin time as 64-bit int. Uses monotonic clock.
pub fn clock64() -> u64 {
    unsafe { ffi::fiber_clock64() }
}

/// Yield control to the scheduler.
///
/// Return control to another fiber and wait until it'll be woken. Equivalent to `fiber.sleep(0)`.
///
/// See also: [Fiber::wakeup()](struct.Fiber.html#method.wakeup)
pub fn fiber_yield() {
    unsafe { ffi::fiber_yield() }
}

/// Reschedule fiber to end of event loop cycle.
pub fn reschedule() {
    unsafe { ffi::fiber_reschedule() }
}

/// Fiber attributes container
pub struct FiberAttr {
    inner: *mut ffi::FiberAttr,
}

impl FiberAttr {
    /// Create a new fiber attribute container and initialize it with default parameters.
    /// Can be used for many fibers creation, corresponding fibers will not take ownership.
    ///
    /// This is safe to drop `FiberAttr` value when fibers created with this attribute still exist.
    pub fn new() -> Self {
        FiberAttr {
            inner: unsafe { ffi::fiber_attr_new() },
        }
    }

    /// Get stack size from the fiber attribute.
    ///
    /// Returns: stack size
    pub fn stack_size(&self) -> usize {
        unsafe { ffi::fiber_attr_getstacksize(self.inner) }
    }

    ///Set stack size for the fiber attribute.
    ///
    /// - `stack_size` - stack size for new fibers
    pub fn set_stack_size(&mut self, stack_size: usize) -> Result<()> {
        if unsafe { ffi::fiber_attr_setstacksize(self.inner, stack_size) } < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(())
        }
    }
}

impl Drop for FiberAttr {
    fn drop(&mut self) {
        unsafe { ffi::fiber_attr_delete(self.inner) }
    }
}

/// Conditional variable for cooperative multitasking (fibers).
///
/// A cond (short for "condition variable") is a synchronization primitive
/// that allow fibers to yield until some predicate is satisfied. Fiber
/// conditions have two basic operations - `wait()` and `signal()`. [cond.wait()](#method.wait)
/// suspends execution of fiber (i.e. yields) until [cond.signal()](#method.signal) is called.
///
/// Example:
///
/// ```rust
/// use tarantool::fiber::Cond;
/// let cond = fiber.cond();
/// cond.wait();
/// ```
///
/// The job will hang because [cond.wait()](#method.wait) – will go to sleep until the condition variable changes.
///
/// ```rust
/// // Call from another fiber:
/// cond.signal();
/// ```
///
/// The waiting stopped, and the [cond.wait()](#method.wait) function returned true.
///
/// This example depended on the use of a global conditional variable with the arbitrary name cond.
/// In real life, programmers would make sure to use different conditional variable names for different applications.
///
/// Unlike `pthread_cond`, [Cond]() doesn't require mutex/latch wrapping.
pub struct Cond {
    inner: *mut ffi::FiberCond,
}

/// - call [Cond::new()](#method.new) to create a named condition variable, which will be called `cond` for examples in this section.
/// - call [cond.wait()](#method.wait) to make a fiber wait for a signal via a condition variable.
/// - call [cond.signal()](#method.signal) to send a signal to wake up a single fiber that has executed [cond.wait()](#method.wait).
/// - call [cond.broadcast()](#method.broadcast) to send a signal to all fibers that have executed [cond.wait()](#method.wait).
impl Cond {
    /// Instantiate a new fiber cond object.
    pub fn new() -> Self {
        Cond {
            inner: unsafe { ffi::fiber_cond_new() },
        }
    }

    /// Wake one fiber waiting for the cond.
    /// Does nothing if no one is waiting. Does not yield.
    pub fn signal(&self) {
        unsafe { ffi::fiber_cond_signal(self.inner) }
    }

    /// Wake up all fibers waiting for the cond.
    /// Does not yield.
    pub fn broadcast(&self) {
        unsafe { ffi::fiber_cond_broadcast(self.inner) }
    }

    /// Suspend the execution of the current fiber (i.e. yield) until [signal()](#method.signal) is called.
    ///
    /// Like pthread_cond, FiberCond can issue spurious wake ups caused by explicit
    /// [Fiber::wakeup()](struct.Fiber.html#method.wakeup) or [Fiber::cancel()](struct.Fiber.html#method.cancel)
    /// calls. It is highly recommended to wrap calls to this function into a loop
    /// and check an actual predicate and `fiber_testcancel()` on every iteration.
    ///
    /// - `timeout` - timeout in seconds
    ///
    /// Returns:
    /// - `true` on [signal()](#method.signal) call or a spurious wake up.
    /// - `false` on timeout, diag is set to `TimedOut`
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        !(unsafe { ffi::fiber_cond_wait_timeout(self.inner, timeout.as_secs_f64()) } < 0)
    }

    /// Shortcut for [wait_timeout()](#method.wait_timeout).
    pub fn wait(&self) -> bool {
        !(unsafe { ffi::fiber_cond_wait(self.inner) } < 0)
    }
}

impl Drop for Cond {
    fn drop(&mut self) {
        unsafe { ffi::fiber_cond_delete(self.inner) }
    }
}

/// A lock for cooperative multitasking environment
pub struct Latch {
    inner: *mut ffi::Latch,
}

impl Latch {
    /// Allocate and initialize the new latch.
    pub fn new() -> Self {
        Latch {
            inner: unsafe { ffi::box_latch_new() },
        }
    }

    /// Lock a latch. Waits indefinitely until the current fiber can gain access to the latch.
    pub fn lock(&self) -> LatchGuard {
        unsafe { ffi::box_latch_lock(self.inner) };
        LatchGuard {
            latch_inner: self.inner,
        }
    }

    /// Try to lock a latch. Return immediately if the latch is locked.
    ///
    /// Returns:
    /// - `Some` - success
    /// - `None` - the latch is locked.
    pub fn try_lock(&self) -> Option<LatchGuard> {
        if unsafe { ffi::box_latch_trylock(self.inner) } == 0 {
            Some(LatchGuard {
                latch_inner: self.inner,
            })
        } else {
            None
        }
    }
}

impl Drop for Latch {
    fn drop(&mut self) {
        unsafe { ffi::box_latch_delete(self.inner) }
    }
}

/// An RAII implementation of a "scoped lock" of a latch. When this structure is dropped (falls out of scope),
/// the lock will be unlocked.
pub struct LatchGuard {
    latch_inner: *mut ffi::Latch,
}

impl Drop for LatchGuard {
    fn drop(&mut self) {
        unsafe { ffi::box_latch_unlock(self.latch_inner) }
    }
}

pub(crate) unsafe fn unpack_callback<F, T>(callback: &mut F) -> (*mut c_void, ffi::FiberFunc)
where
    F: FnMut(Box<T>) -> i32,
{
    unsafe extern "C" fn trampoline<F, T>(mut args: VaList) -> i32
    where
        F: FnMut(Box<T>) -> i32,
    {
        let closure: &mut F = &mut *(args.get::<*const c_void>() as *mut F);
        let boxed_arg = Box::from_raw(args.get::<*const c_void>() as *mut T);
        (*closure)(boxed_arg)
    }
    (callback as *mut F as *mut c_void, Some(trampoline::<F, T>))
}

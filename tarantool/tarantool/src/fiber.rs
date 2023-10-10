//! Сooperative multitasking module with optional async runtime.
//!
//! With the fiber module, you can:
//! - create, run and manage [fibers](struct.Fiber.html),
//! - use a synchronization mechanism for fibers, similar to “condition variables” and similar to operating-system
//! functions such as `pthread_cond_wait()` plus `pthread_cond_signal()`,
//! - spawn a fiber based [async runtime](async).
//!
//! See also:
//! - [Threads, fibers and yields](https://www.tarantool.io/en/doc/latest/book/box/atomic/#threads-fibers-and-yields)
//! - [Lua reference: Module fiber](https://www.tarantool.io/en/doc/latest/reference/reference_lua/fiber/)
//! - [C API reference: Module fiber](https://www.tarantool.io/en/doc/latest/dev_guide/reference_capi/fiber/)
use std::cell::UnsafeCell;
use std::ffi::CString;
use std::future::Future;
use std::marker::PhantomData;
use std::os::raw::c_void;
use std::ptr::NonNull;
use std::time::Duration;

use crate::time::Instant;
use crate::tlua::{self as tlua, AsLua};

use ::va_list::VaList;
use tlua::unwrap_or;

use crate::error::{TarantoolError, TarantoolErrorCode};
use crate::ffi::has_fiber_id;
use crate::ffi::{lua, tarantool as ffi};
use crate::Result;
use crate::{c_ptr, set_error};

pub mod r#async;
pub mod channel;

pub use channel::{
    Channel, RecvError, RecvTimeout, SendError, SendTimeout, TryRecvError, TrySendError,
};

pub mod mutex;
use crate::ffi::tarantool::fiber_sleep;
pub use mutex::Mutex;
pub use r#async::block_on;

mod csw;
pub use csw::check_yield;
pub use csw::YieldResult;

/// Type alias for a fiber id.
pub type FiberId = u64;

/// *OBSOLETE*: This struct is being deprecated in favour of [`Fyber`], due to
/// them being more efficient and idiomatic.
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
/// ```no_run
/// use tarantool::fiber::Fiber;
///
/// let mut f = |_| {
///     println!("I'm a fiber");
///     0
/// };
/// let mut fiber = Fiber::new("test_fiber", &mut f);
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

impl<'a, T> ::std::fmt::Debug for Fiber<'a, T> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        f.debug_struct("Fiber").finish_non_exhaustive()
    }
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
        // The pointer into this variable must be valid until `fiber_new` returns.
        let name_cstr = CString::new(name).expect("fiber name should not contain nul bytes");
        Self {
            inner: unsafe { ffi::fiber_new(name_cstr.as_ptr(), trampoline) },
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
        // The pointer into this variable must be valid until `fiber_new_ex` returns.
        let name_cstr = CString::new(name).expect("fiber name should not contain nul bytes");
        Self {
            inner: unsafe { ffi::fiber_new_ex(name_cstr.as_ptr(), attr.inner, trampoline) },
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

    /// Returns the fiber id.
    ///
    /// # Panicking
    /// This will panic if the current tarantool executable doesn't support the
    /// required api (i.e. [`has_fiber_id`] returns `false`).
    /// Consider using [`Self::id_checked`] if you want to handle this error.
    #[inline(always)]
    #[track_caller]
    pub fn id(&self) -> FiberId {
        self.id_checked().expect("fiber_id api is not supported")
    }

    /// Returns the fiber id or `None` if the current tarantool
    /// executable doesn't support the required api
    /// (i.e. [`has_fiber_id`] returns `false`).
    pub fn id_checked(&self) -> Option<FiberId> {
        // SAFETY: safe as long as we only call this from the tx thread.
        if unsafe { !has_fiber_id() } {
            // There's no way to get fiber id from a fiber pointer in
            // the current version of tarantool.
            return None;
        }
        // SAFETY: safe as long as the fiber pointer is valid.
        let res = unsafe { ffi::fiber_id(self.inner) };
        Some(res)
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
/// * `func`:       specifies the fiber function
///
/// The [`start`](#method.start) and [`defer`](#method.defer) methods will
/// take ownership of the builder and create a [`Result`] to the fiber handle
/// with the given configuration.
///
/// The [`fiber::start`](start), [`fiber::defer`](defer) free functions
/// use a `Builder` with default configuration and unwraps its return value.
pub struct Builder<F> {
    name: Option<String>,
    attr: Option<FiberAttr>,
    f: F,
}

impl<T> ::std::fmt::Debug for Builder<T> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        f.debug_struct("Builder").finish_non_exhaustive()
    }
}

impl Builder<NoFunc> {
    /// Generates the base configuration for spawning a fiber, from which
    /// configuration methods can be chained.
    #[inline(always)]
    pub fn new() -> Self {
        Builder {
            name: None,
            attr: None,
            f: NoFunc,
        }
    }

    /// Sets the callee function for the new fiber.
    #[inline]
    pub fn func<'f, F, T>(self, f: F) -> Builder<F>
    where
        F: FnOnce() -> T,
        F: 'f,
    {
        Builder {
            name: self.name,
            attr: self.attr,
            f,
        }
    }

    /// Sets the callee async function for the new fiber.
    #[inline(always)]
    pub fn func_async<'f, F, T>(self, f: F) -> Builder<impl FnOnce() -> T + 'f>
    where
        F: Future<Output = T> + 'f,
        T: 'f,
    {
        self.func(|| block_on(f))
    }

    /// Sets the callee procedure for the new fiber.
    #[deprecated = "Use `Builder::func` instead"]
    #[inline(always)]
    pub fn proc<'f, F>(self, f: F) -> Builder<F>
    where
        F: FnOnce(),
        F: 'f,
    {
        self.func(f)
    }

    /// Sets the callee async procedure for the new fiber.
    #[deprecated = "Use `Builder::func_async` instead"]
    #[inline(always)]
    pub fn proc_async<'f, F>(self, f: F) -> Builder<impl FnOnce() + 'f>
    where
        F: Future<Output = ()> + 'f,
    {
        self.func_async(f)
    }
}

impl Default for Builder<NoFunc> {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl<F> Builder<F> {
    /// Names the fiber-to-be.
    ///
    /// The name must not contain null bytes (`\0`).
    #[inline(always)]
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
    #[inline(always)]
    pub fn stack_size(mut self, stack_size: usize) -> Result<Self> {
        let mut attr = FiberAttr::new();
        attr.set_stack_size(stack_size)?;
        self.attr = Some(attr);
        Ok(self)
    }
}

impl<'f, F, T> Builder<F>
where
    F: FnOnce() -> T + 'f,
    T: 'f,
{
    /// Spawns a new fiber by taking ownership of the `Builder`, and returns a
    /// [`Result`] to its [`JoinHandle`].
    ///
    /// The current fiber performs a **yield** and the execution is transfered
    /// to the new fiber immediately.
    ///
    /// See the [`start`] free function for more details.
    #[inline(always)]
    pub fn start(self) -> Result<JoinHandle<'f, T>> {
        let Self { name, attr, f } = self;
        let name = name.unwrap_or_else(|| "<rust>".into());
        Fyber::spawn_and_yield(name, f, attr.as_ref())
    }

    /// Spawns a new deferred fiber by taking ownership of the `Builder`, and
    /// returns a [`Result`] to its [`JoinHandle`].
    ///
    /// **NOTE:** On older versions of tarantool this will create a lua fiber
    /// which is less efficient. You can use [`ffi::has_fiber_set_ctx`]
    /// to check if your version of tarantool has api needed for this function
    /// to work efficiently.
    ///
    /// See the [`defer`] free function for more details.
    ///
    /// [`ffi::has_fiber_set_ctx`]: crate::ffi::has_fiber_set_ctx
    #[inline(always)]
    pub fn defer(self) -> Result<JoinHandle<'f, T>> {
        let Self { name, attr, f } = self;
        let name = name.unwrap_or_else(|| "<rust>".into());
        // SAFETY this is safe as long as we only call this from the tx thread.
        if unsafe { crate::ffi::has_fiber_set_ctx() } {
            Fyber::spawn_deferred(name, f, attr.as_ref())
        } else {
            Fyber::spawn_lua(name, f, attr.as_ref())
        }
    }

    /// Spawns a new deferred fiber by taking ownership of the `Builder`, and
    /// returns a [`Result`] to its [`JoinHandle`].
    ///
    /// # Panicking
    /// This may panic on older version of tarantool. You can use
    /// [`ffi::has_fiber_set_ctx`] to check if your version of
    /// tarantool has the needed api.
    ///
    /// Consider using [`Self::defer`] instead.
    ///
    /// [`ffi::has_fiber_set_ctx`]: crate::ffi::has_fiber_set_ctx
    #[inline(always)]
    pub fn defer_ffi(self) -> Result<JoinHandle<'f, T>> {
        let Self { name, attr, f } = self;
        let name = name.unwrap_or_else(|| "<rust>".into());
        Fyber::spawn_deferred(name, f, attr.as_ref())
    }

    /// Spawns a new deferred lua fiber by taking ownership of the `Builder`,
    /// and returns a [`Result`] to its [`JoinHandle`].
    ///
    /// This is legacy api and you probably don't want to use it. This mainly
    /// exists for testing.
    ///
    /// Consider using [`Self::defer`] instead.
    #[inline(always)]
    pub fn defer_lua(self) -> Result<JoinHandle<'f, T>> {
        let Self { name, attr, f } = self;
        let name = name.unwrap_or_else(|| "<rust>".into());
        Fyber::spawn_lua(name, f, attr.as_ref())
    }
}

////////////////////////////////////////////////////////////////////////////////
// Fyber
////////////////////////////////////////////////////////////////////////////////

/// A helper struct which is used to store information about a fiber being
/// created. It's only utility is the generic parameter which are associated
/// with it.
///
/// **TODO**: add support for cancellable fibers.
/// **TODO**: add support for non-joinable fibers.
pub struct Fyber<F, T> {
    _marker: PhantomData<(F, T)>,
}

impl<F, T> ::std::fmt::Debug for Fyber<F, T> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        f.debug_struct("Fyber").finish_non_exhaustive()
    }
}

impl<'f, F, T> Fyber<F, T>
where
    F: FnOnce() -> T + 'f,
    T: 'f,
{
    /// Creates a joinable fiber and immediately **yields** execution to it.
    pub fn spawn_and_yield(
        name: String,
        f: F,
        attr: Option<&FiberAttr>,
    ) -> Result<JoinHandle<'f, T>> {
        let cname = CString::new(name).expect("fiber name may not contain interior null bytes");

        let inner_raw = unsafe {
            if let Some(attr) = attr {
                ffi::fiber_new_ex(
                    cname.as_ptr(),
                    attr.inner,
                    Some(Self::trampoline_for_immediate),
                )
            } else {
                ffi::fiber_new(cname.as_ptr(), Some(Self::trampoline_for_immediate))
            }
        };

        let inner = unwrap_or!(NonNull::new(inner_raw),
            return Err(TarantoolError::last().into());
        );

        unsafe {
            ffi::fiber_set_joinable(inner.as_ptr(), true);

            let result_cell = needs_returning::<T>().then(FiberResultCell::default);
            let result_ptr = result_cell
                .as_ref()
                .map_or(std::ptr::null_mut(), |cell| cell.get());

            let boxed_f = Box::new(f);
            ffi::fiber_start(inner.as_ptr(), Box::into_raw(boxed_f), result_ptr);
            let jh = JoinHandle::ffi(inner, result_cell);
            Ok(jh)
        }
    }

    unsafe extern "C" fn trampoline_for_immediate(mut args: VaList) -> i32 {
        // Extract arugments from the va_list.
        let f = Box::from_raw(args.get::<*const ()>() as *mut F);
        let result_ptr = args.get::<*const ()>() as *mut Option<T>;

        // Call `f` and drop the closure.
        let t = f();

        // Write results into the join handle if needed.
        if needs_returning::<T>() {
            assert!(!result_ptr.is_null());
            std::ptr::write(result_ptr, Some(t));
        } else if cfg!(debug_assertions) {
            assert!(result_ptr.is_null());
        }
        0
    }

    /// Creates a joinable fiber and schedules it for execution at some
    /// point later. Does **NOT** yield.
    ///
    /// # Panicking
    /// May panic if the current tarantool executable doesn't support the
    /// `fiber_set_ctx` api.
    pub fn spawn_deferred(
        name: String,
        f: F,
        attr: Option<&FiberAttr>,
    ) -> Result<JoinHandle<'f, T>> {
        let cname = CString::new(name).expect("fiber name may not contain interior null bytes");

        let inner_raw = unsafe {
            if let Some(attr) = attr {
                ffi::fiber_new_ex(
                    cname.as_ptr(),
                    attr.inner,
                    Some(Self::trampoline_for_deferred_ffi),
                )
            } else {
                ffi::fiber_new(cname.as_ptr(), Some(Self::trampoline_for_deferred_ffi))
            }
        };

        let inner = unwrap_or!(NonNull::new(inner_raw),
            return Err(TarantoolError::last().into());
        );

        unsafe {
            ffi::fiber_set_joinable(inner.as_ptr(), true);

            let result_cell = needs_returning::<T>().then(FiberResultCell::default);
            let result_ptr = result_cell
                .as_ref()
                .map_or(std::ptr::null_mut(), |cell| cell.get());
            let ctx = Box::new(DeferredFiberContext { f, result_ptr });

            ffi::fiber_set_ctx(inner.as_ptr(), Box::into_raw(ctx) as _);
            ffi::fiber_wakeup(inner.as_ptr());
            let jh = JoinHandle::ffi(inner, result_cell);
            Ok(jh)
        }
    }

    unsafe extern "C" fn trampoline_for_deferred_ffi(_: VaList) -> i32 {
        // Extract arugments from fiber context.
        let fiber_self = ffi::fiber_self();
        let ctx = ffi::fiber_get_ctx(fiber_self);
        let ctx = Box::from_raw(ctx.cast::<DeferredFiberContext<F, T>>());

        // Overwrite the context so that the callback doesn't mess it up somehow.
        ffi::fiber_set_ctx(fiber_self, std::ptr::null_mut());

        // Call `f` and drop the closure.
        let t = (ctx.f)();

        // Write results into the join handle if needed.
        if needs_returning::<T>() {
            assert!(!ctx.result_ptr.is_null());
            std::ptr::write(ctx.result_ptr, Some(t));
        } else if cfg!(debug_assertions) {
            assert!(ctx.result_ptr.is_null());
        }
        0
    }
}

struct DeferredFiberContext<F, T> {
    f: F,
    result_ptr: *mut Option<T>,
}

impl<'f, F, T> Fyber<F, T>
where
    F: FnOnce() -> T + 'f,
    T: 'f,
{
    /// Creates a joinable **LUA** fiber and schedules it for execution at some
    /// point later. Does **NOT** yield.
    pub fn spawn_lua(name: String, f: F, _attr: Option<&FiberAttr>) -> Result<JoinHandle<'f, T>> {
        let fiber_ref = unsafe {
            let l = ffi::luaT_state();
            lua::lua_getglobal(l, c_ptr!("require"));
            lua::lua_pushstring(l, c_ptr!("fiber"));
            impl_details::guarded_pcall(l, 1, 1)?; // stack[top] = require('fiber')

            lua::lua_getfield(l, -1, c_ptr!("new"));
            impl_details::push_userdata(l, f);
            lua::lua_pushcclosure(l, Self::trampoline_for_lua, 1);
            impl_details::guarded_pcall(l, 1, 1).map_err(|e| {
                // Pop the fiber module from the stack
                lua::lua_pop(l, 1);
                e
            })?; // stack[top] = fiber.new(c_closure)

            lua::lua_getfield(l, -1, c_ptr!("set_joinable"));
            lua::lua_pushvalue(l, -2); // duplicate the fiber object
            lua::lua_pushboolean(l, true as _);
            impl_details::guarded_pcall(l, 2, 0) // f:set_joinable(true)
                .map_err(|e| panic!("{}", e))
                .unwrap();

            lua::lua_getfield(l, -1, c_ptr!("name"));
            lua::lua_pushvalue(l, -2); // duplicate the fiber object
            lua::lua_pushlstring(l, name.as_ptr() as _, name.len());
            impl_details::guarded_pcall(l, 2, 0) // f:name(name)
                .map_err(|e| panic!("{}", e))
                .unwrap();

            let fiber_ref = lua::luaL_ref(l, lua::LUA_REGISTRYINDEX);
            // pop the fiber module from the stack
            lua::lua_pop(l, 1);

            fiber_ref
        };

        Ok(JoinHandle::lua(fiber_ref))
    }

    unsafe extern "C" fn trampoline_for_lua(l: *mut lua::lua_State) -> i32 {
        let ud_ptr = lua::lua_touserdata(l, lua::lua_upvalueindex(1));

        let f = (ud_ptr as *mut Option<F>)
            .as_mut()
            .unwrap_or_else(||
                // lua_touserdata returned NULL
                tlua::error!(l, "failed to extract upvalue"))
            // put None back into userdata
            .take()
            .unwrap_or_else(||
                // userdata originally contained None
                tlua::error!(l, "rust FnOnce callback was called more than once"));

        // call f and drop it afterwards
        let res = f();

        // return results to lua
        if needs_returning::<T>() {
            impl_details::push_userdata(l, res);
            1
        } else {
            0
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// impl_details
////////////////////////////////////////////////////////////////////////////////

mod impl_details {
    use super::*;
    use crate::tlua::{AsLua, LuaError, PushGuard, StaticLua};

    pub(super) unsafe fn lua_error_from_top(l: *mut lua::lua_State) -> LuaError {
        let mut len = std::mem::MaybeUninit::uninit();
        let data = lua::lua_tolstring(l, -1, len.as_mut_ptr());
        assert!(!data.is_null());
        let msg_bytes = std::slice::from_raw_parts(data as *mut u8, len.assume_init());
        let msg = String::from_utf8_lossy(msg_bytes);
        tlua::LuaError::ExecutionError(msg)
    }

    /// In case of success, the stack contains the results.
    ///
    /// In case of error, pops the error from the stack and wraps it into
    /// tarantool::error::Error.
    pub(super) unsafe fn guarded_pcall(
        lptr: *mut lua::lua_State,
        nargs: i32,
        nresults: i32,
    ) -> Result<()> {
        match lua::lua_pcall(lptr, nargs, nresults, 0) {
            lua::LUA_OK => Ok(()),
            lua::LUA_ERRRUN => {
                let err = lua_error_from_top(lptr).into();
                lua::lua_pop(lptr, 1);
                Err(err)
            }
            code => panic!("lua_pcall: Unrecoverable failure code: {}", code),
        }
    }

    pub(super) unsafe fn lua_fiber_join(f_ref: i32) -> Result<PushGuard<StaticLua>> {
        let l = crate::global_lua();
        let lptr = l.as_lua();
        let top_svp = lua::lua_gettop(lptr);
        lua::lua_rawgeti(lptr, lua::LUA_REGISTRYINDEX, f_ref);
        lua::lua_getfield(lptr, -1, c_ptr!("join"));
        lua::lua_pushvalue(lptr, -2);

        // fiber instance can now be garbage collected by lua
        lua::luaL_unref(lptr, lua::LUA_REGISTRYINDEX, f_ref);

        guarded_pcall(lptr, 1, 2).map_err(|e| {
            // Pop the fiber value from the stack
            lua::lua_pop(lptr, 1);
            e
        })?;

        // 3 values on the stack that need to be dropped:
        // 1) fiber; 2) flag; 3) return value / error
        let top = lua::lua_gettop(lptr);
        assert_eq!(top - top_svp, 3);
        let guard = PushGuard::new(l, 3);

        // check fiber return code
        assert_ne!(lua::lua_toboolean(lptr, -2), 0);

        Ok(guard)
    }

    // pub(super) unsafe fn lua_fiber_set_joinable_and_unref(f_ref: i32) -> Result<()> {
    //     let mut l = Lua::from_existing_state(ffi::luaT_state(), false);
    //     let lptr = l.as_mut_lua().state_ptr();
    //     let top_before = lua::lua_gettop(lptr);

    //     lua::lua_rawgeti(lptr, lua::LUA_REGISTRYINDEX, f_ref);
    //     lua::lua_getfield(lptr, -1, c_ptr!("set_joinable"));
    //     lua::lua_pushvalue(lptr, -2);
    //     lua::lua_pushboolean(lptr, false as _);

    //     // fiber instance can now be garbage collected by lua
    //     lua::luaL_unref(lptr, lua::LUA_REGISTRYINDEX, f_ref);

    //     let res = guarded_pcall(lptr, 2, 0);
    //     lua::lua_settop(lptr, top_before);
    //     res
    // }

    /// # Safety
    /// **WARNING** this function is super unsafe in case `T` is not 'static.
    /// It's used to implement non-static fibers which is safe because the
    /// lifetime of `T` is captured in the join handle and so the compiler will
    /// make sure the fiber is joined before the referenced data is dropped.
    /// Keep this in mind if you want to use this function
    pub(super) unsafe fn push_userdata<T>(lua: tlua::LuaState, value: T) {
        use tlua::ffi;
        type UDBox<T> = Option<T>;
        let ud_ptr = ffi::lua_newuserdata(lua, std::mem::size_of::<UDBox<T>>());
        std::ptr::write(ud_ptr.cast::<UDBox<T>>(), Some(value));

        if std::mem::needs_drop::<T>() {
            // Creating a metatable.
            ffi::lua_newtable(lua);

            // Index "__gc" in the metatable calls the object's destructor.
            ffi::lua_pushstring(lua, c_ptr!("__gc"));
            ffi::lua_pushcfunction(lua, wrap_gc::<T>);
            ffi::lua_settable(lua, -3);

            ffi::lua_setmetatable(lua, -2);
        }

        /// A callback for the "__gc" event. It checks if the value was moved out
        /// and if not it drops the value.
        unsafe extern "C" fn wrap_gc<T>(lua: *mut ffi::lua_State) -> i32 {
            let ud_ptr = ffi::lua_touserdata(lua, 1);
            let ud = ud_ptr
                .cast::<UDBox<T>>()
                .as_mut()
                .expect("__gc called with userdata pointing to NULL");
            drop(ud.take());

            0
        }
    }
}

/// This is a *typestate* helper type representing the state of a [`Builder`]
/// that hasn't been assigned a fiber function yet.
pub struct NoFunc;

////////////////////////////////////////////////////////////////////////////////
// JoinHandle
////////////////////////////////////////////////////////////////////////////////

/// An owned permission to join a fiber (block on its termination).
///
/// NOTE: if `JoinHandle` is dropped before [`JoinHandle::join`] is called on it
/// a panic will happen. Moreover some of the memory needed for passing the
/// result from the fiber to the caller will be leaked in case the panic is
/// caught. Note also that panics within tarantool are in general not recoverable.
#[derive(PartialEq, Eq, Hash)]
pub struct JoinHandle<'f, T> {
    inner: Option<JoinHandleImpl<T>>,
    marker: PhantomData<&'f ()>,
}

impl<'f, T> std::fmt::Debug for JoinHandle<'f, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("JoinHandle").finish_non_exhaustive()
    }
}

#[deprecated = "Use `fiber::JoinHandle<'f, ()>` instead"]
pub type UnitJoinHandle<'f> = JoinHandle<'f, ()>;

#[deprecated = "Use `fiber::JoinHandle<'f, T>` instead"]
pub type LuaJoinHandle<'f, T> = JoinHandle<'f, T>;

#[deprecated = "Use `fiber::JoinHandle<'f, ()>` instead"]
pub type LuaUnitJoinHandle<'f> = JoinHandle<'f, ()>;

#[derive(Debug)]
enum JoinHandleImpl<T> {
    Ffi {
        fiber: NonNull<ffi::Fiber>,
        result_cell: Option<FiberResultCell<T>>,
    },
    Lua {
        fiber_ref: i32,
    },
}

type FiberResultCell<T> = Box<UnsafeCell<Option<T>>>;

impl<'f, T> JoinHandle<'f, T> {
    #[inline(always)]
    fn ffi(fiber: NonNull<ffi::Fiber>, result_cell: Option<FiberResultCell<T>>) -> Self {
        Self {
            inner: Some(JoinHandleImpl::Ffi { fiber, result_cell }),
            marker: PhantomData,
        }
    }

    #[inline(always)]
    fn lua(fiber_ref: i32) -> Self {
        Self {
            inner: Some(JoinHandleImpl::Lua { fiber_ref }),
            marker: PhantomData,
        }
    }

    /// Block until the fiber's termination and return it's result value.
    pub fn join(mut self) -> T {
        let inner = self
            .inner
            .take()
            .expect("after construction join is called at most once");
        match inner {
            JoinHandleImpl::Ffi {
                fiber,
                mut result_cell,
            } => {
                // TODO: add error handling
                let _code = unsafe { ffi::fiber_join(fiber.as_ptr()) };

                if needs_returning::<T>() {
                    let mut result_cell = result_cell
                        .take()
                        .expect("should not be None for non unit types");
                    result_cell
                        .get_mut()
                        .take()
                        .expect("should have been set by the fiber function")
                } else {
                    if cfg!(debug_assertions) {
                        assert!(result_cell.is_none());
                    }
                    // SAFETY: this is safe because () is a zero sized type.
                    #[allow(clippy::uninit_assumed_init)]
                    unsafe {
                        std::mem::MaybeUninit::uninit().assume_init()
                    }
                }
            }
            JoinHandleImpl::Lua { fiber_ref } => unsafe {
                let guard = impl_details::lua_fiber_join(fiber_ref)
                    .map_err(|e| panic!("Unrecoverable lua failure: {}", e))
                    .unwrap();

                if needs_returning::<T>() {
                    let ud_ptr = lua::lua_touserdata(guard.as_lua(), -1);
                    let res = (ud_ptr as *mut Option<T>)
                        .as_mut()
                        .expect("fiber:join must return correct userdata")
                        .take()
                        .expect("data can only be taken once from the UDBox");
                    res
                } else {
                    if cfg!(debug_assertions) {
                        assert!(lua::lua_isnil(guard.as_lua(), -1));
                    }
                    // SAFETY: this is safe because () is a zero sized type.
                    #[allow(clippy::uninit_assumed_init)]
                    std::mem::MaybeUninit::uninit().assume_init()
                }
            },
        }
    }

    /// Returns the underlying fiber id.
    ///
    /// # Panicking
    /// This will panic if the current tarantool executable doesn't support the
    /// required api (i.e. [`has_fiber_id`] returns `false`).
    /// Consider using [`Self::id_checked`] if you want to handle this error.
    #[inline(always)]
    #[track_caller]
    pub fn id(&self) -> FiberId {
        self.id_checked().expect("fiber_id api is not supported")
    }

    /// Returns the underlying fiber id or `None` if the current tarantool
    /// executable doesn't support the required api
    /// (i.e. [`has_fiber_id`] returns `false`).
    pub fn id_checked(&self) -> Option<FiberId> {
        match self.inner {
            None => {
                unreachable!("it has either been moved into JoinHandle::join, or been dropped")
            }
            Some(JoinHandleImpl::Ffi { fiber, .. }) => {
                // SAFETY: safe as long as we only call this from the tx thread.
                if unsafe { !has_fiber_id() } {
                    // There's no way to get fiber id from a fiber pointer in
                    // the current version of tarantool.
                    return None;
                }
                // SAFETY: safe as long as the fiber pointer is valid.
                let res = unsafe { ffi::fiber_id(fiber.as_ptr()) };
                return Some(res);
            }
            Some(JoinHandleImpl::Lua { fiber_ref, .. }) => {
                let lua = crate::global_lua();
                let id: FiberId = lua
                    .eval_with("return debug.getregistry()[...]:id()", fiber_ref)
                    .expect("lua error");
                return Some(id);
            }
        }
    }
}

impl<'f, T> Drop for JoinHandle<'f, T> {
    fn drop(&mut self) {
        if let Some(mut inner) = self.inner.take() {
            if let JoinHandleImpl::Ffi { result_cell, .. } = &mut inner {
                // Panics in general aren't recoverable when running inside
                // tarantool. But in our tests we do capture them and we must
                // make sure, that other tests aren't corrupted after the fact.
                // So in case of a failing test the spawned fiber will still at
                // some point finish executing and attempt to write it's result
                // value into the result_cell. For this reason we must make
                // sure it's memory is not freed, and in this case we don't care
                // if the memory leaks.
                std::mem::forget(result_cell.take());
            }
            panic!("JoinHandle dropped before being joined")
        }
    }
}

#[rustfmt::skip]
impl<T> ::std::cmp::PartialEq for JoinHandleImpl<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Ffi { fiber: self_fiber, .. }, Self::Ffi { fiber: other_fiber, .. },) => {
                self_fiber == other_fiber
            }
            (Self::Lua { fiber_ref: self_ref, .. }, Self::Lua { fiber_ref: other_ref, .. },) => {
                self_ref == other_ref
            }
            (_, _) => false,
        }
    }
}

impl<T> ::std::cmp::Eq for JoinHandleImpl<T> {}

impl<T> ::std::hash::Hash for JoinHandleImpl<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: ::std::hash::Hasher,
    {
        match self {
            Self::Ffi { fiber, .. } => fiber.hash(state),
            Self::Lua { fiber_ref, .. } => fiber_ref.hash(state),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Free functions
////////////////////////////////////////////////////////////////////////////////

/// Creates a new fiber and **yields** execution to it immediately, returning a
/// [`JoinHandle`] for the new fiber.
///
/// The join handle will implicitly *detach* the child fiber upon being
/// dropped. In this case, the child fiber may outlive the parent. Additionally,
/// the join handle provides a [`JoinHandle::join`] method that can be used to
/// join the child fiber and acquire the result value of the fiber function.
///
/// This will create a fiber using default parameters of [`Builder`], if you
/// want to specify the stack size or the name of the thread, use builder's API
/// instead.
#[inline(always)]
pub fn start<'f, F, T>(f: F) -> JoinHandle<'f, T>
where
    F: FnOnce() -> T,
    F: 'f,
    T: 'f,
{
    Builder::new().func(f).start().unwrap()
}

/// Async version of [`start`].
///
/// ```ignore
/// use tarantool::fiber;
///
/// let jh = fiber::start_async(async {
///     // do some async work in another fiber
///     do_work().await
/// });
/// jh.join().unwrap();
/// ```
#[inline(always)]
pub fn start_async<'f, F, T>(f: F) -> JoinHandle<'f, T>
where
    F: Future<Output = T> + 'f,
    T: 'f,
{
    start(|| block_on(f))
}

/// Creates a new fiber and **yields** execution to it immediately,
/// returning a [`JoinHandle<()>`] for the new fiber.
///
/// For more details see: [`start`]
#[deprecated = "Use `fiber::start` instead"]
#[inline(always)]
pub fn start_proc<'f, F>(f: F) -> JoinHandle<'f, ()>
where
    F: FnOnce(),
    F: 'f,
{
    start(f)
}

/// Creates a new fiber and schedules it for execution, returning a
/// [`JoinHandle`] for it.
///
/// **NOTE:** On older versions of tarantool this will create a lua fiber
/// which is less efficient. You can use [`ffi::has_fiber_set_ctx`]
/// to check if your version of tarantool has api needed for this function
/// to work efficiently.
///
/// The new fiber can be joined by calling [`JoinHandle::join`] method on
/// it's join handle.
///
/// [`ffi::has_fiber_set_ctx`]: crate::ffi::has_fiber_set_ctx
#[inline(always)]
pub fn defer<'f, F, T>(f: F) -> JoinHandle<'f, T>
where
    F: FnOnce() -> T,
    F: 'f,
    T: 'f,
{
    Builder::new().func(f).defer().unwrap()
}

/// Async version of [`defer`].
///
/// ```ignore
/// use tarantool::fiber;
///
/// let jh = fiber::defer_async(async {
///     // do some async work in another fiber
///     do_work().await
/// });
/// jh.join().unwrap();
/// ```
#[inline(always)]
pub fn defer_async<'f, F, T>(f: F) -> JoinHandle<'f, T>
where
    F: Future<Output = T> + 'f,
    T: 'f,
{
    defer(|| block_on(f))
}

/// Creates a new fiber and schedules it for execution, returning a
/// [`JoinHandle`]`<()>` for it.
///
/// **NOTE:** In the current implementation the fiber is constructed using the
/// lua api, so it's efficiency is far from perfect.
///
/// The new fiber can be joined by calling [`JoinHandle::join`] method on
/// it's join handle.
///
/// This is an optimized version [`defer`]`<F, ()>`.
#[deprecated = "Use `fiber::defer` instead"]
#[inline(always)]
pub fn defer_proc<'f, F>(f: F) -> JoinHandle<'f, ()>
where
    F: FnOnce(),
    F: 'f,
{
    defer(f)
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
#[inline(always)]
pub fn sleep(time: Duration) {
    unsafe { ffi::fiber_sleep(time.as_secs_f64()) }
}

/// Get [`Instant`] corresponding to event loop iteration begin time.
/// Uses monotonic clock.
#[inline(always)]
pub fn clock() -> Instant {
    let secs = unsafe { ffi::fiber_clock() };
    Instant(Duration::from_secs_f64(secs))
}

/// Yield control to the scheduler.
///
/// **NOTE**: currently the only way to wakeup a yielded fiber is to call
/// [`Fiber::wakeup`], which isn't possible if the fiber was created via one of
/// [`fiber::start`], [`fiber::defer`], etc.
///
/// Return control to another fiber and wait until it'll be explicitly awoken by
/// another fiber.
///
/// Consider using [`fiber::sleep`]`(Duration::ZERO)` or [`fiber::yield`] instead, that way the
/// fiber will be automatically awoken and will resume execution shortly.
///
/// [`fiber::sleep`]: crate::fiber::sleep
/// [`fiber::start`]: crate::fiber::start
/// [`fiber::defer`]: crate::fiber::defer
/// [`fiber::yield`]: crate::fiber::yield
#[inline(always)]
pub fn fiber_yield() {
    unsafe { ffi::fiber_yield() }
}

/// Returns control to the scheduler.
/// Works likewise [`fiber::sleep`]`(Duration::ZERO)` but return error if fiber was canceled by another routine.
///
/// [`fiber::sleep`]: crate::fiber::sleep
pub fn r#yield() -> Result<()> {
    unsafe { fiber_sleep(0f64) };
    if is_cancelled() {
        set_error!(TarantoolErrorCode::ProcLua, "fiber is cancelled");
        return Err(TarantoolError::last().into());
    }
    Ok(())
}

/// Reschedule fiber to end of event loop cycle.
#[inline(always)]
pub fn reschedule() {
    unsafe { ffi::fiber_reschedule() }
}

/// Returns id of current fiber.
///
/// NOTE: if [`has_fiber_id`] returns `false` this function uses an
/// inefficient implementation based on the lua api.
#[inline]
pub fn id() -> FiberId {
    // SAFETY this is safe as long as we only call this from the tx thread.
    if unsafe { has_fiber_id() } {
        // SAFETY always safe
        return unsafe { ffi::fiber_id(std::ptr::null_mut()) };
    } else {
        crate::global_lua()
            .eval("return require'fiber'.id()")
            .expect("lua error")
    }
}

/// Returns number of context switches of the current fiber.
///
/// NOTE: if [`has_fiber_id`] returns `false` this function uses an
/// inefficient implementation based on the lua api.
#[inline]
pub fn csw() -> u64 {
    // SAFETY this is safe as long as we only call this from the tx thread.
    if unsafe { has_fiber_id() } {
        // SAFETY always safe
        unsafe { ffi::fiber_csw(std::ptr::null_mut()) }
    } else {
        csw::csw_lua(None).expect("fiber.self() should always work")
    }
}

/// Returns number of context switches of the fiber with given id or
/// `None` if fiber with given id wasn't found.
///
/// NOTE: if [`has_fiber_id`] returns `false` this function uses an
/// inefficient implementation based on the lua api.
#[inline]
pub fn csw_of(id: FiberId) -> Option<u64> {
    // SAFETY this is safe as long as we only call this from the tx thread.
    if unsafe { has_fiber_id() } {
        // SAFETY always safe
        unsafe {
            let f = ffi::fiber_find(id);
            if f.is_null() {
                return None;
            }
            let res = ffi::fiber_csw(f);
            return Some(res);
        }
    } else {
        csw::csw_lua(Some(id))
    }
}

/// Returns the name of the current fiber.
///
/// NOTE: if [`has_fiber_id`] returns `false` this function uses an
/// inefficient implementation based on the lua api.
///
/// NOTE: it uses String::from_utf8_lossy to convert from the c-string, so the
/// data may differ from the actual.
#[inline]
pub fn name() -> String {
    // SAFETY this is safe as long as we only call this from the tx thread, and
    // don't hold the reference after yielding.
    let name = unsafe { name_raw(None) }.expect("fiber_self should always work");
    String::from_utf8_lossy(name).into()
}

/// Returns the name of the fiber with the given id.
///
/// NOTE: if [`has_fiber_id`] returns `false` this function uses an
/// inefficient implementation based on the lua api.
///
/// NOTE: it uses String::from_utf8_lossy to convert from the c-string, so the
/// data may differ from the actual.
#[inline]
pub fn name_of(id: FiberId) -> Option<String> {
    // SAFETY this is safe as long as we only call this from the tx thread, and
    // don't hold the reference after yielding.
    let name = unsafe { name_raw(Some(id)) }?;
    let res = String::from_utf8_lossy(name).into();
    Some(res)
}

/// Returns the name of the fiber with the given id, or `None` if fiber wasn't
/// found. The name is returned as a slice of bytes, because it is allowed to
/// contain nul bytes.
///
/// # Safety
/// This functions returns a reference to the data with a limited lifetime
/// (even though it says `'static` in the signature). The lifetime
/// of the data depends on the implementation, and should be copied ASAP.
/// Holding this reference across yields is definitely NOT safe.
///
/// NOTE: if [`has_fiber_id`] returns `false` this function uses an
/// inefficient implementation based on the lua api.
pub unsafe fn name_raw(id: Option<FiberId>) -> Option<&'static [u8]> {
    if has_fiber_id() {
        let mut f = std::ptr::null_mut();
        if let Some(id) = id {
            f = ffi::fiber_find(id);
            if f.is_null() {
                return None;
            }
        }
        let p = ffi::fiber_name(f);
        let cstr = std::ffi::CStr::from_ptr(p as _);
        Some(cstr.to_bytes())
    } else {
        let lua = crate::global_lua();
        let s: Option<tlua::StringInLua<_>> = lua
            .eval_with(
                "local fiber = require'fiber'
                local f = fiber.find(... or fiber.id())
                return f and f:name()",
                id,
            )
            .expect("lua error");
        let s = s?;
        let res: &'static [u8] = std::mem::transmute(s.as_bytes());
        Some(res)
    }
}

/// Sets the name of the current fiber.
///
/// NOTE: if [`has_fiber_id`] returns `false` this function uses an
/// inefficient implementation based on the lua api.
#[inline]
pub fn set_name(name: &str) {
    // SAFETY: safe as long as we only call this from the tx thread.
    if unsafe { has_fiber_id() } {
        // SAFETY: always safe.
        unsafe { ffi::fiber_set_name_n(std::ptr::null_mut(), name.as_ptr(), name.len() as _) }
    } else {
        let lua = crate::global_lua();
        lua.exec_with("require'fiber'.name(...)", name)
            .expect("lua error");
    }
}

/// Sets the name of the fiber with the given id.
/// Returns `false` if the fiber wasn't found, `true` otherwise.
///
/// NOTE: if [`has_fiber_id`] returns `false` this function uses an
/// inefficient implementation based on the lua api.
#[inline]
pub fn set_name_of(id: FiberId, name: &str) -> bool {
    // SAFETY: safe as long as we only call this from the tx thread.
    if unsafe { has_fiber_id() } {
        // SAFETY: always safe.
        unsafe {
            let f = ffi::fiber_find(id);
            if f.is_null() {
                return false;
            }
            ffi::fiber_set_name_n(f, name.as_ptr(), name.len() as _);
            return true;
        }
    } else {
        let lua = crate::global_lua();
        let res: bool = lua
            .eval_with(
                "local fiber = require'fiber'
                local id, name = ...
                local f = fiber.find(id)
                if f == nil then
                    return false
                end
                f:name(name)
                return true",
                (id, name),
            )
            .expect("lua error");
        return res;
    }
}

////////////////////////////////////////////////////////////////////////////////
// FiberAttr
////////////////////////////////////////////////////////////////////////////////

/// Fiber attributes container
#[derive(Debug)]
pub struct FiberAttr {
    inner: *mut ffi::FiberAttr,
}

impl FiberAttr {
    /// Create a new fiber attribute container and initialize it with default parameters.
    /// Can be used for many fibers creation, corresponding fibers will not take ownership.
    ///
    /// This is safe to drop `FiberAttr` value when fibers created with this attribute still exist.
    #[inline(always)]
    pub fn new() -> Self {
        FiberAttr {
            inner: unsafe { ffi::fiber_attr_new() },
        }
    }

    /// Get stack size from the fiber attribute.
    ///
    /// Returns: stack size
    #[inline(always)]
    pub fn stack_size(&self) -> usize {
        unsafe { ffi::fiber_attr_getstacksize(self.inner) }
    }

    ///Set stack size for the fiber attribute.
    ///
    /// - `stack_size` - stack size for new fibers
    #[inline(always)]
    pub fn set_stack_size(&mut self, stack_size: usize) -> Result<()> {
        if unsafe { ffi::fiber_attr_setstacksize(self.inner, stack_size) } < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(())
        }
    }
}

impl Default for FiberAttr {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for FiberAttr {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { ffi::fiber_attr_delete(self.inner) }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Cond
////////////////////////////////////////////////////////////////////////////////

/// Conditional variable for cooperative multitasking (fibers).
///
/// A cond (short for "condition variable") is a synchronization primitive
/// that allow fibers to yield until some predicate is satisfied. Fiber
/// conditions have two basic operations - `wait()` and `signal()`. [cond.wait()](#method.wait)
/// suspends execution of fiber (i.e. yields) until [cond.signal()](#method.signal) is called.
///
/// Example:
///
/// ```no_run
/// use tarantool::fiber::Cond;
/// let cond = Cond::new();
/// cond.wait();
/// ```
///
/// The job will hang because [cond.wait()](#method.wait) – will go to sleep until the condition variable changes.
///
/// ```no_run
/// // Call from another fiber:
/// # let cond = tarantool::fiber::Cond::new();
/// cond.signal();
/// ```
///
/// The waiting stopped, and the [cond.wait()](#method.wait) function returned true.
///
/// This example depended on the use of a global conditional variable with the arbitrary name cond.
/// In real life, programmers would make sure to use different conditional variable names for different applications.
///
/// Unlike `pthread_cond`, [Cond]() doesn't require mutex/latch wrapping.
#[derive(Debug)]
pub struct Cond {
    inner: *mut ffi::FiberCond,
}

/// - call [Cond::new()](#method.new) to create a named condition variable, which will be called `cond` for examples in this section.
/// - call [cond.wait()](#method.wait) to make a fiber wait for a signal via a condition variable.
/// - call [cond.signal()](#method.signal) to send a signal to wake up a single fiber that has executed [cond.wait()](#method.wait).
/// - call [cond.broadcast()](#method.broadcast) to send a signal to all fibers that have executed [cond.wait()](#method.wait).
impl Cond {
    /// Instantiate a new fiber cond object.
    #[inline(always)]
    pub fn new() -> Self {
        Cond {
            inner: unsafe { ffi::fiber_cond_new() },
        }
    }

    /// Wake one fiber waiting for the cond.
    /// Does nothing if no one is waiting. Does not yield.
    #[inline(always)]
    pub fn signal(&self) {
        unsafe { ffi::fiber_cond_signal(self.inner) }
    }

    /// Wake up all fibers waiting for the cond.
    /// Does not yield.
    #[inline(always)]
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
    #[inline(always)]
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        unsafe { ffi::fiber_cond_wait_timeout(self.inner, timeout.as_secs_f64()) >= 0 }
    }

    /// Shortcut for [wait_timeout()](#method.wait_timeout).
    #[inline(always)]
    pub fn wait(&self) -> bool {
        unsafe { ffi::fiber_cond_wait(self.inner) >= 0 }
    }
}

impl Default for Cond {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Cond {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { ffi::fiber_cond_delete(self.inner) }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Latch
////////////////////////////////////////////////////////////////////////////////

/// A lock for cooperative multitasking environment
#[derive(Debug)]
pub struct Latch {
    inner: *mut ffi::Latch,
}

impl Latch {
    /// Allocate and initialize the new latch.
    #[inline(always)]
    pub fn new() -> Self {
        Latch {
            inner: unsafe { ffi::box_latch_new() },
        }
    }

    /// Lock a latch. Waits indefinitely until the current fiber can gain access to the latch.
    #[inline(always)]
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
    #[inline(always)]
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

impl Default for Latch {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Latch {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { ffi::box_latch_delete(self.inner) }
    }
}

/// An RAII implementation of a "scoped lock" of a latch. When this structure is dropped (falls out of scope),
/// the lock will be unlocked.
#[derive(Debug)]
pub struct LatchGuard {
    latch_inner: *mut ffi::Latch,
}

impl Drop for LatchGuard {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { ffi::box_latch_unlock(self.latch_inner) }
    }
}

////////////////////////////////////////////////////////////////////////////////
// misc
////////////////////////////////////////////////////////////////////////////////

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

/// Returns `true` if a fiber function with this return type needs to return the
/// value to the caller when joined.
///
/// This is used for optimizations. Basically if this function returns `false`
/// for the return type of a fiber then we save on some overhead.
const fn needs_returning<T>() -> bool {
    std::mem::size_of::<T>() != 0 || std::mem::needs_drop::<T>()
}

const _: () = {
    assert!(needs_returning::<i32>());
    assert!(needs_returning::<bool>());
    assert!(!needs_returning::<()>());

    struct UnitStruct;
    assert!(!needs_returning::<UnitStruct>());

    struct DroppableUnitStruct;
    impl Drop for DroppableUnitStruct {
        fn drop(&mut self) {}
    }
    assert!(needs_returning::<DroppableUnitStruct>());
};

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use crate::fiber;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[crate::test(tarantool = "crate")]
    fn builder_async_func() {
        let jh = Builder::new().func_async(async { 69 }).start().unwrap();
        let res = jh.join();
        assert_eq!(res, 69);
    }

    #[crate::test(tarantool = "crate")]
    #[allow(deprecated)]
    fn builder_async_proc() {
        let res = Rc::new(RefCell::new(0u32));
        let res_moved = res.clone();
        let jh = Builder::new()
            .proc_async(async move {
                *res_moved.borrow_mut() = 1;
            })
            .start()
            .unwrap();
        jh.join();
        assert_eq!(*res.borrow(), 1);
    }

    #[crate::test(tarantool = "crate")]
    fn fiber_sleep_and_clock() {
        let before_sleep = clock();
        let sleep_for = Duration::from_millis(100);
        sleep(sleep_for);

        assert!(before_sleep.elapsed() >= sleep_for);
        assert!(clock() >= before_sleep);
        assert!(clock() - before_sleep >= sleep_for);
    }

    #[crate::test(tarantool = "crate", should_panic)]
    fn start_dont_join_no_use_after_free() {
        let f = start(move || {
            reschedule();
            // This return value will be written into the result cell by the
            // wrapper function. Before the fix by the time this happened the
            // memory of the result cell would have been freed and likely reused
            // by some other allocation, which would lead to this bytes
            // overwriting someone else's data and likely resulting in a crash.
            [0xaa; 4096]
        });
        drop(f);
    }

    #[crate::test(tarantool = "crate")]
    fn fiber_id() {
        fiber::id();

        let jh = fiber::defer(|| {});

        if unsafe { has_fiber_id() } {
            assert!(jh.id_checked().is_some());
        } else {
            assert!(jh.id_checked().is_none());
        }

        jh.join();
    }

    #[crate::test(tarantool = "crate")]
    fn fiber_name() {
        const NAME1: &str = "test_fiber_name_1";
        const NAME2: &str = "test_fiber_name_2";

        if unsafe { has_fiber_id() } {
            let jh = fiber::start(|| {
                // Get/set name of current fiber.
                fiber::set_name(NAME1);
                assert_eq!(fiber::name(), NAME1);
                fiber::reschedule();
                // Get name of current fiber set by parent fiber.
                assert_eq!(fiber::name(), NAME2);
            });

            let f_id = jh.id();
            // Get name of child fiber set by itself.
            assert_eq!(fiber::name_of(f_id).unwrap(), NAME1);
            // Set/get name of child fiber.
            assert!(fiber::set_name_of(f_id, NAME2));
            assert_eq!(fiber::name_of(f_id).unwrap(), NAME2);
            jh.join();

            // After the fiber has been joined, it no longer exists.
            assert!(fiber::name_of(f_id).is_none());
            assert!(!fiber::set_name_of(f_id, "foo"));
        } else {
            // Check lua implementation at least works.
            let jh = fiber::start(|| {
                fiber::set_name(NAME1);
                assert_eq!(fiber::name(), NAME1);

                assert!(fiber::set_name_of(fiber::id(), NAME2));
                assert_eq!(fiber::name_of(fiber::id()).unwrap(), NAME2);

                assert!(!fiber::set_name_of(0xCAFE_BABE_DEAD_F00D, "foo"));
                assert!(fiber::name_of(0xCAFE_BABE_DEAD_F00D).is_none());
            });
            jh.join();
        }
    }

    #[crate::test(tarantool = "crate")]
    fn fiber_csw() {
        if unsafe { has_fiber_id() } {
            let csw_parent_0 = fiber::csw();

            let jh = fiber::defer(|| {
                fiber::reschedule();
                1337
            });

            assert_eq!(fiber::csw(), csw_parent_0);
            let child_id = jh.id();
            let csw_child_0 = fiber::csw_of(child_id).unwrap();

            fiber::reschedule();

            assert_eq!(fiber::csw(), csw_parent_0 + 1);
            assert_eq!(fiber::csw_of(child_id).unwrap(), csw_child_0 + 1);

            assert_eq!(jh.join(), 1337);

            assert_eq!(fiber::csw(), csw_parent_0 + 2);
            // After the fiber has been joined, it no longer exists.
            assert!(fiber::csw_of(child_id).is_none());
        } else {
            // Check lua implementation at least works.
            let csw_parent_0 = fiber::csw();

            let jh = fiber::defer(|| {
                let csw_0 = fiber::csw_of(fiber::id()).unwrap();
                fiber::reschedule();
                assert_eq!(fiber::csw_of(fiber::id()).unwrap(), csw_0 + 1);
                1337
            });

            assert_eq!(fiber::csw(), csw_parent_0);

            fiber::reschedule();

            assert_eq!(fiber::csw(), csw_parent_0 + 1);

            assert_eq!(jh.join(), 1337);

            assert_eq!(fiber::csw(), csw_parent_0 + 2);

            assert!(fiber::csw_of(0xFACE_BEEF_BAD_DEED5).is_none());
        }
    }
}

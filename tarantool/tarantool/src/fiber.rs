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

use va_list::VaList;

use crate::error::TarantoolError;
use crate::ffi::tarantool as ffi;
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
/// The two configurations available are:
///
/// * `name`:       specifies an associated name for the fiber
/// * `stack_size`: specifies the desired stack size for the fiber
///
/// The [`start`](#method.start), [`start_unit`](#method.start_unit),
/// [`defer`](#method.defer) and [`defer_unit`](#method.defer_unit) methods will
/// take ownership of the builder and create a [`Result`] to the fiber handle
/// with the given configuration.
///
/// The [`fiber::start`](start), [`fiber::start_unit`](start_unit),
/// [`fiber::defer`](defer) and [`fiber::defer_unit`](defer_unit) free functions
/// use a `Builder` with default configuration and unwraps its return value.
pub struct Builder {
    name: Option<String>,
    attr: Option<FiberAttr>,
}

impl Builder {
    /// Generates the base configuration for spawning a fiber, from which
    /// configuration methods can be chained.
    pub fn new() -> Self {
        Builder {
            name: None,
            attr: None,
        }
    }

    /// Names the fiber-to-be.
    ///
    /// The name must not contain null bytes (`\0`).
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the size of the stack (in bytes) for the new fiber.
    pub fn stack_size(mut self, stack_size: usize) -> Result<Self> {
        let mut attr = FiberAttr::new();
        attr.set_stack_size(stack_size)?;
        self.attr = Some(attr);
        Ok(self)
    }

    /// Sets the callee function for the new fiber.
    ///
    /// Returns a [`CalleeBuilder`] taking ownership of `self`.
    pub fn callee<F, T>(self, f: F) -> CalleeBuilder<F, T>
    where
        F: FnOnce() -> T,
    {
        CalleeBuilder { builder: self, f: f }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// CalleeBuilder
////////////////////////////////////////////////////////////////////////////////

/// An intermediate fiber factory specialized for the given fiber function.
///
/// This type exists to avoid forcing [`Builder`] to know about the type of the
/// function.
pub struct CalleeBuilder<F, T>
where
    F: FnOnce() -> T,
{
    builder: Builder,
    f: F,
}

macro_rules! inner_spawn {
    ($self:expr, $fiber:tt) => {
        {
            let Self { builder: Builder { name, attr }, f } = $self;
            let name = name.unwrap_or_else(|| "<rust>".into());
            Ok($fiber::new(name, f, attr.as_ref())?.spawn())
        }
    };
}

impl<F, T> CalleeBuilder<F, T>
where
    F: FnOnce() -> T,
{
    /// Spawns a new fiber by taking ownership of the `Builder`, and returns a
    /// [`Result`] to its [`JoinHandle`].
    ///
    /// See the [`start`] free function for more details.
    pub fn start(self) -> Result<JoinHandle<T>> {
        inner_spawn!(self, Immediate)
    }

    /// Spawns a new deferred fiber by taking ownership of the `Builder`,
    /// and returns a [`Result`] to its [`JoinHandle`].
    ///
    /// See the [`defer`] free function for more details.
    pub fn defer(self) -> Result<JoinHandle<T>> {
        inner_spawn!(self, Deferred)
    }
}

impl<F> CalleeBuilder<F, ()>
where
    F: FnOnce(),
{
    /// Spawns a new unit fiber by taking ownership of the `Builder`, and
    /// returns a [`Result`] to its [`UnitJoinHandle`].
    ///
    /// See the [`start_unit`] free function for more details.
    pub fn start_unit(self) -> Result<UnitJoinHandle> {
        inner_spawn!(self, UnitImmediate)
    }

    /// Spawns a new deferred unit fiber by taking ownership of the `Builder`,
    /// and returns a [`Result`] to its [`UnitJoinHandle`].
    ///
    /// See the [`defer_unit`] free function for more details.
    pub fn defer_unit(self) -> Result<UnitJoinHandle> {
        inner_spawn!(self, UnitDeferred)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Macros
////////////////////////////////////////////////////////////////////////////////

macro_rules! inner_fiber_new {
    ($name:expr, $attr:expr) => {
        {
            let cname = CString::new($name)
                .expect("fiber name may not contain interior null bytes");

            let inner = unsafe {
                if let Some(attr) = $attr {
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

            inner
        }
    };
}

////////////////////////////////////////////////////////////////////////////////
// Immediate
////////////////////////////////////////////////////////////////////////////////

/// A handle to a fiber.
///
/// There is usually no need to create a `Immediate` struct yourself, one
/// should instead use a function like `start` to create new fibers,
/// see the docs of [`Builder`] and [`start`] for more details.
pub struct Immediate<F, T>
where
    F: FnOnce() -> T,
{
    inner: *mut ffi::Fiber,
    f: Box<F>,
}

impl<F, T> Immediate<F, T>
where
    F: FnOnce() -> T,
{
    pub fn new(name: String, f: F, attr: Option<&FiberAttr>) -> Result<Self> {
        Ok(
            Self {
                inner: inner_fiber_new!(name, attr),
                f: Box::new(f),
            }
        )
    }

    /// See [`start`] for details.
    pub fn spawn(self) -> JoinHandle<T> {
        let result = Box::new(UnsafeCell::new(None));
        unsafe {
            ffi::fiber_set_joinable(self.inner, true);
            ffi::fiber_start(
                self.inner,
                Box::into_raw(self.f),
                result.get(),
            );
        };
        JoinHandle { inner: self.inner, result }
    }

    // A C compatible function which processes the passed arguments:
    // * the rust function object (either a regular `fn` item or a closure) and
    //   a pointer to the cell where
    // * a cell in which the fiber result value will be written
    //
    // This function will be passed to the `ffi::fiber_new` function and will
    // be invoked upon fiber execution start.
    unsafe extern "C" fn trampoline(mut args: VaList) -> i32 {
        let callback = args.get_boxed::<F>();
        let result = args.get_ptr::<Option<T>>();
        std::ptr::write(result, Some(callback()));
        0
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
// UnitImmediate
////////////////////////////////////////////////////////////////////////////////

/// A handle to a unit fiber.
///
/// This is an optimized version of [`Immediate`]`<F, ()>`.
///
/// There is usually no need to create a `UnitImmediate` struct yourself, one
/// should instead use a function like `start_unit` to create new
/// fibers, see the docs of [`Builder`] and [`start_unit`] for more
/// details.
pub struct UnitImmediate<F>
where
    F: FnOnce(),
{
    inner: *mut ffi::Fiber,
    f: Box<F>,
}

impl<F> UnitImmediate<F>
where
    F: FnOnce(),
{
    pub fn new(name: String, f: F, attr: Option<&FiberAttr>) -> Result<Self> {
        Ok(
            Self {
                inner: inner_fiber_new!(name, attr),
                f: Box::new(f),
            }
        )
    }

    /// See [`start_unit`] for details.
    pub fn spawn(self) -> UnitJoinHandle {
        unsafe {
            ffi::fiber_set_joinable(self.inner, true);
            ffi::fiber_start(
                self.inner,
                Box::into_raw(self.f),
            );
        };
        UnitJoinHandle { inner: self.inner }
    }

    // A C compatible function which processes the passed arguments:
    // * the rust function object (either a regular `fn` item or a closure) and
    //   a pointer to the cell where
    //
    // This function will be passed to the `ffi::fiber_new` function and will
    // be invoked upon fiber execution start.
    unsafe extern "C" fn trampoline(mut args: VaList) -> i32 {
        let callback = args.get_boxed::<F>();
        callback();
        0
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
// Deferred
////////////////////////////////////////////////////////////////////////////////

/// A handle to a deferred fiber.
///
/// There is usually no need to create a `Deferred` struct yourself, one
/// should instead use a function like `defer` to create new
/// fibers, see the docs of [`Builder`] and [`defer`] for more
/// details.
pub struct Deferred<F, T>
where
    F: FnOnce() -> T,
{
    inner: *mut ffi::Fiber,
    f: Box<F>,
}

impl<F, T> Deferred<F, T>
where
    F: FnOnce() -> T,
{
    pub fn new(name: String, f: F, attr: Option<&FiberAttr>) -> Result<Self> {
        Ok(
            Self {
                inner: inner_fiber_new!(name, attr),
                f: Box::new(f),
            }
        )
    }

    /// See [`defer`] for details.
    pub fn spawn(self) -> JoinHandle<T> {
        let result = Box::new(UnsafeCell::new(None));
        unsafe {
            ffi::fiber_set_joinable(self.inner, true);
            ffi::fiber_start(
                self.inner,
                Box::into_raw(self.f),
                result.get(),
            );
            // XXX: Not sure if this hack works. The problem is: if we want to
            // pass an arbitrary rust function as a fiber function, we need to
            // be able to pass a pointer to it's closure. We can use the
            // additional arguments to `fiber_start` for this, but what if we
            // don't want to start the fiber and instead just `fiber_wakeup` it?
            // (similarly to how lua's `fiber.new` does it). In here I decided
            // to try do the following:
            // 1.) `fiber_start` with a trampoline and a pointer to the actual
            //     rust function object
            // 2.) the trampoline immediately does a `fiber_yield` before
            //     transfering the control to the rust function
            // 3.) `fiber_wakeup` to make the fiber READY for execution
            ffi::fiber_wakeup(self.inner);
        }

        JoinHandle { inner: self.inner, result }
    }

    // A C compatible function which processes the passed arguments:
    // * the rust function object (either a regular `fn` item or a closure) and
    //   a pointer to the cell where
    // * a cell in which the fiber result value will be written
    //
    // XXX: need to be tested
    // This function will be passed to the `ffi::fiber_new` function and will
    // be invoked upon fiber execution start. The first thing it does (after
    // retrieving the argument) is yields the execution in order to move it to
    // the "detached" mode. After that whenever the execution returns to this
    // fiber the actual rust fiber functino will be executed.
    unsafe extern "C" fn trampoline(mut args: VaList) -> i32 {
        let callback = args.get_boxed::<F>();
        let result = args.get_ptr::<Option<T>>();
        ffi::fiber_yield();
        std::ptr::write(result, Some(callback()));
        0
    }
}

////////////////////////////////////////////////////////////////////////////////
// UnitDeferred
////////////////////////////////////////////////////////////////////////////////

/// A handle to a deferred unit fiber.
///
/// This is an optimized version of [`Deferred`]`<F, ()>`.
///
/// There is usually no need to create a `UnitDeferred` struct yourself, one
/// should instead use a function like `defer_unit` to create new
/// fibers, see the docs of [`Builder`] and [`defer_unit`] for more
/// details.
pub struct UnitDeferred<F>
where
    F: FnOnce(),
{
    inner: *mut ffi::Fiber,
    f: Box<F>,
}

impl<F> UnitDeferred<F>
where
    F: FnOnce(),
{
    pub fn new(name: String, f: F, attr: Option<&FiberAttr>) -> Result<Self> {
        Ok(
            Self {
                inner: inner_fiber_new!(name, attr),
                f: Box::new(f),
            }
        )
    }

    /// See [`defer_unit`] for details.
    pub fn spawn(self) -> UnitJoinHandle {
        unsafe {
            ffi::fiber_set_joinable(self.inner, true);
            ffi::fiber_start(
                self.inner,
                Box::into_raw(self.f),
            );
            // XXX: Not sure if this hack works. The problem is: if we want to
            // pass an arbitrary rust function as a fiber function, we need to
            // be able to pass a pointer to it's closure. We can use the
            // additional arguments to `fiber_start` for this, but what if we
            // don't want to start the fiber and instead just `fiber_wakeup` it?
            // (similarly to how lua's `fiber.new` does it). In here I decided
            // to try do the following:
            // 1.) `fiber_start` with a trampoline and a pointer to the actual
            //     rust function object
            // 2.) the trampoline immediately does a `fiber_yield` before
            //     transfering the control to the rust function
            // 3.) `fiber_wakeup` to make the fiber READY for execution
            ffi::fiber_wakeup(self.inner);
        }

        UnitJoinHandle { inner: self.inner }
    }

    // A C compatible function which processes the passed arguments:
    // * the rust function object (either a regular `fn` item or a closure) and
    //   a pointer to the cell where
    //
    // XXX: need to be tested
    // This function will be passed to the `ffi::fiber_new` function and will
    // be invoked upon fiber execution start. The first thing it does (after
    // retrieving the argument) is yields the execution in order to move it to
    // the "detached" mode. After that whenever the execution returns to this
    // fiber the actual rust fiber functino will be executed.
    unsafe extern "C" fn trampoline(mut args: VaList) -> i32 {
        let callback = args.get_boxed::<F>();
        ffi::fiber_yield();
        callback();
        0
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

/// Creates a new fiber and runs it immediately, returning a [`JoinHandle`] for
/// it.
///
/// **NOTE**: The argument `f` is a function that returns `T`. In case when `T =
/// ()` (no return value) one should instead use [`start_unit`].
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
    Builder::new().callee(f).start().unwrap()
}

/// Creates a new unit fiber and runs it immediately, returning a
/// [`UnitJoinHandle`] for it.
///
/// The *unit fiber* is a special case of a fiber whose function does not return
/// a value. In fact `UnitJoinHandle` is identical to `JoinHanble<()>` is all
/// aspects instead that it is implemented more efficiently and the former
/// should always be used instead of the latter.
///
/// For more details see: [`start`]
pub fn start_unit<F>(f: F) -> UnitJoinHandle
where
    F: FnOnce(),
{
    Builder::new().callee(f).start_unit().unwrap()
}

/// Creates and schedules a new deferred fiber, returning a [`JoinHandle`] for
/// it.
///
/// **NOTE**: The argument `f` is a function that returns `T`. In case when `T =
/// ()` (no return value) one should instead use [`defer_unit`].
///
/// The **deferred fiber** is a fiber which starts in a detached mode. It can be
/// joined by calling the [`JoinHandle::join`] method.
pub fn defer<F, T>(f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T,
{
    Builder::new().callee(f).defer().unwrap()
}

/// Creates and schedules a new deferred unit fiber, returning a
/// [`UnitJoinHandle`] for it.
///
/// The **deferred unit fiber** is a fiber which starts in a detached mode. It
/// can be joined by calling the [`UnitJoinHandle::join`] method.
///
/// This is an optimized version [`defer`]`<F, ()>`.
pub fn defer_unit<F>(f: F) -> UnitJoinHandle
where
    F: FnOnce(),
{
    Builder::new().callee(f).defer_unit().unwrap()
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

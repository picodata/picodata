use std::ffi::CString;
use std::marker::PhantomData;
use std::os::raw::c_void;

use va_list::VaList;

use super::c_api;

pub struct Fiber<'a, T: 'a> {
    inner: *mut c_api::Fiber,
    callback: *mut c_void,
    phantom: PhantomData<&'a T>,
}

impl<'a, T> Fiber<'a, T> {
    pub fn new<F>(name: &str, callback: &mut F) -> Self
        where F: FnMut(Box<T>) -> i32
    {
        let (callback_ptr, trampoline) = unsafe { unpack_callback(callback) };
        Self {
            inner: unsafe { c_api::fiber_new(CString::new(name).unwrap().as_ptr(), trampoline) },
            callback: callback_ptr,
            phantom: PhantomData,
        }
    }

    pub fn start(&mut self, arg: T) {
        unsafe {
            c_api::fiber_start(
                self.inner,
                self.callback,
                Box::into_raw(Box::<T>::new(arg))
            );
        }
    }

    pub fn wakeup(&self) {
        unsafe { c_api::fiber_wakeup(self.inner) }
    }

    pub fn join(&self) -> i32 {
        unsafe { c_api::fiber_join(self.inner) }
    }

    pub fn set_joinable(&mut self, is_joinable: bool) {
        unsafe { c_api::fiber_set_joinable(self.inner, is_joinable) }
    }

    pub fn cancel(&mut self) {
        unsafe { c_api::fiber_cancel(self.inner) }
    }

    pub fn set_cancellable(is_cancellable: bool) -> bool {
        unsafe { c_api::fiber_set_cancellable(is_cancellable) }
    }

    pub fn is_cancelled() -> bool {
        unsafe { c_api::fiber_is_cancelled() }
    }

    pub fn time() -> f64 {
        unsafe { c_api::fiber_time() }
    }

    pub fn time64() -> u64 {
        unsafe { c_api::fiber_time64() }
    }

    pub fn clock() -> f64 {
        unsafe { c_api::fiber_clock() }
    }

    pub fn clock64() -> u64 {
        unsafe { c_api::fiber_clock64() }
    }
}

pub fn fiber_yield() {
    unsafe { c_api::fiber_yield() }
}

pub fn fiber_reschedule() {
    unsafe { c_api::fiber_reschedule() }
}

unsafe fn unpack_callback<F, T>(callback: &mut F) -> (*mut c_void, c_api::FiberFunc)
    where F: FnMut(Box<T>) -> i32
{
    unsafe extern "C" fn trampoline<F, T>(mut args: VaList) -> i32 where F: FnMut(Box<T>) -> i32,
    {
        let closure: &mut F = &mut *(args.get::<*const c_void>() as *mut F);
        let arg = Box::from_raw(args.get::<*const c_void>() as *mut T);
        (*closure)(arg)
    }
    (callback as *mut F as *mut c_void, Some(trampoline::<F, T>))
}

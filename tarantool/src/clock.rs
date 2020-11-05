#[inline(always)]
pub fn time() -> f64 {
    unsafe { ffi::clock_realtime() }
}

#[inline(always)]
pub fn monotonic() -> f64 {
    unsafe { ffi::clock_monotonic() }
}

#[inline(always)]
pub fn process() -> f64 {
    unsafe { ffi::clock_process() }
}

#[inline(always)]
pub fn thread() -> f64 {
    unsafe { ffi::clock_thread() }
}

#[inline(always)]
pub fn time64() -> u64 {
    unsafe { ffi::clock_realtime64() }
}

#[inline(always)]
pub fn monotonic64() -> u64 {
    unsafe { ffi::clock_monotonic64() }
}

#[inline(always)]
pub fn process64() -> u64 {
    unsafe { ffi::clock_process64() }
}

#[inline(always)]
pub fn thread64() -> u64 {
    unsafe { ffi::clock_thread64() }
}

mod ffi {
    extern "C" {
        pub fn clock_realtime() -> f64;
        pub fn clock_monotonic() -> f64;
        pub fn clock_process() -> f64;
        pub fn clock_thread() -> f64;
        pub fn clock_realtime64() -> u64;
        pub fn clock_monotonic64() -> u64;
        pub fn clock_process64() -> u64;
        pub fn clock_thread64() -> u64;
    }
}

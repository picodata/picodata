pub fn clock_realtime() -> f64 {
    unsafe { ffi::clock_realtime() }
}

pub fn clock_monotonic() -> f64 {
    unsafe { ffi::clock_monotonic() }
}

pub fn clock_process() -> f64 {
    unsafe { ffi::clock_process() }
}

pub fn clock_thread() -> f64 {
    unsafe { ffi::clock_thread() }
}

pub fn clock_realtime64() -> u64 {
    unsafe { ffi::clock_realtime64() }
}

pub fn clock_monotonic64() -> u64 {
    unsafe { ffi::clock_monotonic64() }
}

pub fn clock_process64() -> u64 {
    unsafe { ffi::clock_process64() }
}

pub fn clock_thread64() -> u64 {
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

mod ffi;
pub mod txn;

use std::mem::MaybeUninit;

#[inline(always)]
pub(crate) unsafe fn alloc_zeroed<T>() -> Box<T> {
    Box::new(MaybeUninit::zeroed().assume_init())
}

#[inline(always)]
pub(crate) fn reserve(value: &mut i32, count: i32) -> i32 {
    assert!(count > 0);
    let res = *value + 1;
    *value += count;
    res
}

#[inline(always)]
pub(crate) fn reserve_one(value: &mut i32) -> i32 {
    reserve(value, 1)
}

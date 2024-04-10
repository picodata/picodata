use std::cell::Cell;
use std::panic::Location;

////////////////////////////////////////////////////////////////////////////////
// no yields check
////////////////////////////////////////////////////////////////////////////////

/// A helper struct to enforce that a function must not yield. Will cause a
/// panic if fiber yields are detected when drop is called for it.
pub struct NoYieldsGuard {
    message: &'static str,
    location: &'static Location<'static>,
    csw: u64,
}

#[allow(clippy::new_without_default)]
impl NoYieldsGuard {
    #[inline(always)]
    #[track_caller]
    pub fn new() -> Self {
        Self {
            message: "fiber yielded when it wasn't supposed to",
            location: Location::caller(),
            csw: crate::fiber::csw(),
        }
    }

    #[inline(always)]
    #[track_caller]
    pub fn with_message(message: &'static str) -> Self {
        Self {
            message,
            location: Location::caller(),
            csw: crate::fiber::csw(),
        }
    }

    #[inline(always)]
    pub fn has_yielded(&self) -> bool {
        crate::fiber::csw() != self.csw
    }
}

impl Drop for NoYieldsGuard {
    #[inline(always)]
    fn drop(&mut self) {
        if self.has_yielded() {
            crate::say_warn!(
                "[{}:{}] {}",
                self.location.file(),
                self.location.line(),
                self.message
            );

            #[cfg(debug_assertions)]
            panic!(
                "[{}:{}] {}",
                self.location.file(),
                self.location.line(),
                self.message
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// NoYieldsRefCell
////////////////////////////////////////////////////////////////////////////////

/// A `RefCell` wrapper which also enforces that the wrapped value is never
/// borrowed across fiber yields.
#[derive(Debug)]
pub struct NoYieldsRefCell<T: ?Sized> {
    loc: Cell<&'static Location<'static>>,
    inner: std::cell::RefCell<T>,
}

impl<T> Default for NoYieldsRefCell<T>
where
    T: Default,
{
    #[inline(always)]
    #[track_caller]
    fn default() -> Self {
        Self {
            inner: Default::default(),
            loc: Cell::new(Location::caller()),
        }
    }
}

impl<T> NoYieldsRefCell<T> {
    /// Creates a new `NoYieldsRefCell` containing `value`.
    ///
    /// # Examples
    ///
    /// ```
    /// use tarantool::fiber::NoYieldsRefCell;
    ///
    /// let c = NoYieldsRefCell::new(5);
    /// ```
    #[inline(always)]
    #[track_caller]
    pub fn new(inner: T) -> Self {
        Self {
            inner: std::cell::RefCell::new(inner),
            loc: Cell::new(Location::caller()),
        }
    }

    /// Consumes the `NoYieldsRefCell`, returning the wrapped value.
    ///
    /// # Examples
    ///
    /// ```
    /// use tarantool::fiber::NoYieldsRefCell;
    ///
    /// let c = NoYieldsRefCell::new(5);
    ///
    /// let five = c.into_inner();
    /// ```
    #[inline(always)]
    pub fn into_inner(self) -> T {
        // Since this function takes `self` (the `NoYieldsRefCell`) by value, the
        // compiler statically verifies that it is not currently borrowed.
        self.inner.into_inner()
    }
}

impl<T: ?Sized> NoYieldsRefCell<T> {
    #[inline]
    #[track_caller]
    pub fn try_borrow(&self) -> Result<NoYieldsRef<'_, T>, BorrowError> {
        let Ok(inner) = self.inner.try_borrow() else {
            #[rustfmt::skip]
            return Err(BorrowError { loc: self.loc.get() });
        };
        self.loc.set(Location::caller());
        let guard =
            NoYieldsGuard::with_message("yield detected while NoYieldsRefCell was borrowed");
        Ok(NoYieldsRef { inner, guard })
    }

    #[inline]
    #[track_caller]
    pub fn borrow(&self) -> NoYieldsRef<'_, T> {
        match self.try_borrow() {
            Ok(r) => {
                return r;
            }
            Err(e) => {
                panic!("{}", e);
            }
        };
    }

    #[inline]
    #[track_caller]
    pub fn try_borrow_mut(&self) -> Result<NoYieldsRefMut<'_, T>, BorrowError> {
        let Ok(inner) = self.inner.try_borrow_mut() else {
            #[rustfmt::skip]
            return Err(BorrowError { loc: self.loc.get() });
        };
        self.loc.set(Location::caller());
        let guard =
            NoYieldsGuard::with_message("yield detected while NoYieldsRefCell was borrowed");
        Ok(NoYieldsRefMut { inner, guard })
    }

    #[inline]
    #[track_caller]
    pub fn borrow_mut(&self) -> NoYieldsRefMut<'_, T> {
        match self.try_borrow_mut() {
            Ok(r) => {
                return r;
            }
            Err(e) => {
                panic!("{}", e);
            }
        };
    }

    /// Returns a raw pointer to the underlying data in this cell.
    ///
    /// # Examples
    ///
    /// ```
    /// use tarantool::fiber::NoYieldsRefCell;
    ///
    /// let c = NoYieldsRefCell::new(5);
    ///
    /// let ptr = c.as_ptr();
    /// ```
    #[inline(always)]
    pub fn as_ptr(&self) -> *mut T {
        self.inner.as_ptr()
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this method borrows `NoYieldsRefCell` mutably, it is statically guaranteed
    /// that no borrows to the underlying data exist. The dynamic checks inherent
    /// in [`borrow_mut`] and most other methods of `NoYieldsRefCell` are therefore
    /// unnecessary.
    ///
    /// This method can only be called if `NoYieldsRefCell` can be mutably borrowed,
    /// which in general is only the case directly after the `NoYieldsRefCell` has
    /// been created. In these situations, skipping the aforementioned dynamic
    /// borrowing checks may yield better ergonomics and runtime-performance.
    ///
    /// In most situations where `NoYieldsRefCell` is used, it can't be borrowed mutably.
    /// Use [`borrow_mut`] to get mutable access to the underlying data then.
    ///
    /// [`borrow_mut`]: NoYieldsRefCell::borrow_mut()
    ///
    /// # Examples
    ///
    /// ```
    /// use tarantool::fiber::NoYieldsRefCell;
    ///
    /// let mut c = NoYieldsRefCell::new(5);
    /// *c.get_mut() += 1;
    ///
    /// assert_eq!(c.into_inner(), 6);
    /// ```
    #[inline(always)]
    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }
}

unsafe impl<T: ?Sized> Send for NoYieldsRefCell<T> where T: Send {}

// XXX: I'm not sure it's a good idea to have the following trait implementations,
// because they can cause unexpected panics. But they're implemented for `RefCell`,
// so we kinda need them implemented if we advertise `NoYieldsRefCell` as a drop-in
// replacement for `RefCell`...

impl<T: Clone> Clone for NoYieldsRefCell<T> {
    /// # Panics
    ///
    /// Panics if the value is currently mutably borrowed.
    #[inline(always)]
    #[track_caller]
    fn clone(&self) -> NoYieldsRefCell<T> {
        NoYieldsRefCell::new(self.borrow().clone())
    }

    /// # Panics
    ///
    /// Panics if `other` is currently mutably borrowed.
    #[inline(always)]
    #[track_caller]
    fn clone_from(&mut self, other: &Self) {
        self.get_mut().clone_from(&other.borrow())
    }
}

impl<T: ?Sized + PartialEq> PartialEq for NoYieldsRefCell<T> {
    /// # Panics
    ///
    /// Panics if the value in either `NoYieldsRefCell` is currently mutably borrowed.
    #[inline]
    #[track_caller]
    fn eq(&self, other: &NoYieldsRefCell<T>) -> bool {
        *self.borrow() == *other.borrow()
    }
}

impl<T: ?Sized + Eq> Eq for NoYieldsRefCell<T> {}

impl<T: ?Sized + PartialOrd> PartialOrd for NoYieldsRefCell<T> {
    /// # Panics
    ///
    /// Panics if the value in either `NoYieldsRefCell` is currently mutably borrowed.
    #[inline]
    #[track_caller]
    fn partial_cmp(&self, other: &NoYieldsRefCell<T>) -> Option<std::cmp::Ordering> {
        self.borrow().partial_cmp(&*other.borrow())
    }

    /// # Panics
    ///
    /// Panics if the value in either `NoYieldsRefCell` is currently mutably borrowed.
    #[inline]
    #[track_caller]
    fn lt(&self, other: &NoYieldsRefCell<T>) -> bool {
        *self.borrow() < *other.borrow()
    }

    /// # Panics
    ///
    /// Panics if the value in either `NoYieldsRefCell` is currently mutably borrowed.
    #[inline]
    #[track_caller]
    fn le(&self, other: &NoYieldsRefCell<T>) -> bool {
        *self.borrow() <= *other.borrow()
    }

    /// # Panics
    ///
    /// Panics if the value in either `NoYieldsRefCell` is currently mutably borrowed.
    #[inline]
    #[track_caller]
    fn gt(&self, other: &NoYieldsRefCell<T>) -> bool {
        *self.borrow() > *other.borrow()
    }

    /// # Panics
    ///
    /// Panics if the value in either `NoYieldsRefCell` is currently mutably borrowed.
    #[inline]
    #[track_caller]
    fn ge(&self, other: &NoYieldsRefCell<T>) -> bool {
        *self.borrow() >= *other.borrow()
    }
}

impl<T: ?Sized + Ord> Ord for NoYieldsRefCell<T> {
    /// # Panics
    ///
    /// Panics if the value in either `NoYieldsRefCell` is currently mutably borrowed.
    #[inline]
    fn cmp(&self, other: &NoYieldsRefCell<T>) -> std::cmp::Ordering {
        self.borrow().cmp(&*other.borrow())
    }
}

impl<T> From<T> for NoYieldsRefCell<T> {
    /// Creates a new `NoYieldsRefCell<T>` containing the given value.
    #[inline(always)]
    #[track_caller]
    fn from(t: T) -> NoYieldsRefCell<T> {
        NoYieldsRefCell::new(t)
    }
}

////////////////////////////////////////////////////////////////////////////////
// BorrowError
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct BorrowError {
    loc: &'static Location<'static>,
}

impl std::fmt::Display for BorrowError {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "already borrowed at {}", self.loc)
    }
}

////////////////////////////////////////////////////////////////////////////////
// NoYieldsRef
////////////////////////////////////////////////////////////////////////////////

pub struct NoYieldsRef<'a, T: ?Sized> {
    inner: std::cell::Ref<'a, T>,
    /// This is only needed for it's `Drop` implementation.
    #[allow(unused)]
    guard: NoYieldsGuard,
}

impl<T: ?Sized> std::ops::Deref for NoYieldsRef<'_, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

////////////////////////////////////////////////////////////////////////////////
// NoYieldsRefMut
////////////////////////////////////////////////////////////////////////////////

pub struct NoYieldsRefMut<'a, T: ?Sized> {
    inner: std::cell::RefMut<'a, T>,
    /// This is only needed for it's `Drop` implementation.
    #[allow(unused)]
    guard: NoYieldsGuard,
}

impl<T: ?Sized> std::ops::Deref for NoYieldsRefMut<'_, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> std::ops::DerefMut for NoYieldsRefMut<'_, T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use crate::fiber;

    #[crate::test(tarantool = "crate", should_panic = cfg!(debug_assertions))]
    fn no_yields_guard_panic_in_drop() {
        let guard = NoYieldsGuard::new();
        fiber::reschedule();
        // This will panic
        drop(guard);
    }

    #[crate::test(tarantool = "crate")]
    fn no_yields_guard() {
        let guard = NoYieldsGuard::with_message("bla bla");
        assert!(!guard.has_yielded());
        fiber::reschedule();
        assert!(guard.has_yielded());
        fiber::reschedule();
        assert!(guard.has_yielded());
        // Defuse the guard, so it doesn't panic in drop.
        // It's safe to forget the guard, because it doesn't allocate any memory.
        std::mem::forget(guard);
    }

    #[crate::test(tarantool = "crate", should_panic = cfg!(debug_assertions))]
    fn no_yields_ref_cell_yield_when_borrowed() {
        let cell = NoYieldsRefCell::new(());
        let r = cell.borrow();
        fiber::reschedule();
        // Panic happens here
        drop(r);
    }

    #[crate::test(tarantool = "crate", should_panic = cfg!(debug_assertions))]
    fn no_yields_ref_cell_yield_when_borrowed_mut() {
        let cell = NoYieldsRefCell::new(());
        let r = cell.borrow_mut();
        fiber::reschedule();
        // Panic happens here
        drop(r);
    }

    #[crate::test(tarantool = "crate", should_panic)]
    fn no_yields_ref_cell_already_borrowed() {
        let cell = NoYieldsRefCell::new(());
        let _r1 = cell.borrow();
        // Panic happens here
        let _r2 = cell.borrow_mut();
        // TODO: The panic message should contain the location of the last
        // successful borrow, i.e. where `_r1` is defined. Checking that this
        // happens correctly is too complicated currently but maybe we should
        // implement this at some point.
    }

    #[crate::test(tarantool = "crate", should_panic)]
    fn no_yields_ref_cell_already_borrowed_mut() {
        let cell = NoYieldsRefCell::new(());
        let _r1 = cell.borrow_mut();
        // Panic happens here
        let _r2 = cell.borrow();
        // TODO: The panic message should contain the location of the last
        // successful borrow, i.e. where `_r1` is defined. Checking that this
        // happens correctly is too complicated currently but maybe we should
        // implement this at some point.
    }

    #[crate::test(tarantool = "crate")]
    fn no_yields_ref_cell_happy_path() {
        let cell = NoYieldsRefCell::new(vec![1]);
        {
            let mut r = cell.borrow_mut();
            r.push(2);
        }
        fiber::reschedule();
        {
            let mut r = cell.borrow_mut();
            r.push(3);
        }
        fiber::reschedule();
        {
            let r = cell.borrow();
            assert_eq!(&*r, &[1, 2, 3]);
        }
    }
}

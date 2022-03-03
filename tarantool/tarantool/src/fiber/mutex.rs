use std::{
    cell::UnsafeCell,
    fmt,
    ops::{Deref, DerefMut},
};

use crate::fiber::{Latch, LatchGuard};

////////////////////////////////////////////////////////////////////////////////
// Mutex
////////////////////////////////////////////////////////////////////////////////

pub struct Mutex<T: ?Sized> {
    latch: Latch,
    data: UnsafeCell<T>,
}

impl<T: ?Sized> Mutex<T> {
    /// Creates a new mutex in an unlocked state ready for use.
    ///
    /// # Examples
    ///
    /// ```
    /// use tarantool::fiber::mutex::Mutex;
    ///
    /// let mutex = Mutex::new(0);
    /// ```
    pub fn new(t: T) -> Mutex<T>
    where
        T: Sized,
    {
        Mutex {
            latch: Latch::new(),
            data: UnsafeCell::new(t),
        }
    }

    /// Acquires a mutex, yielding the current fiber until it is able to do so.
    ///
    /// This function will yield the current fiber until it is available to
    /// acquire the mutex. Upon returning, the fiber is the only fiber with
    /// the lock held. A RAII guard is returned to allow scoped unlock of the
    /// lock. When the guard goes out of scope, the mutex will be unlocked.
    ///
    /// The exact behavior on locking a mutex in the fiber which already holds
    /// the lock is left unspecified.
    ///
    /// # Abortions
    ///
    /// This function might abort when called if the lock is already held by
    /// the current fiber.
    ///
    /// # Examples
    /// ```
    /// use std::cell::Rc;
    /// use tarantool::fiber::{start_proc, mutex::Mutex}
    ///
    /// let mutex = Rc::new(Mutex::new(0));
    /// let c_mutex = Rc::clone(&mutex);
    ///
    /// start_proc(move || {
    ///     *c_mutex.lock() = 10;
    /// }).join();
    /// assert_eq!(*mutex.lock(), 10);
    /// ```
    pub fn lock(&self) -> MutexGuard<'_, T> {
        unsafe {
            MutexGuard::new(self, self.latch.lock())
        }
    }

    /// Attempts to acquire this lock.
    ///
    /// If the lock could not be acquired at this time, then `None` is returned.
    /// Otherwise, an RAII guard is returned. The lock will be unlocked when the
    /// guard is dropped.
    ///
    /// This function does not yield.
    ///
    /// # Abortions
    ///
    /// This function might abort when called if the lock is already held by
    /// the current fiber.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::cell::Rc;
    /// use tarantool::fiber::{start_proc, mutex::Mutex};
    ///
    /// let mutex = Rc::new(Mutex::new(0));
    /// let c_mutex = Rc::clone(&mutex);
    ///
    /// start_proc(move || {
    ///     let mut lock = c_mutex.try_lock();
    ///     if let Some(ref mut mutex) = lock {
    ///         **mutex = 10;
    ///     } else {
    ///         println!("try_lock failed");
    ///     }
    /// }).join();
    /// assert_eq!(*mutex.lock(), 10);
    /// ```
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        unsafe {
            self.latch.try_lock().map(|guard| MutexGuard::new(self, guard))
        }
    }

    /// Immediately drops the guard, and consequently unlocks the mutex.
    ///
    /// This function is equivalent to calling [`drop`] on the guard but is more
    /// self-documenting. Alternately, the guard will be automatically dropped
    /// when it goes out of scope.
    ///
    /// ```
    /// use tarantool::fiber::mutex::Mutex;
    /// let mutex = Mutex::new(0);
    ///
    /// let mut guard = mutex.lock().unwrap();
    /// *guard += 20;
    /// Mutex::unlock(guard);
    /// ```
    pub fn unlock(guard: MutexGuard<'_, T>) {
        drop(guard);
    }

    /// Consumes this mutex, returning the underlying data.
    ///
    /// # Examples
    ///
    /// ```
    /// use tarantool::fiber::mutex::Mutex;
    ///
    /// let mutex = Mutex::new(0);
    /// assert_eq!(mutex.into_inner(), 0);
    /// ```
    pub fn into_inner(self) -> T
    where
        T: Sized,
    {
        self.data.into_inner()
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the `Mutex` mutably, no actual locking needs to
    /// take place -- the mutable borrow statically guarantees no locks exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use tarantool::fiber::mutex::Mutex;
    ///
    /// let mut mutex = Mutex::new(0);
    /// *mutex.get_mut() = 10;
    /// assert_eq!(*mutex.lock(), 10);
    /// ```
    pub fn get_mut(&mut self) -> &mut T {
        self.data.get_mut()
    }
}

impl<T> From<T> for Mutex<T> {
    /// Creates a new mutex in an unlocked state ready for use.
    /// This is equivalent to [`Mutex::new`].
    fn from(t: T) -> Self {
        Mutex::new(t)
    }
}

impl<T: ?Sized + Default> Default for Mutex<T> {
    /// Creates a `Mutex<T>`, with the `Default` value for T.
    fn default() -> Mutex<T> {
        Mutex::new(Default::default())
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("Mutex");
        match self.try_lock() {
            Some(guard) => {
                d.field("data", &&*guard);
            }
            None => {
                struct LockedPlaceholder;
                impl fmt::Debug for LockedPlaceholder {
                    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str("<locked>")
                    }
                }
                d.field("data", &LockedPlaceholder);
            }
        }
        d.finish_non_exhaustive()
    }
}

////////////////////////////////////////////////////////////////////////////////
// MutexGuard
////////////////////////////////////////////////////////////////////////////////

pub struct MutexGuard<'a, T: ?Sized + 'a> {
    lock: &'a Mutex<T>,
    _latch_guard: LatchGuard,
}

impl<'mutex, T: ?Sized> MutexGuard<'mutex, T> {
    unsafe fn new(lock: &'mutex Mutex<T>, _latch_guard: LatchGuard) -> Self {
        Self { lock, _latch_guard }
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}


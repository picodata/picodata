//! See [`Mutex`] for examples and docs.

use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

/// An asynchronous `Mutex`-like type.
///
/// This type acts similarly to [`std::sync::Mutex`], with two major
/// differences: [`Mutex::lock`] is an async method so does not block, and the lock
/// guard is designed to be held across `.await` points.
///
/// Prefer this type to [`crate::fiber::mutex::Mutex`] if used in async contexts.
/// This [`Mutex`] makes fiber yielding calls to be explicit with `.await` syntax and
/// will help avoid deadlocks in case of multiple futures used in `join_all` or similar combinators.
#[derive(Debug)]
pub struct Mutex<T: ?Sized> {
    locked: Cell<bool>,
    wakers: RefCell<VecDeque<Waker>>,
    data: UnsafeCell<T>,
}

impl<T: ?Sized> Mutex<T> {
    /// Creates a new mutex in an unlocked state ready for use.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tarantool::fiber::r#async::Mutex;
    ///
    /// let mutex = Mutex::new(0);
    /// ```
    pub fn new(t: T) -> Mutex<T>
    where
        T: Sized,
    {
        Mutex {
            data: UnsafeCell::new(t),
            locked: Cell::new(false),
            wakers: Default::default(),
        }
    }

    /// Locks this mutex, causing the current future/fiber to yield until the lock has
    /// been acquired.  When the lock has been acquired, function returns a
    /// [`MutexGuard`]. The lock will be unlocked when the
    /// guard is dropped.
    ///
    /// # Examples
    /// ```no_run
    /// use std::rc::Rc;
    /// use tarantool::fiber::{start_async, block_on, r#async::Mutex};
    ///
    /// let mutex = Rc::new(Mutex::new(0));
    /// let c_mutex = Rc::clone(&mutex);
    ///
    /// start_async(async move {
    ///     *c_mutex.lock().await = 10;
    /// }).join();
    /// block_on(async { assert_eq!(*mutex.lock().await, 10) });
    /// ```
    pub async fn lock(&self) -> MutexGuard<'_, T> {
        struct Lock<'a, T: ?Sized + 'a> {
            mutex: &'a Mutex<T>,
        }

        impl<'a, T: ?Sized> Future for Lock<'a, T> {
            type Output = MutexGuard<'a, T>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                if self.mutex.locked.get() {
                    self.mutex.add_waker(cx.waker());
                    Poll::Pending
                } else {
                    Poll::Ready(MutexGuard::new(self.mutex))
                }
            }
        }

        Lock { mutex: self }.await
    }

    /// Attempts to acquire this lock.
    ///
    /// If the lock could not be acquired at this time, then `None` is returned.
    /// Otherwise, a [`MutexGuard`] is returned. The lock will be unlocked when the
    /// guard is dropped.
    ///
    /// This function does not yield.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::rc::Rc;
    /// use tarantool::fiber::{start_proc, r#async::Mutex};
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
    /// assert_eq!(*mutex.try_lock().unwrap(), 10);
    /// ```
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        if self.locked.get() {
            None
        } else {
            Some(MutexGuard::new(self))
        }
    }

    /// Immediately drops the guard, and consequently unlocks the mutex.
    ///
    /// This function is equivalent to calling [`drop`] on the guard but is more
    /// self-documenting. Alternately, the guard will be automatically dropped
    /// when it goes out of scope.
    ///
    /// ```no_run
    /// use tarantool::fiber::r#async::Mutex;
    /// let mutex = Mutex::new(0);
    ///
    /// let mut guard = mutex.try_lock().unwrap();
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
    /// ```no_run
    /// use tarantool::fiber::r#async::Mutex;
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
    /// ```no_run
    /// use tarantool::fiber::r#async::Mutex;
    ///
    /// let mut mutex = Mutex::new(0);
    /// *mutex.get_mut() = 10;
    /// assert_eq!(*mutex.try_lock().unwrap(), 10);
    /// ```
    pub fn get_mut(&mut self) -> &mut T {
        self.data.get_mut()
    }

    fn add_waker(&self, waker: &Waker) {
        self.wakers.borrow_mut().push_back(waker.clone());
    }

    fn wake_one(&self) {
        if let Some(waker) = self.wakers.borrow_mut().pop_front() {
            waker.wake();
        }
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

/// A handle to a held [`Mutex`]. The guard can be held across any `.await`.
///
/// As long as you have this guard, you have exclusive access to the underlying
/// `T`. The guard internally borrows the `Mutex`, so the mutex will not be
/// dropped while a guard exists. To access `T` data use [`Deref`] and [`DerefMut`] methods or operators.
///
/// The lock is automatically released whenever the guard is dropped, at which
/// point `lock` will succeed yet again.
pub struct MutexGuard<'a, T: ?Sized + 'a> {
    mutex: &'a Mutex<T>,
}

impl<'mutex, T: ?Sized> MutexGuard<'mutex, T> {
    fn new(mutex: &'mutex Mutex<T>) -> Self {
        mutex.locked.set(true);
        Self { mutex }
    }
}

impl<'a, T: ?Sized + 'a> Drop for MutexGuard<'a, T> {
    fn drop(&mut self) {
        self.mutex.locked.set(false);
        self.mutex.wake_one();
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.mutex.data.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self, f)
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use std::{rc::Rc, time::Duration};

    use crate::fiber;
    use crate::fiber::r#async::{timeout::IntoTimeout, watch};
    use crate::test::util::ok;

    use super::*;

    #[crate::test(tarantool = "crate")]
    fn smoke() {
        fiber::block_on(async {
            let m = Mutex::new(());
            drop(m.lock().await);
            drop(m.lock().await);
        })
    }

    #[crate::test(tarantool = "crate")]
    fn timeouts() {
        fiber::block_on(async {
            let m = Mutex::new(());
            let _guard = m.lock().await;
            let _guard_2 = async { ok(m.lock().await) }
                .timeout(Duration::from_millis(50))
                .await
                .unwrap_err();
        })
    }

    #[crate::test(tarantool = "crate")]
    fn try_lock() {
        let m = Mutex::new(());
        *m.try_lock().unwrap() = ();
    }

    #[crate::test(tarantool = "crate")]
    fn into_inner() {
        let m = Mutex::new(10);
        assert_eq!(m.into_inner(), 10);
    }

    #[crate::test(tarantool = "crate")]
    fn get_mut() {
        let mut m = Mutex::new(10);
        *m.get_mut() = 20;
        assert_eq!(m.into_inner(), 20);
    }

    #[crate::test(tarantool = "crate")]
    fn contention_multiple_fibers() {
        let mutex = Rc::new(Mutex::new(0));
        let num_tasks = 100;
        let mut handles = Vec::new();
        let (tx, rx) = watch::channel(());
        let tx = Rc::new(tx);

        for _ in 0..num_tasks {
            let mut rx = rx.clone();
            let mutex = mutex.clone();
            handles.push(fiber::start_async(async move {
                let mut lock = mutex.lock().await;
                *lock += 1;
                // Holding lock while awaiting
                rx.changed().await.unwrap();
                drop(lock);
            }));
        }

        for _ in 0..num_tasks {
            tx.send(()).unwrap();
            fiber::r#yield().unwrap();
        }
        for handle in handles.into_iter() {
            handle.join();
        }
        fiber::block_on(async {
            let lock = mutex.lock().await;
            assert_eq!(num_tasks, *lock);
        });
    }

    #[crate::test(tarantool = "crate")]
    fn contention_one_fiber() {
        let mutex = Rc::new(Mutex::new(0));
        let num_tasks = 100;
        let mut tasks = Vec::new();
        let (tx, rx) = watch::channel(());
        let tx = Rc::new(tx);

        for _ in 0..num_tasks {
            let mut rx = rx.clone();
            let mutex = mutex.clone();
            tasks.push(async move {
                let mut lock = mutex.lock().await;
                *lock += 1;
                // Holding lock while awaiting
                rx.changed().await.unwrap();
                drop(lock);
            });
        }

        let handle = fiber::defer(|| {
            for _ in 0..num_tasks {
                tx.send(()).unwrap();
                fiber::r#yield().unwrap();
            }
        });
        fiber::block_on(async {
            futures::future::join_all(tasks).await;
            let lock = mutex.lock().await;
            assert_eq!(num_tasks, *lock);
        });
        handle.join();
    }
}

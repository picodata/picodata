use std::sync::atomic::{
    AtomicBool, AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicIsize, AtomicU16, AtomicU32,
    AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use std::sync::{Arc, OnceLock};

/// A trait abstracting over standard atomic types.
///
/// Implemented for all Atomic* types from [`std::sync::atomic`].
///
/// This trait should be able go away once [`generic_atomic`](https://github.com/rust-lang/rust/issues/130539) is stabilized.
pub trait Atomic {
    type Value;
    fn new(value: Self::Value) -> Self;
    fn load(&self, ordering: Ordering) -> Self::Value;
    fn store(&self, value: Self::Value, ordering: Ordering);
}

/// A type that has a corresponding [`Atomic`].
pub trait TypeWithAtomic: Copy {
    type Atomic: Atomic<Value = Self>;
}

macro_rules! impl_type_with_atomic {
    ($(($ty:ty, $atomic:ty)),*) => {
        $(
            impl Atomic for $atomic {
                type Value = $ty;

                fn new(value: Self::Value) -> Self {
                    <$atomic>::new(value)
                }

                fn load(&self, ordering: Ordering) -> Self::Value {
                    <$atomic>::load(self, ordering)
                }

                fn store(&self, value: Self::Value, ordering: Ordering) {
                    <$atomic>::store(self, value, ordering)
                }
            }
            impl TypeWithAtomic for $ty {
                type Atomic = $atomic;
            }
        )*
    };
}

impl_type_with_atomic! {
    (u64, AtomicU64),
    (u32, AtomicU32),
    (u16, AtomicU16),
    (u8, AtomicU8),
    (i64, AtomicI64),
    (i32, AtomicI32),
    (i16, AtomicI16),
    (i8, AtomicI8),
    (usize, AtomicUsize),
    (isize, AtomicIsize),
    (bool, AtomicBool)
}

/// A synchronization primitive that can be used to broadcast value to any amount of observers. Usable as a static.
pub struct AtomicObserverProvider<T: TypeWithAtomic> {
    shared: OnceLock<Arc<T::Atomic>>,
}

impl<T: TypeWithAtomic> AtomicObserverProvider<T> {
    /// Creates a new uninitialized observer provider
    pub const fn new() -> Self {
        Self {
            shared: OnceLock::new(),
        }
    }

    fn get_or_init(&self, initial_value: T) -> &Arc<T::Atomic> {
        self.shared
            .get_or_init(|| Arc::new(T::Atomic::new(initial_value)))
    }

    fn get_noinit(&self) -> &Arc<T::Atomic> {
        self.shared
            .get()
            .expect("Attempt to access an uninitialized AtomicObserverProvider")
    }

    /// Gets the current value stored in the provider. Panics if the provider is not initialized.
    pub fn current_value(&self) -> T {
        self.get_noinit().load(Ordering::Relaxed)
    }

    /// Makes an observer that can be used to get the value stored in the provider. Panics if the provider is not initialized.
    pub fn make_observer(&self) -> AtomicObserver<T> {
        AtomicObserver {
            shared: self.get_noinit().clone(),
        }
    }

    /// Initialized the observer (if needed) and updates the stored value.
    pub fn update(&self, new_value: T) {
        self.get_or_init(new_value)
            .store(new_value, Ordering::Relaxed)
    }
}

impl<T: TypeWithAtomic> Default for AtomicObserverProvider<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A receiver counterpart to [`AtomicObserverProvider`]. Can be used to receive value stored in the provider.
pub struct AtomicObserver<T: TypeWithAtomic> {
    shared: Arc<T::Atomic>,
}

impl<T: TypeWithAtomic> AtomicObserver<T> {
    /// Get the current value stored in the provider.
    pub fn current_value(&self) -> T {
        self.shared.load(Ordering::Relaxed)
    }
}

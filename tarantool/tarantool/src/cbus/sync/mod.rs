#![cfg(any(feature = "picodata", doc))]

/// A synchronous channels for popular runtimes.
/// Synchronous channel - means that channel has internal buffer with user-defined capacity.
/// Synchronous channel differs against of unbounded channel in the semantics of the sender: if
/// channel buffer is full then all sends called from producer will block a runtime, until channel
/// buffer is freed.
///
/// It is important to use a channel that suits the runtime in which the producer works.

/// A channels for messaging between an OS thread (producer) and tarantool cord (consumer).
pub mod std;
/// A channels for messaging between a tokio task (producer) and tarantool cord (consumer).
pub mod tokio;

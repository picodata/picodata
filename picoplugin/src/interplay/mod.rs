//! Communication between `picodata` runtime and any third party runtimes such as `tokio`,
//! `smol` or even your custom runtime.
//! This module provides two API's:
//! - low level `cbus` API - with raw channels between custom runtime and `picodata` runtime
//! - high level `tros` API - with the ability to execute async tasks in third party async runtimes
//! and wait for execution results.
//! You should prefer a second one.
//! The usage of cbus API at one's own risk
//! as there are some pitfalls unbeknownst even to the authors of the runtime.
//!
//! # Problem
//!
//! `Picodata` uses its own asynchronous runtime, and all plugin code is executed via this runtime.
//! You can see more in [documentation](https://docs.binary.picodata.io/tarantool/en/reference/reference_lua/fiber.html).
//! If you want to use any other runtimes, you need a way to communicate
//! with them from `picodata` runtime; that's why `interplay` module exists.
//!
//! # Tros
//!
//! Tros is a high-level bridge between `picodata` and external runtimes.
//! With `tros`, you don't care about the complexity of interaction -
//! you don't even use channels directly.
//! It hides all details and provides a simplified API.
//!
//! Here is how to do async http request with `tros`:
//!
//! ```no_run
//! use picoplugin::interplay::tros;
//! use picoplugin::system::tarantool::fiber;
//!
//! let text = tros::TokioExecutor::new(tros::transport::PicodataTransport::default())
//!             .exec(async { reqwest::get("http://example.com").await?.text().await })
//!             .unwrap()
//!             .unwrap();
//! assert!(text.contains("Example Domain"))
//! ```
//!
//! # Cbus
//!
//! When you communicate with `tokio`, or third party system thread, or any other runtime,
//! there are two questions that need to be answered:
//! 1) how to send data from `picodata` plugin into this runtime?
//! 2) how to receive data from third party runtime into `picodata` plugin?
//!
//! The answer to the first question is pretty straightforward, you can use channels
//! that are provided by third party runtime.
//! For example, if you send data into `tokio` runtime,
//! just use [tokio channels](https://tokio.rs/tokio/tutorial/channels).
//!
//! But this solution doesn't help in the second question, because `tokio` channels don't know anything
//! about `picodata` runtime.
//! It matters because when you try to receive data in picodata plugin code,
//! `picodata` runtime should know it and suspend current fiber.
//! Then wake up it when data is ready.
//! That's why we need special channels which will allow working in completely
//! asynchronous mode - `cbus` channels.
//!
//! Check [`cbus`](https://github.com/picodata/tarantool-module/blob/master/tarantool/src/cbus/mod.rs)
//! documentation if you want to know internals.
//!
//! Note that `cbus` is more low-level tool than `tros`, so use it may be more complex than
//! you expect.
//! We recommend to use `cbus` API for stream data processing or similar tasks.
//! Otherwise, we recommend trying to use `tros` API due to its simplicity.  
//!
//! Here we will give a description of the `cbus` channels:
//! - [`cbus::oneshot::channel`] - provide the oneshot channels, using these channels you can send
//! data just once.
//! - [`cbus::unbounded::channel`] - provide unbounded channels, using these channels you can
//! send data any number of times.
//! - [`cbus::sync::std::channel`] - provide sync std channels, this channels similar to unbounded
//! but when you call `send` function from std thread it returns only when `picodata` plugin code
//! receive data.
//! - [`cbus::sync::tokio::channel`] - provide sync `tokio` channels, this channels is similar to unbounded one
//! but when you call `send` function from `tokio` task it returns only when `picodata` plugin code
//! receive data.
//!
//! ## Cbus example
//!
//! Let's try to solve a little task - we need to parse multiple files.
//! Imagine we have files with big json encoded objects in it.
//! We want to parse files in parallel because the number of files is huge, and we would like to
//! get the result as quickly as possible.
//! And we would additionally like to free up the `picodata` runtime from the heavy task of parsing,
//! because open and parse file is blocking operations that may "lock" `picodata`
//! runtime for a long time.
//!
//! Okay, now let's solve it.
//! We will use a separate threadpool, to parse file, threads will
//! receive parsing tasks (file names) and send parsed data back
//! to `picodata` runtime (using `cbus` unbounded channel).
//!
//! First, create a `parse` function.
//! This function will open a file, parse data, and send a result using an unbounded channel.
//!
//! ```no_run
//! use std::fs::read_to_string;
//! use std::path::Path;
//! use picoplugin::interplay::cbus;
//! use serde_json;
//!
//! fn parse(file: &Path, tx: cbus::unbounded::Sender<serde_json::Value>) {
//!     let raw_json = read_to_string(file).unwrap();
//!     let object = serde_json::from_str(&raw_json).unwrap();
//!     tx.send(object).unwrap();
//! }
//! ```
//!
//! Second, lets create a function for parse a list of files that may be used in `picodata` runtime.
//!
//! ```no_run
//! use std::path::PathBuf;
//! use picoplugin::interplay::channel::unbounded;
//! use threadpool::ThreadPool;
//!
//! static THREAD_POOL: ThreadPool = ThreadPool::new(16);
//!
//! fn parse_files(files: &[PathBuf]) -> Vec<serde_json::Value> {
//!     let (tx, rx) = unbounded::channel();
//!     for file in files {
//!         THREAD_POOL.execute(parse(file, tx.clone()));
//!     }
//!     
//!     let mut result = Vec::with_capacity(files.len());
//!     while let Ok(object) = rx.receive() {
//!         result.push(object)
//!     };
//!     result
//! }
//! ```

pub use tarantool::cbus;
pub use tros;

pub mod channel {
    /// ***For internal usage***
    pub const DEFAULT_CBUS_ENDPOINT: &str = "picodata-channels";

    pub mod oneshot {
        use crate::interplay::channel::DEFAULT_CBUS_ENDPOINT;
        pub use tarantool::cbus::oneshot::{EndpointReceiver, Sender};

        /// Creates a new oneshot channel, returning the sender/receiver halves.
        ///
        /// See [`tarantool::cbus::oneshot::channel`] for more.
        pub fn channel<T>() -> (Sender<T>, EndpointReceiver<T>) {
            tarantool::cbus::oneshot::channel(DEFAULT_CBUS_ENDPOINT)
        }
    }

    pub mod unbounded {
        use crate::interplay::channel::DEFAULT_CBUS_ENDPOINT;
        pub use tarantool::cbus::unbounded::{EndpointReceiver, Sender};

        /// Creates a new unbounded channel, returning the sender/receiver halves.
        ///
        /// See [`tarantool::cbus::unbounded::channel`] for more.
        pub fn channel<T>() -> (Sender<T>, EndpointReceiver<T>) {
            tarantool::cbus::unbounded::channel(DEFAULT_CBUS_ENDPOINT)
        }
    }

    pub mod sync {
        pub mod std {
            use crate::interplay::channel::DEFAULT_CBUS_ENDPOINT;
            use std::num::NonZeroUsize;
            pub use tarantool::cbus::sync::std::{EndpointReceiver, Sender};

            /// Creates a new synchronous channel for communication with std threads,
            /// returning the sender/receiver halves.
            ///
            /// See [`tarantool::cbus::sync::std::channel`] for more.
            pub fn channel<T>(cap: NonZeroUsize) -> (Sender<T>, EndpointReceiver<T>) {
                tarantool::cbus::sync::std::channel(DEFAULT_CBUS_ENDPOINT, cap)
            }
        }

        pub mod tokio {
            use crate::interplay::channel::DEFAULT_CBUS_ENDPOINT;
            use std::num::NonZeroUsize;
            pub use tarantool::cbus::sync::tokio::{EndpointReceiver, Sender};

            /// Creates a new synchronous channel for communication with tokio runtime,
            /// returning the sender/receiver halves.
            ///
            /// See [`tarantool::cbus::sync::tokio::channel`] for more.
            pub fn channel<T>(cap: NonZeroUsize) -> (Sender<T>, EndpointReceiver<T>) {
                tarantool::cbus::sync::tokio::channel(DEFAULT_CBUS_ENDPOINT, cap)
            }
        }
    }
}

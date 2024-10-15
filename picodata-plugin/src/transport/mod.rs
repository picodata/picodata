//! Transport module provides APIs for communication between `picodata` instances (using RPC),
//! or between `picodata` instance and outer world (for example, by HTTP protocol).
//!
//! This module also contains useful functional for integrate tracing, metrics and other tools into
//! your application.
pub mod context;
pub mod http;
pub mod rpc;

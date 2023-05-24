//! Picodata synchronization primitives.

use ::tarantool::{fiber, proc};
use std::time::{Duration, Instant};

use crate::tarantool;
use crate::traft;
use crate::traft::RaftIndex;

#[derive(thiserror::Error, Debug)]
#[error("timeout")]
pub struct TimeoutError;

/// Tarantool log sequence number.
///
/// To ensure data persistence, Tarantool records updates to the
/// database in the so-called write-ahead log (WAL) files. LSN is the
/// index of the entry applied last. It tracks the current database
/// state.
pub type Lsn = u64;

#[proc]
fn proc_get_lsn() -> Lsn {
    get_lsn()
}

/// Returns current [`Lsn`].
pub fn get_lsn() -> Lsn {
    tarantool::eval("return box.info.lsn").expect("this code should never fail")
}

#[proc]
fn proc_wait_lsn(target: Lsn, timeout: f64) -> Result<Lsn, TimeoutError> {
    wait_lsn(target, Duration::from_secs_f64(timeout))
}

/// Block current fiber until Tarantool [`Lsn`] reaches the `target`.
///
/// Returns the actual LSN. It can be equal to or greater than the
/// target one. If timeout expires beforehand, the function returns
/// `Err(TimeoutError)`.
///
/// **This function yields**
///
pub fn wait_lsn(target: Lsn, timeout: Duration) -> Result<Lsn, TimeoutError> {
    // TODO: this all should be a part of tarantool C API
    let deadline = Instant::now() + timeout;
    loop {
        let current = get_lsn();
        if current >= target {
            return Ok(current);
        }

        if Instant::now() < deadline {
            fiber::sleep(crate::traft::node::MainLoop::TICK);
        } else {
            return Err(TimeoutError);
        }
    }
}

#[proc]
fn proc_get_index() -> traft::Result<RaftIndex> {
    let node = traft::node::global()?;
    Ok(node.get_index())
}

#[proc]
fn proc_read_index(timeout: f64) -> traft::Result<RaftIndex> {
    let node = traft::node::global()?;
    node.read_index(Duration::from_secs_f64(timeout))
}

#[proc]
fn proc_wait_index(target: RaftIndex, timeout: f64) -> traft::Result<RaftIndex> {
    let node = traft::node::global()?;
    node.wait_index(target, Duration::from_secs_f64(timeout))
}

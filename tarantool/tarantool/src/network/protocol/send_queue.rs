use std::cell::{Cell, RefCell};
use std::io::Cursor;
use std::time::Duration;

use crate::error::Error;
use crate::fiber::Cond;

use super::SyncIndex;

pub struct SendQueue {
    pub(crate) is_active: Cell<bool>,
    sync: Cell<u64>,
    pub(crate) front_buffer: RefCell<Cursor<Vec<u8>>>,
    pub(crate) back_buffer: RefCell<Cursor<Vec<u8>>>,
    pub(crate) swap_cond: Cond,
    buffer_limit: u64,
    pub(crate) flush_interval: Duration,
}

impl SendQueue {
    pub fn new(buffer_size: usize, buffer_limit: usize, flush_interval: Duration) -> Self {
        SendQueue {
            is_active: Cell::new(true),
            sync: Cell::new(0),
            front_buffer: RefCell::new(Cursor::new(Vec::with_capacity(buffer_size))),
            back_buffer: RefCell::new(Cursor::new(Vec::with_capacity(buffer_size))),
            swap_cond: Cond::new(),
            buffer_limit: buffer_limit as u64,
            flush_interval,
        }
    }

    pub fn send<F>(&self, payload_producer: F) -> Result<SyncIndex, Error>
    where
        F: FnOnce(&mut Cursor<Vec<u8>>, SyncIndex) -> Result<(), Error>,
    {
        unimplemented!("Function is in the process of being moved into different structure")
    }

    pub fn close(&self) {
        self.is_active.set(false);
        self.swap_cond.signal();
    }
}

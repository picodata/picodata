use std::cell::{Cell, RefCell};
use std::io::Cursor;
use std::time::Duration;

use crate::error::Error;
use crate::fiber::Cond;

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

    pub fn send<F>(&self, payload_producer: F) -> Result<u64, Error>
    where
        F: FnOnce(&mut Cursor<Vec<u8>>, u64) -> Result<(), Error>,
    {
        let sync = self.next_sync();

        if self.back_buffer.borrow().position() >= self.buffer_limit {
            self.swap_cond.signal();
        }

        let offset = {
            let buffer = &mut *self.back_buffer.borrow_mut();

            let offset = buffer.position();
            match write_to_buffer(buffer, sync, payload_producer) {
                Err(err) => {
                    // rollback buffer position on error
                    buffer.set_position(offset);
                    return Err(err);
                }
                Ok(_) => offset,
            }
        };

        // trigger swap condition if buffer was empty before
        if offset == 0 {
            self.swap_cond.signal();
        }

        Ok(sync)
    }

    pub fn next_sync(&self) -> u64 {
        let sync = self.sync.get() + 1;
        self.sync.set(sync);
        sync
    }

    pub fn close(&self) {
        self.is_active.set(false);
        self.swap_cond.signal();
    }
}

pub fn write_to_buffer<F>(
    buffer: &mut Cursor<Vec<u8>>,
    sync: u64,
    payload_producer: F,
) -> Result<(), Error>
where
    F: FnOnce(&mut Cursor<Vec<u8>>, u64) -> Result<(), Error>,
{
    // write MSG_SIZE placeholder
    let msg_start_offset = buffer.position();
    rmp::encode::write_u32(buffer, 0)?;

    // write message payload
    let payload_start_offset = buffer.position();
    payload_producer(buffer, sync)?;
    let payload_end_offset = buffer.position();

    // calculate and write MSG_SIZE
    buffer.set_position(msg_start_offset);
    rmp::encode::write_u32(buffer, (payload_end_offset - payload_start_offset) as u32)?;
    buffer.set_position(payload_end_offset);

    Ok(())
}

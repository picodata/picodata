use std::cell::{Cell, RefCell};
use std::io::{self, Cursor, Write};

use crate::error::Error;
use crate::fiber::{Cond, Latch};

pub struct SendQueue {
    is_active: Cell<bool>,
    sync: Cell<u64>,
    front_buffer: RefCell<Cursor<Vec<u8>>>,
    back_buffer: RefCell<Cursor<Vec<u8>>>,
    lock: Latch,
    swap_cond: Cond,
}

impl SendQueue {
    pub fn new(buffer_size: usize) -> Self {
        SendQueue {
            is_active: Cell::new(true),
            sync: Cell::new(0),
            front_buffer: RefCell::new(Cursor::new(Vec::with_capacity(buffer_size))),
            back_buffer: RefCell::new(Cursor::new(Vec::with_capacity(buffer_size))),
            lock: Latch::new(),
            swap_cond: Cond::new(),
        }
    }

    pub fn send<F>(&self, payload_producer: F) -> Result<u64, Error>
    where
        F: FnOnce(&mut Cursor<Vec<u8>>, u64) -> Result<(), Error>,
    {
        self.sync.set(self.sync.get() + 1);
        let offset = {
            let _lock = self.lock.lock();
            let buffer = &mut *self.back_buffer.borrow_mut();

            let offset = buffer.position();
            match write_to_buffer(self.sync.get(), buffer, payload_producer, offset) {
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

        Ok(self.sync.get())
    }

    pub fn flush_to_stream(&self, stream: &mut impl Write) -> io::Result<()> {
        loop {
            let is_data_available = {
                let _lock = self.lock.lock();

                if !self.is_active.get() {
                    return Err(io::Error::from(io::ErrorKind::TimedOut));
                }

                let is_data_available = self.back_buffer.borrow().position() > 0;
                if is_data_available {
                    self.back_buffer.swap(&self.front_buffer);
                }
                is_data_available
            };

            // await for data (if buffer is empty)
            if is_data_available {
                break;
            } else {
                self.swap_cond.wait();
            }
        }

        // write front buffer contents to stream + clear front buffer
        let mut buffer = self.front_buffer.borrow_mut();
        stream.write(buffer.get_ref())?;
        buffer.set_position(0);
        buffer.get_mut().clear();
        Ok(())
    }

    pub fn close(&self) {
        {
            let _lock = self.lock.lock();
            self.is_active.set(false);
        }
        self.swap_cond.signal();
    }
}

fn write_to_buffer<F>(
    sync: u64,
    buffer: &mut Cursor<Vec<u8>>,
    payload_producer: F,
    msg_start_offset: u64,
) -> Result<(), Error>
where
    F: FnOnce(&mut Cursor<Vec<u8>>, u64) -> Result<(), Error>,
{
    // write MSG_SIZE placeholder
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

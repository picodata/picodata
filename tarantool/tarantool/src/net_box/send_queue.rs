use std::cell::{Cell, RefCell};
use std::io::{self, Cursor, Write};
use std::time::{Duration, SystemTime};

use crate::error::Error;
use crate::fiber::{reschedule, Cond};
use crate::network::protocol;
use crate::network::protocol::SyncIndex;

pub struct SendQueue {
    is_active: Cell<bool>,
    sync: Cell<SyncIndex>,
    front_buffer: RefCell<Cursor<Vec<u8>>>,
    back_buffer: RefCell<Cursor<Vec<u8>>>,
    swap_cond: Cond,
    buffer_limit: u64,
    flush_interval: Duration,
}

impl SendQueue {
    pub fn new(buffer_size: usize, buffer_limit: usize, flush_interval: Duration) -> Self {
        SendQueue {
            is_active: Cell::new(true),
            sync: Cell::new(SyncIndex(0)),
            front_buffer: RefCell::new(Cursor::new(Vec::with_capacity(buffer_size))),
            back_buffer: RefCell::new(Cursor::new(Vec::with_capacity(buffer_size))),
            swap_cond: Cond::new(),
            buffer_limit: buffer_limit as u64,
            flush_interval,
        }
    }

    pub fn send<R>(&self, request: &R) -> Result<SyncIndex, Error>
    where
        R: protocol::Request,
    {
        let sync = self.next_sync();

        if self.back_buffer.borrow().position() >= self.buffer_limit {
            self.swap_cond.signal();
        }

        let mut buffer = self.back_buffer.borrow_mut();
        // Convert cursor type `Cursor<Vec<u8>>` -> `Cursor<&mut Vec<u8>>`
        let msg_start_offset = buffer.position();
        let mut adapted_buffer = Cursor::new(buffer.get_mut());
        adapted_buffer.set_position(msg_start_offset);

        protocol::write_to_buffer(&mut adapted_buffer, sync, request)?;

        // Advance the shared cursor's position,
        // because now only the adapted one knows the correct position
        let new_offset = adapted_buffer.position();
        buffer.set_position(new_offset);

        // trigger swap condition if buffer was empty before
        if msg_start_offset == 0 {
            self.swap_cond.signal();
        }

        Ok(sync)
    }

    pub fn next_sync(&self) -> SyncIndex {
        let sync = self.sync.get();
        self.sync.set(SyncIndex(sync.0 + 1));
        sync
    }

    pub fn flush_to_stream(&self, stream: &mut impl Write) -> io::Result<()> {
        let start_ts = SystemTime::now();
        let mut prev_data_size = 0u64;

        loop {
            if !self.is_active.get() {
                return Err(io::Error::from(io::ErrorKind::TimedOut));
            }

            let data_size = self.back_buffer.borrow().position();
            if data_size == 0 {
                // await for data (if buffer is empty)
                self.swap_cond.wait();
                continue;
            }

            if let Ok(elapsed) = start_ts.elapsed() {
                if data_size > prev_data_size && elapsed <= self.flush_interval {
                    prev_data_size = data_size;
                    reschedule();
                    continue;
                }
            }

            self.back_buffer.swap(&self.front_buffer);
            break;
        }

        // write front buffer contents to stream + clear front buffer
        let mut buffer = self.front_buffer.borrow_mut();
        stream.write_all(buffer.get_ref())?;
        buffer.set_position(0);
        buffer.get_mut().clear();
        Ok(())
    }

    pub fn close(&self) {
        self.is_active.set(false);
        self.swap_cond.signal();
    }
}

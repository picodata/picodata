use std::io::{self, Cursor, Write};

use crate::error::Error;
use crate::fiber::{Cond, Latch};

pub struct SendQueue {
    sync: u64,
    front_buffer: Cursor<Vec<u8>>,
    back_buffer: Cursor<Vec<u8>>,
    lock: Latch,
    swap_cond: Cond,
}

impl SendQueue {
    pub fn new() -> Self {
        SendQueue {
            sync: 0,
            front_buffer: Cursor::new(Vec::with_capacity(4 * 1024 * 1024)),
            back_buffer: Cursor::new(Vec::with_capacity(4 * 1024 * 1024)),
            lock: Latch::new(),
            swap_cond: Cond::new(),
        }
    }

    pub fn send<F>(&mut self, payload_producer: F) -> Result<u64, Error>
    where
        F: FnOnce(&mut dyn Write, u64) -> Result<(), Error>,
    {
        self.sync += 1;

        let offset = {
            let offset = self.back_buffer.position();
            match self.write_to_buffer(payload_producer, offset) {
                Err(err) => {
                    // rollback buffer position on error
                    self.back_buffer.set_position(offset);
                    return Err(err);
                }
                Ok(_) => offset,
            }
        };

        // trigger swap condition if buffer was empty before
        if offset == 0 {
            self.swap_cond.signal();
        }

        Ok(self.sync)
    }

    fn write_to_buffer<F>(
        &mut self,
        payload_producer: F,
        msg_start_offset: u64,
    ) -> Result<(), Error>
    where
        F: FnOnce(&mut dyn Write, u64) -> Result<(), Error>,
    {
        let _lock = self.lock.lock();

        // write MSG_SIZE placeholder
        rmp::encode::write_u32(&mut self.back_buffer, 0)?;

        // write message payload
        let payload_start_offset = self.back_buffer.position();
        payload_producer(&mut self.back_buffer, self.sync)?;
        let payload_end_offset = self.back_buffer.position();

        // calculate and write MSG_SIZE
        self.back_buffer.set_position(msg_start_offset);
        rmp::encode::write_u32(
            &mut self.back_buffer,
            (payload_end_offset - payload_start_offset) as u32,
        )?;
        self.back_buffer.set_position(payload_end_offset);

        Ok(())
    }

    pub fn flush_to_stream(&mut self, stream: &mut dyn Write) -> io::Result<()> {
        loop {
            let is_data_available = {
                let _lock = self.lock.lock();

                let is_data_available = self.back_buffer.position() > 0;
                if is_data_available {
                    std::mem::swap(&mut self.back_buffer, &mut self.front_buffer);
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
        stream.write(self.front_buffer.get_ref())?;
        self.front_buffer.set_position(0);
        self.front_buffer.get_mut().clear();
        Ok(())
    }
}

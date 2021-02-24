use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::io::{self, Cursor, Read};

use refpool::{Pool, PoolRef};
use rmp::decode;

use crate::error::Error;
use crate::fiber::Cond;

use super::options::Options;
use super::protocol::{decode_error, decode_header, Header, Response};

pub struct RecvQueue {
    buffer: RefCell<Cursor<Vec<u8>>>,
    header: RefCell<Option<Header>>,
    chunks: RefCell<Vec<u64>>,
    cond_map: RefCell<HashMap<u64, PoolRef<Cond>>>,
    cond_pool: Pool<Cond>,
    read_offset: Cell<usize>,
    read_completed_cond: Cond,
}

impl RecvQueue {
    pub fn new(buffer_size: usize) -> Self {
        let mut buffer = Vec::with_capacity(buffer_size);
        buffer.resize(buffer_size, 0);

        RecvQueue {
            buffer: RefCell::new(Cursor::new(buffer)),
            header: RefCell::new(None),
            chunks: RefCell::new(Vec::with_capacity(1024)),
            cond_map: RefCell::new(HashMap::new()),
            cond_pool: Pool::new(1024),
            read_offset: Cell::new(0),
            read_completed_cond: Cond::new(),
        }
    }

    pub fn recv<F, R>(
        &self,
        sync: u64,
        payload_consumer: F,
        options: &Options,
    ) -> Result<Response<R>, Error>
    where
        F: FnOnce(&mut Cursor<Vec<u8>>, &Header) -> Result<R, Error>,
    {
        let cond_ref = PoolRef::new(&self.cond_pool, Cond::new());
        {
            self.cond_map.borrow_mut().insert(sync, cond_ref.clone());
        }

        let is_signaled = match options.timeout {
            None => cond_ref.wait(),
            Some(timeout) => cond_ref.wait_timeout(timeout),
        };

        if is_signaled {
            let result = {
                let header = self.header.replace(None).unwrap();
                if header.status_code != 0 {
                    return Err(decode_error(self.buffer.borrow_mut().by_ref())?.into());
                }

                payload_consumer(self.buffer.borrow_mut().by_ref(), &header)
                    .map(|payload| Response { payload, header })
            };
            self.read_completed_cond.signal();
            result
        } else {
            self.cond_map.borrow_mut().remove(&sync);
            Err(io::Error::from(io::ErrorKind::TimedOut).into())
        }
    }

    pub fn pull(&self, stream: &mut impl Read) -> Result<(), Error> {
        let mut chunks = self.chunks.borrow_mut();

        let mut overflow_range = 0..0;
        {
            let mut buffer = self.buffer.borrow_mut();
            let data_len = stream.read(&mut buffer.get_mut()[self.read_offset.get()..])? as u64;
            chunks.clear();
            buffer.set_position(0);

            loop {
                let prefix_chunk_offset = buffer.position();
                let chunk_len = decode::read_u32(&mut *buffer)? as u64;
                let chunk_offset = buffer.position();
                let new_offset = chunk_offset + chunk_len;
                if new_offset > data_len {
                    overflow_range = (prefix_chunk_offset as usize)..(data_len as usize);
                    break;
                }

                chunks.push(chunk_offset);

                if new_offset == data_len {
                    break;
                }

                buffer.set_position(new_offset as u64);
            }
        };

        for chunk_offset in chunks.iter() {
            let header = {
                let mut buffer = self.buffer.borrow_mut();
                buffer.set_position(*chunk_offset);
                decode_header(buffer.by_ref())?
            };

            let cond_ref = {
                let sync = header.sync;
                self.header.replace(Some(header));
                self.cond_map.borrow_mut().remove(&sync)
            };

            if let Some(cond_ref) = cond_ref {
                cond_ref.signal();
                self.read_completed_cond.wait();
            }
        }

        let new_read_offset = if !overflow_range.is_empty() {
            let new_read_offset = overflow_range.end - overflow_range.end;
            self.buffer
                .borrow_mut()
                .get_mut()
                .copy_within(overflow_range, 0);
            new_read_offset as usize
        } else {
            0
        };
        self.read_offset.set(new_read_offset);

        Ok(())
    }
}

pub fn recv_message(
    stream: &mut impl Read,
    buffer: &mut Cursor<Vec<u8>>,
    response_len: usize,
) -> Result<usize, Error> {
    buffer.set_position(0);
    {
        let buffer = buffer.get_mut();
        buffer.clear();
        buffer.reserve(response_len);
    }

    stream
        .take(response_len as u64)
        .read_to_end(buffer.get_mut())
        .map_err(|err| err.into())
}

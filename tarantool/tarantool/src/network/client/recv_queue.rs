use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::{hash_map::Iter as HashMapIter, HashMap};
use std::io::{self, Cursor, Read};
use std::ops::Range;
use std::rc::{Rc, Weak};

use refpool::{Pool, PoolRef};
use rmp::decode;

use crate::error::Error;
use crate::fiber::{Cond, Latch};

use crate::network::protocol::codec::{
    decode_error, decode_header, Consumer, Header, Response, Sync,
};
use crate::network::protocol::options::Options;

type Consumers = HashMap<Sync, Weak<dyn Consumer>>;

pub struct RecvQueue {
    is_active: Cell<bool>,
    buffer: RefCell<Cursor<Vec<u8>>>,
    chunks: RefCell<Vec<Range<usize>>>,
    cond_map: RefCell<HashMap<Sync, PoolRef<Cond>>>,
    cond_pool: Pool<Cond>,
    async_consumers: UnsafeCell<Consumers>,
    read_offset: Cell<usize>,
    read_completed_cond: Cond,
    header_recv_result: RefCell<Option<Result<Header, Error>>>,
    notification_lock: Latch,
}

impl RecvQueue {
    pub fn new(buffer_size: usize) -> Self {
        let buffer = vec![0; buffer_size];
        RecvQueue {
            is_active: Cell::new(true),
            buffer: RefCell::new(Cursor::new(buffer)),
            chunks: RefCell::new(Vec::with_capacity(1024)),
            cond_map: RefCell::new(HashMap::new()),
            cond_pool: Pool::new(1024),
            async_consumers: UnsafeCell::new(HashMap::new()),
            read_offset: Cell::new(0),
            read_completed_cond: Cond::new(),
            header_recv_result: RefCell::new(None),
            notification_lock: Latch::new(),
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
        if !self.is_active.get() {
            return Err(io::Error::from(io::ErrorKind::ConnectionAborted).into());
        }

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
                let header = self.header_recv_result.replace(None).unwrap();

                match header {
                    Ok(header) => {
                        if header.status_code != 0 {
                            return Err(decode_error(self.buffer.borrow_mut().by_ref())?.into());
                        }

                        payload_consumer(self.buffer.borrow_mut().by_ref(), &header)
                            .map(|payload| Response { payload, header })
                    }
                    Err(e) => return Err(e),
                }
            };
            self.read_completed_cond.signal();
            result
        } else {
            self.cond_map.borrow_mut().remove(&sync);
            Err(io::Error::from(io::ErrorKind::TimedOut).into())
        }
    }

    pub fn add_consumer(&self, sync: Sync, consumer: Weak<dyn Consumer>) {
        unsafe { (*self.async_consumers.get()).insert(sync, consumer) };
    }

    pub fn get_consumer(&self, sync: Sync) -> Option<Rc<dyn Consumer>> {
        unsafe { &mut *self.async_consumers.get() }
            .remove(&sync)
            .and_then(|c| c.upgrade())
    }

    pub fn iter_consumers(&self) -> HashMapIter<Sync, Weak<dyn Consumer>> {
        unsafe { &*self.async_consumers.get() }.iter()
    }

    pub fn pull(&self, stream: &mut impl Read) -> Result<bool, Error> {
        if !self.is_active.get() {
            return Ok(false);
        }

        let mut chunks = self.chunks.borrow_mut();

        let mut overflow_range = 0..0;
        {
            let mut buffer = self.buffer.borrow_mut();
            let data_len = stream.read(&mut buffer.get_mut()[self.read_offset.get()..])?;
            if data_len == 0 {
                return Ok(false);
            }

            chunks.clear();
            buffer.set_position(0);

            loop {
                let prefix_chunk_offset = buffer.position();
                let chunk_len = decode::read_u32(&mut *buffer)? as usize;
                let chunk_offset = buffer.position() as _;
                let new_offset = chunk_offset + chunk_len;
                if new_offset > data_len {
                    overflow_range = (prefix_chunk_offset as usize)..(data_len as usize);
                    break;
                }

                chunks.push(chunk_offset..new_offset);

                if new_offset == data_len {
                    break;
                }

                buffer.set_position(new_offset as u64);
            }
        };

        {
            let _lock = self.notification_lock.lock();
            for &Range { start, end } in chunks.iter() {
                let header = {
                    let mut buffer = self.buffer.borrow_mut();
                    buffer.set_position(start as _);
                    decode_header(buffer.by_ref())?
                };

                let sync = header.sync;
                let cond_ref = self.cond_map.borrow_mut().remove(&sync);
                if let Some(cond_ref) = cond_ref {
                    self.header_recv_result.replace(Some(Ok(header)));
                    cond_ref.signal();
                    self.read_completed_cond.wait();
                } else if let Some(consumer) = self.get_consumer(sync) {
                    let buffer = self.buffer.borrow();
                    let body_start = buffer.position() as usize;
                    consumer.consume(&header, &buffer.get_ref()[body_start..end]);
                }
            }
        }

        let new_read_offset = if !overflow_range.is_empty() {
            let new_read_offset = overflow_range.end - overflow_range.start;
            self.buffer
                .borrow_mut()
                .get_mut()
                .copy_within(overflow_range, 0);
            new_read_offset as usize
        } else {
            0
        };
        self.read_offset.set(new_read_offset);

        Ok(true)
    }

    pub fn close(&self) {
        let _lock = self.notification_lock.lock();
        self.is_active.set(false);
        for (_, cond_ref) in self.cond_map.borrow_mut().drain() {
            self.header_recv_result
                .replace(Some(Err(
                    io::Error::from(io::ErrorKind::ConnectionAborted).into()
                )));
            cond_ref.signal();
        }
        for consumer in self.iter_consumers().filter_map(|(_, c)| c.upgrade()) {
            consumer.handle_disconnect();
        }
    }
}

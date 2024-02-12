use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::{hash_map::Iter as HashMapIter, HashMap};
use std::io::{self, Cursor, Read};
use std::ops::Range;
use std::rc::{Rc, Weak};

use refpool::{Pool, PoolRef};
use rmp::decode;

use crate::clock;
use crate::error::Error;
use crate::fiber;
use crate::fiber::{Cond, Latch};

use super::options::Options;
use super::promise::Consumer;
use crate::network::protocol;
use crate::network::protocol::SyncIndex;
use crate::network::protocol::{Header, Response};

type Consumers = HashMap<SyncIndex, Weak<dyn Consumer>>;

pub struct RecvQueue {
    is_active: Cell<bool>,
    buffer: RefCell<Cursor<Vec<u8>>>,
    chunks: RefCell<Vec<Range<usize>>>,
    cond_map: RefCell<HashMap<SyncIndex, PoolRef<Cond>>>,
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

    pub fn recv<R>(
        &self,
        sync: SyncIndex,
        options: &Options,
    ) -> Result<Response<R::Response>, Error>
    where
        R: protocol::Request,
    {
        if !self.is_active.get() {
            return Err(io::Error::from(io::ErrorKind::ConnectionAborted).into());
        }

        let cond_ref = PoolRef::new(&self.cond_pool, Cond::new());
        {
            self.cond_map.borrow_mut().insert(sync, cond_ref.clone());
        }

        let timeout = options.timeout.unwrap_or(clock::INFINITY);
        let deadline = fiber::clock().saturating_add(timeout);

        let header = loop {
            if fiber::clock() > deadline {
                self.cond_map.borrow_mut().remove(&sync);
                return Err(io::Error::from(io::ErrorKind::TimedOut).into());
            }

            cond_ref.wait_deadline(deadline);

            let Some(header) = self.header_recv_result.take() else {
                // Spurious wakeup
                continue;
            };

            let header = crate::unwrap_ok_or!(header,
                Err(e) => {
                    // Connection closed
                    return Err(e);
                }
            );

            break header;
        };

        if header.iproto_type == protocol::IProtoType::Error as u32 {
            // Wakeup the recv_worker before returning
            self.read_completed_cond.signal();

            let mut buf = self.buffer.borrow_mut();
            let error = protocol::decode_error(buf.by_ref(), &header)?;
            return Err(Error::Remote(error));
        }

        let res = R::decode_response_body(self.buffer.borrow_mut().by_ref());
        // Don't signal until payload_consumer returns, just in case it yields,
        // which it definetly shouldn't do, but better safe than sorry
        self.read_completed_cond.signal();

        let payload = res?;
        return Ok(Response { payload, header });
    }

    pub fn add_consumer(&self, sync: SyncIndex, consumer: Weak<dyn Consumer>) {
        unsafe { (*self.async_consumers.get()).insert(sync, consumer) };
    }

    pub fn get_consumer(&self, sync: SyncIndex) -> Option<Rc<dyn Consumer>> {
        unsafe { &mut *self.async_consumers.get() }
            .remove(&sync)
            .and_then(|c| c.upgrade())
    }

    pub fn iter_consumers(&self) -> HashMapIter<SyncIndex, Weak<dyn Consumer>> {
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
                    overflow_range = (prefix_chunk_offset as usize)..data_len;
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
                    protocol::decode_header(buffer.by_ref())?
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
            new_read_offset
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

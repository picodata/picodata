use std::time::Duration;

use raft::prelude::Message;

use crate::fiber::Cond;

use super::fsm::Command;

pub struct Protocol {}

impl Protocol {
    pub fn new() -> Self {
        Protocol {}
    }

    pub fn send(&self, msg: Message) {
        unimplemented!()
    }
}

pub struct Queue {
    recv_cond: Cond,
}

pub enum QueueMsg {
    Propose(Command),
    Raft(Message),
}

impl Queue {
    pub fn new() -> Self {
        Queue {
            recv_cond: Cond::new(),
        }
    }

    pub fn recv(&self, timeout: Duration) -> Option<QueueMsg> {
        unimplemented!()
    }

    pub fn send(&self, msg: QueueMsg) {
        unimplemented!()
    }
}

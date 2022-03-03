use raft::prelude::Entry;
use serde::Serialize;

use crate::index::IndexOptions;
use crate::space::SpaceCreateOptions;

#[derive(Serialize)]
pub enum Command {
    CreateSpace(String, SpaceCreateOptions),
    CreateIndex(String, String, IndexOptions),
    DropSpace(String),
    DropIndex(String, String),
}

pub struct Fsm {}

impl Fsm {
    pub fn new() -> Self {
        Fsm {}
    }

    pub fn handle_normal(&mut self, entry: Entry) {
        unimplemented!();
    }

    pub fn handle_conf_change(&mut self, entry: Entry) {
        unimplemented!();
    }
}

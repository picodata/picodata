#[macro_use]
extern crate lazy_static;

use std::cell::RefCell;
use std::os::raw::c_int;
use std::rc::{Rc, Weak};
use tarantool::raft::Node;
use tarantool::tuple::{FunctionArgs, FunctionCtx};

#[derive(Default)]
struct Global {
    node: RefCell<Weak<Node>>,
}

unsafe impl Sync for Global {}
unsafe impl Send for Global {}

lazy_static! {
    static ref GLOBAL: Global = Default::default();
}

#[no_mangle]
pub extern "C" fn luaopen_libcluster_node(_: FunctionCtx, _: FunctionArgs) -> c_int {
    let node = Rc::new(Node::new("raft_rpc", Default::default()));
    GLOBAL.node.replace(Rc::downgrade(&node));
    node.run(vec!["127.0.0.1:3302", "127.0.0.1:3303"]);
    0
}

#[no_mangle]
pub extern "C" fn raft_rpc(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    match GLOBAL.node.borrow().upgrade() {
        None => 0,
        Some(node) => node.call_rpc(ctx, args),
    }
}

use std::cell::RefCell;
use std::os::raw::c_int;
use std::rc::{Rc, Weak};

use tarantool::{
    proc,
    raft::Node,
    tuple::{FunctionArgs, FunctionCtx},
};

use once_cell::sync::Lazy;

#[derive(Default)]
struct Global {
    node: RefCell<Weak<Node>>,
}

unsafe impl Sync for Global {}
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for Global {}

static GLOBAL: Lazy<Global> = Lazy::new(Default::default);

#[proc]
fn run_node(bootstrap_addrs: Vec<&str>) {
    let node = Rc::new(Node::new("libcluster_node.rpc", bootstrap_addrs, Default::default()).unwrap());
    GLOBAL.node.replace(Rc::downgrade(&node));
    node.run().unwrap();
}

#[no_mangle]
pub extern "C" fn rpc(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    match GLOBAL.node.borrow().upgrade() {
        None => 0,
        Some(node) => node.handle_rpc(ctx, args),
    }
}

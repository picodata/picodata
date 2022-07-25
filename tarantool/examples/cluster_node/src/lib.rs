#[macro_use]
extern crate lazy_static;

use std::cell::RefCell;
use std::os::raw::c_int;
use std::rc::{Rc, Weak};

use tarantool::raft::Node;
use tarantool::tuple::{FunctionArgs, FunctionCtx, Tuple};

#[derive(Default)]
struct Global {
    node: RefCell<Weak<Node>>,
}

unsafe impl Sync for Global {}
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for Global {}

lazy_static! {
    static ref GLOBAL: Global = Default::default();
}

#[no_mangle]
pub extern "C" fn run_node(_: FunctionCtx, args: FunctionArgs) -> c_int {
    let args: Tuple = args.into();
    let (bootstrap_addrs,) = args.decode::<(Vec<String>,)>().unwrap();

    let node =
        Rc::new(Node::new("libcluster_node.rpc", bootstrap_addrs, Default::default()).unwrap());
    GLOBAL.node.replace(Rc::downgrade(&node));
    node.run().unwrap();
    0
}

#[no_mangle]
pub extern "C" fn rpc(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    match GLOBAL.node.borrow().upgrade() {
        None => 0,
        Some(node) => node.handle_rpc(ctx, args),
    }
}

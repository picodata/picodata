use crate::{rpc, tarantool_main, tlog};

use super::args;

pub async fn tt_expel(args: args::Expel) {
    let req = rpc::expel::Request {
        cluster_id: args.cluster_id,
        instance_id: args.instance_id,
    };
    let res = rpc::network_call(
        &format!("{}:{}", args.peer_address.host, args.peer_address.port),
        &rpc::expel::redirect::Request(req),
    )
    .await;
    match res {
        Ok(_) => {
            tlog!(Info, "Success expel call");
            std::process::exit(0);
        }
        Err(e) => {
            tlog!(Error, "Failed to expel instance: {e}");
            std::process::exit(-1);
        }
    }
}

pub fn main(args: args::Expel) -> ! {
    let rc = tarantool_main!(
        args.tt_args().unwrap(),
        callback_data: (args,),
        callback_data_type: (args::Expel,),
        callback_body: {
            ::tarantool::fiber::block_on(tt_expel(args))
        }
    );
    std::process::exit(rc);
}

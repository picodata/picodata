use crate::cli::args;
use crate::cli::connect::determine_credentials_and_connect;
use crate::rpc::expel::Request as ExpelRequest;
use crate::rpc::RequestArgs;
use crate::tarantool_main;
use crate::tlog;
use crate::traft::error::Error;
use tarantool::fiber;
use tarantool::network::client::AsClient;

pub async fn tt_expel(args: args::Expel) -> Result<(), Error> {
    let (client, _) = determine_credentials_and_connect(
        &args.peer_address,
        Some("admin"),
        args.password_file.as_deref(),
        tarantool::auth::AuthMethod::ChapSha1,
    )?;

    let req = ExpelRequest {
        cluster_id: args.cluster_id,
        instance_id: args.instance_id.clone(),
    };
    fiber::block_on(client.call(ExpelRequest::PROC_NAME, &req))
        .map_err(|e| Error::other(format!("Failed to expel instance: {e}")))?;

    tlog!(Info, "Instance {} successfully expelled", args.instance_id);

    Ok(())
}

pub fn main(args: args::Expel) -> ! {
    let rc = tarantool_main!(
        args.tt_args().unwrap(),
        callback_data: (args,),
        callback_data_type: (args::Expel,),
        callback_body: {
            if let Err(e) = ::tarantool::fiber::block_on(tt_expel(args)) {
                tlog!(Critical, "{e}");
                std::process::exit(1);
            }
            std::process::exit(0);
        }
    );
    std::process::exit(rc);
}

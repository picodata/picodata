use crate::cli::args;
use crate::cli::connect::determine_credentials_and_connect;
use crate::rpc::expel::redirect::proc_expel_redirect;
use crate::rpc::expel::Request as ExpelRequest;
use crate::tlog;
use crate::traft::error::Error;
use tarantool::fiber;
use tarantool::network::client::AsClient;

pub async fn tt_expel(args: args::Expel) -> Result<(), Error> {
    let (client, _) = determine_credentials_and_connect(
        &args.peer_address,
        Some("admin"),
        args.password_file.as_deref(),
        args.auth_method,
        args.timeout,
    )?;

    let req = ExpelRequest {
        cluster_name: args.cluster_name,
        instance_uuid: args.instance_uuid.clone(),
    };
    fiber::block_on(client.call(crate::proc_name!(proc_expel_redirect), &req))
        .map_err(|e| Error::other(format!("Failed to expel instance: {e}")))?;

    tlog!(
        Info,
        "Instance {} successfully expelled",
        args.instance_uuid
    );

    Ok(())
}

pub fn main(args: args::Expel) -> ! {
    let tt_args = args.tt_args().unwrap();
    super::tarantool::main_cb(&tt_args, || -> Result<(), Error> {
        ::tarantool::fiber::block_on(tt_expel(args))?;
        std::process::exit(0)
    })
}

use crate::cli::args;
use crate::cli::connect::determine_credentials_and_connect;
use crate::error_code::ErrorCode;
use crate::rpc::expel::redirect::proc_expel_redirect;
use crate::rpc::expel::Request as ExpelRequest;
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::tlog;
use crate::traft::error::Error;
use std::time::Duration;
use tarantool::error::IntoBoxError;
use tarantool::fiber;
use tarantool::network::client::AsClient;

pub async fn tt_expel(args: args::Expel) -> Result<(), Error> {
    if let Some(cluster_name) = &args.cluster_name {
        tlog!(
            Warning,
            "The --cluster-name '{}' parameter is deprecated for use in picodata expel command and no longer used. It will be removed in the future major release (version 26).",
            cluster_name
        );
    }

    if let Ok(env_cluster_name) = std::env::var("PICODATA_CLUSTER_NAME") {
        if !env_cluster_name.is_empty() {
            tlog!(
                Warning,
                "The PICODATA_CLUSTER_NAME environment variable '{}' is deprecated for use in picodata expel command and no longer used. It will be removed in the future major release (version 26).",
                env_cluster_name
            );
        }
    }

    let timeout = Duration::from_secs(args.timeout);
    let deadline = fiber::clock().saturating_add(timeout);
    let (client, _) = determine_credentials_and_connect(
        &args.peer_address,
        Some(PICO_SERVICE_USER_NAME),
        args.password_file.as_deref(),
        args.auth_method,
        timeout,
    )?;

    let timeout = deadline.duration_since(fiber::clock());
    let force = args.force;
    let req = ExpelRequest {
        cluster_name: args.cluster_name.unwrap_or_default(),
        instance_uuid: args.instance_uuid.clone(),
        force,
        timeout,
    };
    let res = fiber::block_on(client.call(crate::proc_name!(proc_expel_redirect), &req));
    if let Err(e) = res {
        let error_code = error_code_from_client_error(&e);
        if error_code == ErrorCode::ExpelNotAllowed as u32 {
            let error_message =
                format!("{e}\nrerun with --force if you still want to expel the instance");
            return Err(Error::other(error_message));
        }
        return Err(Error::other(format!("Failed to expel instance: {e}")));
    }

    tlog!(
        Info,
        "Instance {} successfully expelled",
        args.instance_uuid
    );

    Ok(())
}

// TODO ClientError should implement IntoBoxError
fn error_code_from_client_error(e: &tarantool::network::client::ClientError) -> u32 {
    use tarantool::network::client::ClientError::*;
    match e {
        ConnectionClosed(e) => e.error_code(),
        RequestEncode(e) => e.error_code(),
        ResponseDecode(e) => e.error_code(),
        ErrorResponse(e) => e.error_code(),
    }
}

pub fn main(args: args::Expel) -> ! {
    let tt_args = args.tt_args().unwrap();
    super::tarantool::main_cb(&tt_args, || -> Result<(), Error> {
        ::tarantool::fiber::block_on(tt_expel(args))?;
        std::process::exit(0)
    })
}

use crate::config::PicodataConfig;
use crate::pgproto;
use crate::tlog;
use crate::traft::node;
use crate::traft::{RaftIndex, RaftTerm};
use smol_str::SmolStr;

use std::time::Duration;

pub(super) fn before_online_inner(req: Request) -> crate::traft::Result<Response> {
    let node = node::global()?;
    node.wait_index(req.applied, req.timeout)?;
    node.status().check_term(req.term)?;

    pgproto::start_once()?;

    let result = node.plugin_manager.handle_instance_online();
    if let Err(e) = result {
        tlog!(Error, "failed initializing plugin system: {e}");
        return Err(e.into());
    }

    let name = node.topology_cache.my_instance_name();
    tlog!(Info, "instance is Online name={name}");

    print_ready_banner(name);

    Ok(Response {})
}

fn print_ready_banner(name: &str) {
    // SAFETY: only called from the tx thread.
    static mut PRINTED: bool = false;
    if unsafe { PRINTED } {
        return;
    }
    unsafe { PRINTED = true };

    let config = PicodataConfig::get();
    let iproto = config.instance.iproto.advertise().to_host_port();
    let pgproto = if config.instance.pgproto.enabled() {
        config.instance.pgproto.advertise().to_host_port()
    } else {
        SmolStr::default()
    };
    let http = if config.instance.http.enabled() {
        let prefix = if config.instance.http.tls.enabled() {
            "https://"
        } else {
            "http://"
        };
        SmolStr::from(format!(
            "{prefix}{}",
            config.instance.http.advertise().to_host_port()
        ))
    } else {
        SmolStr::default()
    };

    let socket = config.instance.admin_socket();
    let admin = std::fs::canonicalize(socket).unwrap_or_else(|_| socket.into());
    let admin = admin.display();

    tlog!(
        Info,
        r#"
           ___  _____________  ___  ___ _________
          / _ \/  _/ ___/ __ \/ _ \/ _ /_  __/ _ |
         / ___// // /__/ /_/ / // / __ |/ / / __ |
        /_/  /___/\___/\____/____/_/ |_/_/ /_/ |_|

         instance:  {name}
         pgproto:   {pgproto}
         iproto:    {iproto}
         http:      {http}
         admin:     {admin}
        "#
    );
}

crate::define_rpc_request! {
    /// Enables communication by PostgreSQL protocol and all
    /// plugins on instance locally that are marked as "enabled".
    ///
    /// Called by governor at newly online instance.
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving peer is not yet initialized
    /// 2. Storage failure
    /// 3. Timeout while waiting for an index from request
    /// 4. Request has an incorrect term - leader changed
    /// 5. Address for pgproto cannot be parsed or is busy
    /// 6. Any of "enabled" plugins was loaded with errors
    fn proc_before_online(req: Request) -> crate::traft::Result<Response> {
        before_online_inner(req)
    }

    pub struct Request {
        pub term: RaftTerm,
        pub applied: RaftIndex,
        pub timeout: Duration,
    }

    pub struct Response {}
}

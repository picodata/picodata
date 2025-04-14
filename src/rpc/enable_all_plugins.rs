use crate::rpc::before_online::{self, before_online_inner};

crate::define_rpc_request! {
    /// !SOFT DEPRECATED!
    /// For the actual RPC, see [`crate::rpc::before_online::proc_before_online`].
    fn proc_enable_all_plugins(req: Request) -> crate::traft::Result<Response> {
        before_online_inner(req.0).map(Response)
    }

    pub struct Request(pub before_online::Request);

    pub struct Response(pub before_online::Response);
}

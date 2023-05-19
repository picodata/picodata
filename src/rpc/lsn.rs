use crate::traft::Result;

crate::define_rpc_request! {
    fn proc_get_lsn(req: Request) -> Result<Response> {
        let _ = req;
        let lsn = crate::tarantool::eval("return box.info.lsn")?;
        Ok(Response { lsn })
    }

    pub struct Request {
    }

    pub struct Response {
        pub lsn: u64,
    }
}

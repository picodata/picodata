use ::tarantool::proc;

use crate::tarantool::set_cfg_field;
use crate::traft::{
    error::Error,
    node,
    storage::peer_field::{PeerAddress, ReplicasetId},
};
use crate::InstanceId;

#[proc(packed_args)]
fn proc_replication(req: Request) -> Result<Response, Error> {
    let node = node::global()?;
    let peer_storage = &node.storage.peers;
    let this_rsid = peer_storage.peer_field::<ReplicasetId>(&node.raft_id())?;
    let mut peer_addresses = Vec::with_capacity(req.replicaset_instances.len());
    for id in &req.replicaset_instances {
        let (address, rsid) = peer_storage.peer_field::<(PeerAddress, ReplicasetId)>(id)?;
        if rsid != this_rsid {
            return Err(Error::ReplicasetIdMismatch {
                instance_rsid: this_rsid,
                requested_rsid: rsid,
            });
        }
        peer_addresses.push(address)
    }
    // box.cfg checks if the replication is already the same
    // and ignores it if nothing changed
    set_cfg_field("replication", peer_addresses)?;
    let lsn = crate::tarantool::eval("return box.info.lsn")?;
    Ok(Response { lsn })
}

/// Request to configure tarantool replication.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Request {
    pub replicaset_instances: Vec<InstanceId>,
    pub replicaset_id: String,
}
impl ::tarantool::tuple::Encode for Request {}

/// Response to [`replication::Request`].
///
/// [`replication::Request`]: Request
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Response {
    pub lsn: u64,
}
impl ::tarantool::tuple::Encode for Response {}

impl super::Request for Request {
    const PROC_NAME: &'static str = crate::stringify_cfunc!(proc_replication);
    type Response = Response;
}

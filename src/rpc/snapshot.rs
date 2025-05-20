use crate::storage::snapshot::SnapshotData;
use crate::storage::snapshot::SnapshotPosition;
use crate::storage::Catalog;
use crate::traft::RaftEntryId;
use crate::traft::Result;

crate::define_rpc_request! {
    fn proc_raft_snapshot_next_chunk(req: Request) -> Result<Response> {
        let storage = Catalog::try_get(false)?;
        let snapshot_data = storage.next_snapshot_data_chunk(
            req.entry_id,
            req.position,
        )?;
        Ok(Response { snapshot_data })
    }

    /// Request to get the next chunk of the raft snapshot data.
    pub struct Request {
        // TODO:
        // pub cluster_name: String,
        /// Entry id from the snapshot metadata received from raft.
        pub entry_id: RaftEntryId,

        /// Position of the read view iterator from which the snapshot chunks
        /// should resume.
        pub position: SnapshotPosition,
    }

    pub struct Response {
        pub snapshot_data: SnapshotData,
    }
}

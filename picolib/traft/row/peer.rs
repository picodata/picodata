#[derive(serde::Deserialize)]
pub struct Peer {
    pub raft_id: u64,
    pub uri: String,
}

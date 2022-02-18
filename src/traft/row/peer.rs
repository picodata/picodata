use tarantool::tlua;

#[derive(Debug, serde::Deserialize, tlua::Push, tlua::PushInto, tlua::LuaRead)]
pub struct Peer {
    pub raft_id: u64,
    pub uri: String,
}

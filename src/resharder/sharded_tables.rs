use smol_str::SmolStr;
use tarantool::index::IndexId;
use tarantool::space::SpaceId;
use tarantool::tuple::TupleBuffer;

/// Helper struct which stores information about a sharded table relevant during
/// resharding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ShardedTableInfo {
    /// Table id.
    pub id: SpaceId,

    /// Table name.
    pub name: SmolStr,

    /// Index id of the index over bucket ids.
    pub sharding_index_id: IndexId,

    /// Index of the column storing bucket id in the table's format.
    /// Used to extract bucket_id from table tuples.
    pub bucket_id_column_index: u32,
}

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, PartialEq, Eq)]
pub struct TableData {
    pub table_id: SpaceId,

    /// Msgpack encoded array of tuples from the given table
    #[serde(with = "serde_bytes")]
    pub tuples: TupleBuffer,
}
impl ::tarantool::tuple::Encode for TableData {}

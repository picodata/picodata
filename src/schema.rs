use serde::{Deserialize, Serialize};
use tarantool::{
    index::{IndexId, Part},
    space::{Field, SpaceId},
    tuple::{Encode, TupleBuffer},
};

/// Space definition.
///
/// Describes a user-defined space.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpaceDef {
    pub id: SpaceId,
    pub name: String,
    pub distribution: Distribution,
    pub format: Vec<Field>,
    pub schema_version: u64,
    pub operable: bool,
}
impl Encode for SpaceDef {}

/// Defines how to distribute tuples in a space across replicasets.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "kind")]
pub enum Distribution {
    /// Tuples will be replicated to each instance.
    Global,
    /// Tuples will be implicitely sharded. E.g. sent to the corresponding bucket
    /// which will be determined by a hash of the provided `sharding_key`.
    ShardedImplicitly {
        #[serde(with = "serde_bytes")]
        sharding_key: TupleBuffer,
        #[serde(default)]
        sharding_fn: ShardingFn,
    },
    /// Tuples will be explicitely sharded. E.g. sent to the bucket
    /// which id is provided by field that is specified here.
    ///
    /// Default field name: "bucket_id"
    ShardedByField {
        #[serde(default = "default_bucket_id_field")]
        field: String,
    },
}

fn default_bucket_id_field() -> String {
    "bucket_id".into()
}

/// Custom sharding functions are not yet supported.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum ShardingFn {
    Crc32,
    #[default]
    Murmur3,
    Xxhash,
    Md5,
}

/// Index definition.
///
/// Describes a user-defined index.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexDef {
    pub space_id: SpaceId,
    pub id: IndexId,
    pub name: String,
    pub local: bool,
    pub parts: Vec<Part>,
    pub schema_version: u64,
    pub operable: bool,
}
impl Encode for IndexDef {}

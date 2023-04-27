use crate::traft;
use serde::{Deserialize, Serialize};
use tarantool::{
    index::Metadata as IndexMetadata,
    index::{IndexId, Part},
    schema::space::SpaceMetadata,
    space::{Field, SpaceId},
    tuple::Encode,
    util::Value,
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

impl SpaceDef {
    // Don't forget to update this, if fields of `SpaceDef` change.
    pub const FIELD_OPERABLE: usize = 5;

    pub fn to_space_metadata(&self) -> traft::Result<SpaceMetadata> {
        use tarantool::session::uid;
        use tarantool::space::SpaceEngineType;

        // FIXME: this is copy pasted from tarantool::schema::space::create_space
        let format = self
            .format
            .iter()
            .map(|f| {
                IntoIterator::into_iter([
                    ("name".into(), Value::Str(f.name.as_str().into())),
                    ("type".into(), Value::Str(f.field_type.as_str().into())),
                    ("is_nullable".into(), Value::Bool(f.is_nullable)),
                ])
                .collect()
            })
            .collect();

        let space_def = SpaceMetadata {
            id: self.id,
            // Do we want to be more explicit about user_id?
            user_id: uid()? as _,
            name: self.name.as_str().into(),
            engine: SpaceEngineType::Memtx,
            field_count: 0,
            flags: Default::default(),
            format,
        };

        Ok(space_def)
    }
}

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
        sharding_key: Vec<String>,
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

::tarantool::define_str_enum! {
    /// Custom sharding functions are not yet supported.
    #[derive(Default)]
    pub enum ShardingFn {
        Crc32 = "crc32",
        #[default]
        Murmur3 = "murmur3",
        Xxhash = "xxhash",
        Md5 = "md5",
    }
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

impl IndexDef {
    // Don't forget to update this, if fields of `IndexDef` change.
    pub const FIELD_OPERABLE: usize = 6;

    pub fn to_index_metadata(&self) -> traft::Result<IndexMetadata> {
        use tarantool::index::IndexType;

        let index_meta = IndexMetadata {
            space_id: self.space_id,
            index_id: self.id,
            name: self.name.as_str().into(),
            r#type: IndexType::Tree,
            opts: Default::default(),
            parts: self.parts.clone(),
        };

        Ok(index_meta)
    }
}

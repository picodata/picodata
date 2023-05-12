use crate::storage::set_pico_schema_version;
use crate::traft;
use crate::traft::op::Ddl;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;
use tarantool::{
    index::Metadata as IndexMetadata,
    index::{IndexId, Part},
    schema::space::SpaceMetadata,
    space::{Field, SpaceId},
    space::{Space, SystemSpace},
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
    pub unique: bool,
}
impl Encode for IndexDef {}

impl IndexDef {
    // Don't forget to update this, if fields of `IndexDef` change.
    pub const FIELD_OPERABLE: usize = 6;

    pub fn to_index_metadata(&self) -> IndexMetadata {
        use tarantool::index::IndexType;

        let mut opts = BTreeMap::new();
        opts.insert(Cow::from("unique"), Value::Bool(self.unique));
        let index_meta = IndexMetadata {
            space_id: self.space_id,
            index_id: self.id,
            name: self.name.as_str().into(),
            r#type: IndexType::Tree,
            opts,
            parts: self.parts.clone(),
        };

        index_meta
    }
}

pub fn ddl_abort_on_master(ddl: &Ddl, version: u64) -> traft::Result<()> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_space = Space::from(SystemSpace::Space);
    let sys_index = Space::from(SystemSpace::Index);

    match *ddl {
        Ddl::CreateSpace { id, .. } => {
            sys_index.delete(&[id, 1])?;
            sys_index.delete(&[id, 0])?;
            sys_space.delete(&[id])?;
            set_pico_schema_version(version)?;
        }
        _ => {
            todo!();
        }
    }

    Ok(())
}

// TODO: this should be a TryFrom in tarantool-module
pub fn try_space_field_type_to_index_field_type(
    ft: tarantool::space::FieldType,
) -> Option<tarantool::index::FieldType> {
    use tarantool::index::FieldType as IFT;
    use tarantool::space::FieldType as SFT;
    let res = match ft {
        SFT::Any => None,
        SFT::Unsigned => Some(IFT::Unsigned),
        SFT::String => Some(IFT::String),
        SFT::Number => Some(IFT::Number),
        SFT::Double => Some(IFT::Double),
        SFT::Integer => Some(IFT::Integer),
        SFT::Boolean => Some(IFT::Boolean),
        SFT::Varbinary => Some(IFT::Varbinary),
        SFT::Scalar => Some(IFT::Scalar),
        SFT::Decimal => Some(IFT::Decimal),
        SFT::Uuid => Some(IFT::Uuid),
        SFT::Datetime => Some(IFT::Datetime),
        SFT::Interval => None,
        SFT::Array => Some(IFT::Array),
        SFT::Map => None,
    };
    res
}

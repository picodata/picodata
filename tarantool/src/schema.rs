//! Box: schema
use serde::{Serialize, Serializer};
use serde_json::{Map, Value};

use crate::error::Error;
use crate::index::IteratorType;
use crate::space::{Space, SystemSpace};
use crate::tuple::{AsTuple, Tuple};

/// Type of engine, used by space.
#[derive(Copy, Clone, Debug)]
pub enum SpaceEngineType {
    Memtx,
    Vinyl,
}

impl Serialize for SpaceEngineType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            SpaceEngineType::Memtx => serializer.serialize_str("memtx"),
            SpaceEngineType::Vinyl => serializer.serialize_str("vinyl"),
        }
    }
}

/// SpaceInternal is tuple, holding space metadata in system `_space` space.
#[derive(Serialize, Debug)]
pub struct SpaceMetadata {
    pub id: u32,
    pub uid: u32,
    pub name: String,
    pub engine: SpaceEngineType,
    pub field_count: u32,
    pub options: Map<String, Value>,
    pub format: Vec<Value>,
}

impl AsTuple for SpaceMetadata {}

/// Type of index.
#[derive(Copy, Clone, Debug, Serialize)]
pub enum IndexType {
    Hash,
    Tree,
    Bitset,
    Rtree,
}

/// Type of index part.
#[derive(Copy, Clone, Debug, Serialize)]
pub enum IndexFieldType {
    Unsigned,
    String,
    Integer,
    Number,
    Double,
    Decimal,
    Boolean,
    Varbinary,
    Uuid,
    Array,
    Scalar,
}

/// Index part.
#[derive(Serialize)]
pub struct IndexPart {
    pub field_index: u32,
    pub field_type: IndexFieldType,
    pub collation: Option<String>,
    pub is_nullable: Option<bool>,
    pub path: Option<String>,
}

/// Type of distance for retree index.
#[derive(Copy, Clone, Debug, Serialize)]
pub enum RtreeIndexDistanceType {
    Euclid,
    Manhattan,
}

/// Revoke all privileges associated with the given object.
///
/// - `obj_type` - string representation of object's type. Can be one of the following: "space", "sequence" or "function".
/// - `obj_id` - object's ID
pub fn revoke_object_privileges(obj_type: &str, obj_id: u32) -> Result<(), Error> {
    let sys_vpriv: Space = SystemSpace::VPriv.into();
    let mut sys_priv: Space = SystemSpace::Priv.into();

    let index_obj = sys_vpriv.index("object").unwrap();
    let privs: Vec<Tuple> = index_obj
        .select(IteratorType::Eq, &(obj_type, obj_id))?
        .collect();

    for t in privs {
        let uid = t.field::<u32>(1)?.unwrap();
        sys_priv.delete(&(uid,))?;
    }

    Ok(())
}

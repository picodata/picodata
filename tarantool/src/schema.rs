//! Box: schema

use crate::serde::{Serialize, Serializer};
use crate::serde_json::{Map, Value};

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

/// SpaceInternal is tuple, holdiing space metdata in system `_space` space.
/// For details see internal Space::insert_new_space function.
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

/// Revoke all privileges associated with the given object.
///
/// - `obj_type` - string representation of object's type. Can be one of the following: "space", "sequence" or "function".
/// - `obj_id` - object's ID
pub fn revoke_object_priveleges(obj_type: &str, obj_id: u32) -> Result<(), Error> {
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

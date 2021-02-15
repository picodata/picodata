//! Box: schema

use crate::serde_json::{Map, Value};

use crate::error::Error;
use crate::index::IteratorType;
use crate::space::{Space, SystemSpace};
use crate::tuple::{AsTuple, Tuple};

/// SpaceInternal is tuple, hodiing space metdata in system `_space` space.
/// For details see internal Space::insert_new_space function.
#[derive(Serialize)]
pub struct SpaceMetadata {
    pub id: u32,
    pub uid: u32,
    pub name: String,
    pub engine: String,
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

pub mod index;
pub mod sequence;
pub mod space;

use crate::error::Error;
use crate::index::IteratorType;
use crate::space::{Space, SystemSpace};
use crate::tuple::Tuple;

fn resolve_user_or_role(user: &str) -> Result<Option<u32>, Error> {
    let space_vuser: Space = SystemSpace::VUser.into();
    let name_idx = space_vuser.index("name").unwrap();
    Ok(match name_idx.get(&(user,))? {
        None => None,
        Some(user_tuple) => Some(user_tuple.field::<u32>(0)?.unwrap()),
    })
}

/// Revoke all privileges associated with the given object.
///
/// - `obj_type` - string representation of object's type. Can be one of the following: "space", "sequence" or "function".
/// - `obj_id` - object's ID
fn revoke_object_privileges(obj_type: &str, obj_id: u32) -> Result<(), Error> {
    let sys_vpriv: Space = SystemSpace::VPriv.into();
    let sys_priv: Space = SystemSpace::Priv.into();

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

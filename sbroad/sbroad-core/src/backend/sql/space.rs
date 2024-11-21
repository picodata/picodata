use std::fmt::Debug;

use crate::errors::SbroadError;
use crate::executor::ir::ExecutionPlan;
use crate::executor::protocol::VTablesMeta;
use crate::ir::node::NodeId;
use crate::ir::relation::SpaceEngine;

#[cfg(not(feature = "mock"))]
mod prod_imports {
    pub use crate::backend::sql::space::ADMIN_ID;
    pub use crate::error;
    pub use crate::errors::{Action, Entity, SbroadError};
    pub use crate::executor::engine::helpers::{pk_name, table_name};
    pub use crate::executor::ir::ExecutionPlan;
    use crate::executor::protocol::VTablesMeta;
    pub use crate::ir::node::NodeId;
    pub use crate::ir::relation::SpaceEngine;
    pub use smol_str::{format_smolstr, SmolStr};
    pub use tarantool::index::{FieldType, IndexOptions, IndexType, Part};
    pub use tarantool::session::with_su;
    pub use tarantool::space::{Field, Space, SpaceCreateOptions, SpaceType};

    pub fn create_tmp_space_impl(
        exec_plan: &ExecutionPlan,
        plan_id: &str,
        motion_id: NodeId,
        engine: &SpaceEngine,
        vtables_meta: Option<&VTablesMeta>,
    ) -> Result<SmolStr, SbroadError> {
        let cleanup = |space: Space, name: &str| match with_su(ADMIN_ID, || space.drop()) {
            Ok(_) => {}
            Err(e) => {
                error!(
                    Option::from("Temporary space"),
                    &format!("Failed to drop {name}: {e}")
                );
            }
        };

        let table_name = table_name(plan_id, motion_id);

        // If the space already exists, it is possible that admin has
        // populated it with data (by mistake?). Clean the space up.
        if let Some(space) = Space::find(table_name.as_str()) {
            cleanup(space, table_name.as_str());
        }

        let mut fields: Vec<Field> = if let Some(vtables_meta) = vtables_meta {
            let meta = vtables_meta
                .get(&motion_id)
                .expect("invalid execution plan");
            meta.columns
                .iter()
                .map(|c| Field::from(c.clone()))
                .collect()
        } else {
            let vtable = exec_plan.get_motion_vtable(motion_id)?;
            vtable
                .get_columns()
                .iter()
                .map(|c| Field::from(c.clone()))
                .collect()
        };

        let pk_name = pk_name(plan_id, motion_id);
        fields.push(Field::unsigned(pk_name.clone()));
        let fields_len = fields.len() as u32;
        // Vinyl engine does not support temporary spaces.
        let space_type = match engine {
            SpaceEngine::Memtx => SpaceType::Temporary,
            SpaceEngine::Vinyl => SpaceType::Normal,
        };
        let options = SpaceCreateOptions {
            format: Some(fields),
            engine: engine.into(),
            space_type,
            if_not_exists: false,
            ..Default::default()
        };

        let pk = IndexOptions {
            r#type: Some(IndexType::Tree),
            unique: Some(true),
            parts: Some(vec![Part::new(fields_len, FieldType::Unsigned)]),
            if_not_exists: Some(false),
            ..Default::default()
        };

        let space = with_su(ADMIN_ID, || -> Result<Space, SbroadError> {
            Space::create(table_name.as_str(), &options).map_err(|e| {
                SbroadError::FailedTo(
                    Action::Create,
                    Some(Entity::Space),
                    format_smolstr!("{table_name}: {e}"),
                )
            })
        })??;
        let create_index_res = with_su(ADMIN_ID, || space.create_index(&pk_name, &pk));
        match create_index_res {
            Ok(Ok(_)) => {}
            Err(e) | Ok(Err(e)) => {
                cleanup(space, table_name.as_str());
                return Err(SbroadError::FailedTo(
                    Action::Create,
                    Some(Entity::Index),
                    format_smolstr!("{pk_name} for space {table_name}: {e}"),
                ));
            }
        }
        Ok(table_name)
    }
}

#[cfg(not(feature = "mock"))]
use prod_imports::*;
use smol_str::SmolStr;

#[allow(clippy::module_name_repetitions)]
#[derive(Hash, Eq, PartialEq, Debug)]
pub struct TableGuard {
    pub name: SmolStr,
    do_truncate: bool,
}

impl TableGuard {
    pub fn skip_truncate(&mut self) {
        self.do_truncate = false;
    }
}

pub const ADMIN_ID: u32 = 1;

/// Create a temporary space to store the intermediate results of the query.
///
/// # Errors
/// - The space with the same name but different schema already exists.
pub fn create_table(
    exec_plan: &ExecutionPlan,
    plan_id: &str,
    motion_id: NodeId,
    engine: &SpaceEngine,
    #[allow(unused_variables)] vtables_meta: Option<&VTablesMeta>,
) -> Result<TableGuard, SbroadError> {
    #[cfg(not(feature = "mock"))]
    {
        let name = create_tmp_space_impl(exec_plan, plan_id, motion_id, engine, vtables_meta)?;
        Ok(TableGuard {
            name,
            do_truncate: true,
        })
    }
    #[cfg(feature = "mock")]
    {
        use crate::executor::engine::helpers::table_name;

        let (_, _) = (exec_plan, engine);
        let name = table_name(plan_id, motion_id);
        Ok(TableGuard {
            name,
            do_truncate: true,
        })
    }
}

impl Drop for TableGuard {
    fn drop(&mut self) {
        #[cfg(not(feature = "mock"))]
        {
            if !self.do_truncate {
                return;
            }
            let space_find_res = with_su(ADMIN_ID, || Space::find(&self.name));
            if let Ok(Some(space)) = space_find_res {
                let space_drop_res = with_su(ADMIN_ID, || space.truncate());
                if let Err(e) = space_drop_res {
                    error!(
                        Option::from("Temporary space"),
                        &format!("Failed to drop {} space: {e}", self.name)
                    );
                }
            } else {
                error!(
                    Option::from("Temporary space"),
                    &format!("Failed to find {} space for drop", self.name)
                );
            }
        }
    }
}

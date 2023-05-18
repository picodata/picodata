use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};

use tarantool::space::FieldType;
use tarantool::{
    index::Metadata as IndexMetadata,
    index::{IndexId, Part},
    schema::space::SpaceMetadata,
    space::SpaceId,
    space::{Space, SystemSpace},
    tlua::{self, LuaRead},
    tuple::Encode,
    util::Value,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::compare_and_swap;
use crate::storage::{set_pico_schema_version, Clusterwide, ClusterwideSpace, PropertyName};
use crate::traft::op::{Ddl, DdlBuilder, Op};
use crate::traft::{self, event, node, rpc, RaftIndex};

/// Space definition.
///
/// Describes a user-defined space.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpaceDef {
    pub id: SpaceId,
    pub name: String,
    pub distribution: Distribution,
    pub format: Vec<tarantool::space::Field>,
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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, LuaRead)]
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

#[derive(Debug, Error)]
pub enum DdlError {
    #[error("space creation failed: {0}")]
    CreateSpace(#[from] CreateSpaceError),
    #[error("ddl operation was aborted")]
    Aborted,
}

#[derive(Debug, Error)]
pub enum CreateSpaceError {
    #[error("space with id {0} already exists")]
    IdExists(SpaceId),
    #[error("space with name {0} already exists")]
    NameExists(String),
    #[error("several fields have the same name: {0}")]
    DuplicateFieldName(String),
    #[error("no field with name: {0}")]
    FieldUndefined(String),
}

// TODO: Add `LuaRead` to tarantool::space::Field and use it
#[derive(Clone, Debug, LuaRead)]
pub struct Field {
    pub name: String, // TODO(gmoshkin): &str
    pub r#type: FieldType,
    pub is_nullable: bool,
}

impl From<Field> for tarantool::space::Field {
    fn from(field: Field) -> Self {
        tarantool::space::Field {
            name: field.name,
            field_type: field.r#type,
            is_nullable: field.is_nullable,
        }
    }
}

#[derive(Clone, Debug, LuaRead)]
pub struct CreateSpaceParams {
    id: Option<SpaceId>,
    name: String,
    format: Vec<Field>,
    primary_key: Vec<String>,
    distribution: Distribution,
}

impl CreateSpaceParams {
    pub fn validate(&self, storage: &Clusterwide) -> traft::Result<()> {
        // Check that there is no space with this name
        if storage.spaces.by_name(&self.name)?.is_some() {
            return Err(
                DdlError::CreateSpace(CreateSpaceError::NameExists(self.name.clone())).into(),
            );
        }
        // Check that there is no space with this id (if specified)
        if let Some(id) = self.id {
            if storage.spaces.get(id)?.is_some() {
                return Err(DdlError::CreateSpace(CreateSpaceError::IdExists(id)).into());
            }
        }
        // All field names are unique
        let mut field_names = HashSet::new();
        for field in &self.format {
            if !field_names.insert(field.name.as_str()) {
                return Err(DdlError::CreateSpace(CreateSpaceError::DuplicateFieldName(
                    field.name.clone(),
                ))
                .into());
            }
        }
        // All primary key components exist in fields
        for part in &self.primary_key {
            if !field_names.contains(part.as_str()) {
                return Err(
                    DdlError::CreateSpace(CreateSpaceError::FieldUndefined(part.clone())).into(),
                );
            }
        }
        // All sharding key components exist in fields
        match &self.distribution {
            Distribution::Global => (),
            Distribution::ShardedImplicitly { sharding_key, .. } => {
                let mut parts = HashSet::new();
                for part in sharding_key {
                    if !field_names.contains(part.as_str()) {
                        return Err(DdlError::CreateSpace(CreateSpaceError::FieldUndefined(
                            part.clone(),
                        ))
                        .into());
                    }
                    // And all parts are unique
                    if !parts.insert(part.as_str()) {
                        return Err(DdlError::CreateSpace(CreateSpaceError::DuplicateFieldName(
                            part.clone(),
                        ))
                        .into());
                    }
                }
            }
            Distribution::ShardedByField { field } => {
                if !field_names.contains(field.as_str()) {
                    return Err(DdlError::CreateSpace(CreateSpaceError::FieldUndefined(
                        field.clone(),
                    ))
                    .into());
                }
            }
        }
        Ok(())
    }

    pub fn into_ddl(self, _storage: &Clusterwide) -> Ddl {
        let id = if let Some(id) = self.id {
            id
        } else {
            todo!("max_id + 1");
        };
        let primary_key: Vec<_> = self.primary_key.into_iter().map(Part::field).collect();
        let format: Vec<_> = self
            .format
            .into_iter()
            .map(tarantool::space::Field::from)
            .collect();
        Ddl::CreateSpace {
            id,
            name: self.name,
            format,
            primary_key,
            distribution: self.distribution,
        }
    }
}

/// Waits for a pending ddl to be either `Committed` or `Aborted` by the governor.
///
/// Returns an index of the corresponding `DdlCommit` entry.
///
/// If `timeout` is reached earlier returns an error.
pub fn wait_for_ddl_commit(
    prepare_commit: RaftIndex,
    timeout: Duration,
) -> traft::Result<RaftIndex> {
    let raft_storage = &node::global()?.raft_storage;
    let deadline = Instant::now() + timeout;
    let last_seen = prepare_commit;
    loop {
        let cur_applied = raft_storage.applied()?.expect("commit is always persisted");
        let new_entries = raft_storage.entries(last_seen + 1, cur_applied + 1)?;
        for entry in new_entries {
            if entry.entry_type != raft::prelude::EntryType::EntryNormal {
                continue;
            }
            let index = entry.index;
            let op = entry.into_op().unwrap_or(Op::Nop);
            match op {
                Op::DdlCommit => return Ok(index),
                Op::DdlAbort => return Err(DdlError::Aborted.into()),
                _ => (),
            }
        }

        if let Some(timeout) = deadline.checked_duration_since(Instant::now()) {
            event::wait_timeout(event::Event::EntryApplied, timeout)?;
        } else {
            return Err(traft::error::Error::Timeout);
        }
    }
}

/// Waits until there is no pending schema change.
///
/// If `timeout` is reached earlier returns an error.
fn wait_for_no_pending_schema_change(
    storage: &Clusterwide,
    timeout: Duration,
) -> traft::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if storage.properties.pending_schema_change()?.is_none() {
            return Ok(());
        }

        if let Some(timeout) = deadline.checked_duration_since(Instant::now()) {
            event::wait_timeout(event::Event::EntryApplied, timeout)?;
        } else {
            return Err(traft::error::Error::Timeout);
        }
    }
}

/// Prepares a ddl for execution on a cluster-wide schema
/// by proposing DdlPrepare Raft entry.
/// After that ddl will be either `Committed` or `Aborted` by the governor.
///
/// If `timeout` is reached earlier returns an error.
pub fn prepare_ddl(op: Ddl, timeout: Duration) -> traft::Result<RaftIndex> {
    loop {
        let storage = &node::global()?.storage;
        let raft_storage = &node::global()?.raft_storage;
        let op = DdlBuilder::new(storage)?.with_op(op.clone());
        wait_for_no_pending_schema_change(storage, timeout)?;
        let index = node::global()?.wait_for_read_state(timeout)?;
        let term = raft::Storage::term(raft_storage, index)?;
        let predicate = rpc::cas::Predicate {
            index,
            term,
            ranges: vec![
                rpc::cas::Range::new(ClusterwideSpace::Property)
                    .eq((PropertyName::PendingSchemaChange,)),
                rpc::cas::Range::new(ClusterwideSpace::Property)
                    .eq((PropertyName::CurrentSchemaVersion,)),
                rpc::cas::Range::new(ClusterwideSpace::Property)
                    .eq((PropertyName::NextSchemaVersion,)),
            ],
        };
        let (index, term) = compare_and_swap(op, predicate)?;
        rpc::sync::wait_for_index_timeout(index, raft_storage, timeout)?;
        if raft::Storage::term(raft_storage, index)? != term {
            // leader switched - retry
            continue;
        }
        return Ok(index);
    }
}

mod tests {
    use tarantool::space::FieldType;

    use super::*;

    #[::tarantool::test]
    fn ddl() {
        let storage = Clusterwide::new().unwrap();
        let existing_space = "existing_space";
        let existing_id = 0;
        storage
            .spaces
            .insert(&SpaceDef {
                id: existing_id,
                name: existing_space.into(),
                distribution: Distribution::Global,
                format: vec![],
                schema_version: 0,
                operable: true,
            })
            .unwrap();

        let new_space = "new_space";
        let new_id = 1;
        let field1 = Field {
            name: "field1".into(),
            r#type: FieldType::Any,
            is_nullable: false,
        };
        let field2 = Field {
            name: "field2".into(),
            r#type: FieldType::Any,
            is_nullable: false,
        };

        let err = CreateSpaceParams {
            id: Some(existing_id),
            name: new_space.into(),
            format: vec![],
            primary_key: vec![],
            distribution: Distribution::Global,
        }
        .validate(&storage)
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "ddl failed: space creation failed: space with id 0 already exists"
        );

        let err = CreateSpaceParams {
            id: Some(new_id),
            name: existing_space.into(),
            format: vec![],
            primary_key: vec![],
            distribution: Distribution::Global,
        }
        .validate(&storage)
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "ddl failed: space creation failed: space with name existing_space already exists"
        );

        let err = CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1.clone(), field1.clone()],
            primary_key: vec![],
            distribution: Distribution::Global,
        }
        .validate(&storage)
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "ddl failed: space creation failed: several fields have the same name: field1"
        );

        let err = CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1.clone()],
            primary_key: vec![field2.name.clone()],
            distribution: Distribution::Global,
        }
        .validate(&storage)
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "ddl failed: space creation failed: no field with name: field2"
        );

        let err = CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1.clone()],
            primary_key: vec![],
            distribution: Distribution::ShardedImplicitly {
                sharding_key: vec![field2.name.clone()],
                sharding_fn: ShardingFn::Md5,
            },
        }
        .validate(&storage)
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "ddl failed: space creation failed: no field with name: field2"
        );

        let err = CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1.clone()],
            primary_key: vec![],
            distribution: Distribution::ShardedImplicitly {
                sharding_key: vec![field1.name.clone(), field1.name.clone()],
                sharding_fn: ShardingFn::Md5,
            },
        }
        .validate(&storage)
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "ddl failed: space creation failed: several fields have the same name: field1"
        );

        let err = CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1.clone()],
            primary_key: vec![],
            distribution: Distribution::ShardedByField {
                field: field2.name.clone(),
            },
        }
        .validate(&storage)
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "ddl failed: space creation failed: no field with name: field2"
        );

        CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1, field2.clone()],
            primary_key: vec![field2.name.clone()],
            distribution: Distribution::ShardedByField { field: field2.name },
        }
        .validate(&storage)
        .unwrap();
    }
}

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use tarantool::auth::AuthDef;
use tarantool::fiber;
use tarantool::space::{FieldType, SpaceCreateOptions, SpaceEngineType};
use tarantool::space::{Metadata as SpaceMetadata, Space, SpaceType, SystemSpace};
use tarantool::transaction::{transaction, TransactionError};
use tarantool::{
    index::IteratorType,
    index::Metadata as IndexMetadata,
    index::{IndexId, Part},
    space::SpaceId,
    tlua::{self, LuaRead},
    tuple::Encode,
    util::Value,
};

use serde::{Deserialize, Serialize};

use crate::cas::{self, compare_and_swap};
use crate::storage::SPACE_ID_INTERNAL_MAX;
use crate::storage::{Clusterwide, ClusterwideSpace, PropertyName};
use crate::traft::error::Error;
use crate::traft::op::{Ddl, Op};
use crate::traft::{self, event, node, RaftIndex};

////////////////////////////////////////////////////////////////////////////////
// SpaceDef
////////////////////////////////////////////////////////////////////////////////

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
    pub engine: SpaceEngineType,
}

impl Encode for SpaceDef {}

impl SpaceDef {
    // Don't forget to update this, if fields of `SpaceDef` change.
    pub const FIELD_OPERABLE: usize = 5;

    pub fn to_space_metadata(&self) -> traft::Result<SpaceMetadata> {
        use tarantool::session::uid;

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

        let mut flags = BTreeMap::new();
        if matches!(self.distribution, Distribution::Global) {
            flags.insert("group_id".into(), 1.into());
        }

        let space_def = SpaceMetadata {
            id: self.id,
            // Do we want to be more explicit about user_id?
            user_id: uid()? as _,
            name: self.name.as_str().into(),
            engine: self.engine,
            field_count: 0,
            flags,
            format,
        };

        Ok(space_def)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Distribution
////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////
// IndexDef
////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////
// UserDef
////////////////////////////////////////////////////////////////////////////////

/// User definition.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserDef {
    pub id: UserId,
    pub name: String,
    pub schema_version: u64,
    pub auth: AuthDef,
}

pub type UserId = u32;

impl Encode for UserDef {}

impl UserDef {
    pub const FIELD_AUTH: usize = 3;
}

////////////////////////////////////////////////////////////////////////////////
// RoleDef
////////////////////////////////////////////////////////////////////////////////

/// Role definition.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoleDef {
    pub id: UserId,
    pub name: String,
    pub schema_version: u64,
}

impl Encode for RoleDef {}

////////////////////////////////////////////////////////////////////////////////
// PrivilegeDef
////////////////////////////////////////////////////////////////////////////////

/// Privilege definition.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PrivilegeDef {
    /// Id of the user or role to whom the privilege is granted.
    ///
    /// In tarantool users and roles are stored in the same space, which means a
    /// role and a user cannot have the same id or name.
    pub grantor_id: UserId,
    pub grantee_id: UserId,
    pub object_type: String,
    pub object_name: String,
    pub privilege: String,
    pub schema_version: u64,
}

impl Encode for PrivilegeDef {}

////////////////////////////////////////////////////////////////////////////////
// ...
////////////////////////////////////////////////////////////////////////////////

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

#[derive(Debug, thiserror::Error)]
pub enum DdlError {
    #[error("{0}")]
    CreateSpace(#[from] CreateSpaceError),
    #[error("ddl operation was aborted")]
    Aborted,
    #[error("there is no pending ddl operation")]
    NoPendingDdl,
}

#[derive(Debug, thiserror::Error)]
pub enum CreateSpaceError {
    #[error("space with id {id} exists with a different name '{actual_name}', but expected '{expected_name}'")]
    ExistsWithDifferentName {
        id: SpaceId,
        expected_name: String,
        actual_name: String,
    },
    #[error("several fields have the same name: {0}")]
    DuplicateFieldName(String),
    #[error("no field with name: {0}")]
    FieldUndefined(String),
    #[error("distribution is `sharded`, but neither `by_field` nor `sharding_key` is set")]
    ShardingPolicyUndefined,
    #[error("only one of sharding policy fields (`by_field`, `sharding_key`) should be set")]
    ConflictingShardingPolicy,
    #[error("global spaces only support memtx engine")]
    IncompatibleGlobalSpaceEngine,
}

impl From<CreateSpaceError> for Error {
    fn from(err: CreateSpaceError) -> Self {
        DdlError::CreateSpace(err).into()
    }
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

::tarantool::define_str_enum! {
    #[derive(Default)]
    pub enum DistributionParam {
        #[default]
        Global = "global",
        Sharded = "sharded",
    }
}

#[derive(Clone, Debug, LuaRead)]
pub struct CreateSpaceParams {
    pub(crate) id: Option<SpaceId>,
    pub(crate) name: String,
    pub(crate) format: Vec<Field>,
    pub(crate) primary_key: Vec<String>,
    pub(crate) distribution: DistributionParam,
    pub(crate) by_field: Option<String>,
    pub(crate) sharding_key: Option<Vec<String>>,
    pub(crate) sharding_fn: Option<ShardingFn>,
    pub(crate) engine: Option<SpaceEngineType>,
    /// Timeout in seconds.
    ///
    /// Specifying the timeout identifies how long user is ready to wait for ddl to be applied.
    /// But it does not provide guarantees that a ddl will be aborted if wait for commit timeouts.
    pub timeout: Option<f64>,
}

impl CreateSpaceParams {
    /// Checks if space described by options already exists. Returns an error if
    /// the space with given id exists, but has a different name.
    pub fn space_exists(&self) -> traft::Result<bool> {
        // The check is performed using `box.space` API, so that local spaces are counted too.
        let sys_space = Space::from(SystemSpace::Space);

        let Some(id) = self.id else {
            let sys_space_by_name = sys_space
                .index_cached("name")
                .expect("_space should have an index by name");
            let t = sys_space_by_name
                .get(&[&self.name])
                .expect("reading from _space shouldn't fail");
            return Ok(t.is_some());
        };

        let t = sys_space
            .get(&[id])
            .expect("reading from _space shouldn't fail");
        let Some(t) = t else {
            return Ok(false);
        };

        let existing_name: &str = t.get("name").expect("space metadata should contain a name");
        if existing_name == self.name {
            return Ok(true);
        } else {
            // TODO: check everything else is the same
            // https://git.picodata.io/picodata/picodata/picodata/-/issues/331
            return Err(CreateSpaceError::ExistsWithDifferentName {
                id,
                expected_name: self.name.clone(),
                actual_name: existing_name.into(),
            }
            .into());
        }
    }

    pub fn validate(&self) -> traft::Result<()> {
        // Check space id fits in the allowed range
        if let Some(id) = self.id {
            if id <= SPACE_ID_INTERNAL_MAX {
                crate::tlog!(Warning, "requested space id {id} is in the range 0..={SPACE_ID_INTERNAL_MAX} reserved for future use by picodata, you may have a conflict in a future version");
            }
        }
        // All field names are unique
        let mut field_names = HashSet::new();
        for field in &self.format {
            if !field_names.insert(field.name.as_str()) {
                return Err(CreateSpaceError::DuplicateFieldName(field.name.clone()).into());
            }
        }
        // All primary key components exist in fields
        for part in &self.primary_key {
            if !field_names.contains(part.as_str()) {
                return Err(CreateSpaceError::FieldUndefined(part.clone()).into());
            }
        }
        // Global spaces must have memtx engine
        if self.distribution == DistributionParam::Global
            && self.engine.is_some_and(|e| e != SpaceEngineType::Memtx)
        {
            return Err(CreateSpaceError::IncompatibleGlobalSpaceEngine.into());
        }
        // All sharding key components exist in fields
        if self.distribution == DistributionParam::Sharded {
            match (&self.by_field, &self.sharding_key) {
                (Some(by_field), None) => {
                    if !field_names.contains(by_field.as_str()) {
                        return Err(CreateSpaceError::FieldUndefined(by_field.clone()).into());
                    }
                    if self.sharding_fn.is_some() {
                        crate::tlog!(
                            Warning,
                            "`sharding_fn` is specified but will be ignored, as sharding `by_field` is set"
                        );
                    }
                }
                (None, Some(sharding_key)) => {
                    let mut parts = HashSet::new();
                    for part in sharding_key {
                        if !field_names.contains(part.as_str()) {
                            return Err(CreateSpaceError::FieldUndefined(part.clone()).into());
                        }
                        // And all parts are unique
                        if !parts.insert(part.as_str()) {
                            return Err(CreateSpaceError::DuplicateFieldName(part.clone()).into());
                        }
                    }
                }
                (None, None) => return Err(CreateSpaceError::ShardingPolicyUndefined.into()),
                (Some(_), Some(_)) => {
                    return Err(CreateSpaceError::ConflictingShardingPolicy.into())
                }
            }
        } else {
            if self.by_field.is_some() {
                crate::tlog!(
                    Warning,
                    "`by_field` is specified but will be ignored, as `distribution` is `global`"
                );
            }
            if self.sharding_key.is_some() {
                crate::tlog!(
                    Warning,
                    "`sharding_key` is specified but will be ignored, as `distribution` is `global`"
                );
            }
            if self.sharding_fn.is_some() {
                crate::tlog!(
                    Warning,
                    "`sharding_fn` is specified but will be ignored, as `distribution` is `global`"
                );
            }
        }
        Ok(())
    }

    /// Create space and then rollback.
    ///
    /// Should be used for checking if a space with these params can be created.
    pub fn test_create_space(&self) -> traft::Result<()> {
        let id = self.id.expect("space id should've been chosen by now");
        let err = transaction(|| -> Result<(), Option<tarantool::error::Error>> {
            ::tarantool::schema::space::create_space(
                &self.name,
                &SpaceCreateOptions {
                    if_not_exists: false,
                    engine: self.engine.unwrap_or_default(),
                    id: Some(id),
                    field_count: self.format.len() as u32,
                    user: None,
                    space_type: SpaceType::Normal,
                    format: Some(
                        self.format
                            .iter()
                            .cloned()
                            .map(tarantool::space::Field::from)
                            .collect(),
                    ),
                },
            )
            .map_err(Some)?;
            // Rollback space creation
            Err(None)
        })
        .unwrap_err();
        match err {
            // Space was successfully created and rolled back
            TransactionError::RolledBack(None) => Ok(()),
            // Space creation failed
            TransactionError::RolledBack(Some(err)) => Err(err.into()),
            // Error during commit or rollback
            err => panic!("transaction mechanism should not fail: {err:?}"),
        }
    }

    /// Chooses an id for the new space if it's not set yet and sets `self.id`.
    pub fn choose_id_if_not_specified(&mut self) -> traft::Result<()> {
        let sys_space = Space::from(SystemSpace::Space);

        let id = if let Some(id) = self.id {
            id
        } else {
            let mut id = SPACE_ID_INTERNAL_MAX;

            let mut taken_ids = Vec::with_capacity(32);
            // IteratorType::All iterates in an order of ascending space ids
            for space in sys_space.select(IteratorType::All, &())? {
                let space_id: SpaceId = space
                    .field(0)?
                    .expect("space metadata should contain a space_id");
                taken_ids.push(space_id);
            }

            // Find the first accessible space id.
            for space_id in taken_ids {
                // We aren't forcing users to not use internal range, so we have
                // to ignore those
                if space_id <= SPACE_ID_INTERNAL_MAX {
                    continue;
                }
                if space_id != id + 1 {
                    break;
                }
                id += 1;
            }
            id + 1
        };
        self.id = Some(id);
        Ok(())
    }

    pub fn into_ddl(self) -> traft::Result<Ddl> {
        let id = self.id.expect("space id should've been chosen by now");
        let primary_key: Vec<_> = self.primary_key.into_iter().map(Part::field).collect();
        let format: Vec<_> = self
            .format
            .into_iter()
            .map(tarantool::space::Field::from)
            .collect();
        let distribution = match self.distribution {
            DistributionParam::Global => Distribution::Global,
            DistributionParam::Sharded => {
                if let Some(field) = self.by_field {
                    Distribution::ShardedByField { field }
                } else {
                    Distribution::ShardedImplicitly {
                        sharding_key: self
                            .sharding_key
                            .expect("should be checked during `validate`"),
                        sharding_fn: self.sharding_fn.unwrap_or_default(),
                    }
                }
            }
        };
        let res = Ddl::CreateSpace {
            id,
            name: self.name,
            format,
            primary_key,
            distribution,
            engine: self.engine.unwrap_or_default(),
        };
        Ok(res)
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
    let deadline = fiber::clock().saturating_add(timeout);
    let last_seen = prepare_commit;
    loop {
        let cur_applied = raft_storage.applied()?;
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

        if event::wait_deadline(event::Event::EntryApplied, deadline)?.is_timeout() {
            return Err(Error::Timeout);
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
    let deadline = fiber::clock().saturating_add(timeout);
    loop {
        if storage.properties.pending_schema_change()?.is_none() {
            return Ok(());
        }

        if event::wait_deadline(event::Event::EntryApplied, deadline)?.is_timeout() {
            return Err(Error::Timeout);
        }
    }
}

/// Prepares an `op` for execution on a cluster-wide schema
/// by proposing a corresponding Raft entry.
/// Retries entry proposal if leader changes until the entry is added to the log.
/// Waits for any pending schema change to finalize.
///
/// If `timeout` is reached earlier returns an error.
// TODO: Use deadline instead of timeout
pub fn prepare_schema_change(op: Op, timeout: Duration) -> traft::Result<RaftIndex> {
    debug_assert!(op.is_schema_change());

    loop {
        let node = node::global()?;
        let storage = &node.storage;
        let raft_storage = &node.raft_storage;
        let mut op = op.clone();
        op.set_schema_version(storage.properties.next_schema_version()?);
        wait_for_no_pending_schema_change(storage, timeout)?;
        let index = node::global()?
            .read_index(timeout)
            .map_err(|e| Error::other(format!("read_index failed: {e}")))?;
        let term = raft::Storage::term(raft_storage, index)?;
        let predicate = cas::Predicate {
            index,
            term,
            ranges: cas::schema_change_ranges().into(),
        };
        let (index, term) = compare_and_swap(op, predicate, timeout)?;
        node.wait_index(index, timeout)?;
        if raft::Storage::term(raft_storage, index)? != term {
            // leader switched - retry
            continue;
        }
        return Ok(index);
    }
}

/// Aborts a pending DDL operation and waits for abort to be committed localy.
/// If `timeout` is reached earlier returns an error.
///
/// Returns an index of the corresponding DdlAbort raft entry, or an error if
/// there is no pending DDL operation.
pub fn abort_ddl(timeout: Duration) -> traft::Result<RaftIndex> {
    let node = node::global()?;
    loop {
        if node.storage.properties.pending_schema_change()?.is_none() {
            return Err(DdlError::NoPendingDdl.into());
        }
        let index = node.get_index();
        let term = raft::Storage::term(&node.raft_storage, index)?;
        #[rustfmt::skip]
        let predicate = cas::Predicate {
            index,
            term,
            ranges: vec![
                cas::Range::new(ClusterwideSpace::Property).eq([PropertyName::PendingSchemaChange]),
                cas::Range::new(ClusterwideSpace::Property).eq([PropertyName::GlobalSchemaVersion]),
                cas::Range::new(ClusterwideSpace::Property).eq([PropertyName::NextSchemaVersion]),
            ],
        };
        let (index, term) = compare_and_swap(Op::DdlAbort, predicate, timeout)?;
        node.wait_index(index, timeout)?;
        if raft::Storage::term(&node.raft_storage, index)? != term {
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
    fn test_create_space() {
        CreateSpaceParams {
            id: Some(1337),
            name: "friends_of_peppa".into(),
            format: vec![
                Field {
                    name: "id".into(),
                    r#type: FieldType::Number,
                    is_nullable: false,
                },
                Field {
                    name: "name".into(),
                    r#type: FieldType::String,
                    is_nullable: false,
                },
            ],
            primary_key: vec![],
            distribution: DistributionParam::Global,
            by_field: None,
            sharding_key: None,
            sharding_fn: None,
            engine: None,
            timeout: None,
        }
        .test_create_space()
        .unwrap();
        assert!(tarantool::space::Space::find("friends_of_peppa").is_none());

        CreateSpaceParams {
            id: Some(1337),
            name: "friends_of_peppa".into(),
            format: vec![],
            primary_key: vec![],
            distribution: DistributionParam::Sharded,
            by_field: None,
            sharding_key: None,
            sharding_fn: None,
            engine: Some(SpaceEngineType::Vinyl),
            timeout: None,
        }
        .test_create_space()
        .unwrap();
        assert!(tarantool::space::Space::find("friends_of_peppa").is_none());

        let err = CreateSpaceParams {
            id: Some(0),
            name: "friends_of_peppa".into(),
            format: vec![],
            primary_key: vec![],
            distribution: DistributionParam::Global,
            by_field: None,
            sharding_key: None,
            sharding_fn: None,
            engine: None,
            timeout: None,
        }
        .test_create_space()
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "tarantool error: CreateSpace: Failed to create space 'friends_of_peppa': space id 0 is reserved"
        );
    }

    #[::tarantool::test]
    fn ddl() {
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
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1.clone(), field1.clone()],
            primary_key: vec![],
            distribution: DistributionParam::Global,
            by_field: None,
            sharding_key: None,
            sharding_fn: None,
            engine: None,
            timeout: None,
        }
        .validate()
        .unwrap_err();
        assert_eq!(err.to_string(), "several fields have the same name: field1");

        let err = CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1.clone()],
            primary_key: vec![field2.name.clone()],
            distribution: DistributionParam::Global,
            by_field: None,
            sharding_key: None,
            sharding_fn: None,
            engine: None,
            timeout: None,
        }
        .validate()
        .unwrap_err();
        assert_eq!(err.to_string(), "no field with name: field2");

        let err = CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1.clone()],
            primary_key: vec![],
            distribution: DistributionParam::Sharded,
            by_field: None,
            sharding_key: Some(vec![field2.name.clone()]),
            sharding_fn: None,
            engine: None,
            timeout: None,
        }
        .validate()
        .unwrap_err();
        assert_eq!(err.to_string(), "no field with name: field2");

        let err = CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1.clone()],
            primary_key: vec![],
            distribution: DistributionParam::Sharded,
            by_field: None,
            sharding_key: Some(vec![field1.name.clone(), field1.name.clone()]),
            sharding_fn: None,
            engine: None,
            timeout: None,
        }
        .validate()
        .unwrap_err();
        assert_eq!(err.to_string(), "several fields have the same name: field1");

        let err = CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1.clone()],
            primary_key: vec![],
            distribution: DistributionParam::Sharded,
            by_field: Some(field2.name.clone()),
            sharding_key: None,
            sharding_fn: None,
            engine: None,
            timeout: None,
        }
        .validate()
        .unwrap_err();
        assert_eq!(err.to_string(), "no field with name: field2");

        let err = CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1.clone()],
            primary_key: vec![],
            distribution: DistributionParam::Sharded,
            by_field: None,
            sharding_key: None,
            sharding_fn: None,
            engine: None,
            timeout: None,
        }
        .validate()
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "distribution is `sharded`, but neither `by_field` nor `sharding_key` is set"
        );

        let err = CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1.clone()],
            primary_key: vec![],
            distribution: DistributionParam::Sharded,
            by_field: Some(field2.name.clone()),
            sharding_key: Some(vec![]),
            sharding_fn: None,
            engine: None,
            timeout: None,
        }
        .validate()
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "only one of sharding policy fields (`by_field`, `sharding_key`) should be set"
        );

        CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1.clone(), field2.clone()],
            primary_key: vec![field2.name.clone()],
            distribution: DistributionParam::Sharded,
            by_field: Some(field2.name.clone()),
            sharding_key: None,
            sharding_fn: None,
            engine: None,
            timeout: None,
        }
        .validate()
        .unwrap();

        let err = CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1, field2.clone()],
            primary_key: vec![field2.name],
            distribution: DistributionParam::Global,
            by_field: None,
            sharding_key: None,
            sharding_fn: None,
            engine: Some(SpaceEngineType::Vinyl),
            timeout: None,
        }
        .validate()
        .unwrap_err();
        assert_eq!(err.to_string(), "global spaces only support memtx engine");
    }
}

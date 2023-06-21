use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};

use tarantool::space::{FieldType, SpaceCreateOptions, SpaceEngineType};
use tarantool::transaction::{transaction, TransactionError};
use tarantool::{
    index::Metadata as IndexMetadata,
    index::{IndexId, Part},
    schema::space::SpaceMetadata,
    space::SpaceId,
    tlua::{self, LuaRead},
    tuple::Encode,
    util::Value,
};

use serde::{Deserialize, Serialize};

use crate::cas::{self, compare_and_swap};
use crate::storage::ToEntryIter;
use crate::storage::SPACE_ID_INTERNAL_MAX;
use crate::storage::{Clusterwide, ClusterwideSpaceId, PropertyName};
use crate::traft::error::Error;
use crate::traft::op::{Ddl, Op};
use crate::traft::{self, event, node, RaftIndex};
use crate::util::instant_saturating_add;

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
            engine: SpaceEngineType::Memtx,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthDef {
    pub method: AuthMethod,
    /// Base64 encoded digest.
    pub data: String,
}

::tarantool::define_str_enum! {
    pub enum AuthMethod {
        ChapSha1 = "chap-sha1",
    }
}

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
    #[error("space creation failed: {0}")]
    CreateSpace(#[from] CreateSpaceError),
    #[error("ddl operation was aborted")]
    Aborted,
    #[error("there is no pending ddl operation")]
    NoPendingDdl,
}

#[derive(Debug, thiserror::Error)]
pub enum CreateSpaceError {
    #[error("space with id {0} already exists")]
    IdExists(SpaceId),
    #[error("space with name {0} already exists")]
    NameExists(String),
    #[error("several fields have the same name: {0}")]
    DuplicateFieldName(String),
    #[error("no field with name: {0}")]
    FieldUndefined(String),
    #[error("distribution is `sharded`, but neither `by_field` nor `sharding_key` is set")]
    ShardingPolicyUndefined,
    #[error("only one of sharding policy fields (`by_field`, `sharding_key`) should be set")]
    ConflictingShardingPolicy,
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
    id: Option<SpaceId>,
    name: String,
    format: Vec<Field>,
    primary_key: Vec<String>,
    distribution: DistributionParam,
    by_field: Option<String>,
    sharding_key: Option<Vec<String>>,
    sharding_fn: Option<ShardingFn>,
    /// Timeout in seconds.
    ///
    /// Specifying the timeout identifies how long user is ready to wait for ddl to be applied.
    /// But it does not provide guarantees that a ddl will be aborted if wait for commit timeouts.
    pub timeout: f64,
}

impl CreateSpaceParams {
    pub fn validate(self, storage: &Clusterwide) -> traft::Result<ValidCreateSpaceParams> {
        // Check that there is no space with this name
        if storage.spaces.by_name(&self.name)?.is_some() {
            return Err(CreateSpaceError::NameExists(self.name).into());
        }
        // Check that there is no space with this id (if specified)
        if let Some(id) = self.id {
            if storage.spaces.get(id)?.is_some() {
                return Err(CreateSpaceError::IdExists(id).into());
            }
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
        Ok(ValidCreateSpaceParams(self))
    }
}

#[derive(Debug)]
pub struct ValidCreateSpaceParams(CreateSpaceParams);

impl ValidCreateSpaceParams {
    /// Create space and then rollback.
    ///
    /// Should be used for checking if a space with these params can be created.
    pub fn test_create_space(&mut self, storage: &Clusterwide) -> traft::Result<()> {
        let id = self.id(storage)?;
        let params = &self.0;
        let err = transaction(|| -> Result<(), Option<tarantool::error::Error>> {
            ::tarantool::schema::space::create_space(
                &params.name,
                &SpaceCreateOptions {
                    if_not_exists: false,
                    engine: SpaceEngineType::Memtx,
                    id: Some(id),
                    field_count: params.format.len() as u32,
                    user: None,
                    is_local: false,
                    is_temporary: false,
                    is_sync: false,
                    format: Some(
                        params
                            .format
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

    /// Memoizes id if it is automatically selected.
    fn id(&mut self, storage: &Clusterwide) -> traft::Result<SpaceId> {
        let id = if let Some(id) = self.0.id {
            id
        } else {
            let mut id = SPACE_ID_INTERNAL_MAX;
            // FIXME: space _bucket is the first space in the user space id range.
            // See https://git.picodata.io/picodata/picodata/picodata/-/issues/288
            id += 1;

            // ToEntryIter::iter iterates in an order of ascending space ids
            let iter = storage.spaces.iter()?;
            // Find the first accessible space id.
            for space in iter {
                // We aren't forcing users to not use internal range, so we have
                // to ignore those
                if space.id <= SPACE_ID_INTERNAL_MAX {
                    continue;
                }
                if space.id != id + 1 {
                    break;
                }
                id += 1;
            }
            id + 1
        };
        self.0.id = Some(id);
        Ok(id)
    }

    pub fn into_ddl(mut self, storage: &Clusterwide) -> traft::Result<Ddl> {
        let id = self.id(storage)?;
        let primary_key: Vec<_> = self.0.primary_key.into_iter().map(Part::field).collect();
        let format: Vec<_> = self
            .0
            .format
            .into_iter()
            .map(tarantool::space::Field::from)
            .collect();
        let distribution = match self.0.distribution {
            DistributionParam::Global => Distribution::Global,
            DistributionParam::Sharded => {
                if let Some(field) = self.0.by_field {
                    Distribution::ShardedByField { field }
                } else {
                    Distribution::ShardedImplicitly {
                        sharding_key: self
                            .0
                            .sharding_key
                            .expect("should be checked during `validate`"),
                        sharding_fn: self.0.sharding_fn.unwrap_or_default(),
                    }
                }
            }
        };
        let res = Ddl::CreateSpace {
            id,
            name: self.0.name,
            format,
            primary_key,
            distribution,
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
    let deadline = instant_saturating_add(Instant::now(), timeout);
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

        if let Some(timeout) = deadline.checked_duration_since(Instant::now()) {
            event::wait_timeout(event::Event::EntryApplied, timeout)?;
        } else {
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
    let deadline = instant_saturating_add(Instant::now(), timeout);
    loop {
        if storage.properties.pending_schema_change()?.is_none() {
            return Ok(());
        }

        if let Some(timeout) = deadline.checked_duration_since(Instant::now()) {
            event::wait_timeout(event::Event::EntryApplied, timeout)?;
        } else {
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
            ranges: vec![
                cas::Range::new(ClusterwideSpaceId::Property as _)
                    .eq((PropertyName::PendingSchemaChange,)),
                cas::Range::new(ClusterwideSpaceId::Property as _)
                    .eq((PropertyName::PendingSchemaVersion,)),
                cas::Range::new(ClusterwideSpaceId::Property as _)
                    .eq((PropertyName::GlobalSchemaVersion,)),
                cas::Range::new(ClusterwideSpaceId::Property as _)
                    .eq((PropertyName::NextSchemaVersion,)),
            ],
        };
        let (index, term) = compare_and_swap(op, predicate)?;
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
        let predicate = cas::Predicate {
            index,
            term,
            ranges: vec![
                cas::Range::new(ClusterwideSpaceId::Property as _)
                    .eq((PropertyName::PendingSchemaChange,)),
                cas::Range::new(ClusterwideSpaceId::Property as _)
                    .eq((PropertyName::GlobalSchemaVersion,)),
                cas::Range::new(ClusterwideSpaceId::Property as _)
                    .eq((PropertyName::NextSchemaVersion,)),
            ],
        };
        let (index, term) = compare_and_swap(Op::DdlAbort, predicate)?;
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
        let storage = Clusterwide::new().unwrap();
        ValidCreateSpaceParams(CreateSpaceParams {
            id: None,
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
            timeout: 0.0,
        })
        .test_create_space(&storage)
        .unwrap();
        assert!(tarantool::space::Space::find("friends_of_peppa").is_none());

        let err = ValidCreateSpaceParams(CreateSpaceParams {
            id: Some(0),
            name: "friends_of_peppa".into(),
            format: vec![],
            primary_key: vec![],
            distribution: DistributionParam::Global,
            by_field: None,
            sharding_key: None,
            sharding_fn: None,
            timeout: 0.0,
        })
        .test_create_space(&storage)
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "tarantool error: CreateSpace: Failed to create space 'friends_of_peppa': space id 0 is reserved"
        );
    }

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
            distribution: DistributionParam::Global,
            by_field: None,
            sharding_key: None,
            sharding_fn: None,
            timeout: 0.0,
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
            distribution: DistributionParam::Global,
            by_field: None,
            sharding_key: None,
            sharding_fn: None,
            timeout: 0.0,
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
            distribution: DistributionParam::Global,
            by_field: None,
            sharding_key: None,
            sharding_fn: None,
            timeout: 0.0,
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
            distribution: DistributionParam::Global,
            by_field: None,
            sharding_key: None,
            sharding_fn: None,
            timeout: 0.0,
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
            distribution: DistributionParam::Sharded,
            by_field: None,
            sharding_key: Some(vec![field2.name.clone()]),
            sharding_fn: None,
            timeout: 0.0,
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
            distribution: DistributionParam::Sharded,
            by_field: None,
            sharding_key: Some(vec![field1.name.clone(), field1.name.clone()]),
            sharding_fn: None,
            timeout: 0.0,
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
            distribution: DistributionParam::Sharded,
            by_field: Some(field2.name.clone()),
            sharding_key: None,
            sharding_fn: None,
            timeout: 0.0,
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
            distribution: DistributionParam::Sharded,
            by_field: None,
            sharding_key: None,
            sharding_fn: None,
            timeout: 0.0,
        }
        .validate(&storage)
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "ddl failed: space creation failed: distribution is `sharded`, but neither `by_field` nor `sharding_key` is set"
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
            timeout: 0.0,
        }
        .validate(&storage)
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "ddl failed: space creation failed: only one of sharding policy fields (`by_field`, `sharding_key`) should be set"
        );

        CreateSpaceParams {
            id: Some(new_id),
            name: new_space.into(),
            format: vec![field1, field2.clone()],
            primary_key: vec![field2.name.clone()],
            distribution: DistributionParam::Sharded,
            by_field: Some(field2.name),
            sharding_key: None,
            sharding_fn: None,
            timeout: 0.0,
        }
        .validate(&storage)
        .unwrap();
    }
}

use crate::audit::policy::AuditPolicyId;
use crate::plugin::PluginIdentifier;
use crate::schema::{
    Distribution, IndexDef, IndexOption, PrivilegeDef, RoutineLanguage, RoutineParams,
    RoutineSecurity, UserDef, ADMIN_ID, GUEST_ID, PICO_SERVICE_ID, PUBLIC_ID, ROLE_REPLICATION_ID,
    SUPER_ID,
};
use crate::sql::storage::GlobalDeleteInfo;
use crate::storage::{self, Catalog};
use crate::storage::{space_by_name, RoutineId};
use crate::traft::error::Error as TRaftError;
use crate::traft::error::ErrorInfo;
use ::tarantool::auth::AuthDef;
use ::tarantool::index::{IndexId, Part};
use ::tarantool::space::{Field, SpaceId};
use ::tarantool::tlua;
use ::tarantool::tuple::{ToTupleBuffer, TupleBuffer};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use sql::ir::operator::ConflictStrategy;
use std::collections::BTreeMap;
use tarantool::datetime::Datetime;
use tarantool::error::{TarantoolError, TarantoolErrorCode};
use tarantool::ffi::datetime::MP_DATETIME;
use tarantool::index::IndexType;
use tarantool::msgpack::ExtStruct;
use tarantool::session::UserId;
use tarantool::space::SpaceEngineType;

////////////////////////////////////////////////////////////////////////////////
/// The operation on the raft state machine.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "kind")]
pub enum Op {
    /// No operation.
    Nop,
    /// Cluster-wide data modification operation.
    /// Should be used to manipulate the cluster-wide configuration.
    Dml(Dml),
    /// Batch cluster-wide data modification operation.
    // TODO: in case there's a large amount of operations in the batch we will
    // be duplicating a lot of the data. In particular the initiator fields is
    // going to be the same in 99.999% of the times, but we'll carry N copies of
    // it. The table is also likely to be duplicate in the batch. So it would be
    // better if we refactor the Dml operations somthing like this:
    // ```
    // struct DmlBatch {
    //     initiator: UserId,
    //     sub_batches: Vec<DmlSubBatch>,
    // }
    // struct DmlSubBatch {
    //     table_id: SpaceId,
    //     ops: Vec<DmlArgs>,
    // }
    // enum DmlArgs {
    //     Insert { tuple: TupleBuffer },
    //     Replace { tuple: TupleBuffer },
    //     Update { key: TupleBuffer, ops: Vec<TupleBuffer> },
    //     Delete { key: TupleBuffer },
    // }
    // ```
    // This will however reduce the performance in the single dml operation
    // case, because of the added indirection (the operation is now stored in a
    // vec inside a vec). To eliminate that inefficiency we could introduce a
    // `struct NonEmptyVec<T> { head: T, tail: Vec<T> }`, which will make it so
    // the first operation is always in the same memory chunk as the parent `Op`.
    BatchDml { ops: Vec<Dml> },
    /// Start cluster-wide data schema definition operation.
    /// Should be used to manipulate the cluster-wide schema.
    ///
    /// The provided DDL operation will be set as pending.
    /// Only one pending DDL operation can exist at the same time.
    DdlPrepare {
        schema_version: u64,
        ddl: Ddl,
        #[serde(default)]
        governor_op_id: Option<u64>,
    },
    /// Commit the pending DDL operation.
    ///
    /// Only one pending DDL operation can exist at the same time.
    ///
    /// If `tier` is specified, ddl should be applied only for
    /// instances of given tier. Otherwise only local_schema_version
    /// should be raised.
    DdlCommit,
    /// Abort the pending DDL operation.
    ///
    /// Only one pending DDL operation can exist at the same time.
    DdlAbort { cause: ErrorInfo },
    /// Cluster-wide access control list change operation.
    Acl(Acl),
    /// Plugin system change.
    Plugin(PluginRaftOp),
}

impl Eq for Op {}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return match self {
            Self::Nop => f.write_str("Nop"),
            Self::BatchDml { ops } => {
                write!(f, "BatchDml(")?;
                let mut ops = ops.iter();
                if let Some(first) = ops.next() {
                    write!(f, "{}", DisplayDml(first))?;
                }
                for next in ops {
                    write!(f, ", {}", DisplayDml(next))?;
                }
                write!(f, ")")?;
                Ok(())
            }
            Self::Dml(dml) => {
                write!(f, "{}", DisplayDml(dml))
            }
            Self::DdlPrepare {
                schema_version,
                ddl: Ddl::Backup { timestamp },
                ..
            } => {
                write!(f, "DdlPrepare({schema_version}, Backup({timestamp}))")
            }
            Self::DdlPrepare {
                schema_version,
                ddl: Ddl::CreateTable {
                    id, distribution, ..
                },
                ..
            } => {
                let distr = match distribution {
                    Distribution::Global => "Global",
                    Distribution::ShardedImplicitly { .. } => "ShardedImplicitly",
                    Distribution::ShardedByField { .. } => "ShardedByField",
                };
                write!(
                    f,
                    "DdlPrepare({schema_version}, CreateTable({id}, {distr}))"
                )
            }
            Self::DdlPrepare {
                schema_version,
                ddl: Ddl::DropTable { id, .. },
                ..
            } => {
                write!(f, "DdlPrepare({schema_version}, DropTable({id}))")
            }
            Self::DdlPrepare {
                schema_version,
                ddl:
                    Ddl::RenameTable {
                        table_id,
                        old_name,
                        new_name,
                        ..
                    },
                ..
            } => {
                write!(
                    f,
                    "DdlPrepare({schema_version}, RenameTable({table_id}, {old_name} -> {new_name}))"
                )
            }
            Self::DdlPrepare {
                schema_version,
                ddl: Ddl::TruncateTable { id, .. },
                ..
            } => {
                write!(f, "DdlPrepare({schema_version}, TruncateTable({id}))")
            }
            Self::DdlPrepare {
                schema_version,
                ddl: Ddl::ChangeFormat { table_id, .. },
                ..
            } => {
                write!(f, "DdlPrepare({schema_version}, ChangeFormat({table_id}))")
            }
            Self::DdlPrepare {
                schema_version,
                ddl: Ddl::CreateIndex {
                    space_id, index_id, ..
                },
                ..
            } => {
                write!(
                    f,
                    "DdlPrepare({schema_version}, CreateIndex({space_id}, {index_id}))"
                )
            }
            Self::DdlPrepare {
                schema_version,
                ddl: Ddl::DropIndex {
                    space_id, index_id, ..
                },
                ..
            } => {
                write!(
                    f,
                    "DdlPrepare({schema_version}, DropIndex({space_id}, {index_id}))"
                )
            }
            Self::DdlPrepare {
                schema_version,
                ddl:
                    Ddl::RenameIndex {
                        space_id,
                        index_id,
                        old_name,
                        new_name,
                        ..
                    },
                ..
            } => {
                write!(
                    f,
                    "DdlPrepare({schema_version}, RenameIndex({space_id}, {index_id}, {old_name} -> {new_name}))"
                )
            }
            Self::DdlPrepare {
                schema_version,
                ddl: Ddl::CreateProcedure { id, name, .. },
                ..
            } => {
                write!(
                    f,
                    "DdlPrepare({schema_version}, CreateProcedure({id}, {name}))"
                )
            }
            Self::DdlPrepare {
                schema_version,
                ddl: Ddl::DropProcedure { id, .. },
                ..
            } => {
                write!(f, "DdlPrepare({schema_version}, DropProcedure({id}))")
            }
            Self::DdlPrepare {
                schema_version,
                ddl:
                    Ddl::RenameProcedure {
                        routine_id,
                        old_name,
                        new_name,
                        ..
                    },
                ..
            } => {
                write!(
                    f,
                    "DdlPrepare({schema_version}, RenameProcedure({routine_id}, {old_name} -> {new_name}))"
                )
            }
            Self::DdlCommit => write!(f, "DdlCommit"),
            Self::DdlAbort { cause } => write!(f, "DdlAbort({cause})"),
            Self::Acl(Acl::CreateUser { user_def }) => {
                let UserDef {
                    id,
                    name,
                    schema_version,
                    ..
                } = user_def;
                write!(f, r#"CreateUser({schema_version}, {id}, "{name}")"#,)
            }
            Self::Acl(Acl::RenameUser {
                user_id,
                name,
                schema_version,
                ..
            }) => {
                write!(f, r#"RenameUser({schema_version}, {user_id}, "{name}")"#,)
            }
            Self::Acl(Acl::ChangeAuth {
                user_id,
                initiator,
                schema_version,
                ..
            }) => {
                write!(f, "ChangeAuth({schema_version}, {user_id}, {initiator})")
            }
            Self::Acl(Acl::DropUser {
                user_id,
                initiator,
                schema_version,
            }) => {
                write!(f, "DropUser({schema_version}, {user_id}, {initiator})")
            }
            Self::Acl(Acl::CreateRole { role_def }) => {
                let UserDef {
                    id,
                    name,
                    schema_version,
                    ..
                } = role_def;
                write!(f, r#"CreateRole({schema_version}, {id}, "{name}")"#,)
            }
            Self::Acl(Acl::DropRole {
                role_id,
                schema_version,
                ..
            }) => {
                write!(f, "DropRole({schema_version}, {role_id})")
            }
            Self::Acl(Acl::GrantPrivilege { priv_def }) => {
                let object_id = priv_def.object_id();

                write!(
                    f,
                    "GrantPrivilege({schema_version}, {grantor_id}, {grantee_id}, {object_type}, {object_id:?}, {privilege})",
                    schema_version = priv_def.schema_version(),
                    grantor_id = priv_def.grantor_id(),
                    grantee_id = priv_def.grantee_id(),
                    object_type = priv_def.object_type(),
                    privilege = priv_def.privilege(),
                )
            }
            Self::Acl(Acl::RevokePrivilege { priv_def, .. }) => {
                let object_id = priv_def.object_id();
                write!(
                    f,
                    "RevokePrivilege({schema_version}, {grantor_id}, {grantee_id}, {object_type}, {object_id:?}, {privilege})",
                    schema_version = priv_def.schema_version(),
                    grantor_id = priv_def.grantor_id(),
                    grantee_id = priv_def.grantee_id(),
                    object_type = priv_def.object_type(),
                    privilege = priv_def.privilege(), )
            }
            Self::Acl(Acl::AuditPolicy {
                user_id,
                policy_id,
                enable,
                schema_version,
                initiator,
            }) => {
                write!(
                    f,
                    "AuditPolicy({schema_version}, {user_id}, {policy_id}, {enable}, {initiator})"
                )
            }
            Self::Plugin(PluginRaftOp::DisablePlugin { ident, cause }) => {
                #[rustfmt::skip]
                write!(f, "DisablePlugin({ident}{})", DisplayCause(cause))?;

                struct DisplayCause<'a>(&'a Option<ErrorInfo>);
                impl std::fmt::Display for DisplayCause<'_> {
                    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                        if let Some(cause) = self.0 {
                            write!(f, ", cause: {cause}")?;
                        }
                        Ok(())
                    }
                }

                Ok(())
            }
            Self::Plugin(PluginRaftOp::DropPlugin { ident }) => {
                write!(f, "DropPlugin({ident})")
            }
            Self::Plugin(PluginRaftOp::Abort { cause }) => write!(f, "PluginAbort({cause})"),
            Self::Plugin(PluginRaftOp::PluginConfigPartialUpdate { ident, updates }) => {
                write!(f, "PluginConfigPartialUpdate({ident}, {updates:?})")
            }
        };

        struct DisplayDml<'a>(&'a Dml);
        impl std::fmt::Display for DisplayDml<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                let table = DisplayGlobalTable(self.0.table_id());
                match self.0 {
                    Dml::Insert { tuple, .. } => {
                        write!(f, "Insert({table}, {})", DisplayAsJson(tuple))
                    }
                    Dml::Replace { tuple, .. } => {
                        write!(f, "Replace({table}, {})", DisplayAsJson(tuple))
                    }
                    Dml::Update { key, ops, .. } => {
                        let key = DisplayAsJson(key);
                        let ops = DisplayAsJson(&**ops);
                        write!(f, "Update({table}, {key}, {ops})")
                    }
                    Dml::Delete { key, .. } => {
                        write!(f, "Delete({table}, {})", DisplayAsJson(key))
                    }
                }
            }
        }

        struct DisplayGlobalTable(SpaceId);
        impl std::fmt::Display for DisplayGlobalTable {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                if let Some(table_name) = storage::Catalog::system_space_name_by_id(self.0) {
                    f.write_str(table_name)
                } else {
                    self.0.fmt(f)
                }
            }
        }

        struct DisplayAsJson<T>(pub T);

        impl std::fmt::Display for DisplayAsJson<&TupleBuffer> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                if let Some(data) = rmp_serde::from_slice::<rmpv::Value>(self.0.as_ref())
                    .ok()
                    .and_then(|v| serde_json::to_string(&ValueWithTruncations(&v)).ok())
                {
                    return write!(f, "{data}");
                }

                write!(f, "{:?}", self.0)
            }
        }

        const TRUNCATION_THRESHOLD_FOR_STRING: usize = 100;
        const TRUNCATION_THRESHOLD_FOR_ARRAY: usize = 16;
        const TRUNCATION_THRESHOLD_FOR_MAP: usize = 10;
        struct ValueWithTruncations<'a>(&'a rmpv::Value);
        impl Serialize for ValueWithTruncations<'_> {
            #[inline]
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                use rmpv::Value;

                match self.0 {
                    Value::String(s) => {
                        let threshold = TRUNCATION_THRESHOLD_FOR_STRING;
                        if let Some(s) = s.as_str() {
                            if s.len() > threshold {
                                if let Some((i, _)) = s.char_indices().nth(threshold) {
                                    let s = format!("{}<TRUNCATED>...", &s[..i]);
                                    serializer.serialize_str(&s)
                                } else {
                                    serializer.serialize_str(s)
                                }
                            } else {
                                serializer.serialize_str(s)
                            }
                        } else {
                            let s = s.as_bytes();
                            let s = picodata_plugin::util::DisplayAsHexBytesLimitted(s).to_string();
                            serializer.serialize_str(&s)
                        }
                    }
                    Value::Array(v) => {
                        use serde::ser::SerializeSeq;
                        let threshold = TRUNCATION_THRESHOLD_FOR_ARRAY;
                        let mut seq = serializer.serialize_seq(Some(v.len()))?;
                        for item in v.iter().take(threshold) {
                            seq.serialize_element(&ValueWithTruncations(item))?;
                        }
                        if v.len() > threshold {
                            seq.serialize_element(&Value::from("<TRUNCATED>"))?;
                        }
                        seq.end()
                    }
                    Value::Map(m) => {
                        use serde::ser::SerializeMap;
                        let mut map = serializer.serialize_map(Some(m.len()))?;
                        let threshold = TRUNCATION_THRESHOLD_FOR_MAP;
                        for (k, v) in m.iter().take(threshold) {
                            map.serialize_entry(
                                &ValueWithTruncations(k),
                                &ValueWithTruncations(v),
                            )?;
                        }
                        if m.len() > threshold {
                            map.serialize_entry(
                                &Value::from("<TRUNCATED>"),
                                &Value::from("<TRUNCATED>"),
                            )?;
                        }
                        map.end()
                    }
                    Value::Ext(MP_DATETIME, bytes) => {
                        let res = Datetime::try_from(ExtStruct::new(MP_DATETIME, bytes));
                        match res {
                            Ok(datetime) => serializer.serialize_str(&datetime.to_string()),
                            Err(e) => serializer.serialize_str(&e),
                        }
                    }
                    other => other.serialize(serializer),
                }
            }
        }

        impl std::fmt::Display for DisplayAsJson<&[TupleBuffer]> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "[")?;
                if let Some(elem) = self.0.first() {
                    write!(f, "{}", DisplayAsJson(elem))?;
                }
                for elem in self.0.iter().skip(1) {
                    write!(f, ", {}", DisplayAsJson(elem))?;
                }
                write!(f, "]")
            }
        }
    }
}

impl Op {
    #[inline]
    pub fn is_schema_change(&self) -> bool {
        match self {
            Self::Nop
            | Self::Dml(_)
            | Self::DdlAbort { .. }
            | Self::DdlCommit { .. }
            | Self::BatchDml { .. }
            | Self::Plugin { .. } => false,
            Self::DdlPrepare { .. } | Self::Acl(_) => true,
        }
    }

    /// Returns `true` if this op finalizes a multi-phase DDL operation.
    ///
    /// For plugin operations see [`Self::is_plugin_op_finalizer`].
    #[inline(always)]
    pub fn is_ddl_finalizer(&self) -> bool {
        matches!(self, Self::DdlCommit { .. } | Self::DdlAbort { .. })
    }

    /// Returns `true` if this op finalizes a multi-phase plugin operation.
    #[inline(always)]
    pub fn is_plugin_op_finalizer(&self) -> bool {
        matches!(
            self,
            Self::Plugin(PluginRaftOp::DisablePlugin { cause: Some(_), .. })
                | Self::Plugin(PluginRaftOp::Abort { .. })
        )
    }

    #[inline]
    pub fn single_dml_or_batch(ops: Vec<Dml>) -> Self {
        if ops.len() == 1 {
            // Yay rust!
            let dml = ops.into_iter().next().expect("just made sure it's there");
            // No need to wrap it in a BatchDml and confuse the users
            Self::Dml(dml)
        } else {
            Self::BatchDml { ops }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Dml
////////////////////////////////////////////////////////////////////////////////

/// Cluster-wide data modification operation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "op_kind")]
pub enum Dml {
    Insert {
        table: SpaceId,
        #[serde(with = "serde_bytes")]
        tuple: TupleBuffer,
        initiator: UserId,
        #[serde(default)]
        conflict_strategy: ConflictStrategy,
    },
    Replace {
        table: SpaceId,
        #[serde(with = "serde_bytes")]
        tuple: TupleBuffer,
        initiator: UserId,
    },
    Update {
        table: SpaceId,
        /// Key in primary index
        #[serde(with = "serde_bytes")]
        key: TupleBuffer,
        #[serde(with = "vec_of_raw_byte_buf")]
        ops: Vec<TupleBuffer>,
        initiator: UserId,
    },
    Delete {
        table: SpaceId,
        /// Key in primary index
        #[serde(with = "serde_bytes")]
        key: TupleBuffer,
        initiator: UserId,
        metainfo: Option<GlobalDeleteInfo>,
    },
}

impl Dml {
    #[inline(always)]
    pub fn table_id(&self) -> SpaceId {
        match self {
            Dml::Insert { table, .. } => *table,
            Dml::Replace { table, .. } => *table,
            Dml::Update { table, .. } => *table,
            Dml::Delete { table, .. } => *table,
        }
    }

    #[inline(always)]
    pub fn initiator(&self) -> UserId {
        match self {
            Dml::Insert { initiator, .. } => *initiator,
            Dml::Replace { initiator, .. } => *initiator,
            Dml::Update { initiator, .. } => *initiator,
            Dml::Delete { initiator, .. } => *initiator,
        }
    }

    #[inline(always)]
    pub fn kind(&self) -> DmlKind {
        match self {
            Dml::Insert { .. } => DmlKind::Insert,
            Dml::Replace { .. } => DmlKind::Replace,
            Dml::Update { .. } => DmlKind::Update,
            Dml::Delete { .. } => DmlKind::Delete,
        }
    }
}

::tarantool::define_str_enum! {
    pub enum DmlKind {
        Insert = "insert",
        Replace = "replace",
        Update = "update",
        Delete = "delete",
    }
}

impl From<Dml> for Op {
    fn from(op: Dml) -> Op {
        Op::Dml(op)
    }
}

impl Dml {
    /// Serializes `tuple` and returns an [`Dml::Insert`] in case of success.
    #[inline(always)]
    pub fn insert(
        space: impl Into<SpaceId>,
        tuple: &impl ToTupleBuffer,
        initiator: UserId,
    ) -> tarantool::Result<Self> {
        let res = Self::Insert {
            table: space.into(),
            tuple: tuple.to_tuple_buffer()?,
            initiator,
            conflict_strategy: ConflictStrategy::DoFail,
        };
        Ok(res)
    }

    #[inline(always)]
    pub fn insert_with_on_conflict(
        space: impl Into<SpaceId>,
        tuple: &impl ToTupleBuffer,
        initiator: UserId,
        on_conflict: ConflictStrategy,
    ) -> tarantool::Result<Self> {
        let res = Self::Insert {
            table: space.into(),
            tuple: tuple.to_tuple_buffer()?,
            initiator,
            conflict_strategy: on_conflict,
        };
        Ok(res)
    }

    #[inline(always)]
    pub fn insert_raw(
        space: impl Into<SpaceId>,
        tuple: Vec<u8>,
        initiator: UserId,
    ) -> tarantool::Result<Self> {
        let res = Self::Insert {
            table: space.into(),
            tuple: TupleBuffer::try_from_vec(tuple)?,
            initiator,
            conflict_strategy: ConflictStrategy::DoFail,
        };
        Ok(res)
    }

    /// Serializes `tuple` and returns an [`Dml::Replace`] in case of success.
    #[inline(always)]
    pub fn replace(
        space: impl Into<SpaceId>,
        tuple: &impl ToTupleBuffer,
        initiator: UserId,
    ) -> tarantool::Result<Self> {
        let res = Self::Replace {
            table: space.into(),
            tuple: tuple.to_tuple_buffer()?,
            initiator,
        };
        Ok(res)
    }

    /// Serializes `key` and returns an [`Dml::Update`] in case of success.
    #[inline(always)]
    pub fn update(
        space: impl Into<SpaceId>,
        key: &impl ToTupleBuffer,
        ops: impl Into<Vec<TupleBuffer>>,
        initiator: UserId,
    ) -> tarantool::Result<Self> {
        let res = Self::Update {
            table: space.into(),
            key: key.to_tuple_buffer()?,
            ops: ops.into(),
            initiator,
        };
        Ok(res)
    }

    /// Serializes `key` and returns an [`Dml::Delete`] in case of success.
    #[inline(always)]
    pub fn delete(
        space: impl Into<SpaceId>,
        key: &impl ToTupleBuffer,
        initiator: UserId,
        metainfo: Option<GlobalDeleteInfo>,
    ) -> tarantool::Result<Self> {
        let res = Self::Delete {
            table: space.into(),
            key: key.to_tuple_buffer()?,
            initiator,
            metainfo,
        };
        Ok(res)
    }

    /// Parse lua arguments to an api function such as `pico.cas`.
    pub fn from_lua_args(op: DmlInLua, initiator: UserId) -> Result<Self, String> {
        let space = space_by_name(&op.table).map_err(|e| e.to_string())?;
        let table = space.id();
        match op.kind {
            DmlKind::Insert => {
                let Some(tuple) = op.tuple else {
                    return Err("insert operation must have a tuple".into());
                };
                Ok(Self::Insert {
                    table,
                    tuple,
                    initiator,
                    conflict_strategy: ConflictStrategy::DoFail,
                })
            }
            DmlKind::Replace => {
                let Some(tuple) = op.tuple else {
                    return Err("replace operation must have a tuple".into());
                };
                Ok(Self::Replace {
                    table,
                    tuple,
                    initiator,
                })
            }
            DmlKind::Update => {
                let Some(key) = op.key else {
                    return Err("update operation must have a key".into());
                };
                let Some(ops) = op.ops else {
                    return Err("update operation must have ops".into());
                };
                Ok(Self::Update {
                    table,
                    key,
                    ops,
                    initiator,
                })
            }
            DmlKind::Delete => {
                let Some(key) = op.key else {
                    return Err("delete operation must have a key".into());
                };
                Ok(Self::Delete {
                    table,
                    key,
                    initiator,
                    metainfo: None,
                })
            }
        }
    }
}

/// Represents a lua table describing a [`Dml`] operation.
///
/// This is only used to parse lua arguments from lua api functions such as
/// `pico.cas`.
#[derive(Clone, Debug, PartialEq, Eq, tlua::LuaRead)]
pub struct DmlInLua {
    pub table: String,
    pub kind: DmlKind,
    pub tuple: Option<TupleBuffer>,
    pub key: Option<TupleBuffer>,
    pub ops: Option<Vec<TupleBuffer>>,
}

#[derive(Clone, Debug, PartialEq, Eq, tlua::LuaRead)]
pub struct BatchDmlInLua {
    pub ops: Vec<DmlInLua>,
}

////////////////////////////////////////////////////////////////////////////////
// Ddl
////////////////////////////////////////////////////////////////////////////////

/// Represents Ddl operations performed on the cluster.
///
/// Note: for the purpose of audit log in some variants we keep initiator field.
/// For Create<...> operations initiator and owner are the same,
/// so owner is used to avoid duplication.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "kind")]
pub enum Ddl {
    CreateTable {
        id: SpaceId,
        name: SmolStr,
        format: Vec<Field>,
        primary_key: Vec<Part<String>>,
        distribution: Distribution,
        engine: SpaceEngineType,
        owner: UserId,
    },
    DropTable {
        id: SpaceId,
        initiator: UserId,
    },
    TruncateTable {
        id: SpaceId,
        initiator: UserId,
    },
    CreateIndex {
        space_id: SpaceId,
        index_id: IndexId,
        name: SmolStr,
        ty: IndexType,
        opts: Vec<IndexOption>,
        by_fields: Vec<Part<String>>,
        initiator: UserId,
    },
    DropIndex {
        space_id: SpaceId,
        index_id: IndexId,
        initiator: UserId,
    },
    RenameIndex {
        space_id: SpaceId,
        index_id: IndexId,
        old_name: SmolStr,
        new_name: SmolStr,
        initiator_id: UserId,
        owner_id: UserId,
        schema_version: u64,
    },
    CreateProcedure {
        id: RoutineId,
        name: SmolStr,
        params: RoutineParams,
        language: RoutineLanguage,
        body: SmolStr,
        security: RoutineSecurity,
        owner: UserId,
    },
    DropProcedure {
        id: RoutineId,
        initiator: UserId,
    },
    RenameProcedure {
        routine_id: u32,
        old_name: SmolStr,
        new_name: SmolStr,
        initiator_id: UserId,
        owner_id: UserId,
        schema_version: u64,
    },
    RenameTable {
        table_id: u32,
        old_name: SmolStr,
        new_name: SmolStr,
        initiator_id: UserId,
        owner_id: UserId,
        schema_version: u64,
    },
    ChangeFormat {
        table_id: SpaceId,
        new_format: Vec<Field>,
        old_format: Vec<Field>,
        column_renames: RenameMapping,
        initiator_id: UserId,
        schema_version: u64,
    },
    Backup {
        timestamp: i64,
    },
}

impl Ddl {
    pub fn is_truncate_on_global_table(&self, storage: &Catalog) -> bool {
        let Ddl::TruncateTable { id, .. } = self else {
            return false;
        };

        let table_def = storage
            .pico_table
            .get(*id)
            .expect("table definition should decode correctly")
            .expect("should be called with valid table ids");

        table_def.distribution.is_global()
    }
}

/// Builds [`RenameMapping`] by adding renames one-by-one
#[derive(Debug, Default)]
pub struct RenameMappingBuilder {
    // this is a "new -> old" (sic!) mapping
    map: BTreeMap<SmolStr, SmolStr>,
}

impl RenameMappingBuilder {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    /// Add a rename from `old` to `new`
    ///
    /// NOTE: this function does not check for bijectivity of the constructed mapping, which will
    ///   lead to incorrect results if not satisfied.
    ///
    /// This function will correctly handle chained renames.
    ///
    /// ```
    /// use std::collections::BTreeMap;
    /// use smol_str::SmolStr;
    /// use picodata::traft::op::RenameMappingBuilder;
    /// let mut builder = RenameMappingBuilder::new();
    /// builder.add_rename("older", "intermediate");
    /// builder.add_rename("intermediate", "new");
    /// let mapping = builder.build();
    ///
    /// assert_eq!(mapping.into_map(), BTreeMap::from([("older".into(), "new".into())]));
    /// ```
    pub fn add_rename(&mut self, old: impl Into<SmolStr>, new: impl Into<SmolStr>) {
        let old = old.into();
        let new = new.into();

        // check if there was already a rename "older -> old"
        if let Some(older) = self.map.remove(&old) {
            // short-circuit the rename to be "older -> new" directly if this is a case
            self.map.insert(new, older);
        } else {
            self.map.insert(new, old);
        }
    }

    pub fn build(self) -> RenameMapping {
        RenameMapping {
            // reverse the mapping, as `RenameMapping` is "old to new"
            // this is fine to do as long as user only supplies us with a bijective mapping
            map: self.map.into_iter().map(|(old, new)| (new, old)).collect(),
        }
    }
}

/// Represents a batch rename operation
///
/// Stores a bijectional mapping from old names to new names
///
/// Created with [`RenameMappingBuilder`]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct RenameMapping {
    map: BTreeMap<SmolStr, SmolStr>,
}

impl RenameMapping {
    /// Extract the underlying map from old to new names
    pub fn into_map(self) -> BTreeMap<SmolStr, SmolStr> {
        self.map
    }

    /// Reverse the mapping from (old -> new) to (new -> old), which can be used to execute a rename in inverse.
    ///
    /// NOTE: this will not work correctly if the stored `map` is not bijective
    pub fn reversed(&self) -> Self {
        let map = self
            .map
            .iter()
            .map(|(old, new)| (new.clone(), old.clone()))
            .collect();
        Self { map }
    }

    /// Apply a rename to a string. Returns whether `name` was present in the mapping
    ///
    /// ```
    /// use std::collections::BTreeMap;
    /// use picodata::traft::op::RenameMappingBuilder;
    /// let mut builder = RenameMappingBuilder::new();
    /// builder.add_rename("old_name", "new_name");
    /// let mapping = builder.build();
    ///
    /// // gets renamed
    /// let mut existing_name = "old_name".to_string();
    /// assert_eq!(mapping.transform_name(&mut existing_name), true);
    /// assert_eq!(existing_name.as_str(), "new_name");
    ///
    /// // stays the same
    /// let mut nonexisting_name = "weird_name".to_string();
    /// assert_eq!(mapping.transform_name(&mut nonexisting_name), false);
    /// assert_eq!(nonexisting_name.as_str(), "weird_name");
    /// ```
    pub fn transform_name<S>(&self, name: &mut S) -> bool
    where
        S: AsRef<str>,
        S: From<SmolStr>,
    {
        if let Some(new_name) = self.map.get(name.as_ref()) {
            *name = new_name.clone().into();
            true
        } else {
            false
        }
    }

    /// Apply a rename to a [`Distribution`] by renaming the table columns
    // NOTE: the name mentions columns specifically because distribution also mentions `tier` by name
    // and tiers can also be renamed (theoretically)
    pub fn transform_distribution_columns(&self, distribution: &mut Distribution) -> bool {
        let mut result = false;

        match distribution {
            Distribution::Global => {}
            Distribution::ShardedImplicitly { sharding_key, .. } => {
                for part in sharding_key {
                    result |= self.transform_name(part);
                }
            }
            Distribution::ShardedByField { field, .. } => {
                result |= self.transform_name(field);
            }
        }

        result
    }

    /// Apply a rename to an [`IndexDef`] by renaming the table columns
    pub fn transform_index_columns(&self, index_def: &mut IndexDef) -> bool {
        let mut result = false;

        for part in &mut index_def.parts {
            if part.path.is_some() {
                unimplemented!("renaming `part` of `IndexDef` is not implemented")
            }
            result |= self.transform_name(&mut part.field);
        }

        result
    }
}

/// Builder for [`Op::DdlPrepare`] operations.
///
/// # Example
/// ```no_run
/// use picodata::traft::op::{DdlBuilder, Ddl};
///
/// // Assuming that space `1` was created.
/// let op = DdlBuilder::with_schema_version(1)
///     .with_op(Ddl::DropTable { id: 1, initiator: 1 });
/// ```
pub struct DdlBuilder {
    schema_version: u64,
}

impl DdlBuilder {
    pub fn new(storage: &Catalog) -> super::Result<Self> {
        let version = storage.properties.next_schema_version()?;
        Ok(Self::with_schema_version(version))
    }

    /// Sets current schema version.
    pub fn with_schema_version(version: u64) -> Self {
        Self {
            schema_version: version,
        }
    }

    pub fn with_op(&self, op: Ddl) -> Op {
        Op::DdlPrepare {
            schema_version: self.schema_version,
            ddl: op,
            governor_op_id: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Acl
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "op_kind")]
pub enum Acl {
    /// Create a tarantool user. Grant it default privileges.
    CreateUser { user_def: UserDef },

    /// Rename a tarantool user.
    RenameUser {
        user_id: UserId,
        name: SmolStr,
        initiator: UserId,
        schema_version: u64,
    },

    /// Update the tarantool user's authentication details (e.g. password).
    ChangeAuth {
        user_id: UserId,
        auth: AuthDef,
        initiator: UserId,
        schema_version: u64,
    },

    /// Drop a tarantool user and any entities owned by it.
    DropUser {
        user_id: UserId,
        initiator: UserId,
        schema_version: u64,
    },

    /// Create a tarantool role. Grant it default privileges.
    CreateRole { role_def: UserDef },

    /// Drop a tarantool role and revoke it from any grantees.
    DropRole {
        role_id: UserId,
        initiator: UserId,
        schema_version: u64,
    },

    /// Grant some privilege to a user or a role.
    GrantPrivilege { priv_def: PrivilegeDef },

    /// Revoke some privilege from a user or a role.
    RevokePrivilege {
        priv_def: PrivilegeDef,
        initiator: UserId,
    },

    /// Enable/disable audit logging for an user.
    AuditPolicy {
        user_id: UserId,
        policy_id: AuditPolicyId,
        enable: bool,
        initiator: UserId,
        schema_version: u64,
    },
}

impl Acl {
    pub fn schema_version(&self) -> u64 {
        match self {
            Self::CreateUser { user_def } => user_def.schema_version,
            Self::RenameUser { schema_version, .. } => *schema_version,
            Self::ChangeAuth { schema_version, .. } => *schema_version,
            Self::DropUser { schema_version, .. } => *schema_version,
            Self::CreateRole { role_def, .. } => role_def.schema_version,
            Self::DropRole { schema_version, .. } => *schema_version,
            Self::GrantPrivilege { priv_def } => priv_def.schema_version(),
            Self::RevokePrivilege { priv_def, .. } => priv_def.schema_version(),
            Self::AuditPolicy { schema_version, .. } => *schema_version,
        }
    }

    /// Performs preliminary checks on acl so that it will not fail when applied.
    /// These checks do not include authorization checks, which are done separately in
    /// [`crate::access_control::access_check_op`].
    pub fn validate(&self, storage: &Catalog) -> Result<(), TRaftError> {
        // THOUGHT: should we move access_check_* fns here as it's done in tarantool?
        match self {
            Self::ChangeAuth { user_id, .. } => {
                // See https://git.picodata.io/picodata/tarantool/-/blob/da5ad0fa3ab8940f524cfa9bf3d582347c01fc4a/src/box/alter.cc#L2925
                if *user_id == GUEST_ID {
                    return Err(TRaftError::other(
                        "altering guest user's password is not allowed",
                    ));
                }
            }
            Self::DropUser { user_id, .. } => {
                // See https://git.picodata.io/picodata/tarantool/-/blob/da5ad0fa3ab8940f524cfa9bf3d582347c01fc4a/src/box/alter.cc#L3080
                if *user_id == GUEST_ID || *user_id == ADMIN_ID || *user_id == PICO_SERVICE_ID {
                    return Err(TRaftError::other("dropping system user is not allowed"));
                }

                let user_def = storage.users.by_id(*user_id)?.ok_or_else(|| {
                    TarantoolError::new(
                        TarantoolErrorCode::NoSuchUser,
                        format!("no such user #{user_id}"),
                    )
                })?;
                user_def.ensure_no_dependent_objects(storage)?;

                // user_has_data will be successful in any case https://git.picodata.io/picodata/tarantool/-/blob/da5ad0fa3ab8940f524cfa9bf3d582347c01fc4a/src/box/alter.cc#L2846
                // as box.schema.user.drop(..) deletes all the related spaces/priveleges/etc.
            }

            Self::DropRole { role_id, .. } => {
                // See https://git.picodata.io/picodata/tarantool/-/blob/da5ad0fa3ab8940f524cfa9bf3d582347c01fc4a/src/box/alter.cc#L3080
                if *role_id == PUBLIC_ID || *role_id == SUPER_ID || *role_id == ROLE_REPLICATION_ID
                {
                    return Err(TRaftError::other("dropping system role is not allowed"));
                }
            }
            _ => (),
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////
// PluginRaftOp
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "op_kind")]
pub enum PluginRaftOp {
    /// Disable selected plugin.
    DisablePlugin {
        ident: PluginIdentifier,
        /// This is `None` if the operation is proposed by the user's request to disable the plugin.
        ///
        /// Otherwise it is the error which happened during handling of [`PluginOp::EnablePlugin`]
        /// which is the cause for disabling the plugin.
        ///
        /// [`PluginOp::EnablePlugin`]: crate::plugin::PluginOp::EnablePlugin
        cause: Option<ErrorInfo>,
    },
    /// Remove records for the given plugin and records in other tables which
    /// indirectly depend on it (foreign keys).
    ///
    /// Note that in an ideal world this Op can be replaced with a BatchDml, but
    /// in practice the code would be a nightmare to write and maintain.
    /// It would be much easier if we supported FOREIGN KEY/ON DELETE CASCADE.
    DropPlugin { ident: PluginIdentifier },
    /// Update plugin service configuration.
    PluginConfigPartialUpdate {
        ident: PluginIdentifier,
        /// Pairs of { service name -> list of new key-value }
        updates: Vec<(SmolStr, Vec<(SmolStr, rmpv::Value)>)>,
    },
    /// Abort one of the mutlistage plugin change operations.
    Abort { cause: ErrorInfo },
}

////////////////////////////////////////////////////////////////////////////////
// vec_of_raw_byte_buf
////////////////////////////////////////////////////////////////////////////////

mod vec_of_raw_byte_buf {
    use super::TupleBuffer;
    use serde::de::Error as _;
    use serde::ser::SerializeSeq;
    use serde::{self, Deserialize, Deserializer, Serializer};
    use serde_bytes::{ByteBuf, Bytes};
    use std::convert::TryFrom;

    pub fn serialize<S>(v: &[TupleBuffer], ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = ser.serialize_seq(Some(v.len()))?;
        for buf in v {
            seq.serialize_element(Bytes::new(buf.as_ref()))?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(de: D) -> Result<Vec<TupleBuffer>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tmp = Vec::<ByteBuf>::deserialize(de)?;
        // FIXME(gmoshkin): redundant copy happens here,
        // because ByteBuf and TupleBuffer are essentially the same struct,
        // but there's no easy foolproof way
        // to convert a Vec<ByteBuf> to Vec<TupleBuffer>
        // because of borrow and drop checkers
        let res: tarantool::Result<_> = tmp
            .into_iter()
            .map(|bb| TupleBuffer::try_from(bb.into_vec()))
            .collect();
        res.map_err(D::Error::custom)
    }
}

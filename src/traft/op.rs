use crate::instance::Instance;
use crate::schema::Distribution;
use crate::storage::{ClusterwideSpace, ClusterwideSpaceIndex};
use ::tarantool::index::{IndexId, Part};
use ::tarantool::space::{Field, SpaceId};
use ::tarantool::tlua;
use ::tarantool::tlua::LuaError;
use ::tarantool::tuple::{ToTupleBuffer, Tuple, TupleBuffer};
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////
// OpResult
////////////////////////////////////////////////////////////////////////////////

pub trait OpResult {
    type Result: 'static;
    // FIXME: this signature makes it look like result of any operation depends
    // only on what is contained within the operation which is almost never true
    // And it makes it hard to do anything useful inside this function.
    fn result(self) -> Self::Result;
}

////////////////////////////////////////////////////////////////////////////////
/// The operation on the raft state machine.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "kind")]
pub enum Op {
    /// No operation.
    Nop,
    /// Print the message in tarantool log.
    Info { msg: String },
    /// Evaluate the code on every instance in cluster.
    EvalLua(EvalLua),
    ///
    ReturnOne(ReturnOne),
    /// Update the given instance's entry in [`storage::Instances`].
    PersistInstance(PersistInstance),
    /// Cluster-wide data modification operation.
    /// Should be used to manipulate the cluster-wide configuration.
    Dml(Dml),
    /// Start cluster-wide data schema definition operation.
    /// Should be used to manipulate the cluster-wide schema.
    ///
    /// The provided DDL operation will be set as pending.
    /// Only one pending DDL operation can exist at the same time.
    DdlPrepare { schema_version: u64, ddl: Ddl },
    /// Commit the pending DDL operation.
    ///
    /// Only one pending DDL operation can exist at the same time.
    DdlCommit,
    /// Abort the pending DDL operation.
    ///
    /// Only one pending DDL operation can exist at the same time.
    DdlAbort,
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return match self {
            Self::Nop => f.write_str("Nop"),
            Self::Info { msg } => write!(f, "Info({msg:?})"),
            Self::EvalLua(EvalLua { code }) => write!(f, "EvalLua({code:?})"),
            Self::ReturnOne(_) => write!(f, "ReturnOne"),
            Self::PersistInstance(PersistInstance(instance)) => {
                write!(f, "PersistInstance{}", instance)
            }
            Self::Dml(Dml::Insert { space, tuple }) => {
                write!(f, "Insert({space}, {})", DisplayAsJson(tuple))
            }
            Self::Dml(Dml::Replace { space, tuple }) => {
                write!(f, "Replace({space}, {})", DisplayAsJson(tuple))
            }
            Self::Dml(Dml::Update { index, key, ops }) => {
                let key = DisplayAsJson(key);
                let ops = DisplayAsJson(&**ops);
                write!(f, "Update({index}, {key}, {ops})")
            }
            Self::Dml(Dml::Delete { index, key }) => {
                write!(f, "Delete({index}, {})", DisplayAsJson(key))
            }
            Self::DdlPrepare {
                schema_version,
                ddl: Ddl::CreateSpace {
                    id, distribution, ..
                },
            } => {
                let distr = match distribution {
                    Distribution::Global => "Global",
                    Distribution::ShardedImplicitly { .. } => "ShardedImplicitly",
                    Distribution::ShardedByField { .. } => "ShardedByField",
                };
                write!(
                    f,
                    "DdlPrepare({schema_version}, CreateSpace({id}, {distr}))"
                )
            }
            Self::DdlPrepare {
                schema_version,
                ddl: Ddl::DropSpace { id },
            } => {
                write!(f, "DdlPrepare({schema_version}, DropSpace({id}))")
            }
            Self::DdlPrepare {
                schema_version,
                ddl: Ddl::CreateIndex {
                    space_id, index_id, ..
                },
            } => {
                write!(
                    f,
                    "DdlPrepare({schema_version}, CreateIndex({space_id}, {index_id}))"
                )
            }
            Self::DdlPrepare {
                schema_version,
                ddl: Ddl::DropIndex { space_id, index_id },
            } => {
                write!(
                    f,
                    "DdlPrepare({schema_version}, DropIndex({space_id}, {index_id}))"
                )
            }
            Self::DdlCommit => write!(f, "DdlCommit"),
            Self::DdlAbort => write!(f, "DdlAbort"),
        };

        struct DisplayAsJson<T>(pub T);

        impl std::fmt::Display for DisplayAsJson<&TupleBuffer> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                if let Some(data) = rmp_serde::from_slice::<serde_json::Value>(self.0.as_ref())
                    .ok()
                    .and_then(|v| serde_json::to_string(&v).ok())
                {
                    return write!(f, "{data}");
                }

                write!(f, "{:?}", self.0)
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

impl OpResult for Op {
    type Result = ();
    fn result(self) -> Self::Result {}
}

////////////////////////////////////////////////////////////////////////////////
// ReturnOne
////////////////////////////////////////////////////////////////////////////////

impl From<ReturnOne> for Op {
    fn from(op: ReturnOne) -> Op {
        Op::ReturnOne(op)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReturnOne;

impl OpResult for ReturnOne {
    type Result = u8;
    fn result(self) -> Self::Result {
        1
    }
}

////////////////////////////////////////////////////////////////////////////////
// EvalLua
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvalLua {
    pub code: String,
}

impl OpResult for EvalLua {
    type Result = Result<(), LuaError>;
    fn result(self) -> Self::Result {
        crate::tarantool::exec(&self.code)
    }
}

impl From<EvalLua> for Op {
    fn from(op: EvalLua) -> Op {
        Op::EvalLua(op)
    }
}

////////////////////////////////////////////////////////////////////////////////
// PersistInstance
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistInstance(pub Box<Instance>);

impl PersistInstance {
    pub fn new(instance: Instance) -> Self {
        Self(Box::new(instance))
    }
}

impl OpResult for PersistInstance {
    type Result = Box<Instance>;
    fn result(self) -> Self::Result {
        self.0
    }
}

impl From<PersistInstance> for Op {
    #[inline]
    fn from(op: PersistInstance) -> Op {
        Op::PersistInstance(op)
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
        space: ClusterwideSpace,
        #[serde(with = "serde_bytes")]
        tuple: TupleBuffer,
    },
    Replace {
        space: ClusterwideSpace,
        #[serde(with = "serde_bytes")]
        tuple: TupleBuffer,
    },
    Update {
        index: ClusterwideSpaceIndex,
        #[serde(with = "serde_bytes")]
        key: TupleBuffer,
        #[serde(with = "vec_of_raw_byte_buf")]
        ops: Vec<TupleBuffer>,
    },
    Delete {
        index: ClusterwideSpaceIndex,
        #[serde(with = "serde_bytes")]
        key: TupleBuffer,
    },
}

::tarantool::define_str_enum! {
    pub enum DmlKind {
        Insert = "insert",
        Replace = "replace",
        Update = "update",
        Delete = "delete",
    }
}

impl OpResult for Dml {
    type Result = tarantool::Result<Option<Tuple>>;
    fn result(self) -> Self::Result {
        match self {
            Self::Insert { space, tuple } => space.insert(&tuple).map(Some),
            Self::Replace { space, tuple } => space.replace(&tuple).map(Some),
            Self::Update { index, key, ops } => index.update(&key, &ops),
            Self::Delete { index, key } => index.delete(&key),
        }
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
    pub fn insert(space: ClusterwideSpace, tuple: &impl ToTupleBuffer) -> tarantool::Result<Self> {
        let res = Self::Insert {
            space,
            tuple: tuple.to_tuple_buffer()?,
        };
        Ok(res)
    }

    /// Serializes `tuple` and returns an [`Dml::Replace`] in case of success.
    #[inline(always)]
    pub fn replace(space: ClusterwideSpace, tuple: &impl ToTupleBuffer) -> tarantool::Result<Self> {
        let res = Self::Replace {
            space,
            tuple: tuple.to_tuple_buffer()?,
        };
        Ok(res)
    }

    /// Serializes `key` and returns an [`Dml::Update`] in case of success.
    #[inline(always)]
    pub fn update(
        index: impl Into<ClusterwideSpaceIndex>,
        key: &impl ToTupleBuffer,
        ops: impl Into<Vec<TupleBuffer>>,
    ) -> tarantool::Result<Self> {
        let res = Self::Update {
            index: index.into(),
            key: key.to_tuple_buffer()?,
            ops: ops.into(),
        };
        Ok(res)
    }

    /// Serializes `key` and returns an [`Dml::Delete`] in case of success.
    #[inline(always)]
    pub fn delete(
        index: impl Into<ClusterwideSpaceIndex>,
        key: &impl ToTupleBuffer,
    ) -> tarantool::Result<Self> {
        let res = Self::Delete {
            index: index.into(),
            key: key.to_tuple_buffer()?,
        };
        Ok(res)
    }

    #[rustfmt::skip]
    pub fn space(&self) -> ClusterwideSpace {
        match &self {
            Self::Insert { space, .. } => *space,
            Self::Replace { space, .. } => *space,
            Self::Update { index, .. } => index.space(),
            Self::Delete { index, .. } => index.space(),
        }
    }

    #[rustfmt::skip]
    pub fn index(&self) -> ClusterwideSpaceIndex {
        match &self {
            Self::Insert { space, .. } => (*space).into(),
            Self::Replace { space, .. } => (*space).into(),
            Self::Update { index, .. } => *index,
            Self::Delete { index, .. } => *index,
        }
    }

    /// Parse lua arguments to an api function such as `pico.cas`.
    pub fn from_lua_args(op: DmlInLua, index: ClusterwideSpaceIndex) -> Result<Self, String> {
        match op.kind {
            DmlKind::Insert => {
                let Some(tuple) = op.tuple else {
                    return Err("insert operation must have a tuple".into());
                };
                Ok(Self::Insert {
                    space: index.space(),
                    tuple,
                })
            }
            DmlKind::Replace => {
                let Some(tuple) = op.tuple else {
                    return Err("replace operation must have a tuple".into());
                };
                Ok(Self::Replace {
                    space: index.space(),
                    tuple,
                })
            }
            DmlKind::Update => {
                let Some(key) = op.key else {
                    return Err("update operation must have a key".into());
                };
                let Some(ops) = op.ops else {
                    return Err("update operation must have ops".into());
                };
                Ok(Self::Update { index, key, ops })
            }
            DmlKind::Delete => {
                let Some(key) = op.key else {
                    return Err("delete operation must have a key".into());
                };
                Ok(Self::Delete { index, key })
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
    pub kind: DmlKind,
    pub tuple: Option<TupleBuffer>,
    pub key: Option<TupleBuffer>,
    pub ops: Option<Vec<TupleBuffer>>,
}

////////////////////////////////////////////////////////////////////////////////
// Ddl
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "kind")]
pub enum Ddl {
    CreateSpace {
        id: SpaceId,
        name: String,
        format: Vec<Field>,
        primary_key: Vec<Part>,
        distribution: Distribution,
    },
    DropSpace {
        id: SpaceId,
    },
    CreateIndex {
        space_id: SpaceId,
        index_id: IndexId,
        by_fields: Vec<Part>,
    },
    DropIndex {
        space_id: SpaceId,
        index_id: IndexId,
    },
}

/// Builder for [`Op::DdlPrepare`] operations.
///
/// # Example
/// ```no_run
/// use picodata::traft::op::DdlBuilder;
///
/// // Assuming that space `1` was created.
/// let op = DdlBuilder::with_schema_version(1)
///     .drop_space(1);
/// ```
pub struct DdlBuilder {
    schema_version: u64,
}

impl DdlBuilder {
    /// Sets current schema version.
    pub fn with_schema_version(version: u64) -> Self {
        Self {
            schema_version: version,
        }
    }
    /// Creates an `Op::DdlPrepare(Ddl::CreateSpace)`.
    #[inline(always)]
    pub fn create_space(
        &self,
        id: SpaceId,
        name: String,
        format: Vec<tarantool::space::Field>,
        primary_key: Vec<Part>,
        distribution: Distribution,
    ) -> Op {
        Op::DdlPrepare {
            ddl: Ddl::CreateSpace {
                id,
                name,
                format,
                primary_key,
                distribution,
            },
            schema_version: self.schema_version,
        }
    }

    /// Creates an `Op::DdlPrepare(Ddl::DropSpace)`.
    #[inline(always)]
    pub fn drop_space(&self, id: SpaceId) -> Op {
        Op::DdlPrepare {
            ddl: Ddl::DropSpace { id },
            schema_version: self.schema_version,
        }
    }

    /// Creates an `Op::DdlPrepare(Ddl::CreateIndex)`.
    #[inline(always)]
    pub fn create_index(&self, space_id: SpaceId, index_id: IndexId, by_fields: Vec<Part>) -> Op {
        Op::DdlPrepare {
            ddl: Ddl::CreateIndex {
                space_id,
                index_id,
                by_fields,
            },
            schema_version: self.schema_version,
        }
    }

    /// Creates an `Op::DdlPrepare(Ddl::DropIndex)`.
    #[inline(always)]
    pub fn drop_index(&self, space_id: SpaceId, index_id: IndexId) -> Op {
        Op::DdlPrepare {
            ddl: Ddl::DropIndex { space_id, index_id },
            schema_version: self.schema_version,
        }
    }
}

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

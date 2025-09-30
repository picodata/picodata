use std::fmt::{Debug, Display, Formatter};

use crate::error_code::ErrorCode;
use crate::instance::InstanceName;
use crate::plugin::PluginError;
use crate::traft::{RaftId, RaftTerm};
use smol_str::SmolStr;
use tarantool::error::IntoBoxError;
use tarantool::error::{BoxError, TarantoolErrorCode};
use tarantool::fiber::r#async::timeout;
use tarantool::tlua::LuaError;
use thiserror::Error;

#[derive(Debug)]
pub struct Unsupported {
    entity: String,
    help: Option<String>,
}

impl Unsupported {
    pub(crate) fn new(entity: String, help: Option<String>) -> Self {
        Self { entity, help }
    }
}

impl Display for Unsupported {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "unsupported action/entity: {}", self.entity)?;
        if let Some(ref help) = self.help {
            write!(f, ", {help}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum AlreadyExists {
    #[error("table {0} already exists")]
    Table(SmolStr),
    #[error("column {0} already exists")]
    Column(SmolStr),
    #[error("index {0} already exists")]
    Index(SmolStr),
    #[error("procedure {0} already exists")]
    Procedure(SmolStr),
    #[error("user {0} already exists")]
    User(SmolStr),
    #[error("role {0} already exists")]
    Role(SmolStr),
}

#[derive(Debug, Error)]
pub enum DoesNotExist {
    #[error("table {0} does not exist")]
    Table(SmolStr),
    #[error("column {0} does not exist")]
    Column(SmolStr),
    #[error("index {0} does not exist")]
    Index(SmolStr),
    #[error("procedure {0} does not exist")]
    Procedure(SmolStr),
    #[error("user {0} does not exist")]
    User(SmolStr),
    #[error("role {0} does not exist")]
    Role(SmolStr),
    #[error("audit policy {0} does not exist")]
    AuditPolicy(SmolStr),
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("uninitialized yet")]
    Uninitialized,

    #[error("{0}")]
    Raft(#[from] raft::Error),
    #[error("downcast error: expected {expected:?}, actual: {actual:?}")]
    DowncastError {
        expected: &'static str,
        actual: &'static str,
    },
    /// cluster_name of the joining instance mismatches the cluster_name of the cluster
    #[error("cluster_name mismatch: cluster_name of the instance = {instance_cluster_name:?}, cluster_name of the cluster = {cluster_name:?}")]
    ClusterNameMismatch {
        instance_cluster_name: String,
        cluster_name: String,
    },
    #[error("cluster UUID mismatch: instance {instance_uuid}, cluster {cluster_uuid}")]
    ClusterUuidMismatch {
        instance_uuid: String,
        cluster_uuid: String,
    },
    /// Instance was requested to configure replication with different replicaset.
    #[error("cannot replicate with different replicaset: expected {instance_rsid:?}, requested {requested_rsid:?}")]
    ReplicasetNameMismatch {
        instance_rsid: String,
        requested_rsid: String,
    },
    #[error("picodata version of the joining instance = {instance_version:?} mismatches the leader's version = {leader_version:?}")]
    PicodataVersionMismatch {
        leader_version: String,
        instance_version: String,
    },
    #[error("operation request from different term {requested}, current term is {current}")]
    TermMismatch {
        requested: RaftTerm,
        current: RaftTerm,
    },
    #[error("not a leader")]
    NotALeader,
    #[error("lua error: {0}")]
    Lua(#[from] LuaError),
    #[error("{}", DisplayBoxError(.0))]
    BoxError(BoxError),
    #[error("{0}")]
    Tarantool(#[from] ::tarantool::error::Error),
    #[error("instance with {} not found", *.0)]
    NoSuchInstance(IdOfInstance),
    #[error("replicaset with {} \"{name}\" not found", if *.id_is_uuid { "uuid" } else { "name" })]
    NoSuchReplicaset { name: String, id_is_uuid: bool },
    #[error("tier with name \"{0}\" not found")]
    NoSuchTier(String),
    #[error("address of peer with id {0} not found")]
    AddressUnknownForRaftId(RaftId),
    #[error("address of peer with id \"{0}\" not found")]
    AddressUnknownForInstanceName(InstanceName),
    #[error("address of peer is incorrectly formatted: {0}")]
    AddressParseFailure(String),
    #[error("leader is unknown yet")]
    LeaderUnknown,
    #[error("governor has stopped")]
    GovernorStopped,

    #[error("{0}")]
    Cas(#[from] crate::cas::Error),
    #[error("{0}")]
    Ddl(#[from] crate::schema::DdlError),

    #[error("sbroad: {0}")]
    Sbroad(#[from] sbroad::errors::SbroadError),

    #[error("transaction: {0}")]
    Transaction(String),

    #[error("storage corrupted: failed to decode field '{field}' from table '{table}'")]
    StorageCorrupted { table: String, field: String },

    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error(transparent)]
    Plugin(#[from] PluginError),

    #[error("{0}")]
    Unsupported(Unsupported),

    #[error(transparent)]
    AlreadyExists(#[from] AlreadyExists),

    #[error(transparent)]
    DoesNotExist(#[from] DoesNotExist),

    #[error("{0}")]
    Other(Box<dyn std::error::Error>),
}

#[derive(Debug)]
pub enum IdOfInstance {
    RaftId(RaftId),
    Name(InstanceName),
    Uuid(String),
}

impl std::fmt::Display for IdOfInstance {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            IdOfInstance::RaftId(raft_id) => write!(f, "raft_id {raft_id}"),
            IdOfInstance::Name(name) => write!(f, "name \"{name}\""),
            IdOfInstance::Uuid(uuid) => write!(f, "uuid \"{uuid}\""),
        }
    }
}

impl Error {
    #[track_caller]
    #[inline(always)]
    pub fn timeout() -> Self {
        BoxError::new(TarantoolErrorCode::Timeout, "timeout").into()
    }

    pub fn error_code(&self) -> u32 {
        match self {
            Self::Uninitialized => ErrorCode::Uninitialized as _,
            Self::BoxError(e) => e.error_code(),
            Self::Tarantool(e) => e.error_code(),
            Self::Cas(e) => e.error_code(),
            Self::Raft(raft::Error::Store(raft::StorageError::Compacted)) => {
                ErrorCode::RaftLogCompacted as _
            }
            Self::Raft(raft::Error::Store(raft::StorageError::Unavailable))
            | Self::Raft(raft::Error::Store(raft::StorageError::LogTemporarilyUnavailable)) => {
                ErrorCode::RaftLogUnavailable as _
            }
            Self::Raft(raft::Error::ProposalDropped) => ErrorCode::RaftProposalDropped as _,
            Self::Raft(_) => ErrorCode::Other as _,
            Self::Plugin(e) => e.error_code(),
            // TODO: when sbroad will need boxed errors, implement
            // `IntoBoxError` for `sbroad::errors::SbroadError` and
            // use it here:
            Self::Sbroad(_) => ErrorCode::SbroadError as _,
            Self::LeaderUnknown => ErrorCode::LeaderUnknown as _,
            Self::NotALeader => ErrorCode::NotALeader as _,
            Self::TermMismatch { .. } => ErrorCode::TermMismatch as _,
            Self::NoSuchInstance(_) => ErrorCode::NoSuchInstance as _,
            Self::NoSuchReplicaset { .. } => ErrorCode::NoSuchReplicaset as _,
            Self::Unsupported { .. } => TarantoolErrorCode::Unsupported as _,
            // TODO: give other error types specific codes
            _ => ErrorCode::Other as _,
        }
    }

    #[inline(always)]
    pub fn other<E>(error: E) -> Self
    where
        E: Into<Box<dyn std::error::Error>>,
    {
        Self::Other(error.into())
    }

    #[inline(always)]
    pub fn invalid_configuration(msg: impl ToString) -> Self {
        Self::InvalidConfiguration(msg.to_string())
    }

    #[inline]
    pub fn is_retriable(&self) -> bool {
        let code = self.error_code();
        let Ok(code) = ErrorCode::try_from(code) else {
            return false;
        };
        code.is_retriable_for_cas()
    }
}

#[inline(always)]
#[track_caller]
pub fn to_error_other(message: impl ToString) -> BoxError {
    BoxError::new(ErrorCode::Other, message.to_string())
}

#[track_caller]
pub fn with_modified_message(e: BoxError, message: String) -> BoxError {
    if let Some((file, line)) = e.file().zip(e.line()) {
        BoxError::with_location(e.error_code(), message, file, line)
    } else {
        BoxError::new(e.error_code(), message)
    }
}

impl From<std::io::Error> for Error {
    #[inline(always)]
    fn from(e: std::io::Error) -> Error {
        Error::Tarantool(e.into())
    }
}

impl<E> From<timeout::Error<E>> for Error
where
    Error: From<E>,
{
    #[track_caller]
    fn from(err: timeout::Error<E>) -> Self {
        match err {
            timeout::Error::Expired => Self::timeout(),
            timeout::Error::Failed(err) => err.into(),
        }
    }
}

impl From<::tarantool::network::ClientError> for Error {
    fn from(err: ::tarantool::network::ClientError) -> Self {
        Self::Tarantool(err.into())
    }
}

impl<E: Display> From<::tarantool::transaction::TransactionError<E>> for Error {
    fn from(err: ::tarantool::transaction::TransactionError<E>) -> Self {
        Self::Transaction(err.to_string())
    }
}

impl From<BoxError> for Error {
    #[inline(always)]
    fn from(err: BoxError) -> Self {
        Self::BoxError(err)
    }
}

struct DisplayBoxError<'a>(&'a BoxError);
impl std::fmt::Display for DisplayBoxError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let code = self.0.error_code();
        let message = self.0.message();
        if let Some(code) = TarantoolErrorCode::from_i64(code as _) {
            return write!(f, "{code:?}: {message}");
        }
        if let Some(code) = ErrorCode::from_i64(code as _) {
            return write!(f, "{code:?}: {message}");
        }
        write!(f, "#{code}: {message}")
    }
}

impl<V> From<tarantool::tlua::CallError<V>> for Error
where
    V: Into<tarantool::tlua::Void>,
{
    fn from(err: tarantool::tlua::CallError<V>) -> Self {
        Self::Lua(err.into())
    }
}

impl IntoBoxError for Error {
    #[inline(always)]
    fn error_code(&self) -> u32 {
        // Redirect to the inherent method
        Error::error_code(self)
    }

    #[inline]
    #[track_caller]
    fn into_box_error(self) -> BoxError {
        match self {
            Self::BoxError(e) => e,
            Self::Tarantool(e) => {
                // Optimization
                return e.into_box_error();
            }
            other => {
                // FIXME: currently these errors capture the source location of where this function is called (see #[track_caller]),
                // but we probably want to instead capture the location where the original error was created.
                BoxError::new(other.error_code(), other.to_string())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ErrorInfo
////////////////////////////////////////////////////////////////////////////////

/// This is a serializable version of [`BoxError`].
///
/// TODO<https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/221> just make BoxError serializable.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ErrorInfo {
    pub error_code: u32,
    pub message: String,
    pub instance_name: InstanceName,
}

impl ErrorInfo {
    #[inline(always)]
    pub fn timeout(instance_name: impl Into<InstanceName>, message: impl Into<String>) -> Self {
        Self {
            error_code: TarantoolErrorCode::Timeout as _,
            message: message.into(),
            instance_name: instance_name.into(),
        }
    }

    #[inline(always)]
    pub fn new(instance_name: impl Into<InstanceName>, error: impl IntoBoxError) -> Self {
        let box_error = error.into_box_error();
        Self {
            error_code: box_error.error_code(),
            message: box_error.message().into(),
            instance_name: instance_name.into(),
        }
    }

    /// Should only be used in tests.
    pub fn for_tests() -> Self {
        Self {
            error_code: ErrorCode::Other as _,
            message: "there's a snake in my boot".into(),
            instance_name: InstanceName::from("i3378"),
        }
    }
}

impl std::fmt::Display for ErrorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[instance name:{}] ", self.instance_name)?;
        if let Some(c) = ErrorCode::from_i64(self.error_code as _) {
            write!(f, "{c:?}")?;
        } else if let Some(c) = TarantoolErrorCode::from_i64(self.error_code as _) {
            write!(f, "{c:?}")?;
        } else {
            write!(f, "#{}", self.error_code)?;
        }
        write!(f, ": {}", self.message)
    }
}

impl IntoBoxError for ErrorInfo {
    #[inline]
    fn error_code(&self) -> u32 {
        self.error_code
    }
}

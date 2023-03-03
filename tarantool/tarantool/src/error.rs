//! Error handling utils. See ["failure" crate documentation](https://docs.rs/failure/) for details
//!
//! The Tarantool error handling works most like libc's errno. All API calls
//! return -1 or `NULL` in the event of error. An internal pointer to
//! `box_error_t` type is set by API functions to indicate what went wrong.
//! This value is only significant if API call failed (returned -1 or `NULL`).
//!
//! Successful function can also touch the last error in some
//! cases. You don't have to clear the last error before calling
//! API functions. The returned object is valid only until next
//! call to **any** API function.
//!
//! You must set the last error using `set_error()` in your stored C
//! procedures if you want to return a custom error message.
//! You can re-throw the last API error to IPROTO client by keeping
//! the current value and returning -1 to Tarantool from your
//! stored procedure.

use std::ffi::CStr;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::str::Utf8Error;
use std::sync::Arc;

use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::FromPrimitive;
use rmp::decode::{MarkerReadError, NumValueReadError, ValueReadError};
use rmp::encode::ValueWriteError;

use crate::ffi::tarantool as ffi;
use crate::tlua::LuaError;

/// A specialized [`Result`] type for the crate
pub type Result<T> = std::result::Result<T, Error>;

/// Represents all error cases for all routines of crate (including Tarantool errors)
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Tarantool error: {0}")]
    Tarantool(TarantoolError),

    #[error("IO error: {0}")]
    IO(#[from] io::Error),

    #[error("Failed to encode tuple: {0}")]
    Encode(#[from] Encode),

    #[error("Failed to decode tuple: {0}")]
    Decode(#[from] rmp_serde::decode::Error),

    #[error("Unicode string decode error: {0}")]
    Unicode(#[from] Utf8Error),

    #[error("Numeric value read error: {0}")]
    NumValueRead(#[from] NumValueReadError),

    #[error("Value read error: {0}")]
    ValueRead(#[from] ValueReadError),

    #[error("Value write error: {0}")]
    ValueWrite(#[from] ValueWriteError),

    #[error("Transaction issue: {0}")]
    Transaction(TransactionError),

    #[cfg(feature = "net_box")]
    #[error("Server responded with error: {0}")]
    Remote(#[from] crate::net_box::ResponseError),

    /// The error is wrapped in a [`Arc`], because some libraries require
    /// error types to implement [`Sync`], which isn't implemented for [`Rc`].
    ///
    /// [`Rc`]: std::rc::Rc
    #[error("Server responded with error: {0}")]
    Protocol(Arc<crate::network::protocol::Error>),

    /// The error is wrapped in a [`Arc`], because some libraries require
    /// error types to implement [`Sync`], which isn't implemented for [`Rc`].
    ///
    /// [`Rc`]: std::rc::Rc
    #[cfg(feature = "network_client")]
    #[error("Server responded with error: {0}")]
    Tcp(Arc<crate::network::client::tcp::Error>),

    #[error("Lua error: {0}")]
    LuaError(#[from] LuaError),

    #[cfg(feature = "schema")]
    #[error("Space metadata not found")]
    MetaNotFound,
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(error: rmp_serde::encode::Error) -> Self {
        Encode::from(error).into()
    }
}

impl From<crate::network::client::tcp::Error> for Error {
    fn from(err: crate::network::client::tcp::Error) -> Self {
        Error::Tcp(Arc::new(err))
    }
}

impl From<crate::network::protocol::Error> for Error {
    fn from(err: crate::network::protocol::Error) -> Self {
        Error::Protocol(Arc::new(err))
    }
}

impl From<MarkerReadError> for Error {
    fn from(error: MarkerReadError) -> Self {
        Error::ValueRead(error.into())
    }
}

/// Transaction-related error cases
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("Transaction has already been started")]
    AlreadyStarted,

    #[error("Failed to commit")]
    FailedToCommit,

    #[error("Failed to rollback")]
    FailedToRollback,
}

impl From<TransactionError> for Error {
    fn from(error: TransactionError) -> Self {
        Error::Transaction(error)
    }
}

/// Settable by Tarantool error type
pub struct TarantoolError {
    code: u32,
    message: String,
    error_ptr: Box<ffi::BoxError>,
}

impl std::fmt::Debug for TarantoolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TarantoolError")
            .field("code", &self.code)
            .field("message", &self.message)
            .finish_non_exhaustive()
    }
}

impl TarantoolError {
    /// Tries to get the information about the last API call error. If error was not set
    /// returns `Ok(())`
    pub fn maybe_last() -> std::result::Result<(), Self> {
        let error_ptr = unsafe { ffi::box_error_last() };
        if error_ptr.is_null() {
            return Ok(());
        }

        let code = unsafe { ffi::box_error_code(error_ptr) };

        let message = unsafe { CStr::from_ptr(ffi::box_error_message(error_ptr)) };
        let message = message.to_string_lossy().into_owned();

        Err(TarantoolError {
            code,
            message,
            error_ptr: unsafe { Box::from_raw(error_ptr) },
        })
    }

    /// Get the information about the last API call error.
    pub fn last() -> Self {
        TarantoolError::maybe_last().err().unwrap()
    }

    /// Return IPROTO error code
    pub fn error_code(&self) -> u32 {
        self.code
    }

    /// Return the error type, e.g. "ClientError", "SocketError", etc.
    pub fn error_type(&self) -> String {
        let result = unsafe { ffi::box_error_type(&*self.error_ptr) };
        unsafe { CStr::from_ptr(result) }
            .to_string_lossy()
            .to_string()
    }
}

impl Display for TarantoolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(code) = TarantoolErrorCode::from_u32(self.code) {
            return write!(f, "{:?}: {}", code, self.message);
        }
        let kind = crate::lua_state().eval_with::<_, String>(
            "
                for kind, code in pairs(box.error) do
                    if code == ... then
                        return kind
                    end
                end
                ",
            self.code,
        );
        let kind = kind.as_deref().unwrap_or("?");
        write!(f, "{}: {}", kind, self.message)
    }
}

impl From<TarantoolError> for Error {
    fn from(error: TarantoolError) -> Self {
        Error::Tarantool(error)
    }
}

/// Codes of Tarantool errors
#[repr(u32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, ToPrimitive, FromPrimitive)]
pub enum TarantoolErrorCode {
    Unknown = 0,
    IllegalParams = 1,
    MemoryIssue = 2,
    TupleFound = 3,
    TupleNotFound = 4,
    Unsupported = 5,
    NonMaster = 6,
    Readonly = 7,
    Injection = 8,
    CreateSpace = 9,
    SpaceExists = 10,
    DropSpace = 11,
    AlterSpace = 12,
    IndexType = 13,
    ModifyIndex = 14,
    LastDrop = 15,
    TupleFormatLimit = 16,
    DropPrimaryKey = 17,
    KeyPartType = 18,
    ExactMatch = 19,
    InvalidMsgpack = 20,
    ProcRet = 21,
    TupleNotArray = 22,
    FieldType = 23,
    IndexPartTypeMismatch = 24,
    Splice = 25,
    UpdateArgType = 26,
    FormatMismatchIndexPart = 27,
    UnknownUpdateOp = 28,
    UpdateField = 29,
    FunctionTxActive = 30,
    KeyPartCount = 31,
    ProcLua = 32,
    NoSuchProc = 33,
    NoSuchTrigger = 34,
    NoSuchIndexID = 35,
    NoSuchSpace = 36,
    NoSuchFieldNo = 37,
    ExactFieldCount = 38,
    FieldMissing = 39,
    WalIo = 40,
    MoreThanOneTuple = 41,
    AccessDenied = 42,
    CreateUser = 43,
    DropUser = 44,
    NoSuchUser = 45,
    UserExists = 46,
    PasswordMismatch = 47,
    UnknownRequestType = 48,
    UnknownSchemaObject = 49,
    CreateFunction = 50,
    NoSuchFunction = 51,
    FunctionExists = 52,
    BeforeReplaceRet = 53,
    MultistatementTransaction = 54,
    TriggerExists = 55,
    UserMax = 56,
    NoSuchEngine = 57,
    ReloadCfg = 58,
    Cfg = 59,
    SavepointEmptyTx = 60,
    NoSuchSavepoint = 61,
    UnknownReplica = 62,
    ReplicasetUuidMismatch = 63,
    InvalidUuid = 64,
    ReplicasetUuidIsRo = 65,
    InstanceUuidMismatch = 66,
    ReplicaIDIsReserved = 67,
    InvalidOrder = 68,
    MissingRequestField = 69,
    Identifier = 70,
    DropFunction = 71,
    IteratorType = 72,
    ReplicaMax = 73,
    InvalidXlog = 74,
    InvalidXlogName = 75,
    InvalidXlogOrder = 76,
    NoConnection = 77,
    Timeout = 78,
    ActiveTransaction = 79,
    CursorNoTransaction = 80,
    CrossEngineTransaction = 81,
    NoSuchRole = 82,
    RoleExists = 83,
    CreateRole = 84,
    IndexExists = 85,
    SessionClosed = 86,
    RoleLoop = 87,
    Grant = 88,
    PrivGranted = 89,
    RoleGranted = 90,
    PrivNotGranted = 91,
    RoleNotGranted = 92,
    MissingSnapshot = 93,
    CantUpdatePrimaryKey = 94,
    UpdateIntegerOverflow = 95,
    GuestUserPassword = 96,
    TransactionConflict = 97,
    UnsupportedPriv = 98,
    LoadFunction = 99,
    FunctionLanguage = 100,
    RtreeRect = 101,
    ProcC = 102,
    UnknownRtreeIndexDistanceType = 103,
    Protocol = 104,
    UpsertUniqueSecondaryKey = 105,
    WrongIndexRecord = 106,
    WrongIndexParts = 107,
    WrongIndexOptions = 108,
    WrongSchemaVersion = 109,
    MemtxMaxTupleSize = 110,
    WrongSpaceOptions = 111,
    UnsupportedIndexFeature = 112,
    ViewIsRo = 113,
    NoTransaction = 114,
    System = 115,
    Loading = 116,
    ConnectionToSelf = 117,
    KeyPartIsTooLong = 118,
    Compression = 119,
    CheckpointInProgress = 120,
    SubStmtMax = 121,
    CommitInSubStmt = 122,
    RollbackInSubStmt = 123,
    Decompression = 124,
    InvalidXlogType = 125,
    AlreadyRunning = 126,
    IndexFieldCountLimit = 127,
    LocalInstanceIDIsReadOnly = 128,
    BackupInProgress = 129,
    ReadViewAborted = 130,
    InvalidIndexFile = 131,
    InvalidRunFile = 132,
    InvalidVylogFile = 133,
    CheckpointRollback = 134,
    VyQuotaTimeout = 135,
    PartialKey = 136,
    TruncateSystemSpace = 137,
    LoadModule = 138,
    VinylMaxTupleSize = 139,
    WrongDdVersion = 140,
    WrongSpaceFormat = 141,
    CreateSequence = 142,
    AlterSequence = 143,
    DropSequence = 144,
    NoSuchSequence = 145,
    SequenceExists = 146,
    SequenceOverflow = 147,
    NoSuchIndexName = 148,
    SpaceFieldIsDuplicate = 149,
    CantCreateCollation = 150,
    WrongCollationOptions = 151,
    NullablePrimary = 152,
    NoSuchFieldNameInSpace = 153,
    TransactionYield = 154,
    NoSuchGroup = 155,
    SqlBindValue = 156,
    SqlBindType = 157,
    SqlBindParameterMax = 158,
    SqlExecute = 159,
    Unused = 160,
    SqlBindNotFound = 161,
    ActionMismatch = 162,
    ViewMissingSql = 163,
    ForeignKeyConstraint = 164,
    NoSuchModule = 165,
    NoSuchCollation = 166,
    CreateFkConstraint = 167,
    DropFkConstraint = 168,
    NoSuchConstraint = 169,
    ConstraintExists = 170,
    SqlTypeMismatch = 171,
    RowidOverflow = 172,
    DropCollation = 173,
    IllegalCollationMix = 174,
    SqlNoSuchPragma = 175,
    SqlCantResolveField = 176,
    IndexExistsInSpace = 177,
    InconsistentTypes = 178,
    SqlSyntax = 179,
    SqlStackOverflow = 180,
    SqlSelectWildcard = 181,
    SqlStatementEmpty = 182,
    SqlKeywordIsReserved = 183,
    SqlUnrecognizedSyntax = 184,
    SqlUnknownToken = 185,
    SqlParserGeneric = 186,
    SqlAnalyzeArgument = 187,
    SqlColumnCountMax = 188,
    HexLiteralMax = 189,
    IntLiteralMax = 190,
    SqlParserLimit = 191,
    IndexDefUnsupported = 192,
    CkDefUnsupported = 193,
    MultikeyIndexMismatch = 194,
    CreateCkConstraint = 195,
    CkConstraintFailed = 196,
    SqlColumnCount = 197,
    FuncIndexFunc = 198,
    FuncIndexFormat = 199,
    FuncIndexParts = 200,
    NoSuchFieldNameInTuple = 201,
    FuncWrongArgCount = 202,
    BootstrapReadonly = 203,
    SqlFuncWrongRetCount = 204,
    FuncInvalidReturnType = 205,
    SqlParserGenericWithPos = 206,
    ReplicaNotAnon = 207,
    CannotRegister = 208,
    SessionSettingInvalidValue = 209,
    SqlPrepare = 210,
    WrongQueryId = 211,
    SequenceNotStarted = 212,
    NoSuchSessionSetting = 213,
    UncommittedForeignSyncTxns = 214,
    SyncMasterMismatch = 215,
    SyncQuorumTimeout = 216,
    SyncRollback = 217,
    TupleMetadataIsTooBig = 218,
    XlogGap = 219,
    TooEarlySubscribe = 220,
    SqlCantAddAutoinc = 221,
    QuorumWait = 222,
    InterferingPromote = 223,
    ElectionDisabled = 224,
    TxnRollback = 225,
    NotLeader = 226,
    SyncQueueUnclaimed = 227,
    SyncQueueForeign = 228,
    UnableToProcessInStream = 229,
    UnableToProcessOutOfStream = 230,
    TransactionTimeout = 231,
    ActiveTimer = 232,
    TupleFieldCountLimit = 233,
    CreateConstraint = 234,
    FieldConstraintFailed = 235,
    TupleConstraintFailed = 236,
    CreateForeignKey = 237,
    ForeignKeyIntegrity = 238,
    FieldForeignKeyFailed = 239,
    ComplexForeignKeyFailed = 240,
    WrongSpaceUpgradeOptions = 241,
    NoElectionQuorum = 242,
    Ssl = 243,
    SplitBrain = 244,
}

impl TarantoolErrorCode {
    pub fn try_last() -> Option<Self> {
        unsafe {
            let e_ptr = ffi::box_error_last();
            if e_ptr.is_null() {
                return None;
            }
            let u32_code = ffi::box_error_code(e_ptr);
            TarantoolErrorCode::from_u32(u32_code)
        }
    }

    pub fn last() -> Self {
        Self::try_last().unwrap()
    }
}

/// Clear the last error.
pub fn clear_error() {
    unsafe { ffi::box_error_clear() }
}

/// Set the last error.
#[macro_export]
macro_rules! set_error {
    ($code:expr, $msg:literal) => {
        unsafe {
            let file = std::concat!(file!(), "\0").as_ptr().cast();
            let msg_ptr = std::concat!($msg, "\0").as_ptr().cast();
            $crate::ffi::tarantool::box_error_set(file, line!(), $code as u32, msg_ptr)
        }
    };
    ($code:expr, $($msg_args:expr),+) => {
        unsafe {
            let msg = std::fmt::format(format_args!($($msg_args),*));
            let file = std::concat!(file!(), "\0").as_ptr().cast();
            let msg: std::ffi::CString = std::ffi::CString::new(msg).unwrap();
            // `msg` must outlive `msg_ptr`
            let msg_ptr = msg.as_ptr().cast();
            $crate::ffi::tarantool::box_error_set(file, line!(), $code as u32, msg_ptr)
        }
    };
}

/// Error that can happen when serializing a tuple
#[derive(Debug, thiserror::Error)]
pub enum Encode {
    #[error("{0}")]
    Rmp(#[from] rmp_serde::encode::Error),

    #[error("Invalid msgpack value (epxected array, found {:?})", DebugAsMPValue(.0))]
    InvalidMP(Vec<u8>),
}

struct DebugAsMPValue<'a>(&'a [u8]);

impl std::fmt::Debug for DebugAsMPValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut read = self.0;
        match rmp_serde::from_read::<_, rmpv::Value>(&mut read) {
            Ok(v) => write!(f, "{:?}", v),
            Err(_) => write!(f, "{:?}", self.0),
        }
    }
}

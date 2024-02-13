//! Error handling utils.
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
use std::ptr::NonNull;
use std::str::Utf8Error;
use std::sync::Arc;

use rmp::decode::{MarkerReadError, NumValueReadError, ValueReadError};
use rmp::encode::ValueWriteError;

use crate::ffi::tarantool as ffi;
use crate::tlua::LuaError;
use crate::transaction::TransactionError;

/// A specialized [`Result`] type for the crate
pub type Result<T> = std::result::Result<T, Error>;

pub type TimeoutError<E> = crate::fiber::r#async::timeout::Error<E>;

////////////////////////////////////////////////////////////////////////////////
// Error
////////////////////////////////////////////////////////////////////////////////

/// Represents all error cases for all routines of crate (including Tarantool errors)
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("tarantool error: {0}")]
    Tarantool(TarantoolError),

    #[error("io error: {0}")]
    IO(#[from] io::Error),

    #[error("failed to encode tuple: {0}")]
    Encode(#[from] EncodeError),

    #[error("failed to decode tuple: {error} when decoding msgpack {} into rust type {expected_type}", crate::util::DisplayAsHexBytes(.actual_msgpack))]
    Decode {
        error: rmp_serde::decode::Error,
        expected_type: String,
        actual_msgpack: Vec<u8>,
    },

    #[error("failed to decode tuple: {0}")]
    DecodeRmpValue(#[from] rmp_serde::decode::Error),

    #[error("unicode string decode error: {0}")]
    Unicode(#[from] Utf8Error),

    #[error("numeric value read error: {0}")]
    NumValueRead(#[from] NumValueReadError),

    #[error("msgpack read error: {0}")]
    ValueRead(#[from] ValueReadError),

    #[error("msgpack write error: {0}")]
    ValueWrite(#[from] ValueWriteError),

    #[error("server responded with error: {0}")]
    Remote(#[from] crate::network::protocol::ResponseError),

    /// The error is wrapped in a [`Arc`], because some libraries require
    /// error types to implement [`Sync`], which isn't implemented for [`Rc`].
    ///
    /// [`Rc`]: std::rc::Rc
    #[error("{0}")]
    Protocol(#[from] crate::network::protocol::ProtocolError),

    /// The error is wrapped in a [`Arc`], because some libraries require
    /// error types to implement [`Sync`], which isn't implemented for [`Rc`].
    ///
    /// [`Rc`]: std::rc::Rc
    #[cfg(feature = "network_client")]
    #[error("{0}")]
    Tcp(Arc<crate::network::client::tcp::Error>),

    #[error("lua error: {0}")]
    LuaError(#[from] LuaError),

    #[error("space metadata not found")]
    MetaNotFound,

    #[error("msgpack encode error: {0}")]
    MsgpackEncode(#[from] crate::msgpack::EncodeError),

    #[error("msgpack decode error: {0}")]
    MsgpackDecode(#[from] crate::msgpack::DecodeError),

    /// A network connection was closed for the given reason.
    #[error("{0}")]
    ConnectionClosed(Arc<Error>),

    /// This should only be used if the error doesn't fall into one of the above
    /// categories.
    #[error("{0}")]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

const _: () = {
    /// Assert Error implements Send + Sync
    const fn if_this_compiles_the_type_implements_send_and_sync<T: Send + Sync>() {}
    if_this_compiles_the_type_implements_send_and_sync::<Error>();
};

impl Error {
    #[inline(always)]
    pub fn other<E>(error: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::Other(error.into())
    }

    #[inline(always)]
    pub fn decode<T>(error: rmp_serde::decode::Error, data: Vec<u8>) -> Self {
        Error::Decode {
            error,
            expected_type: std::any::type_name::<T>().into(),
            actual_msgpack: data,
        }
    }

    /// Returns the name of the variant as it is spelled in the source code.
    pub const fn variant_name(&self) -> &'static str {
        match self {
            Self::Tarantool(_) => "Tarantool",
            Self::IO(_) => "IO",
            Self::Encode(_) => "Encode",
            Self::Decode { .. } => "Decode",
            Self::DecodeRmpValue(_) => "DecodeRmpValue",
            Self::Unicode(_) => "Unicode",
            Self::NumValueRead(_) => "NumValueRead",
            Self::ValueRead(_) => "ValueRead",
            Self::ValueWrite(_) => "ValueWrite",
            Self::Remote(_) => "Remote",
            Self::Protocol(_) => "Protocol",
            #[cfg(feature = "network_client")]
            Self::Tcp(_) => "Tcp",
            Self::LuaError(_) => "LuaError",
            Self::MetaNotFound => "MetaNotFound",
            Self::MsgpackEncode(_) => "MsgpackEncode",
            Self::MsgpackDecode(_) => "MsgpackDecode",
            Self::ConnectionClosed(_) => "ConnectionClosed",
            Self::Other(_) => "Other",
        }
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(error: rmp_serde::encode::Error) -> Self {
        EncodeError::from(error).into()
    }
}

#[cfg(feature = "network_client")]
impl From<crate::network::client::tcp::Error> for Error {
    fn from(err: crate::network::client::tcp::Error) -> Self {
        Error::Tcp(Arc::new(err))
    }
}

impl From<MarkerReadError> for Error {
    fn from(error: MarkerReadError) -> Self {
        Error::ValueRead(error.into())
    }
}

impl<E> From<TransactionError<E>> for Error
where
    Error: From<E>,
{
    #[inline]
    fn from(e: TransactionError<E>) -> Self {
        match e {
            TransactionError::FailedToCommit(e) => e.into(),
            TransactionError::FailedToRollback(e) => e.into(),
            TransactionError::RolledBack(e) => e.into(),
            TransactionError::AlreadyStarted => crate::set_and_get_error!(
                TarantoolErrorCode::ActiveTransaction,
                "transaction has already been started"
            )
            .into(),
        }
    }
}

impl<E> From<TimeoutError<E>> for Error
where
    Error: From<E>,
{
    #[inline]
    fn from(e: TimeoutError<E>) -> Self {
        match e {
            TimeoutError::Expired => {
                crate::set_and_get_error!(TarantoolErrorCode::Timeout, "timeout").into()
            }
            TimeoutError::Failed(e) => e.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// TarantoolError
////////////////////////////////////////////////////////////////////////////////

/// Settable by Tarantool error type
#[derive(Debug, Clone, Default)]
pub struct TarantoolError {
    pub(crate) code: u32,
    pub(crate) message: Option<Box<str>>,
    pub(crate) error_type: Option<Box<str>>,
}

impl TarantoolError {
    /// Tries to get the information about the last API call error. If error was not set
    /// returns `Ok(())`
    #[inline]
    pub fn maybe_last() -> std::result::Result<(), Self> {
        // This is safe as long as tarantool runtime is initialized
        let error_ptr = unsafe { ffi::box_error_last() };
        let Some(error_ptr) = NonNull::new(error_ptr) else {
            return Ok(());
        };

        // This is safe, because box_error_last returns a valid pointer
        Err(unsafe { Self::from_ptr(error_ptr) })
    }

    /// Create a `TarantoolError` from a poniter to the underlying struct.
    ///
    /// Use [`Self::maybe_last`] to automatically get the last error set by tarantool.
    ///
    /// # Safety
    /// The pointer must point to a valid struct of type `BoxError`.
    pub unsafe fn from_ptr(error_ptr: NonNull<ffi::BoxError>) -> Self {
        let code = ffi::box_error_code(error_ptr.as_ptr());

        let message = CStr::from_ptr(ffi::box_error_message(error_ptr.as_ptr()));
        let message = message.to_string_lossy().into_owned().into_boxed_str();

        let error_type = CStr::from_ptr(ffi::box_error_type(error_ptr.as_ptr()));
        let error_type = error_type.to_string_lossy().into_owned().into_boxed_str();

        TarantoolError {
            code,
            message: Some(message),
            error_type: Some(error_type),
        }
    }

    /// Get the information about the last API call error.
    #[inline(always)]
    pub fn last() -> Self {
        TarantoolError::maybe_last().err().unwrap()
    }

    /// Return IPROTO error code
    #[inline(always)]
    pub fn error_code(&self) -> u32 {
        self.code
    }

    /// Return the error type, e.g. "ClientError", "SocketError", etc.
    #[inline(always)]
    pub fn error_type(&self) -> &str {
        self.error_type.as_deref().unwrap_or("Unknown")
    }

    /// Return the error message
    #[inline(always)]
    pub fn message(&self) -> &str {
        self.message
            .as_deref()
            .unwrap_or("<error message is missing>")
    }
}

impl Display for TarantoolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(code) = TarantoolErrorCode::from_i64(self.code as _) {
            return write!(f, "{:?}: {}", code, self.message());
        }
        write!(f, "tarantool error #{}: {}", self.code, self.message())
    }
}

impl From<TarantoolError> for Error {
    fn from(error: TarantoolError) -> Self {
        Error::Tarantool(error)
    }
}

////////////////////////////////////////////////////////////////////////////////
// TarantoolErrorCode
////////////////////////////////////////////////////////////////////////////////

crate::define_enum_with_introspection! {
    /// Codes of Tarantool errors
    #[repr(u32)]
    #[non_exhaustive]
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
}

#[allow(clippy::assertions_on_constants)]
const _: () = {
    assert!(TarantoolErrorCode::DISCRIMINANTS_ARE_SUBSEQUENT);
};

impl TarantoolErrorCode {
    pub fn try_last() -> Option<Self> {
        unsafe {
            let e_ptr = ffi::box_error_last();
            if e_ptr.is_null() {
                return None;
            }
            let u32_code = ffi::box_error_code(e_ptr);
            TarantoolErrorCode::from_i64(u32_code as _)
        }
    }

    pub fn last() -> Self {
        Self::try_last().unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////
// ...
////////////////////////////////////////////////////////////////////////////////

/// Clear the last error.
pub fn clear_error() {
    unsafe { ffi::box_error_clear() }
}

/// Set the last error.
///
/// # Example:
/// ```rust
/// # use tarantool::error::{TarantoolErrorCode, TarantoolError};
/// # fn foo() -> Result<(), tarantool::error::TarantoolError> {
/// let reason = "just 'cause";
/// tarantool::set_error!(TarantoolErrorCode::Unsupported, "this you cannot do, because: {reason}");
/// return Err(TarantoolError::last());
/// # }
/// ```
#[macro_export]
macro_rules! set_error {
    ($code:expr, $($msg_args:tt)+) => {{
        let msg = ::std::fmt::format(::std::format_args!($($msg_args)+));
        let msg = ::std::ffi::CString::new(msg).unwrap();
        // `msg` must outlive `msg_ptr`
        let msg_ptr = msg.as_ptr().cast();
        let file = $crate::c_ptr!(::std::file!());
        #[allow(unused_unsafe)]
        unsafe {
            $crate::ffi::tarantool::box_error_set(file as _, ::std::line!(), $code as u32, msg_ptr)
        }
    }};
}

/// Set the last tarantool error and return it immediately.
///
/// # Example:
/// ```rust
/// # use tarantool::set_and_get_error;
/// # use tarantool::error::TarantoolErrorCode;
/// # fn foo() -> Result<(), tarantool::error::TarantoolError> {
/// let reason = "just 'cause";
/// return Err(set_and_get_error!(TarantoolErrorCode::Unsupported, "this you cannot do, because: {reason}"));
/// # }
/// ```
#[macro_export]
macro_rules! set_and_get_error {
    ($code:expr, $($msg_args:tt)+) => {{
        $crate::set_error!($code, $($msg_args)+);
        $crate::error::TarantoolError::last()
    }};
}

////////////////////////////////////////////////////////////////////////////////
// EncodeError
////////////////////////////////////////////////////////////////////////////////

#[deprecated = "use `EncodeError` instead"]
pub type Encode = EncodeError;

/// Error that can happen when serializing a tuple
#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("{0}")]
    Rmp(#[from] rmp_serde::encode::Error),

    #[error("invalid msgpack value (epxected array, found {:?})", crate::util::DebugAsMPValue(.0))]
    InvalidMP(Vec<u8>),
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

#[test]
fn tarantool_error_doesnt_depend_on_link_error() {
    let err = Error::from(rmp_serde::decode::Error::OutOfRange);
    // This test checks that tarantool::error::Error can be displayed without
    // the need for linking to tarantool symbols, because `#[test]` tests are
    // linked into a standalone executable without access to those symbols.
    assert!(!err.to_string().is_empty());
    assert!(!format!("{}", err).is_empty());
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;

    #[crate::test(tarantool = "crate")]
    fn set_error_expands_format() {
        let msg = "my message";
        let e = set_and_get_error!(TarantoolErrorCode::Unknown, "{msg}");
        assert_eq!(e.to_string(), "Unknown: my message");
    }

    #[crate::test(tarantool = "crate")]
    fn set_error_with_no_semicolon() {
        // Basically you should always put double {{}} in your macros if there's
        // a let statement in it, otherwise it will suddenly stop compiling in
        // some weird context. And neither the compiler nor clippy will tell you
        // anything about this.
        _ = set_error!(TarantoolErrorCode::Unknown, "idk");

        if true {
            set_error!(TarantoolErrorCode::Unknown, "idk")
        } else {
            unreachable!()
        }; // <- Look at this beauty
           // Also never put ; after the if statement (unless it's required
           // for example if it's nested in a let statement), you should always
           // put ; inside both branches instead.
    }

    #[crate::test(tarantool = "crate")]
    fn tarantool_error_use_after_free() {
        let e = set_and_get_error!(TarantoolErrorCode::Unknown, "foo");
        assert_eq!(e.error_type(), "ClientError");
        clear_error();
        // This used to crash before the fix
        assert_eq!(e.error_type(), "ClientError");
    }
}

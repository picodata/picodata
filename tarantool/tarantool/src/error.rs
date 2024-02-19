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

use std::collections::HashMap;
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
use crate::util::to_cstring_lossy;

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
    #[error("box error: {0}")]
    Tarantool(BoxError),

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

    /// Error returned from the Tarantool server.
    ///
    /// It represents an error with which Tarantool server
    /// answers to the client in case of faulty request or an error
    /// during request execution on the server side.
    #[error("server responded with error: {0}")]
    Remote(BoxError),

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
            Self::Tarantool(_) => "Box",
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
    #[track_caller]
    fn from(e: TransactionError<E>) -> Self {
        match e {
            TransactionError::FailedToCommit(e) => e.into(),
            TransactionError::FailedToRollback(e) => e.into(),
            TransactionError::RolledBack(e) => e.into(),
            TransactionError::AlreadyStarted => BoxError::new(
                TarantoolErrorCode::ActiveTransaction,
                "transaction has already been started",
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
    #[track_caller]
    fn from(e: TimeoutError<E>) -> Self {
        match e {
            TimeoutError::Expired => BoxError::new(TarantoolErrorCode::Timeout, "timeout").into(),
            TimeoutError::Failed(e) => e.into(),
        }
    }
}

impl From<std::string::FromUtf8Error> for Error {
    #[inline(always)]
    fn from(error: std::string::FromUtf8Error) -> Self {
        // FIXME: we loose the data here
        error.utf8_error().into()
    }
}

////////////////////////////////////////////////////////////////////////////////
// BoxError
////////////////////////////////////////////////////////////////////////////////

/// Structured info about an error which can happen as a result of an internal
/// API or a remote procedure call.
///
/// Can also be used in user code to return structured error info from stored
/// procedures.
#[derive(Debug, Clone, Default)]
pub struct BoxError {
    pub(crate) code: u32,
    pub(crate) message: Option<Box<str>>,
    pub(crate) error_type: Option<Box<str>>,
    pub(crate) errno: Option<u32>,
    pub(crate) file: Option<Box<str>>,
    pub(crate) line: Option<u32>,
    pub(crate) fields: HashMap<Box<str>, rmpv::Value>,
    pub(crate) cause: Option<Box<BoxError>>,
}

// TODO mark this as deprecated
pub type TarantoolError = BoxError;

impl BoxError {
    /// Construct an error object with given error `code` and `message`. The
    /// resulting error will have `file` & `line` fields set from the caller's
    /// location.
    ///
    /// Use [`Self::with_location`] to override error location.
    #[inline(always)]
    #[track_caller]
    pub fn new(code: impl Into<u32>, message: impl Into<String>) -> Self {
        let location = std::panic::Location::caller();
        Self {
            code: code.into(),
            message: Some(message.into().into_boxed_str()),
            file: Some(location.file().into()),
            line: Some(location.line()),
            ..Default::default()
        }
    }

    /// Construct an error object with given error `code` and `message` and
    /// source location.
    ///
    /// Use [`Self::new`] to use the caller's location.
    #[inline(always)]
    pub fn with_location(
        code: impl Into<u32>,
        message: impl Into<String>,
        file: impl Into<String>,
        line: u32,
    ) -> Self {
        Self {
            code: code.into(),
            message: Some(message.into().into_boxed_str()),
            file: Some(file.into().into_boxed_str()),
            line: Some(line),
            ..Default::default()
        }
    }

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

    /// Create a `BoxError` from a poniter to the underlying struct.
    ///
    /// Use [`Self::maybe_last`] to automatically get the last error set by tarantool.
    ///
    /// # Safety
    /// The pointer must point to a valid struct of type `BoxError`.
    ///
    /// Also must only be called from the `tx` thread.
    pub unsafe fn from_ptr(error_ptr: NonNull<ffi::BoxError>) -> Self {
        let code = ffi::box_error_code(error_ptr.as_ptr());

        let message = CStr::from_ptr(ffi::box_error_message(error_ptr.as_ptr()));
        let message = message.to_string_lossy().into_owned().into_boxed_str();

        let error_type = CStr::from_ptr(ffi::box_error_type(error_ptr.as_ptr()));
        let error_type = error_type.to_string_lossy().into_owned().into_boxed_str();

        let mut file = None;
        let mut line = None;
        if let Some((f, l)) = error_get_file_line(error_ptr.as_ptr()) {
            file = Some(f.into());
            line = Some(l);
        }

        Self {
            code,
            message: Some(message),
            error_type: Some(error_type),
            errno: None,
            file,
            line,
            fields: HashMap::default(),
            cause: None,
        }
    }

    /// Get the information about the last API call error.
    #[inline(always)]
    pub fn last() -> Self {
        Self::maybe_last().err().unwrap()
    }

    /// Set `self` as the last API call error.
    /// Useful when returning errors from stored prcoedures.
    #[inline(always)]
    #[track_caller]
    pub fn set_last(&self) {
        let mut loc = None;
        if let Some(f) = self.file() {
            debug_assert!(self.line().is_some());
            loc = Some((f, self.line().unwrap_or(0)));
        }
        let message = to_cstring_lossy(self.message());
        set_last_error(loc, self.error_code(), &message);
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

    /// Return the name of the source file where the error was created,
    /// if it's available.
    #[inline(always)]
    pub fn file(&self) -> Option<&str> {
        self.file.as_deref()
    }

    /// Return the source line number where the error was created,
    /// if it's available.
    #[inline(always)]
    pub fn line(&self) -> Option<u32> {
        self.line
    }

    /// Return the system `errno` value of the cause of this error,
    /// if it's available.
    ///
    /// You can use [`std::io::Error::from_raw_os_error`] to get more details
    /// for the returned error code.
    #[inline(always)]
    pub fn errno(&self) -> Option<u32> {
        self.errno
    }

    /// Return the error which caused this one, if it's available.
    #[inline(always)]
    pub fn cause(&self) -> Option<&Self> {
        self.cause.as_deref()
    }

    /// Return the map of additional fields.
    #[inline(always)]
    pub fn fields(&self) -> &HashMap<Box<str>, rmpv::Value> {
        &self.fields
    }
}

impl Display for BoxError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(code) = TarantoolErrorCode::from_i64(self.code as _) {
            return write!(f, "{:?}: {}", code, self.message());
        }
        write!(f, "box error #{}: {}", self.code, self.message())
    }
}

impl From<BoxError> for Error {
    fn from(error: BoxError) -> Self {
        Error::Tarantool(error)
    }
}

/// # Safety
/// Only safe to be called from `tx` thread. Also `ptr` must point at a valid
/// instance of `ffi::BoxError`.
unsafe fn error_get_file_line(ptr: *const ffi::BoxError) -> Option<(String, u32)> {
    #[derive(Clone, Copy)]
    struct Failure;
    static mut FIELD_OFFSETS: Option<std::result::Result<(u32, u32), Failure>> = None;

    if FIELD_OFFSETS.is_none() {
        let lua = crate::lua_state();
        let res = lua.eval::<(u32, u32)>(
            "ffi = require 'ffi'
            return
                ffi.offsetof('struct error', '_file'),
                ffi.offsetof('struct error', '_line')",
        );
        let (file_ofs, line_ofs) = crate::unwrap_ok_or!(res,
            Err(e) => {
                crate::say_warn!("failed getting struct error type info: {e}");
                FIELD_OFFSETS = Some(Err(Failure));
                return None;
            }
        );
        FIELD_OFFSETS = Some(Ok((file_ofs, line_ofs)));
    }
    let (file_ofs, line_ofs) = crate::unwrap_ok_or!(
        FIELD_OFFSETS.expect("always Some at this point"),
        Err(Failure) => {
            return None;
        }
    );

    let ptr = ptr.cast::<u8>();
    // TODO: check that struct error::_file is an array of bytes via lua-jit's ffi.typeinfo
    let file_ptr = ptr.add(file_ofs as _).cast::<std::ffi::c_char>();
    let file = CStr::from_ptr(file_ptr).to_string_lossy().into_owned();
    // TODO: check that struct error::_line has type u32 via lua-jit's ffi.typeinfo
    let line_ptr = ptr.add(line_ofs as _).cast::<u32>();
    let line = *line_ptr;

    Some((file, line))
}

/// Sets the last tarantool error. The `file_line` specifies source location to
/// be set for the error. If it is `None`, the location of the caller is used
/// (see [`std::panic::Location::caller`] for details on caller location).
#[inline]
#[track_caller]
pub fn set_last_error(file_line: Option<(&str, u32)>, code: u32, message: &CStr) {
    let (file, line) = crate::unwrap_or!(file_line, {
        let file_line = std::panic::Location::caller();
        (file_line.file(), file_line.line())
    });

    // XXX: we allocate memory each time this is called (sometimes even more
    // than once). This is very sad...
    let file = to_cstring_lossy(file);

    // Safety: this is safe, because all pointers point to nul-terimnated
    // strings, and the "%s" format works with any nul-terimnated string.
    unsafe {
        ffi::box_error_set(
            file.as_ptr(),
            line,
            code,
            crate::c_ptr!("%s"),
            message.as_ptr(),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////
// IntoBoxError
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait represent an error which can be converted to
/// a structured tarantool internal error. In simple cases this may just be an
/// conversion into an error message, but may also add an error code and/or
/// additional custom fields. (custom fields not yet implemented).
pub trait IntoBoxError: Sized {
    /// Set `self` as the current fiber's last error.
    #[inline(always)]
    #[track_caller]
    fn set_last_error(self) {
        self.into_box_error().set_last();
    }

    /// Convert `self` to `BoxError`.
    fn into_box_error(self) -> BoxError;
}

impl IntoBoxError for BoxError {
    #[inline(always)]
    #[track_caller]
    fn set_last_error(self) {
        self.set_last()
    }

    #[inline(always)]
    fn into_box_error(self) -> BoxError {
        self
    }
}

impl IntoBoxError for Error {
    #[inline(always)]
    #[track_caller]
    fn into_box_error(self) -> BoxError {
        match self {
            Error::Tarantool(e) => e,
            Error::Remote(e) => {
                // TODO: maybe we want actually to set the last error to
                // something like ProcC, "server responded with error" and then
                // set `e` to be the `cause` of that error. But for now there's
                // no way to do that
                e
            }
            Error::Decode { .. } => {
                BoxError::new(TarantoolErrorCode::InvalidMsgpack, self.to_string())
            }
            Error::DecodeRmpValue(e) => {
                BoxError::new(TarantoolErrorCode::InvalidMsgpack, e.to_string())
            }
            Error::ValueRead(e) => BoxError::new(TarantoolErrorCode::InvalidMsgpack, e.to_string()),
            _ => BoxError::new(TarantoolErrorCode::ProcC, self.to_string()),
        }
    }
}

impl IntoBoxError for String {
    #[inline(always)]
    #[track_caller]
    fn into_box_error(self) -> BoxError {
        BoxError::new(TarantoolErrorCode::ProcC, self)
    }
}

impl IntoBoxError for &str {
    #[inline(always)]
    #[track_caller]
    fn into_box_error(self) -> BoxError {
        self.to_owned().into_box_error()
    }
}

impl IntoBoxError for Box<dyn std::error::Error> {
    #[inline(always)]
    #[track_caller]
    fn into_box_error(self) -> BoxError {
        (&*self).into_box_error()
    }
}

impl IntoBoxError for &dyn std::error::Error {
    #[inline(always)]
    #[track_caller]
    fn into_box_error(self) -> BoxError {
        let mut res = BoxError::new(TarantoolErrorCode::ProcC, self.to_string());
        if let Some(cause) = self.source() {
            res.cause = Some(Box::new(cause.into_box_error()));
        }
        res
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

impl From<TarantoolErrorCode> for u32 {
    #[inline(always)]
    fn from(code: TarantoolErrorCode) -> u32 {
        code as _
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
/// # use tarantool::error::{TarantoolErrorCode, BoxError};
/// # fn foo() -> Result<(), tarantool::error::BoxError> {
/// let reason = "just 'cause";
/// tarantool::set_error!(TarantoolErrorCode::Unsupported, "this you cannot do, because: {reason}");
/// return Err(BoxError::last());
/// # }
/// ```
#[macro_export]
macro_rules! set_error {
    ($code:expr, $($msg_args:tt)+) => {{
        let msg = ::std::fmt::format(::std::format_args!($($msg_args)+));
        let msg = ::std::ffi::CString::new(msg).unwrap();
        $crate::error::set_last_error(None, $code as _, &msg);
    }};
}

/// Set the last tarantool error and return it immediately.
///
/// # Example:
/// ```rust
/// # use tarantool::set_and_get_error;
/// # use tarantool::error::TarantoolErrorCode;
/// # fn foo() -> Result<(), tarantool::error::BoxError> {
/// let reason = "just 'cause";
/// return Err(set_and_get_error!(TarantoolErrorCode::Unsupported, "this you cannot do, because: {reason}"));
/// # }
/// ```
#[macro_export]
#[deprecated = "use `BoxError::new` instead"]
macro_rules! set_and_get_error {
    ($code:expr, $($msg_args:tt)+) => {{
        $crate::set_error!($code, $($msg_args)+);
        $crate::error::BoxError::last()
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
        set_error!(TarantoolErrorCode::Unknown, "{msg}");
        let e = BoxError::last();
        assert_eq!(e.to_string(), "Unknown: my message");
    }

    #[crate::test(tarantool = "crate")]
    fn set_error_format_sequences() {
        for c in b'a'..=b'z' {
            let c = c as char;
            set_error!(TarantoolErrorCode::Unknown, "%{c}");
            let e = BoxError::last();
            assert_eq!(e.to_string(), format!("Unknown: %{c}"));
        }
    }

    #[crate::test(tarantool = "crate")]
    fn set_error_caller_location() {
        //
        // If called in a function without `#[track_caller]`, the location of macro call is used
        //
        fn no_track_caller() {
            set_error!(TarantoolErrorCode::Unknown, "custom error");
        }
        let line_1 = line!() - 2; // line number where `set_error!` is called above

        no_track_caller();
        let e = BoxError::last();
        assert_eq!(e.file(), Some(file!()));
        assert_eq!(e.line(), Some(line_1));

        //
        // If called in a function with `#[track_caller]`, the location of the caller is used
        //
        #[track_caller]
        fn with_track_caller() {
            set_error!(TarantoolErrorCode::Unknown, "custom error");
        }

        with_track_caller();
        let line_2 = line!() - 1; // line number where `with_track_caller()` is called above

        let e = BoxError::last();
        assert_eq!(e.file(), Some(file!()));
        assert_eq!(e.line(), Some(line_2));

        //
        // If specified explicitly, the provided values are used
        //
        set_last_error(
            Some(("foobar", 420)),
            69,
            crate::c_str!("super custom error"),
        );
        let e = BoxError::last();
        assert_eq!(e.file(), Some("foobar"));
        assert_eq!(e.line(), Some(420));
    }

    #[crate::test(tarantool = "crate")]
    fn box_error_location() {
        //
        // If called in a function without `#[track_caller]`, the location where the error is constructed is used
        //
        fn no_track_caller() {
            let e = BoxError::new(69105_u32, "too many leaves");
            e.set_last();
        }
        let line_1 = line!() - 3; // line number where `BoxError` is constructed above

        no_track_caller();
        let e = BoxError::last();
        assert_eq!(e.file(), Some(file!()));
        assert_eq!(e.line(), Some(line_1));

        //
        // If called in a function with `#[track_caller]`, the location of the caller is used
        //
        #[track_caller]
        fn with_track_caller() {
            let e = BoxError::new(69105_u32, "too many leaves");
            e.set_last();
        }

        with_track_caller();
        let line_2 = line!() - 1; // line number where `with_track_caller()` is called above

        let e = BoxError::last();
        assert_eq!(e.file(), Some(file!()));
        assert_eq!(e.line(), Some(line_2));

        //
        // If specified explicitly, the provided values are used
        //
        BoxError::with_location(69105_u32, "too many leaves", "nice", 69).set_last();
        let e = BoxError::last();
        assert_eq!(e.file(), Some("nice"));
        assert_eq!(e.line(), Some(69));
    }

    #[crate::test(tarantool = "crate")]
    #[allow(clippy::let_unit_value)]
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
        set_error!(TarantoolErrorCode::Unknown, "foo");
        let e = BoxError::last();
        assert_eq!(e.error_type(), "ClientError");
        clear_error();
        // This used to crash before the fix
        assert_eq!(e.error_type(), "ClientError");
    }
}

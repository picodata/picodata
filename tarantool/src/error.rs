use std::{fmt, io};
use std::ffi::CStr;

use failure::_core::fmt::{Display, Formatter};
use num_traits::FromPrimitive;
use rmp_serde::decode::Error as DecodeError;
use rmp_serde::encode::Error as EncodeError;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Tarantool error: {}", _0)]
    Tarantool(TarantoolError),

    #[fail(display = "IO error: {}", _0)]
    IO(io::Error),

    #[fail(display = "Failed to encode tuple: {}", _0)]
    Encode(EncodeError),

    #[fail(display = "Failed to decode tuple: {}", _0)]
    Decode(DecodeError),

    #[fail(display = "Transaction issue: {}", _0)]
    Transaction(TransactionError),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IO(error)
    }
}

impl From<EncodeError> for Error {
    fn from(error: EncodeError) -> Self {
        Error::Encode(error)
    }
}

impl From<DecodeError> for Error {
    fn from(error: DecodeError) -> Self {
        Error::Decode(error)
    }
}

#[derive(Debug, Fail)]
pub enum TransactionError {
    #[fail(display = "Transaction has already been started")]
    AlreadyStarted,

    #[fail(display = "Failed to commit")]
    FailedToCommit,

    #[fail(display = "Failed to rollback")]
    FailedToRollback,
}

impl From<TransactionError> for Error {
    fn from(error: TransactionError) -> Self {
        Error::Transaction(error)
    }
}

#[derive(Debug)]
pub struct TarantoolError {
    code: TarantoolErrorCode,
    message: String
}

impl TarantoolError {
    pub fn maybe_last() -> Result<(), Self> {
        let error_ptr = unsafe { ffi::box_error_last() };
        if error_ptr.is_null() {
            return Ok(())
        }

        let code = unsafe { ffi::box_error_code(error_ptr) };
        let code = match TarantoolErrorCode::from_u32(code) {
            Some(code) => code,
            None => TarantoolErrorCode::Unknown,
        };

        let message = unsafe { CStr::from_ptr(ffi::box_error_message(error_ptr)) };
        let message = message.to_string_lossy().into_owned();

        Err(TarantoolError{
            code,
            message
        })
    }

    pub fn last() -> Self {
        TarantoolError::maybe_last().err().unwrap()
    }
}

impl Display for TarantoolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl From<TarantoolError> for Error {
    fn from(error: TarantoolError) -> Self {
        Error::Tarantool(error)
    }
}

#[repr(u32)]
#[derive(Debug, FromPrimitive)]
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
    NoSuchFieldName = 153,
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
    BootstrapReadonly = 201,
}

mod ffi {
    use std::os::raw::c_char;

    #[repr(C)]
    #[derive(Debug, Copy, Clone)]
    pub struct BoxError {
        _unused: [u8; 0],
    }

    extern "C" {
        pub fn box_error_code(error: *const BoxError) -> u32;
        pub fn box_error_message(error: *const BoxError) -> *const c_char;
        pub fn box_error_last() -> *mut BoxError;

        #[allow(dead_code)]
        pub fn box_error_type(error: *const BoxError) -> *const c_char;
    }
}

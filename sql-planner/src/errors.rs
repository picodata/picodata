use crate::executor::vdbe::SqlError;
use crate::ir::types::UnrestrictedType;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use sql_protocol::error::ProtocolError;
use sql_type_system::error::Error as TypeSystemError;
use std::fmt;
use tarantool::error::Error;
use tarantool::transaction::TransactionError;

const DO_SKIP: &str = "do skip";

/// Reason or object of errors.
#[derive(Debug, PartialEq)]
pub enum Entity {
    /// corresponding to operations on ACL.
    Acl,
    /// corresponding to enum Args
    Args,
    /// corresponding to struct 'AbstractSyntaxTree'
    AST,
    /// corresponding to struct 'ParseNode'
    ParseNode,
    /// corresponding to trait Aggregate
    Aggregate,
    /// corresponding to struct 'AggregateSignature'
    AggregateSignature,
    /// corresponding to struct 'AggregateCollector'
    AggregateCollector,
    /// corresponding to struct Buckets
    Buckets,
    /// raw bytes
    Bytes,
    /// general variant for cache
    Cache,
    /// corresponding to struct Chain
    Chain,
    /// general variant
    Column,
    /// CTE
    Cte,
    /// corresponding to operations on DDL.
    Ddl,
    /// corresponds to enum Distribution
    Distribution,
    /// tarantool distribution key
    DistributionKey,
    /// tarantool engine (memtx, vinyl)
    Engine,
    /// corresponds to enum Expression
    Expression,
    /// corresponds to struct 'ExpressionMapper'
    ExpressionMapper,
    /// corresponds to struct Histogram
    Histogram,
    /// tarantool index
    Index,
    /// tuple key definition
    KeyDef,
    /// corresponds to metadata field of struct ProducerResult
    Metadata,
    /// corresponds to enum MotionPolicy
    Motion,
    /// corresponds to enum MotionOpcode
    MotionOpcode,
    /// tarantool msgpack
    MsgPack,
    /// general variant for Name of some object
    Name,
    /// variant for node of tree
    Node,
    /// SQL operator
    Operator,
    /// corresponds to struct PatternWithParams
    PatternWithParams,
    /// corresponds to struct Plan
    Plan,
    /// primary key of tarantool space
    PrimaryKey,
    /// privilege participating in GRANT/REVOKE query
    Privilege,
    /// SQL query
    Query,
    /// corresponds to enum Relational
    Relational,
    /// corresponds to struct RequiredData
    RequiredData,
    /// corresponds to enum OptionKind
    Option,
    /// corresponds to struct OptionalData
    OptionalData,
    /// corresponds to struct OptionSpec
    OptionSpec,
    /// corresponds to struct ReferredNodes
    ReferredNodes,
    /// Routine
    Routine,
    /// parser rule
    Rule,
    /// corresponds to struct RouterRuntime
    Runtime,
    /// Metadata schema
    Schema,
    /// sharding key of tarantool space
    ShardingKey,
    /// tarantool space
    Space,
    // tarantool space engine type
    SpaceEngine,
    /// tarantool space metadata
    SpaceMetadata,
    /// corresponds to Function structs
    SQLFunction,
    /// corresponds to Volatile structs
    VolatileFunction,
    /// corresponds to struct Statement
    Statement,
    /// corresponds to CBO statistics
    Statistics,
    /// SQL sub-query
    SubQuery,
    /// sub-tree of the Plan
    SubTree,
    /// corresponds to sctruct SyntaxNode
    SyntaxNode,
    /// corresponds to struct SyntaxNodes
    SyntaxNodes,
    /// corresponds to struct SyntaxPlan
    SyntaxPlan,
    /// corresponds to struct Table
    Table,
    /// corresponds to struct BoxError
    Box,
    /// corresponds to struct Target
    Target,
    /// tarantool transaction
    Transaction,
    /// Tuple builder command
    TupleBuilderCommand,
    /// general variant for tuple
    Tuple,
    /// general variant for type of some object
    Type,
    /// corresponds to relational node Update
    Update,
    /// general variant for value of some object
    Value,
    /// corresponds to struct VirtualTable
    VirtualTable,
    /// corresponds to struct VTableKey
    VTableKey,
}

impl fmt::Display for Entity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let p = match self {
            Entity::Acl => "ACL".to_smolstr(),
            Entity::Args => "args".to_smolstr(),
            Entity::AST => "AST".to_smolstr(),
            Entity::Aggregate => "aggregate".to_smolstr(),
            Entity::AggregateCollector => "aggregate collector".to_smolstr(),
            Entity::AggregateSignature => "aggregate signature".to_smolstr(),
            Entity::Buckets => "buckets".to_smolstr(),
            Entity::Bytes => "bytes".to_smolstr(),
            Entity::Cache => "cache".to_smolstr(),
            Entity::Chain => "chain".to_smolstr(),
            Entity::Column => "column".to_smolstr(),
            Entity::Cte => "CTE".to_smolstr(),
            Entity::Ddl => "DDL".to_smolstr(),
            Entity::Distribution => "distribution".to_smolstr(),
            Entity::DistributionKey => "distribution key".to_smolstr(),
            Entity::Engine => "engine".to_smolstr(),
            Entity::Expression => "expression".to_smolstr(),
            Entity::ExpressionMapper => "expression mapper".to_smolstr(),
            Entity::Histogram => "histogram".to_smolstr(),
            Entity::Index => "index".to_smolstr(),
            Entity::KeyDef => "key definition".to_smolstr(),
            Entity::Metadata => "metadata".to_smolstr(),
            Entity::Motion => "motion".to_smolstr(),
            Entity::MotionOpcode => "motion opcode".to_smolstr(),
            Entity::MsgPack => "msgpack".to_smolstr(),
            Entity::Name => "name".to_smolstr(),
            Entity::Node => "node".to_smolstr(),
            Entity::Operator => "operator".to_smolstr(),
            Entity::PatternWithParams => "pattern with parameters".to_smolstr(),
            Entity::Plan => "plan".to_smolstr(),
            Entity::PrimaryKey => "primary key".to_smolstr(),
            Entity::Privilege => "privilege".to_smolstr(),
            Entity::ParseNode => "parse node".to_smolstr(),
            Entity::Query => "query".to_smolstr(),
            Entity::Relational => "relational".to_smolstr(),
            Entity::RequiredData => "required data".to_smolstr(),
            Entity::ReferredNodes => "referred nodes".to_smolstr(),
            Entity::Routine => "routine".to_smolstr(),
            Entity::Rule => "rule".to_smolstr(),
            Entity::Runtime => "runtime".to_smolstr(),
            Entity::Option => "option".to_smolstr(),
            Entity::OptionalData => "optional data".to_smolstr(),
            Entity::OptionSpec => "OptionSpec".to_smolstr(),
            Entity::Schema => "schema".to_smolstr(),
            Entity::ShardingKey => "sharding key".to_smolstr(),
            Entity::Space => "space".to_smolstr(),
            Entity::SpaceEngine => "space engine".to_smolstr(),
            Entity::SpaceMetadata => "space metadata".to_smolstr(),
            Entity::SQLFunction => "SQL function".to_smolstr(),
            Entity::VolatileFunction => "volatile function".to_smolstr(),
            Entity::Statement => "statement".to_smolstr(),
            Entity::Statistics => "statistics".to_smolstr(),
            Entity::SubQuery => "sub-query plan subtree".to_smolstr(),
            Entity::SubTree => "execution plan subtree".to_smolstr(),
            Entity::SyntaxNode => "syntax node".to_smolstr(),
            Entity::SyntaxNodes => "syntax nodes".to_smolstr(),
            Entity::SyntaxPlan => "syntax plan".to_smolstr(),
            Entity::Table => "table".to_smolstr(),
            Entity::Box => "box".to_smolstr(),
            Entity::Target => "target".to_smolstr(),
            Entity::Transaction => "transaction".to_smolstr(),
            Entity::Tuple => "tuple".to_smolstr(),
            Entity::TupleBuilderCommand => "TupleBuilderCommand".to_smolstr(),
            Entity::Type => "type".to_smolstr(),
            Entity::Update => "Update node".to_smolstr(),
            Entity::Value => "value".to_smolstr(),
            Entity::VirtualTable => "virtual table".to_smolstr(),
            Entity::VTableKey => "virtual table key".to_smolstr(),
        };
        write!(f, "{p}")
    }
}

/// Action that failed
#[derive(Debug, PartialEq)]
pub enum Action {
    Add,
    Borrow,
    Build,
    Clear,
    Create,
    Delete,
    Decode,
    Deserialize,
    Drop,
    Encode,
    Find,
    Get,
    Insert,
    Prepare,
    Put,
    Replace,
    ReplaceOnConflict,
    Retrieve,
    Serialize,
    Truncate,
    Update,
    Upsert,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let p = match self {
            Action::Add => "add".to_smolstr(),
            Action::Borrow => "borrow".to_smolstr(),
            Action::Build => "build".to_smolstr(),
            Action::Clear => "clear".to_smolstr(),
            Action::Create => "create".to_smolstr(),
            Action::Delete => "delete".to_smolstr(),
            Action::Decode => "decode".to_smolstr(),
            Action::Drop => "drop".to_smolstr(),
            Action::Encode => "encode".to_smolstr(),
            Action::Find => "find".to_smolstr(),
            Action::Deserialize => "deserialize".to_smolstr(),
            Action::Get => "get".to_smolstr(),
            Action::Insert => "insert".to_smolstr(),
            Action::Prepare => "prepare".to_smolstr(),
            Action::Put => "put".to_smolstr(),
            Action::Replace => "replace".to_smolstr(),
            Action::ReplaceOnConflict => "replace on conflict".to_smolstr(),
            Action::Retrieve => "retrieve".to_smolstr(),
            Action::Serialize => "serialize".to_smolstr(),
            Action::Truncate => "truncate".to_smolstr(),
            Action::Update => "update".to_smolstr(),
            Action::Upsert => "upsert".to_smolstr(),
        };
        write!(f, "{p}")
    }
}

#[derive(Debug, PartialEq)]
pub enum TypeError {
    AmbiguousParameterType(u16, UnrestrictedType, UnrestrictedType),
    CouldNotDetermineParameterType(usize),
    TypesCannotBeMatched(&'static str, UnrestrictedType, UnrestrictedType),
}

impl fmt::Display for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let p: SmolStr = match self {
            TypeError::AmbiguousParameterType(param_idx, ty1, ty2) => {
                let param_num = param_idx + 1;
                format_smolstr!(
                    "parameter ${param_num} is ambiguous, it can be either {ty1} or {ty2}"
                )
            }
            TypeError::CouldNotDetermineParameterType(param_idx) => {
                let param_num = param_idx + 1;
                format_smolstr!("could not determine data type of parameter ${param_num}")
            }
            TypeError::TypesCannotBeMatched(f, t1, t2) => {
                format_smolstr!("{f} types {t1} and {t2} cannot be matched")
            }
        };

        write!(f, "{p}")
    }
}

impl std::error::Error for TypeError {}

/// Types of error
#[derive(Debug, PartialEq)]
pub enum SbroadError {
    /// DoSkip is a special case of an error - nothing bad had happened, the target node doesn't contain
    /// anything interesting for us, skip it without any serious error.
    DoSkip,
    /// Some value that is considered to be unique is duplicated.
    /// Second param represents description.
    DuplicatedValue(SmolStr),
    /// Process of Action variant failed.
    /// Second param represents object of action.
    /// Third param represents reason of fail.
    FailedTo(Action, Option<Entity>, SmolStr),
    /// Object is invalid.
    /// Second param represents description and can be empty (None).
    Invalid(Entity, Option<SmolStr>),
    LuaError(SmolStr),
    /// Object not found.
    /// Second param represents description or name that let to identify object.
    NotFound(Entity, SmolStr),
    /// Object is not implemented yet.
    /// Second param represents description or name.
    NotImplemented(Entity, SmolStr),
    /// Error raised by object parsing.
    /// Second param represents error description.
    ParsingError(Entity, SmolStr),
    /// Unexpected number of values (list length etc.).
    /// Second param is information what was expected and what got.
    UnexpectedNumberOfValues(SmolStr),
    TypeError(TypeError),
    TypeSystemError(TypeSystemError),
    /// Object is not supported.
    /// Second param represents description or name that let to identify object.
    /// and can be empty (None).
    Unsupported(Entity, Option<SmolStr>),
    OutdatedStorageSchema,
    UseOfBothParamsStyles,
    GlobalDml(SmolStr),
    DispatchError(SmolStr),
    ProtocolError(ProtocolError),
    // Can't use vdbe::SqlError because it doesn't implement PartialEq :(
    VdbeError(SmolStr),
    Other(SmolStr),
}

impl fmt::Display for SbroadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let p: SmolStr = match self {
            SbroadError::DoSkip => DO_SKIP.to_smolstr(),
            SbroadError::DuplicatedValue(s) => format_smolstr!("duplicated value: {s}"),
            SbroadError::FailedTo(a, e, s) => match e {
                Some(entity) => format_smolstr!("failed to {a} {entity}: {s}"),
                None => format_smolstr!("failed to {a} {s}"),
            },
            SbroadError::GlobalDml(s) => format_smolstr!("dml operation on global tbl: {s}"),
            SbroadError::Invalid(e, s) => match s {
                Some(msg) => format_smolstr!("invalid {e}: {msg}"),
                None => format_smolstr!("invalid {e}"),
            },
            SbroadError::NotFound(e, s) => format_smolstr!("{e} {s} not found"),
            SbroadError::NotImplemented(e, s) => format_smolstr!("{e} {s} not implemented"),
            SbroadError::ParsingError(e, s) => format_smolstr!("{e} parsing error: {s}"),
            SbroadError::Unsupported(e, s) => match s {
                Some(msg) => format_smolstr!("unsupported {e}: {msg}"),
                None => format_smolstr!("unsupported {e}"),
            },
            SbroadError::UnexpectedNumberOfValues(s) => {
                format_smolstr!("unexpected number of values: {s}")
            }
            SbroadError::LuaError(e) => e.clone(),
            SbroadError::UseOfBothParamsStyles => {
                "invalid parameters usage. Got $n and ? parameters in one query!".into()
            }
            SbroadError::OutdatedStorageSchema => {
                "storage schema version different from router".into()
            }
            SbroadError::TypeError(err) => {
                format_smolstr!("{err}")
            }
            SbroadError::TypeSystemError(err) => {
                format_smolstr!("{err}")
            }
            SbroadError::ProtocolError(err) => {
                format_smolstr!("{err}")
            }
            SbroadError::DispatchError(s) | SbroadError::Other(s) | SbroadError::VdbeError(s) => {
                s.clone()
            }
        };

        write!(f, "{p}")
    }
}

impl std::error::Error for SbroadError {}

impl<E: fmt::Display> From<TransactionError<E>> for SbroadError {
    fn from(error: TransactionError<E>) -> Self {
        SbroadError::Invalid(Entity::Transaction, Some(format_smolstr!("{error}")))
    }
}

impl From<Error> for SbroadError {
    fn from(error: Error) -> Self {
        SbroadError::Invalid(Entity::Box, Some(format_smolstr!("{error}")))
    }
}

impl From<TypeError> for SbroadError {
    fn from(error: TypeError) -> Self {
        SbroadError::TypeError(error)
    }
}

impl From<TypeSystemError> for SbroadError {
    fn from(error: TypeSystemError) -> Self {
        SbroadError::TypeSystemError(error)
    }
}

impl From<ProtocolError> for SbroadError {
    fn from(error: ProtocolError) -> Self {
        SbroadError::ProtocolError(error)
    }
}

impl From<SqlError> for SbroadError {
    fn from(value: SqlError) -> Self {
        Self::VdbeError(value.to_smolstr())
    }
}

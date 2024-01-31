use once_cell::sync::OnceCell;
use sbroad::ir::ddl::Language;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;
use std::time::Duration;

use tarantool::auth::AuthDef;
use tarantool::auth::AuthMethod;
use tarantool::error::TarantoolError;
use tarantool::error::TarantoolErrorCode;
use tarantool::fiber;
use tarantool::session::UserId;
use tarantool::set_error;
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

use sbroad::ir::value::Value as IrValue;

use serde::{Deserialize, Serialize};

use crate::cas::{self, compare_and_swap};
use crate::storage;
use crate::storage::{Clusterwide, SPACE_ID_INTERNAL_MAX};
use crate::storage::{ClusterwideTable, PropertyName};
use crate::traft::error::Error;
use crate::traft::op::{Ddl, Op};
use crate::traft::{self, node, RaftIndex};
use crate::util::effective_user_id;

pub const INITIAL_SCHEMA_VERSION: u64 = 0;

////////////////////////////////////////////////////////////////////////////////
// TableDef
////////////////////////////////////////////////////////////////////////////////

/// Database table definition.
///
/// Describes a user-defined table.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableDef {
    pub id: SpaceId,
    pub name: String,
    pub distribution: Distribution,
    pub format: Vec<tarantool::space::Field>,
    pub schema_version: u64,
    pub operable: bool,
    pub engine: SpaceEngineType,
    pub owner: UserId,
}

impl Encode for TableDef {}

impl TableDef {
    /// Index (0-based) of field "operable" in the _pico_table table format.
    pub const FIELD_OPERABLE: usize = 5;

    /// Format of the _pico_table global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("id", FieldType::Unsigned)),
            Field::from(("name", FieldType::String)),
            Field::from(("distribution", FieldType::Array)),
            Field::from(("format", FieldType::Array)),
            Field::from(("schema_version", FieldType::Unsigned)),
            Field::from(("operable", FieldType::Boolean)),
            Field::from(("engine", FieldType::String)),
            Field::from(("owner", FieldType::Unsigned)),
        ]
    }

    /// A dummy instance of the type for use in tests.
    #[inline(always)]
    pub fn for_tests() -> Self {
        Self {
            id: 10569,
            name: "stuff".into(),
            distribution: Distribution::Global,
            format: vec![],
            schema_version: 420,
            operable: true,
            engine: SpaceEngineType::Blackhole,
            owner: 42,
        }
    }

    pub fn to_space_metadata(&self) -> traft::Result<SpaceMetadata> {
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
            user_id: self.owner,
            name: self.name.as_str().into(),
            engine: self.engine,
            field_count: 0,
            flags,
            format,
        };

        Ok(space_def)
    }

    /// Initial entries that should be put into system table [Tables](crate::storage::Tables) at bootstrap.
    pub fn system_tables() -> Vec<Self> {
        ClusterwideTable::all_tables()
            .iter()
            .map(|t| Self {
                id: t.id(),
                name: t.name().into(),
                distribution: Distribution::Global,
                format: t.format(),
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                engine: SpaceEngineType::Memtx,
                owner: ADMIN_ID,
            })
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////
// Distribution
////////////////////////////////////////////////////////////////////////////////

/// Defines how to distribute tuples in a table across replicasets.
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

/// Database index definition.
///
/// Describes a user-defined index.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexDef {
    pub table_id: SpaceId,
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
    /// Index (0-based) of field "operable" in _pico_index table format.
    pub const FIELD_OPERABLE: usize = 6;

    /// Format of the _pico_index global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("table_id", FieldType::Unsigned)),
            Field::from(("id", FieldType::Unsigned)),
            Field::from(("name", FieldType::String)),
            Field::from(("local", FieldType::Boolean)),
            Field::from(("parts", FieldType::Array)),
            Field::from(("schema_version", FieldType::Unsigned)),
            Field::from(("operable", FieldType::Boolean)),
            Field::from(("unique", FieldType::Boolean)),
        ]
    }

    /// A dummy instance of the type for use in tests.
    #[inline(always)]
    pub fn for_tests() -> Self {
        Self {
            table_id: 10569,
            id: 1,
            name: "secondary".into(),
            local: true,
            parts: vec![],
            schema_version: 420,
            operable: true,
            unique: false,
        }
    }

    pub fn to_index_metadata(&self) -> IndexMetadata {
        use tarantool::index::IndexType;

        let mut opts = BTreeMap::new();
        opts.insert(Cow::from("unique"), Value::Bool(self.unique));
        let index_meta = IndexMetadata {
            space_id: self.table_id,
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
    pub owner: UserId,
}

impl Encode for UserDef {}

impl UserDef {
    /// Index of field "auth" in the space _pico_user format.
    ///
    /// Index of first field is 0.
    pub const FIELD_AUTH: usize = 3;

    /// Format of the _pico_user global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("id", FieldType::Unsigned)),
            Field::from(("name", FieldType::String)),
            Field::from(("schema_version", FieldType::Unsigned)),
            Field::from(("auth", FieldType::Array)),
            Field::from(("owner", FieldType::Unsigned)),
        ]
    }

    /// A dummy instance of the type for use in tests.
    #[inline(always)]
    pub fn for_tests() -> Self {
        Self {
            id: 69,
            name: "david".into(),
            schema_version: 421,
            auth: AuthDef::new(tarantool::auth::AuthMethod::ChapSha1, "".into()),
            owner: 42,
        }
    }
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
    pub owner: UserId,
}

impl Encode for RoleDef {}

impl RoleDef {
    /// Format of the _pico_role global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("id", FieldType::Unsigned)),
            Field::from(("name", FieldType::String)),
            Field::from(("schema_version", FieldType::Unsigned)),
            Field::from(("owner", FieldType::Unsigned)),
        ]
    }

    /// A dummy instance of the type for use in tests.
    #[inline(always)]
    pub fn for_tests() -> Self {
        Self {
            id: 13,
            name: "devops".into(),
            schema_version: 419,
            owner: 42,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// PrivilegeDef
////////////////////////////////////////////////////////////////////////////////

/// User id of the builtin user "guest".
///
/// Default "untrusted" user.
///
/// See also <https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/_user/#box-space-user>
pub const GUEST_ID: UserId = 0;

/// User id of the builtin user "admin".
///
/// Note: Admin user id is used when we need to elevate privileges
/// because current user doesnt have access to system spaces
///
/// See also <https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/_user/#box-space-user>
pub const ADMIN_ID: UserId = 1;

/// User id of the builtin role "public".
///
/// Pre-defined role, automatically granted to new users.
/// Granting some privilege to this role is equivalent granting the privilege to everybody.
///
/// See also <https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/_user/#box-space-user>
pub const PUBLIC_ID: UserId = 2;

/// User id of the builtin role "replication".
///
/// Role "replication" has the following grants:
/// - Read access to the "universe"
/// - Write access to the space "_cluster"
pub const ROLE_REPLICATION_ID: i64 = 3;

/// User id of the builtin role "super".
///
/// Users with this role have access to everything.
///
/// See also <https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/_user/#box-space-user>
pub const SUPER_ID: UserId = 31;

/// User id of the builtin user "pico_service".
///
/// A special user for internal communication between instances of picodata.
/// It is equivalent in it's privileges to "admin". The only difference is that
/// only the automated rpc calls are performed as "pico_service".
pub const PICO_SERVICE_ID: UserId = 32;

/// Object id of the special builtin object "universe".
///
/// Object "universe" is basically an alias to the "whole database".
/// Granting access to "universe" is equivalent to granting access
/// to all objects of all types for which the privilege makes sense.
pub const UNIVERSE_ID: i64 = 0;

tarantool::define_str_enum! {
    pub enum SchemaObjectType {
        Table = "table",
        Role = "role",
        Routine = "routine",
        User = "user",
        Universe = "universe",
    }
}

impl SchemaObjectType {
    pub fn as_tarantool(&self) -> &'static str {
        match self {
            SchemaObjectType::Table => "space",
            t => t.as_str(),
        }
    }
}

tarantool::define_str_enum! {
    /// Picodata privilege types. For correspondence with
    /// tarantool privilege types see [`PrivilegeType::as_tarantool`].
    ///
    /// For each variant it is described which SQL queries in picodata
    /// a user with this privilege can execute.
    pub enum PrivilegeType {
        /// Allows SQL queries: `SELECT`
        Read = "read",
        /// Allows SQL queries: `INSERT`, `UPDATE`, `DELETE`
        Write = "write",
        /// Allows SQL queries: `CALL`.
        /// Also can indicate that role is granted to a user
        /// if object type is role.
        Execute = "execute",
        /// Allows a user to log-in when connecting to picodata instance
        Login = "login",
        /// Allows SQL queries: `CREATE`
        Create = "create",
        /// Allows SQL queries: `DROP`
        Drop = "drop",
        /// Allows SQL queries: `ALTER`
        Alter = "alter",
    }
}

impl PrivilegeType {
    /// Converts picodata privilege to a string that can be passed to
    /// `box.schema.user.grant` as `privileges`
    pub fn as_tarantool(&self) -> &'static str {
        match self {
            PrivilegeType::Login => "usage",
            t => t.as_str(),
        }
    }
}

/// Privilege definition.
/// Note the differences between picodata privileges and vanilla tarantool ones.
/// 1) Super role in picodata is a placeholder. It exists because picodata reuses user
///    ids from vanilla and super occupies an id. So to avoid assigning some piciodata
///    user Id of the vanilla role super we keep it as a placeholder. Aside from that
///    vanilla super role has some privileges that cannot be represented in picodata,
///    namely privileges on universe. This hole opens possibility for unwanted edge cases.
/// 2) Only Session and Usage can be granted on Universe.
/// Note that validation is not performed in Deserialize. We assume that for untrusted data
/// the object is created via constructor. In other cases validation was already performed
/// prior to serialization thus deserialization always creates a valid object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PrivilegeDef {
    privilege: PrivilegeType,
    object_type: SchemaObjectType,
    /// `-1` denotes an absense of a target object.
    /// Other values should be >= 0 and denote an existing target object.
    /// When working with `object_type` `universe` it might seem that it does
    /// not have a target object and `object_id` should be `-1`, this is incorrect
    /// universe has a target object with `object_id == 0`.
    ///
    /// To get the value of this field as `Option<u32>` see [`Self::object_id`]
    object_id: i64,
    /// Id of the user or role to whom the privilege is granted.
    ///
    /// In tarantool users and roles are stored in the same space, which means a
    /// role and a user cannot have the same id or name.
    grantee_id: UserId,
    grantor_id: UserId,
    schema_version: u64,
}

impl Encode for PrivilegeDef {}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub struct InvalidPrivilegeError {
    object_type: SchemaObjectType,
    unsupported: PrivilegeType,
    expected_one_of: &'static [PrivilegeType],
}

impl Display for InvalidPrivilegeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Unsupported {} privilege '{}', expected one of {:?}",
            self.object_type, self.unsupported, self.expected_one_of,
        ))
    }
}

impl PrivilegeDef {
    pub fn new(
        privilege: PrivilegeType,
        object_type: SchemaObjectType,
        object_id: i64,
        grantee_id: UserId,
        grantor_id: UserId,
        schema_version: u64,
    ) -> Result<PrivilegeDef, InvalidPrivilegeError> {
        let privilege_def = PrivilegeDef {
            privilege,
            object_type,
            object_id,
            grantee_id,
            grantor_id,
            schema_version,
        };

        use PrivilegeType::*;

        let valid_privileges: &[PrivilegeType] = match privilege_def.object_type {
            SchemaObjectType::Table => match privilege_def.object_id() {
                Some(_) => &[Read, Write, Alter, Drop],
                None => &[Read, Write, Create, Alter, Drop],
            },
            SchemaObjectType::Role => match privilege_def.object_id() {
                Some(_) => &[Execute, Drop],
                None => &[Create, Drop],
            },
            SchemaObjectType::User => match privilege_def.object_id() {
                Some(_) => &[Alter, Drop],
                None => &[Create, Alter, Drop],
            },
            SchemaObjectType::Universe => &[Login],
            SchemaObjectType::Routine => match privilege_def.object_id() {
                Some(_) => &[Execute, Drop],
                None => &[Create, Drop],
            },
        };

        if !valid_privileges.contains(&privilege) {
            return Err(InvalidPrivilegeError {
                object_type,
                unsupported: privilege,
                expected_one_of: valid_privileges,
            });
        }

        Ok(privilege_def)
    }

    #[inline(always)]
    pub fn privilege(&self) -> PrivilegeType {
        self.privilege
    }

    #[inline(always)]
    pub fn object_type(&self) -> SchemaObjectType {
        self.object_type
    }

    /// Get `object_id` field interpreting `-1` as `None`.
    #[inline(always)]
    pub fn object_id(&self) -> Option<u32> {
        if self.object_id >= 0 {
            Some(self.object_id as _)
        } else {
            debug_assert_eq!(self.object_id, -1, "object_id should be >= -1");
            None
        }
    }

    #[inline(always)]
    pub fn object_id_raw(&self) -> i64 {
        self.object_id
    }

    #[inline(always)]
    pub fn grantee_id(&self) -> UserId {
        self.grantee_id
    }

    #[inline(always)]
    pub fn grantor_id(&self) -> UserId {
        self.grantor_id
    }

    #[inline(always)]
    pub fn schema_version(&self) -> u64 {
        self.schema_version
    }

    /// Format of the _pico_privilege global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("privilege", FieldType::String)),
            Field::from(("object_type", FieldType::String)),
            Field::from(("object_id", FieldType::Integer)),
            Field::from(("grantee_id", FieldType::Unsigned)),
            Field::from(("grantor_id", FieldType::Unsigned)),
            Field::from(("schema_version", FieldType::Unsigned)),
        ]
    }

    /// A dummy instance of the type for use in tests.
    #[inline(always)]
    pub fn for_tests() -> Self {
        Self {
            grantor_id: 13,
            grantee_id: 37,
            object_type: SchemaObjectType::User,
            object_id: -1,
            privilege: PrivilegeType::Create,
            schema_version: 337,
        }
    }

    pub fn get_default_privileges() -> &'static Vec<PrivilegeDef> {
        static DEFAULT_PRIVILEGES: OnceCell<Vec<PrivilegeDef>> = OnceCell::new();
        DEFAULT_PRIVILEGES.get_or_init(|| {
            let mut v = Vec::new();

            // SQL: ALTER USER 'guest' WITH LOGIN
            // SQL: ALTER USER 'admin' WITH LOGIN
            for user in [GUEST_ID, ADMIN_ID] {
                v.push(PrivilegeDef {
                    grantor_id: ADMIN_ID,
                    grantee_id: user,
                    object_type: SchemaObjectType::Universe,
                    object_id: UNIVERSE_ID,
                    privilege: PrivilegeType::Login,
                    // This means the local schema is already up to date and main loop doesn't need to do anything
                    schema_version: INITIAL_SCHEMA_VERSION,
                });
            }

            // SQL: GRANT 'public' TO 'guest'
            v.push(PrivilegeDef {
                grantor_id: ADMIN_ID,
                grantee_id: GUEST_ID,
                object_type: SchemaObjectType::Role,
                object_id: PUBLIC_ID as _,
                privilege: PrivilegeType::Execute,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            });

            v
        })
    }

    pub fn is_default_privilege(&self) -> bool {
        Self::get_default_privileges().contains(self)
            // default user permissions
            || (self.object_id == PUBLIC_ID as i64 && self.privilege == PrivilegeType::Execute)
            || (self.object_type == SchemaObjectType::Universe && self.privilege == PrivilegeType::Login)
            // alter on himself
            || (self.object_type == SchemaObjectType::User && self.privilege == PrivilegeType::Alter && self.grantee_id as i64 == self.object_id)
    }

    /// Retrieves object_name from system spaces based on `object_id` and `object_type`.
    /// Returns `Ok(None)` in the case when the privilege has no target object (e.g. `object_id == -1`)
    /// or when target object is universe.
    /// Returns `Err` in case when target object was not found in system tables.
    ///
    /// # Panics
    /// 1. On storage failure
    pub fn resolve_object_name(&self, storage: &Clusterwide) -> Result<Option<String>, Error> {
        let Some(id) = self.object_id() else {
            return Ok(None);
        };
        let name = match self.object_type {
            SchemaObjectType::Table => storage.tables.get(id).map(|t| t.map(|t| t.name)),
            SchemaObjectType::Role => storage.roles.by_id(id).map(|t| t.map(|t| t.name)),
            SchemaObjectType::User => storage.users.by_id(id).map(|t| t.map(|t| t.name)),
            SchemaObjectType::Universe => {
                debug_assert_eq!(self.object_id, 0);
                return Ok(None);
            }
            SchemaObjectType::Routine => storage.routines.by_id(id).map(|t| t.map(|t| t.name)),
        }
        .expect("storage should not fail")
        .ok_or_else(|| Error::other(format!("object with id {id} should exist")))?;
        Ok(Some(name))
    }
}

////////////////////////////////////////////////////////////////////////////////
// init_pico_service
////////////////////////////////////////////////////////////////////////////////

/// Name of the special builtin user for internal communication between
/// instances. It's id is [`PICO_SERVICE_ID`].
///
/// Use this constant instead of literal "pico_service" so that it's easier to
/// find all the places where we refer to "pico_service".
pub const PICO_SERVICE_USER_NAME: &'static str = "pico_service";

#[inline]
pub fn pico_service_user_def() -> &'static UserDef {
    static PICO_SERVICE_USER_DEF: OnceCell<UserDef> = OnceCell::new();
    PICO_SERVICE_USER_DEF.get_or_init(|| UserDef {
        id: PICO_SERVICE_ID,
        name: PICO_SERVICE_USER_NAME.into(),
        // This means the local schema is already up to date and main loop doesn't need to do anything
        schema_version: INITIAL_SCHEMA_VERSION,
        auth: AuthDef::new(
            AuthMethod::ChapSha1,
            tarantool::auth::AuthData::new(&AuthMethod::ChapSha1, PICO_SERVICE_USER_NAME, "")
                .into_string(),
        ),
        owner: ADMIN_ID,
    })
}

#[inline]
pub fn pico_service_privilege_defs() -> &'static [PrivilegeDef] {
    static PICO_SERVICE_PRIVILEGE_DEFS: OnceCell<Vec<PrivilegeDef>> = OnceCell::new();
    PICO_SERVICE_PRIVILEGE_DEFS.get_or_init(|| {
        let mut res = Vec::with_capacity(PrivilegeType::VARIANTS.len());

        for &privilege in PrivilegeType::VARIANTS {
            res.push(PrivilegeDef {
                privilege,
                object_type: SchemaObjectType::Universe,
                object_id: UNIVERSE_ID,
                grantee_id: PICO_SERVICE_ID,
                grantor_id: ADMIN_ID,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            });
        }

        // TODO: explain
        res.push(PrivilegeDef {
            privilege: PrivilegeType::Execute,
            object_type: SchemaObjectType::Role,
            object_id: ROLE_REPLICATION_ID,
            grantee_id: PICO_SERVICE_ID,
            grantor_id: ADMIN_ID,
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
        });

        res
    })
}

pub fn init_user_pico_service() {
    let sys_user = SystemSpace::User.as_space();
    let sys_priv = SystemSpace::Priv.as_space();

    let t = sys_user
        .get(&[PICO_SERVICE_ID])
        .expect("reading from _user shouldn't fail");
    if t.is_some() {
        // Already exists (instance restarted)
        return;
    }

    let user_def = pico_service_user_def();
    let res = storage::acl::on_master_create_user(user_def, false);
    if let Err(e) = res {
        panic!("failed creating user '{PICO_SERVICE_USER_NAME}': {e}");
    }

    // Grant ALL privileges to "every object of every type".
    const PRIVILEGE_ALL: u32 = 0xffff_ffff;
    let res = sys_priv.insert(&(
        ADMIN_ID,
        PICO_SERVICE_ID,
        "universe",
        UNIVERSE_ID,
        PRIVILEGE_ALL,
    ));
    if let Err(e) = res {
        panic!("failed creating user '{PICO_SERVICE_USER_NAME}': {e}");
    }

    // Also grant role "replication", because all privileges to "every object of
    // every type" is not enough.
    const PRIVILEGE_EXECUTE: u32 = 4;
    let res = sys_priv.insert(&(
        ADMIN_ID,
        PICO_SERVICE_ID,
        "role",
        ROLE_REPLICATION_ID,
        PRIVILEGE_EXECUTE,
    ));
    if let Err(e) = res {
        panic!("failed creating user '{PICO_SERVICE_USER_NAME}': {e}");
    }
}

////////////////////////////////////////////////////////////////////////////////
// RoutineDef
////////////////////////////////////////////////////////////////////////////////

/// Routine kind.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RoutineKind {
    #[default]
    Procedure,
}

impl Display for RoutineKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutineKind::Procedure => write!(f, "procedure"),
        }
    }
}

/// Parameter mode.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RoutineParamMode {
    #[default]
    In,
}

impl Display for RoutineParamMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutineParamMode::In => write!(f, "in"),
        }
    }
}

/// Routine parameter definition.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoutineParamDef {
    #[serde(default)]
    pub mode: RoutineParamMode,
    pub r#type: FieldType,
    #[serde(default)]
    pub default: Option<IrValue>,
}

impl Display for RoutineParamDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mode: {}, type: {}", self.mode, self.r#type)?;
        if let Some(default) = &self.default {
            write!(f, ", default: {}", default)?;
        }
        Ok(())
    }
}

impl Default for RoutineParamDef {
    fn default() -> Self {
        Self {
            mode: RoutineParamMode::default(),
            r#type: FieldType::Scalar,
            default: None,
        }
    }
}

impl RoutineParamDef {
    pub fn with_type(self, r#type: FieldType) -> Self {
        Self { r#type, ..self }
    }
}

pub type RoutineParams = Vec<RoutineParamDef>;

pub type RoutineReturns = Vec<IrValue>;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RoutineLanguage {
    #[default]
    SQL,
}

impl From<Language> for RoutineLanguage {
    fn from(language: Language) -> Self {
        match language {
            Language::SQL => Self::SQL,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RoutineSecurity {
    #[default]
    Invoker,
}

/// Routine definition.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoutineDef {
    pub id: u32,
    pub name: String,
    pub kind: RoutineKind,
    pub params: RoutineParams,
    pub returns: RoutineReturns,
    pub language: RoutineLanguage,
    pub body: String,
    pub security: RoutineSecurity,
    pub operable: bool,
    pub schema_version: u64,
    pub owner: UserId,
}

impl Encode for RoutineDef {}

impl RoutineDef {
    /// Index (0-based) of field "operable" in _pico_routine table format.
    pub const FIELD_OPERABLE: usize = 8;

    /// Format of the _pico_routine global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("id", FieldType::Unsigned)),
            Field::from(("name", FieldType::String)),
            Field::from(("kind", FieldType::String)),
            Field::from(("params", FieldType::Array)),
            Field::from(("returns", FieldType::Array)),
            Field::from(("language", FieldType::String)),
            Field::from(("body", FieldType::String)),
            Field::from(("security", FieldType::String)),
            Field::from(("operable", FieldType::Boolean)),
            Field::from(("schema_version", FieldType::Unsigned)),
            Field::from(("owner", FieldType::Unsigned)),
        ]
    }

    /// A dummy instance of the type for use in tests.
    #[inline(always)]
    pub fn for_tests() -> Self {
        Self {
            id: 16005,
            name: "proc".into(),
            kind: RoutineKind::Procedure,
            params: vec![
                RoutineParamDef {
                    mode: RoutineParamMode::In,
                    r#type: FieldType::String,
                    default: Some(IrValue::String("hello".into())),
                },
                RoutineParamDef {
                    mode: RoutineParamMode::In,
                    r#type: FieldType::Unsigned,
                    default: None,
                },
            ],
            returns: vec![],
            language: RoutineLanguage::SQL,
            body: "values (?), (?)".into(),
            security: RoutineSecurity::Invoker,
            operable: true,
            schema_version: 421,
            owner: 42,
        }
    }
}

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
    CreateTable(#[from] CreateTableError),
    #[error("ddl operation was aborted")]
    Aborted,
    #[error("there is no pending ddl operation")]
    NoPendingDdl,
    #[error("{0}")]
    CreateRoutine(#[from] CreateRoutineError),
}

#[derive(Debug, thiserror::Error)]
pub enum CreateTableError {
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

impl From<CreateTableError> for Error {
    fn from(err: CreateTableError) -> Self {
        DdlError::CreateTable(err).into()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CreateRoutineError {
    #[error("routine {name} already exists with a different kind")]
    ExistsWithDifferentKind { name: String },
    #[error("routine {name} already exists with different parameters")]
    ExistsWithDifferentParams { name: String },
    #[error("routine {name} already exists with a different language")]
    ExistsWithDifferentLanguage { name: String },
    #[error("routine {name} already exists with a different body")]
    ExistsWithDifferentBody { name: String },
    #[error("routine {name} already exists with a different security")]
    ExistsWithDifferentSecurity { name: String },
    #[error("routine {name} already exists with a different owner")]
    ExistsWithDifferentOwner { name: String },
}

impl From<CreateRoutineError> for Error {
    fn from(err: CreateRoutineError) -> Self {
        DdlError::CreateRoutine(err).into()
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

#[derive(Clone, Debug)]
pub struct CreateProcParams {
    pub name: String,
    pub params: RoutineParams,
    pub language: RoutineLanguage,
    pub body: String,
    pub security: RoutineSecurity,
    pub owner: UserId,
}

impl CreateProcParams {
    pub fn func_exists(&self) -> bool {
        let func_space = Space::from(SystemSpace::Func);

        let name_idx = func_space
            .index_cached("name")
            .expect("_function should have an index by name");
        let t = name_idx
            .get(&[&self.name])
            .expect("reading from _function shouldn't fail");
        t.is_some()
    }

    pub fn validate(&self, storage: &Clusterwide) -> traft::Result<()> {
        let routine = storage.routines.by_name(&self.name)?;
        if let Some(def) = routine {
            if def.kind != RoutineKind::Procedure {
                return Err(CreateRoutineError::ExistsWithDifferentKind { name: def.name })?;
            }
            if def.params != self.params {
                return Err(CreateRoutineError::ExistsWithDifferentParams { name: def.name })?;
            }
            if def.language != self.language {
                return Err(CreateRoutineError::ExistsWithDifferentLanguage { name: def.name })?;
            }
            if def.body != self.body {
                return Err(CreateRoutineError::ExistsWithDifferentBody { name: def.name })?;
            }
            if def.security != self.security {
                return Err(CreateRoutineError::ExistsWithDifferentSecurity { name: def.name })?;
            }
            if def.owner != self.owner {
                return Err(CreateRoutineError::ExistsWithDifferentOwner { name: def.name })?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, LuaRead)]
pub struct CreateTableParams {
    pub(crate) id: Option<SpaceId>,
    pub(crate) name: String,
    pub(crate) format: Vec<Field>,
    pub(crate) primary_key: Vec<String>,
    pub(crate) distribution: DistributionParam,
    pub(crate) by_field: Option<String>,
    pub(crate) sharding_key: Option<Vec<String>>,
    pub(crate) sharding_fn: Option<ShardingFn>,
    pub(crate) engine: Option<SpaceEngineType>,
    pub(crate) owner: UserId,
    /// Timeout in seconds.
    ///
    /// Specifying the timeout identifies how long user is ready to wait for ddl to be applied.
    /// But it does not provide guarantees that a ddl will be aborted if wait for commit timeouts.
    pub timeout: Option<f64>,
}

impl CreateTableParams {
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
            return Err(CreateTableError::ExistsWithDifferentName {
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
                return Err(CreateTableError::DuplicateFieldName(field.name.clone()).into());
            }
        }
        // All primary key components exist in fields
        for part in &self.primary_key {
            if !field_names.contains(part.as_str()) {
                return Err(CreateTableError::FieldUndefined(part.clone()).into());
            }
        }
        // Global spaces must have memtx engine
        if self.distribution == DistributionParam::Global
            && self.engine.is_some_and(|e| e != SpaceEngineType::Memtx)
        {
            return Err(CreateTableError::IncompatibleGlobalSpaceEngine.into());
        }
        // All sharding key components exist in fields
        if self.distribution == DistributionParam::Sharded {
            match (&self.by_field, &self.sharding_key) {
                (Some(by_field), None) => {
                    if !field_names.contains(by_field.as_str()) {
                        return Err(CreateTableError::FieldUndefined(by_field.clone()).into());
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
                            return Err(CreateTableError::FieldUndefined(part.clone()).into());
                        }
                        // And all parts are unique
                        if !parts.insert(part.as_str()) {
                            return Err(CreateTableError::DuplicateFieldName(part.clone()).into());
                        }
                    }
                }
                (None, None) => return Err(CreateTableError::ShardingPolicyUndefined.into()),
                (Some(_), Some(_)) => {
                    return Err(CreateTableError::ConflictingShardingPolicy.into())
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
    pub fn test_create_space(&self, storage: &Clusterwide) -> traft::Result<()> {
        let id = self.id.expect("space id should've been chosen by now");
        let user = storage
            .users
            .by_id(self.owner)?
            .ok_or_else(|| Error::Other(format!("user with id {} not found", self.owner).into()))?
            .name;
        let err = transaction(|| -> Result<(), Option<tarantool::error::Error>> {
            // TODO: allow create_space to accept user by id
            ::tarantool::schema::space::create_space(
                &self.name,
                &SpaceCreateOptions {
                    if_not_exists: false,
                    engine: self.engine.unwrap_or_default(),
                    id: Some(id),
                    field_count: self.format.len() as u32,
                    user: Some(user),
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
            let id_range_min = SPACE_ID_INTERNAL_MAX + 1;
            let id_range_max = SPACE_ID_TEMPORARY_MIN;

            let mut iter = sys_space.select(IteratorType::LT, &[id_range_max])?;
            let tuple = iter.next().expect("there's always at least system spaces");
            let mut max_id: SpaceId = tuple
                .field(0)
                .expect("space metadata should decode fine")
                .expect("space id should always be present");

            let find_next_unused_id = |start: SpaceId| -> Result<SpaceId, Error> {
                let iter = sys_space.select(IteratorType::GE, &[start])?;
                let mut next_id = start;
                for tuple in iter {
                    let id: SpaceId = tuple
                        .field(0)
                        .expect("space metadata should decode fine")
                        .expect("space id should always be present");
                    if id != next_id {
                        // Found a hole in the id range.
                        return Ok(next_id);
                    }
                    next_id += 1;
                }
                Ok(next_id)
            };

            if max_id < id_range_min {
                max_id = id_range_min;
            }

            let mut id = find_next_unused_id(max_id)?;
            if id >= id_range_max {
                id = find_next_unused_id(id_range_min)?;
                if id >= id_range_max {
                    set_error!(TarantoolErrorCode::CreateSpace, "space id limit is reached");
                    return Err(TarantoolError::last().into());
                }
            }
            id
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
        let res = Ddl::CreateTable {
            id,
            name: self.name,
            format,
            primary_key,
            distribution,
            engine: self.engine.unwrap_or_default(),
            owner: self.owner,
        };
        Ok(res)
    }
}

/// Minimum id in the range of ids reserved for temporary spaces.
///
/// Temporary spaces need a special range of ids to avoid conflicts with
/// spaces defined on replicas. This value is defined in tarantool, see
/// <https://git.picodata.io/picodata/tarantool/-/blob/5c3c8ed32c7a9c84a0e86c8453269f0925ce63ed/src/box/schema_def.h#L67>
const SPACE_ID_TEMPORARY_MIN: SpaceId = 1 << 30;

/// Waits for a pending ddl to be either `Committed` or `Aborted` by the governor.
///
/// Returns an index of the corresponding `DdlCommit` entry.
///
/// If `timeout` is reached earlier returns an error.
pub fn wait_for_ddl_commit(
    prepare_commit: RaftIndex,
    timeout: Duration,
) -> traft::Result<RaftIndex> {
    let node = node::global()?;
    let raft_storage = &node.raft_storage;
    let deadline = fiber::clock().saturating_add(timeout);
    let last_seen = prepare_commit;
    loop {
        let cur_applied = node.get_index();
        let new_entries = raft_storage.entries(last_seen + 1, cur_applied + 1, None)?;
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

        node.wait_index(cur_applied + 1, deadline.duration_since(fiber::clock()))?;
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
                cas::Range::new(ClusterwideTable::Property).eq([PropertyName::PendingSchemaChange]),
                cas::Range::new(ClusterwideTable::Property).eq([PropertyName::GlobalSchemaVersion]),
                cas::Range::new(ClusterwideTable::Property).eq([PropertyName::NextSchemaVersion]),
            ],
        };

        let (index, term) =
            compare_and_swap(Op::DdlAbort, predicate, effective_user_id(), timeout)?;
        node.wait_index(index, timeout)?;
        if raft::Storage::term(&node.raft_storage, index)? != term {
            // leader switched - retry
            continue;
        }
        return Ok(index);
    }
}

mod tests {
    use tarantool::{auth::AuthMethod, space::FieldType};

    use super::*;

    fn storage() -> Clusterwide {
        let storage = Clusterwide::for_tests();
        storage
            .users
            .insert(&UserDef {
                id: ADMIN_ID,
                name: String::from("admin"),
                schema_version: 0,
                auth: AuthDef::new(AuthMethod::ChapSha1, String::from("")),
                owner: ADMIN_ID,
            })
            .unwrap();
        storage
    }

    #[::tarantool::test]
    fn test_create_space() {
        let storage = storage();
        CreateTableParams {
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
            owner: ADMIN_ID,
        }
        .test_create_space(&storage)
        .unwrap();
        assert!(tarantool::space::Space::find("friends_of_peppa").is_none());

        CreateTableParams {
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
            owner: ADMIN_ID,
        }
        .test_create_space(&storage)
        .unwrap();
        assert!(tarantool::space::Space::find("friends_of_peppa").is_none());

        let err = CreateTableParams {
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
            owner: ADMIN_ID,
        }
        .test_create_space(&storage)
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

        let err = CreateTableParams {
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
            owner: ADMIN_ID,
        }
        .validate()
        .unwrap_err();
        assert_eq!(err.to_string(), "several fields have the same name: field1");

        let err = CreateTableParams {
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
            owner: ADMIN_ID,
        }
        .validate()
        .unwrap_err();
        assert_eq!(err.to_string(), "no field with name: field2");

        let err = CreateTableParams {
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
            owner: ADMIN_ID,
        }
        .validate()
        .unwrap_err();
        assert_eq!(err.to_string(), "no field with name: field2");

        let err = CreateTableParams {
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
            owner: ADMIN_ID,
        }
        .validate()
        .unwrap_err();
        assert_eq!(err.to_string(), "several fields have the same name: field1");

        let err = CreateTableParams {
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
            owner: ADMIN_ID,
        }
        .validate()
        .unwrap_err();
        assert_eq!(err.to_string(), "no field with name: field2");

        let err = CreateTableParams {
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
            owner: ADMIN_ID,
        }
        .validate()
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "distribution is `sharded`, but neither `by_field` nor `sharding_key` is set"
        );

        let err = CreateTableParams {
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
            owner: ADMIN_ID,
        }
        .validate()
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "only one of sharding policy fields (`by_field`, `sharding_key`) should be set"
        );

        CreateTableParams {
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
            owner: ADMIN_ID,
        }
        .validate()
        .unwrap();

        let err = CreateTableParams {
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
            owner: ADMIN_ID,
        }
        .validate()
        .unwrap_err();
        assert_eq!(err.to_string(), "global spaces only support memtx engine");
    }

    #[::tarantool::test]
    fn test_space_id_temporary_min() {
        let lua = tarantool::lua_state();
        let id: SpaceId = lua
            .eval("return box.schema.SPACE_ID_TEMPORARY_MIN")
            .unwrap();
        assert_eq!(id, SPACE_ID_TEMPORARY_MIN);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tarantool::tuple::ToTupleBuffer;

    #[test]
    #[rustfmt::skip]
    fn space_def_matches_format() {
        let i = TableDef::for_tests();
        let tuple_data = i.to_tuple_buffer().unwrap();
        let format = TableDef::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "TableDef::format");

        assert_eq!(format[TableDef::FIELD_OPERABLE].name, "operable");
    }

    #[test]
    #[rustfmt::skip]
    fn index_def_matches_format() {
        let i = IndexDef::for_tests();
        let tuple_data = i.to_tuple_buffer().unwrap();
        let format = IndexDef::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "IndexDef::format");

        assert_eq!(format[IndexDef::FIELD_OPERABLE].name, "operable");
    }

    #[test]
    #[rustfmt::skip]
    fn user_def_matches_format() {
        let i = UserDef::for_tests();
        let tuple_data = i.to_tuple_buffer().unwrap();
        let format = UserDef::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "UserDef::format");

        assert_eq!(format[UserDef::FIELD_AUTH].name, "auth");
    }

    #[test]
    #[rustfmt::skip]
    fn role_def_matches_format() {
        let i = RoleDef::for_tests();
        let tuple_data = i.to_tuple_buffer().unwrap();
        let format = RoleDef::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "RoleDef::format");
    }

    #[test]
    #[rustfmt::skip]
    fn privilege_def_matches_format() {
        let i = PrivilegeDef::for_tests();
        let tuple_data = i.to_tuple_buffer().unwrap();
        let format = PrivilegeDef::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "PrivilegeDef::format");
    }

    #[track_caller]
    fn check_object_privilege(
        object_type: SchemaObjectType,
        valid_privileges: &'static [PrivilegeType],
        object_id: i64,
    ) {
        for privilege in PrivilegeType::VARIANTS {
            let res = PrivilegeDef::new(*privilege, object_type, object_id, 1, 1, 1);

            if valid_privileges.contains(privilege) {
                assert!(res.is_ok())
            } else {
                assert_eq!(
                    res.unwrap_err(),
                    InvalidPrivilegeError {
                        object_type,
                        unsupported: *privilege,
                        expected_one_of: valid_privileges,
                    }
                )
            }
        }
    }

    #[test]
    fn privilege_def_validation() {
        use PrivilegeType::*;

        // table
        let valid = &[Read, Write, Create, Alter, Drop];
        check_object_privilege(SchemaObjectType::Table, valid, -1);

        // particular table
        let valid = &[Read, Write, Alter, Drop];
        check_object_privilege(SchemaObjectType::Table, valid, 42);

        // user
        let valid = &[Create, Alter, Drop];
        check_object_privilege(SchemaObjectType::User, valid, -1);

        // particular user
        let valid = &[Alter, Drop];
        check_object_privilege(SchemaObjectType::User, valid, 42);

        // role
        let valid = &[Create, Drop];
        check_object_privilege(SchemaObjectType::Role, valid, -1);

        // particular role
        let valid = &[Execute, Drop];
        check_object_privilege(SchemaObjectType::Role, valid, 42);

        // universe
        let valid = &[Login];
        check_object_privilege(SchemaObjectType::Universe, valid, 0);
    }

    #[test]
    fn routine_def_matches_format() {
        let i = RoutineDef::for_tests();
        let tuple_data = i.to_tuple_buffer().unwrap();
        let format = RoutineDef::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "RoutineDef::format");
    }
}

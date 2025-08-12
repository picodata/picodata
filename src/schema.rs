use crate::access_control::UserMetadataKind;
use crate::cas;
use crate::config::DEFAULT_USERNAME;
use crate::instance::InstanceName;
use crate::pico_service::pico_service_password;
use crate::plugin::PluginIdentifier;
use crate::plugin::ServiceId;
use crate::storage::*;
use crate::tier::DEFAULT_TIER;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::error::ErrorInfo;
use crate::traft::op::{Ddl, Op};
use crate::traft::{self, node, RaftIndex};
use crate::util::effective_user_id;
use ahash::AHashSet;
use picodata_plugin::error_code::ErrorCode;
use sbroad::ir::ddl::{Language, ParamDef};
use sbroad::ir::value::Value as IrValue;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use tarantool::auth::AuthData;
use tarantool::auth::AuthDef;
use tarantool::auth::AuthMethod;
use tarantool::decimal::Decimal;
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;
use tarantool::fiber;
use tarantool::index::IndexId;
use tarantool::index::IteratorType;
use tarantool::index::Metadata as IndexMetadata;
use tarantool::index::{FieldType as IndexFieldType, IndexType, Part, RtreeIndexDistanceType};
use tarantool::msgpack;
use tarantool::session::{with_su, UserId};
use tarantool::space::SpaceId;
use tarantool::space::{FieldType, SpaceCreateOptions, SpaceEngineType};
use tarantool::space::{Metadata as SpaceMetadata, Space, SpaceType, SystemSpace};
use tarantool::time::Instant;
use tarantool::tlua;
use tarantool::tlua::LuaRead;
use tarantool::transaction::{transaction, TransactionError};
use tarantool::tuple::Encode;
use tarantool::util::Value;

/// The initial local schema version. Immediately after the cluster is booted
/// it has this schema version.
///
/// If a schema definition is marked with this version, it means the schema
/// definition is builtin and is applied by default on all instances.
pub const INITIAL_SCHEMA_VERSION: u64 = 0;

////////////////////////////////////////////////////////////////////////////////
// TableDef
////////////////////////////////////////////////////////////////////////////////

/// Database table definition.
///
/// Describes a user-defined table.
#[derive(Clone, Debug, msgpack::Encode, msgpack::Decode, PartialEq, Eq)]
pub struct TableDef {
    pub id: SpaceId,
    pub name: String,
    pub distribution: Distribution,
    pub format: Vec<tarantool::space::Field>,
    pub schema_version: u64,
    pub operable: bool,
    pub engine: SpaceEngineType,
    pub owner: UserId,
    pub description: String,
}

impl TableDef {
    /// Index (0-based) of field "operable" in the _pico_table table format.
    pub const FIELD_OPERABLE: usize = 5;

    /// Format of the _pico_table global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("id", FieldType::Unsigned)).is_nullable(false),
            Field::from(("name", FieldType::String)).is_nullable(false),
            Field::from(("distribution", FieldType::Map)).is_nullable(false),
            Field::from(("format", FieldType::Array)).is_nullable(false),
            Field::from(("schema_version", FieldType::Unsigned)).is_nullable(false),
            Field::from(("operable", FieldType::Boolean)).is_nullable(false),
            Field::from(("engine", FieldType::String)).is_nullable(false),
            Field::from(("owner", FieldType::Unsigned)).is_nullable(false),
            Field::from(("description", FieldType::String)).is_nullable(false),
        ]
    }

    /// A dummy instance of the type for use in tests.
    #[inline(always)]
    pub fn for_tests() -> Self {
        Self {
            id: 10569,
            name: "stuff".into(),
            distribution: Distribution::Global,
            format: vec![tarantool::space::Field {
                name: "field_1".into(),
                field_type: tarantool::space::FieldType::Unsigned,
                is_nullable: false,
            }],
            schema_version: 420,
            operable: true,
            engine: SpaceEngineType::Blackhole,
            owner: 42,
            description: "A table for tests".into(),
        }
    }

    pub fn to_space_metadata(&self) -> traft::Result<SpaceMetadata<'_>> {
        let format = fields_to_format(&self.format);

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
}

/// Definitions of builtin tables & their respective indexes.
/// These should be inserted into "_pico_table" & "_pico_index" at cluster bootstrap.
pub fn system_table_definitions() -> Vec<(TableDef, Vec<IndexDef>)> {
    let mut result = Vec::new();

    macro_rules! push_definitions {
        ($result:ident, $( $table:ident ),*) => {
            $(
                let table_def = TableDef {
                    id: $table::TABLE_ID,
                    name: $table::TABLE_NAME.into(),
                    distribution: Distribution::Global,
                    format: $table::format(),
                    // This means the local schema is already up to date and main loop doesn't need to do anything
                    schema_version: INITIAL_SCHEMA_VERSION,
                    operable: true,
                    engine: SpaceEngineType::Memtx,
                    owner: ADMIN_ID,
                    description: $table::DESCRIPTION.into(),
                };
                let index_defs = $table::index_definitions();
                $result.push((table_def, index_defs));
            )*
        };
    }

    use crate::catalog::governor_queue::GovernorQueue;
    use crate::storage::*;
    push_definitions!(
        result,
        Tables,
        Indexes,
        PeerAddresses,
        Instances,
        Properties,
        Replicasets,
        Users,
        Privileges,
        Tiers,
        Routines,
        Plugins,
        Services,
        ServiceRouteTable,
        PluginMigrations,
        PluginConfig,
        DbConfig,
        GovernorQueue
    );

    // TODO: there's also "_raft_log" & "_raft_state" spaces, but we don't treat
    // them the same as others for some reason?

    result
}

// FIXME: move this to tarantool-module
pub fn fields_to_format(
    fields: &[tarantool::space::Field],
) -> Vec<BTreeMap<Cow<'static, str>, Value<'_>>> {
    let mut result = Vec::with_capacity(fields.len());
    for field in fields {
        let mut field_map = BTreeMap::new();
        field_map.insert("name".into(), Value::Str(field.name.as_str().into()));
        field_map.insert("type".into(), Value::Str(field.field_type.as_str().into()));
        field_map.insert("is_nullable".into(), Value::Bool(field.is_nullable));
        result.push(field_map);
    }
    result
}

////////////////////////////////////////////////////////////////////////////////
// Distribution
////////////////////////////////////////////////////////////////////////////////

/// Defines how to distribute tuples in a table across replicasets.
#[derive(
    Clone, Debug, Serialize, Deserialize, PartialEq, Eq, LuaRead, msgpack::Encode, msgpack::Decode,
)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "kind")]
pub enum Distribution {
    /// Tuples will be replicated to each instance.
    Global,
    /// Tuples will be implicitly sharded. E.g. sent to the corresponding bucket
    /// which will be determined by a hash of the provided `sharding_key`.
    ShardedImplicitly {
        sharding_key: Vec<String>,
        #[serde(default)]
        sharding_fn: ShardingFn,
        tier: String,
    },
    /// Tuples will be explicitly sharded. E.g. sent to the bucket
    /// which id is provided by field that is specified here.
    ///
    /// Default field name: "bucket_id"
    ShardedByField {
        #[serde(default = "default_bucket_id_field")]
        field: String,
        tier: String,
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

/// Picodata index options.
///
/// While tarantool module already includes an `IndexOptions` structure
/// for local indexes, the current enum is designed for picodata ones.
/// Currently, these options coincide, but we intend to introduce a
/// REDISTRIBUTION option for secondary indexes. It will be implemented
/// using materialized views instead of tarantool indexes. Therefore,
/// it's important to maintain a distinction between these features,
/// emphasizing that it is specific to picodata.
#[derive(Clone, Debug, Deserialize, Serialize, Hash, PartialEq, Eq)]
pub enum IndexOption {
    /// Vinyl only. The false positive rate for the bloom filter.
    #[serde(rename = "bloom_fpr")]
    BloomFalsePositiveRate(Decimal),
    /// The RTREE index dimension.
    #[serde(rename = "dimension")]
    Dimension(u32),
    /// The RTREE index distance type.
    #[serde(rename = "distance")]
    Distance(RtreeIndexDistanceType),
    /// Specify whether hint optimization is enabled for the TREE index.
    /// If true, the index works faster, if false, the index size is reduced by half.
    #[serde(rename = "hint")]
    // FIXME: this option is disabled in the current version of the module.
    Hint(bool),
    /// Vinyl only. The page size in bytes used for read and write disk operations.
    #[serde(rename = "page_size")]
    PageSize(u32),
    /// Vinyl only. The range size in bytes used for vinyl index.
    #[serde(rename = "range_size")]
    RangeSize(u32),
    /// Vinyl only. The number of runs per level in the LSM tree.
    #[serde(rename = "run_count_per_level")]
    RunCountPerLevel(u32),
    /// Vinyl only. The ratio between the size of different levels in the LSM tree.
    #[serde(rename = "run_size_ratio")]
    RunSizeRatio(Decimal),
    /// Specify whether the index is unique. When true, the index cannot contain duplicate values.
    #[serde(rename = "unique")]
    Unique(bool),
}

impl IndexOption {
    pub fn as_kv(&self) -> (Cow<'static, str>, Value<'_>) {
        let dec_to_f64 = |d: Decimal| f64::from_str(&d.to_string()).expect("decimal to f64");
        match self {
            IndexOption::BloomFalsePositiveRate(rate) => {
                ("bloom_fpr".into(), Value::Double(dec_to_f64(*rate)))
            }
            IndexOption::Dimension(dim) => ("dimension".into(), Value::Num(*dim)),
            IndexOption::Distance(dist) => ("distance".into(), Value::Str(dist.as_str().into())),
            IndexOption::Hint(hint) => ("hint".into(), Value::Bool(*hint)),
            IndexOption::PageSize(size) => ("page_size".into(), Value::Num(*size)),
            IndexOption::RangeSize(size) => ("range_size".into(), Value::Num(*size)),
            IndexOption::RunCountPerLevel(count) => {
                ("run_count_per_level".into(), Value::Num(*count))
            }
            IndexOption::RunSizeRatio(ratio) => {
                ("run_size_ratio".into(), Value::Double(dec_to_f64(*ratio)))
            }
            IndexOption::Unique(unique) => ("unique".into(), Value::Bool(*unique)),
        }
    }

    pub fn is_vinyl(&self) -> bool {
        matches!(
            self,
            IndexOption::BloomFalsePositiveRate(_)
                | IndexOption::PageSize(_)
                | IndexOption::RangeSize(_)
                | IndexOption::RunCountPerLevel(_)
                | IndexOption::RunSizeRatio(_)
        )
    }

    pub fn is_rtree(&self) -> bool {
        matches!(self, IndexOption::Dimension(_) | IndexOption::Distance(_))
    }

    pub fn is_tree(&self) -> bool {
        matches!(self, IndexOption::Hint(_))
    }

    pub fn type_name(&self) -> &str {
        match self {
            IndexOption::BloomFalsePositiveRate(_) => "bloom_fpr",
            IndexOption::Dimension(_) => "dimension",
            IndexOption::Distance(_) => "distance",
            IndexOption::Hint(_) => "hint",
            IndexOption::PageSize(_) => "page_size",
            IndexOption::RangeSize(_) => "range_size",
            IndexOption::RunCountPerLevel(_) => "run_count_per_level",
            IndexOption::RunSizeRatio(_) => "run_size_ratio",
            IndexOption::Unique(_) => "unique",
        }
    }
}

/// Database index definition.
///
/// Describes a user-defined index.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct IndexDef {
    pub table_id: SpaceId,
    pub id: IndexId,
    pub name: String,
    #[serde(rename = "type")]
    pub ty: IndexType,
    pub opts: Vec<IndexOption>,
    pub parts: Vec<Part<String>>,
    pub operable: bool,
    pub schema_version: u64,
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
            Field::from(("table_id", FieldType::Unsigned)).is_nullable(false),
            Field::from(("id", FieldType::Unsigned)).is_nullable(false),
            Field::from(("name", FieldType::String)).is_nullable(false),
            Field::from(("type", FieldType::String)).is_nullable(false),
            Field::from(("opts", FieldType::Array)).is_nullable(false),
            Field::from(("parts", FieldType::Array)).is_nullable(false),
            Field::from(("operable", FieldType::Boolean)).is_nullable(false),
            Field::from(("schema_version", FieldType::Unsigned)).is_nullable(false),
        ]
    }

    /// A dummy instance of the type for use in tests.
    #[inline(always)]
    pub fn for_tests() -> Self {
        Self {
            table_id: 10569,
            id: 1,
            name: "secondary".into(),
            ty: IndexType::Tree,
            opts: vec![],
            parts: vec![],
            operable: true,
            schema_version: 420,
        }
    }

    pub fn to_index_metadata(&self, table_def: &TableDef) -> IndexMetadata<'_> {
        let mut opts = BTreeMap::new();
        for opt in &self.opts {
            let (key, value) = opt.as_kv();
            opts.insert(key, value);
        }

        // We must convert any field names to field indexes in the index parts,
        // because it is very important for tarantool, and we are very
        // understanding and supportive of it.
        let mut parts = Vec::with_capacity(self.parts.len());
        for part in &self.parts {
            let field_name = &part.field;

            let mut index = 0;
            for field in &table_def.format {
                if &field.name == field_name {
                    break;
                }
                index += 1;
            }
            // No need to check the field was found,
            // tarantool will tell us if the field index is out of range

            parts.push(Part {
                field: index,
                r#type: part.r#type,
                collation: part.collation.clone(),
                is_nullable: part.is_nullable,
                path: part.path.clone(),
            });
        }

        let index_meta = IndexMetadata {
            space_id: self.table_id,
            index_id: self.id,
            name: self.name.as_str().into(),
            r#type: self.ty,
            opts,
            parts,
        };

        index_meta
    }

    pub fn opts_equal(&self, opts: &[IndexOption]) -> bool {
        let mut set: AHashSet<&IndexOption> = AHashSet::with_capacity(opts.len());
        for opt in &self.opts {
            set.insert(opt);
        }
        for opt in opts {
            if !set.contains(opt) {
                return false;
            }
        }
        true
    }
}

////////////////////////////////////////////////////////////////////////////////
// PluginDef
////////////////////////////////////////////////////////////////////////////////

/// Plugin defenition in _pico_plugin system table.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PluginDef {
    /// Plugin name.
    pub name: String,
    /// Indicates plugin enabled or not.
    pub enabled: bool,
    /// List of plugin services.
    pub services: Vec<String>,
    /// Plugin version.
    // TODO currently this version is unused,
    // expect service loading algorithm (when version from .so file
    // should be equal with plugin version from manifest).
    // This would be change in future, with API breaking changes and plugin rolling update feature.
    pub version: String,
    /// Plugin description
    pub description: String,
    /// List of migration files.
    pub migration_list: Vec<String>,
}

impl Encode for PluginDef {}

impl PluginDef {
    /// Index (0-based) of field "enable" in the _pico_plugin table format.
    pub const FIELD_ENABLE: usize = 1;

    /// Format of the _pico_plugin global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("name", FieldType::String)).is_nullable(false),
            Field::from(("enabled", FieldType::Boolean)).is_nullable(false),
            Field::from(("services", FieldType::Array)).is_nullable(false),
            Field::from(("version", FieldType::String)).is_nullable(false),
            Field::from(("description", FieldType::String)).is_nullable(false),
            Field::from(("migration_list", FieldType::Array)).is_nullable(false),
        ]
    }

    #[cfg(test)]
    pub fn for_tests() -> Self {
        Self {
            name: "plugin".into(),
            enabled: false,
            services: vec!["service_1".to_string(), "service_2".to_string()],
            version: "0.0.1".into(),
            description: "description".to_string(),
            migration_list: vec![],
        }
    }

    #[inline]
    pub fn identifier(&self) -> PluginIdentifier {
        PluginIdentifier {
            name: self.name.clone(),
            version: self.version.clone(),
        }
    }

    #[inline]
    pub fn into_identifier(self) -> PluginIdentifier {
        PluginIdentifier {
            name: self.name,
            version: self.version,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ServiceDef
////////////////////////////////////////////////////////////////////////////////

/// Plugin service definition in _pico_service system table.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ServiceDef {
    /// Plugin name.
    pub plugin_name: String,
    /// Service name.
    pub name: String,
    /// Service version must be the same as a plugin version.
    pub version: String,
    /// List of tiers where service must be running.
    pub tiers: Vec<String>,
    /// Plugin description
    pub description: String,
}

impl Encode for ServiceDef {}

impl ServiceDef {
    /// Format of the _pico_service global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("plugin_name", FieldType::String)).is_nullable(false),
            Field::from(("name", FieldType::String)).is_nullable(false),
            Field::from(("version", FieldType::String)).is_nullable(false),
            Field::from(("tiers", FieldType::Array)).is_nullable(false),
            Field::from(("description", FieldType::String)).is_nullable(false),
        ]
    }

    #[inline]
    pub fn plugin(&self) -> PluginIdentifier {
        PluginIdentifier {
            name: self.plugin_name.clone(),
            version: self.version.clone(),
        }
    }

    #[cfg(test)]
    pub fn for_tests() -> Self {
        Self {
            plugin_name: "plugin".to_string(),
            name: "service".into(),
            version: "0.0.1".into(),
            tiers: vec!["t1".to_string(), "t2".to_string()],
            description: "description".to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ServiceRouteItem
////////////////////////////////////////////////////////////////////////////////

/// Single route definition in _pico_service_route system table.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ServiceRouteItem {
    pub plugin_name: String,
    pub plugin_version: String,
    pub service_name: String,
    pub instance_name: InstanceName,
    /// `true` if route is poisoned, `false` otherwise.
    pub poison: bool,
}

impl Encode for ServiceRouteItem {}

impl ServiceRouteItem {
    /// Index of field "poison" in the table _pico_service_route format.
    ///
    /// Index of first field is 0.
    pub const FIELD_POISON: u32 = 4;

    pub fn new_healthy(
        instance_name: InstanceName,
        plugin_ident: &PluginIdentifier,
        service_name: impl ToString,
    ) -> Self {
        Self {
            plugin_name: plugin_ident.name.clone(),
            plugin_version: plugin_ident.version.to_string(),
            service_name: service_name.to_string(),
            instance_name,
            poison: false,
        }
    }

    pub fn new_poison(
        instance_name: InstanceName,
        plugin_ident: &PluginIdentifier,
        service_name: impl ToString,
    ) -> Self {
        Self {
            plugin_name: plugin_ident.name.clone(),
            plugin_version: plugin_ident.version.to_string(),
            service_name: service_name.to_string(),
            instance_name,
            poison: true,
        }
    }

    /// Format of the _pico_service_route global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("plugin_name", FieldType::String)).is_nullable(false),
            Field::from(("plugin_version", FieldType::String)).is_nullable(false),
            Field::from(("service_name", FieldType::String)).is_nullable(false),
            Field::from(("instance_name", FieldType::String)).is_nullable(false),
            Field::from(("poison", FieldType::Boolean)).is_nullable(false),
        ]
    }

    #[cfg(test)]
    pub fn for_tests() -> Self {
        Self {
            plugin_name: "plugin".to_string(),
            plugin_version: "version".to_string(),
            service_name: "service".to_string(),
            instance_name: InstanceName("i1".to_string()),
            poison: false,
        }
    }

    pub fn key(&self) -> ServiceRouteKey<'_> {
        ServiceRouteKey {
            plugin_name: &self.plugin_name,
            plugin_version: &self.plugin_version,
            service_name: &self.service_name,
            instance_name: &self.instance_name,
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ServiceRouteKey<'a> {
    pub plugin_name: &'a str,
    pub plugin_version: &'a str,
    pub service_name: &'a str,
    pub instance_name: &'a str,
}
impl Encode for ServiceRouteKey<'_> {}

impl<'a> ServiceRouteKey<'a> {
    #[inline(always)]
    pub fn new(instance_name: &'a str, service: &'a ServiceId) -> Self {
        Self {
            plugin_name: &service.plugin,
            service_name: &service.service,
            plugin_version: &service.version,
            instance_name,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// PluginMigrationRecord
////////////////////////////////////////////////////////////////////////////////

/// Single record in _pico_plugin_migration system table.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PluginMigrationRecord {
    pub plugin_name: String,
    /// Migration file path.
    pub migration_file: String,
    /// MD5 of a migration file content, represented by a hex string.
    hash: String,
}

impl Encode for PluginMigrationRecord {}

impl PluginMigrationRecord {
    /// Format of the _pico_plugin_migration global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("plugin_name", FieldType::String)).is_nullable(false),
            Field::from(("migration_file", FieldType::String)).is_nullable(false),
            Field::from(("hash", FieldType::String)).is_nullable(false),
        ]
    }

    #[inline(always)]
    pub fn hash(&self) -> &str {
        &self.hash
    }

    #[cfg(test)]
    pub fn for_tests() -> Self {
        Self {
            plugin_name: "plugin".to_string(),
            migration_file: "migration_1.db".to_string(),
            hash: format!("{:x}", md5::compute("test")),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// PluginConfigRecord
////////////////////////////////////////////////////////////////////////////////

/// Single record in _pico_plugin_config system table.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PluginConfigRecord {
    /// Plugin name.
    pub plugin: String,
    /// Plugin version.
    pub version: String,
    /// Plugin service or extension name.
    pub entity: String,
    /// Configuration key.
    pub key: String,
    /// Configration value.
    pub value: rmpv::Value,
}

impl Encode for PluginConfigRecord {}

impl PluginConfigRecord {
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("plugin", FieldType::String)).is_nullable(false),
            Field::from(("version", FieldType::String)).is_nullable(false),
            Field::from(("entity", FieldType::String)).is_nullable(false),
            Field::from(("key", FieldType::String)).is_nullable(false),
            Field::from(("value", FieldType::Any)).is_nullable(true),
        ]
    }

    /// Create records from the whole configuration.
    pub fn from_config(
        ident: &PluginIdentifier,
        entity: &str,
        config: rmpv::Value,
    ) -> tarantool::Result<Vec<Self>> {
        let mut result = vec![];

        let map = match config {
            rmpv::Value::Nil => return Ok(result),
            rmpv::Value::Map(map) => map,
            _ => {
                let err = format!(
                    "Plugin configuration should be represented as messagepack map. Got: {config:?}"
                );
                return Err(tarantool::error::Error::DecodeRmpValue(
                    serde::de::Error::custom(err),
                ));
            }
        };

        for (k, v) in map {
            let key = k.as_str().ok_or_else(|| {
                <rmp_serde::decode::Error as serde::de::Error>::custom(
                    "Only string keys allowed in plugin configuration",
                )
            })?;
            result.push(Self {
                plugin: ident.name.to_string(),
                version: ident.version.to_string(),
                entity: entity.to_string(),
                key: key.to_string(),
                value: v,
            });
        }

        Ok(result)
    }

    #[inline]
    pub fn pk(&self) -> [&str; 4] {
        [&self.plugin, &self.version, &self.entity, &self.key]
    }

    #[cfg(test)]
    pub fn for_tests() -> Self {
        Self {
            plugin: "plugin".to_string(),
            version: "0.1.0".to_string(),
            entity: "service_1".to_string(),
            key: "key_1".to_string(),
            value: rmpv::Value::Boolean(true),
        }
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
    pub auth: Option<AuthDef>,
    pub owner: UserId,
    #[serde(rename = "type")]
    pub ty: UserMetadataKind,
}

impl Encode for UserDef {}

impl UserDef {
    /// Index of field "auth" in the space _pico_user format.
    ///
    /// Index of first field is 0.
    pub const FIELD_AUTH: usize = 3;

    /// Index of field "name" in the space _pico_user format.
    pub const FIELD_NAME: usize = 1;

    #[inline(always)]
    pub fn is_role(&self) -> bool {
        matches!(self.ty, UserMetadataKind::Role)
    }

    /// Format of the _pico_user global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("id", FieldType::Unsigned)).is_nullable(false),
            Field::from(("name", FieldType::String)).is_nullable(false),
            Field::from(("schema_version", FieldType::Unsigned)).is_nullable(false),
            Field::from(("auth", FieldType::Array)).is_nullable(true),
            Field::from(("owner", FieldType::Unsigned)).is_nullable(false),
            Field::from(("type", FieldType::String)).is_nullable(false),
        ]
    }

    /// A dummy instance of the type for use in tests.
    #[inline(always)]
    pub fn for_tests() -> Self {
        Self {
            id: 69,
            name: "david".into(),
            schema_version: 421,
            auth: Some(AuthDef::new(AuthMethod::Md5, "".into())),
            owner: 42,
            ty: UserMetadataKind::User,
        }
    }

    pub fn ensure_no_dependent_objects(&self, storage: &Catalog) -> traft::Result<()> {
        let tables: Vec<_> = storage
            .tables
            .by_owner_id(self.id)?
            .map(|def| def.name)
            .collect();

        let routines: Vec<_> = storage
            .routines
            .by_owner_id(self.id)?
            .map(|def| def.name)
            .collect();

        let users: Vec<_> = storage
            .users
            .by_owner_id(self.id)?
            .filter(|def| def.ty == UserMetadataKind::User)
            .map(|def| def.name)
            .collect();

        let roles: Vec<_> = storage
            .users
            .by_owner_id(self.id)?
            .filter(|def| def.ty == UserMetadataKind::Role)
            .map(|def| def.name)
            .collect();

        if tables.is_empty() && routines.is_empty() && roles.is_empty() && users.is_empty() {
            return Ok(());
        }

        let mut err = "user cannot be dropped because some objects depend on it\n".to_string();
        if !tables.is_empty() {
            err.push_str(&format!("owner of tables {}\n", tables.join(", ")));
        }
        if !routines.is_empty() {
            err.push_str(&format!("owner of procedures {}\n", routines.join(", ")));
        }
        if !users.is_empty() {
            err.push_str(&format!("owner of users {}\n", users.join(", ")));
        }
        if !roles.is_empty() {
            err.push_str(&format!("owner of roles {}\n", roles.join(", ")));
        }
        Err(traft::error::Error::Other(err.into()))
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
/// because current user doesn't have access to system spaces
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
pub const ROLE_REPLICATION_ID: UserId = 3;

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
            SchemaObjectType::Routine => "function",
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
/// 2) Only Login can be granted on Universe in picodata.
///    Note that validation is not performed in Deserialize. We assume that for untrusted data
///    the object is created via constructor. In other cases validation was already performed
///    prior to serialization thus deserialization always creates a valid object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PrivilegeDef {
    grantor_id: UserId,
    grantee_id: UserId,
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
        grantor_id: UserId,
        grantee_id: UserId,
        privilege: PrivilegeType,
        object_type: SchemaObjectType,
        object_id: i64,
        schema_version: u64,
    ) -> Result<PrivilegeDef, InvalidPrivilegeError> {
        let privilege_def = PrivilegeDef {
            grantor_id,
            grantee_id,
            privilege,
            object_type,
            object_id,
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
                None => &[Create, Drop, Execute],
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

    pub fn login(grantee_id: UserId, grantor_id: UserId, schema_version: u64) -> Self {
        Self::new(
            grantor_id,
            grantee_id,
            PrivilegeType::Login,
            SchemaObjectType::Universe,
            0,
            schema_version,
        )
        .expect("cant fail, valid login privilege")
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
            Field::from(("grantor_id", FieldType::Unsigned)).is_nullable(false),
            Field::from(("grantee_id", FieldType::Unsigned)).is_nullable(false),
            Field::from(("privilege", FieldType::String)).is_nullable(false),
            Field::from(("object_type", FieldType::String)).is_nullable(false),
            Field::from(("object_id", FieldType::Integer)).is_nullable(false),
            Field::from(("schema_version", FieldType::Unsigned)).is_nullable(false),
        ]
    }

    /// A dummy instance of the type for use in tests.
    #[inline(always)]
    pub fn for_tests() -> Self {
        Self {
            grantor_id: 13,
            grantee_id: 37,
            privilege: PrivilegeType::Create,
            object_type: SchemaObjectType::User,
            object_id: -1,
            schema_version: 337,
        }
    }

    /// Retrieves object_name from system spaces based on `object_id` and `object_type`.
    /// Returns `Ok(None)` in the case when the privilege has no target object (e.g. `object_id == -1`)
    /// or when target object is universe.
    /// Returns `Err` in case when target object was not found in system tables.
    ///
    /// # Panics
    /// 1. On storage failure
    pub fn resolve_object_name(&self, storage: &Catalog) -> Result<Option<String>, Error> {
        let Some(id) = self.object_id() else {
            return Ok(None);
        };
        let name = match self.object_type {
            SchemaObjectType::Table => storage.tables.get(id).map(|t| t.map(|t| t.name)),
            SchemaObjectType::Role | SchemaObjectType::User => {
                storage.users.by_id(id).map(|t| t.map(|t| t.name))
            }
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
// system_user_definitions
////////////////////////////////////////////////////////////////////////////////

/// Definitions of builtin users & their respective privileges.
/// These should be inserted into "_pico_user" & "_pico_privilege" at cluster bootstrap.
pub fn system_user_definitions() -> Vec<(UserDef, Vec<PrivilegeDef>)> {
    let mut result = vec![];

    let initiator = ADMIN_ID;

    //
    // User "guest"
    //
    // equivalent SQL expression: CREATE USER 'guest' WITH PASSWORD '' USING md5
    {
        let user_def = UserDef {
            id: GUEST_ID,
            name: DEFAULT_USERNAME.into(),
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            auth: Some(AuthDef::new(
                AuthMethod::Md5,
                AuthData::new(&AuthMethod::Md5, DEFAULT_USERNAME, "").into_string(),
            )),
            owner: initiator,
            ty: UserMetadataKind::User,
        };
        let priv_defs = vec![
            PrivilegeDef {
                grantor_id: initiator,
                grantee_id: user_def.id,
                privilege: PrivilegeType::Login,
                object_type: SchemaObjectType::Universe,
                object_id: UNIVERSE_ID,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            PrivilegeDef {
                grantor_id: initiator,
                grantee_id: user_def.id,
                privilege: PrivilegeType::Execute,
                object_type: SchemaObjectType::Role,
                object_id: PUBLIC_ID as _,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
        ];
        result.push((user_def, priv_defs));
    }

    //
    // User "admin"
    //
    {
        let user_def = UserDef {
            id: ADMIN_ID,
            name: "admin".into(),
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            // This place slightly differs from the tarantool
            // implementation. In vanilla tarantool the auth_def is an empty
            // MP_MAP. Here for simplicity given available module api we
            // use ChapSha with invalid password (its impossible to get
            // empty string as output of sha1)
            auth: Some(AuthDef::new(AuthMethod::Md5, "".into())),
            owner: initiator,
            ty: UserMetadataKind::User,
        };
        let mut priv_defs = Vec::with_capacity(PrivilegeType::VARIANTS.len());
        // Grant all privileges on "universe" to "admin".
        for &privilege in PrivilegeType::VARIANTS {
            priv_defs.push(PrivilegeDef {
                grantor_id: initiator,
                grantee_id: user_def.id,
                privilege,
                object_type: SchemaObjectType::Universe,
                object_id: UNIVERSE_ID,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            });
        }
        result.push((user_def, priv_defs));
    }

    //
    // User "pico_service"
    //
    {
        let user_def = UserDef {
            id: PICO_SERVICE_ID,
            name: PICO_SERVICE_USER_NAME.into(),
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            auth: Some(AuthDef::new(
                AuthMethod::ChapSha1,
                AuthData::new(
                    &AuthMethod::ChapSha1,
                    PICO_SERVICE_USER_NAME,
                    pico_service_password(),
                )
                .into_string(),
            )),
            owner: initiator,
            ty: UserMetadataKind::User,
        };
        let mut priv_defs = Vec::with_capacity(PrivilegeType::VARIANTS.len() + 1);
        // Grant all privileges on "universe" to "pico_service".
        for &privilege in PrivilegeType::VARIANTS {
            priv_defs.push(PrivilegeDef {
                grantor_id: initiator,
                grantee_id: user_def.id,
                privilege,
                object_type: SchemaObjectType::Universe,
                object_id: UNIVERSE_ID,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            });
        }
        // Role "replication" is needed explicitly
        priv_defs.push(PrivilegeDef {
            grantor_id: initiator,
            grantee_id: user_def.id,
            privilege: PrivilegeType::Execute,
            object_type: SchemaObjectType::Role,
            object_id: ROLE_REPLICATION_ID as i64,
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
        });
        result.push((user_def, priv_defs));
    }

    result
}

/// Definitions of builtin roles & their respective privileges.
/// These should be inserted into "_pico_role" & "_pico_privilege" at cluster bootstrap.
// TODO: maybe this "_pico_role" should be merged with "_pico_user"
pub fn system_role_definitions() -> Vec<(UserDef, Vec<PrivilegeDef>)> {
    let mut result = vec![];

    let public_def = UserDef {
        id: PUBLIC_ID,
        name: "public".into(),
        // This means the local schema is already up to date and main loop doesn't need to do anything
        schema_version: INITIAL_SCHEMA_VERSION,
        owner: ADMIN_ID,
        auth: None,
        ty: UserMetadataKind::Role,
    };
    let public_privs = vec![
        // TODO:
        // - we grant "execute" access to a bunch of stored procs
        // - vanilla tarantool grants "read" access to a bunch of system spaces
    ];
    result.push((public_def, public_privs));

    let super_def = UserDef {
        id: SUPER_ID,
        name: "super".into(),
        // This means the local schema is already up to date and main loop doesn't need to do anything
        schema_version: INITIAL_SCHEMA_VERSION,
        owner: ADMIN_ID,
        auth: None,
        ty: UserMetadataKind::Role,
    };
    let super_privs = vec![
        // Special role, it's privileges are implicit
    ];
    result.push((super_def, super_privs));

    let replication_def = UserDef {
        id: ROLE_REPLICATION_ID,
        name: "replication".into(),
        // This means the local schema is already up to date and main loop doesn't need to do anything
        schema_version: INITIAL_SCHEMA_VERSION,
        owner: ADMIN_ID,
        auth: None,
        ty: UserMetadataKind::Role,
    };
    let replication_privs = vec![];
    result.push((replication_def, replication_privs));

    result
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

pub fn user_pico_service_is_initialized() -> bool {
    let sys_user = SystemSpace::User.as_space();

    let t = sys_user
        .get(&[PICO_SERVICE_ID])
        .expect("reading from _user shouldn't fail");
    t.is_some()
}

pub fn init_user_pico_service() {
    assert!(!user_pico_service_is_initialized());

    let sys_priv = SystemSpace::Priv.as_space();

    let found = system_user_definitions()
        .into_iter()
        .find(|(user_def, _)| user_def.id == PICO_SERVICE_ID);
    let Some((user_def, _)) = &found else {
        panic!("Couldn't find definition for '{PICO_SERVICE_USER_NAME}' system user");
    };

    let res = schema::acl::on_master_create_user(user_def, false);
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
            write!(f, ", default: {default}")?;
        }
        Ok(())
    }
}

impl Default for RoutineParamDef {
    fn default() -> Self {
        Self {
            mode: RoutineParamMode::default(),
            r#type: FieldType::Integer,
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
    pub id: RoutineId,
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
    /// Index (0-based) of field "name" in _pico_routine table format.
    pub const FIELD_NAME: usize = 1;

    /// Format of the _pico_routine global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("id", FieldType::Unsigned)).is_nullable(false),
            Field::from(("name", FieldType::String)).is_nullable(false),
            Field::from(("kind", FieldType::String)).is_nullable(false),
            Field::from(("params", FieldType::Array)).is_nullable(false),
            Field::from(("returns", FieldType::Array)).is_nullable(false),
            Field::from(("language", FieldType::String)).is_nullable(false),
            Field::from(("body", FieldType::String)).is_nullable(false),
            Field::from(("security", FieldType::String)).is_nullable(false),
            Field::from(("operable", FieldType::Boolean)).is_nullable(false),
            Field::from(("schema_version", FieldType::Unsigned)).is_nullable(false),
            Field::from(("owner", FieldType::Unsigned)).is_nullable(false),
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
        SFT::Unsigned => Some(IFT::Unsigned),
        SFT::String => Some(IFT::String),
        SFT::Double => Some(IFT::Double),
        SFT::Integer => Some(IFT::Integer),
        SFT::Boolean => Some(IFT::Boolean),
        SFT::Varbinary => Some(IFT::Varbinary),
        SFT::Decimal => Some(IFT::Decimal),
        SFT::Uuid => Some(IFT::Uuid),
        SFT::Datetime => Some(IFT::Datetime),
        SFT::Array => Some(IFT::Array),
        _ => None,
    };
    res
}

#[derive(Debug, thiserror::Error)]
pub enum DdlError {
    #[error("{0}")]
    CreateTable(#[from] CreateTableError),
    #[error("ddl operation was aborted: {0}")]
    Aborted(ErrorInfo),
    #[error("there is no pending ddl operation")]
    NoPendingDdl,
    #[error("{0}")]
    CreateIndex(#[from] CreateIndexError),
    #[error("DDL in heterogeneous cluster is prohibited. Found `{first_instance_name}` with version `{first_instance_version}`, `{second_instance_name}` with version `{second_instance_version}`")]
    ProhibitedInHeterogeneousCluster {
        first_instance_name: String,
        first_instance_version: String,
        second_instance_name: String,
        second_instance_version: String,
    },
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
    #[error("specified tier '{tier_name}' doesn't exist")]
    UnexistingTier { tier_name: String },
    #[error("specified tier '{tier_name}' doesn't contain at least one instance")]
    EmptyTier { tier_name: String },
}

impl From<CreateTableError> for Error {
    fn from(err: CreateTableError) -> Self {
        DdlError::CreateTable(err).into()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CreateIndexError {
    #[error("table {table_name} not found")]
    TableNotFound { table_name: String },
    #[error("table {table_name} is not operable")]
    TableNotOperable { table_name: String },
    #[error("no field with name: {name}")]
    FieldUndefined { name: String },
    #[error("table engine {engine} does not support option {option}")]
    IncompatibleTableEngineOption { engine: String, option: String },
    #[error("index type {ty} does not support option {option}")]
    IncompatibleIndexTypeOption { ty: String, option: String },
    #[error("index type {ty} does not support column type {ctype}")]
    IncompatibleIndexColumnType { ty: String, ctype: String },
    #[error("index type {ty} does not support nullable columns")]
    IncompatipleNullableColumn { ty: String },
    #[error("unique index for the sharded table must duplicate its sharding key columns")]
    IncompatibleUniqueIndexColumns,
    #[error("index type {ty} does not support unique indexes")]
    UniqueIndexType { ty: String },
    #[error("index type {ty} does not support non-unique indexes")]
    NonUniqueIndexType { ty: String },
    #[error("index type {ty} does not support multiple columns")]
    IncompatibleIndexMultipleColumns { ty: String },
}

impl From<CreateIndexError> for Error {
    fn from(err: CreateIndexError) -> Self {
        DdlError::CreateIndex(err).into()
    }
}

// TODO: Add `LuaRead` to tarantool::space::Field and use it
#[derive(Clone, Debug, LuaRead)]
pub struct Field {
    pub name: String,
    // TODO(gmoshkin): &str
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
pub struct RenameRoutineParams {
    pub new_name: String,
    pub old_name: String,
    pub params: Option<Vec<ParamDef>>,
}

impl RenameRoutineParams {
    pub fn func_exists(&self) -> bool {
        func_exists(&self.old_name)
    }

    pub fn new_name_occupied(&self) -> bool {
        func_exists(&self.new_name)
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

fn func_exists(name: &str) -> bool {
    let func_space = Space::from(SystemSpace::Func);

    let name_idx = func_space
        .index_cached("name")
        .expect("_function should have an index by name");
    let t = name_idx
        .get(&[name])
        .expect("reading from _function shouldn't fail");
    t.is_some()
}

impl CreateProcParams {
    pub fn func_exists(&self) -> bool {
        func_exists(&self.name)
    }
}

#[derive(Clone, Debug)]
pub struct CreateIndexParams {
    pub(crate) name: String,
    pub(crate) space_name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) ty: IndexType,
    pub(crate) opts: Vec<IndexOption>,
    pub(crate) initiator: UserId,
}

impl CreateIndexParams {
    pub fn index_exists(&self) -> bool {
        let sys_index = Space::from(SystemSpace::Index);
        let name_idx = sys_index
            .index_cached("name")
            .expect("_index should have an index by table_id and name");
        let Some(space) = Space::find(&self.space_name) else {
            return false;
        };
        let idx = name_idx
            .get(&(&space.id(), &self.name))
            .expect("reading from _index shouldn't fail");
        idx.is_some()
    }

    pub fn table(&self, storage: &Catalog) -> traft::Result<TableDef> {
        let table = with_su(ADMIN_ID, || storage.tables.by_name(&self.space_name))??;
        let Some(table) = table else {
            return Err(CreateIndexError::TableNotFound {
                table_name: self.space_name.clone(),
            })?;
        };
        Ok(table)
    }

    pub fn next_index_id(&self) -> traft::Result<IndexId> {
        let Some(space) = Space::find(&self.space_name) else {
            return Err(CreateIndexError::TableNotFound {
                table_name: self.space_name.clone(),
            })?;
        };
        // The logic is taken from the box.schema.index.create function
        // (box/lua/schema.lua).
        let sys_vindex = Space::from(SystemSpace::VIndex);
        let mut iter = sys_vindex
            .primary_key()
            .select(IteratorType::LE, &[space.id()])?;
        let Some(tuple) = iter.next() else {
            panic!("no primary key found for space {}", self.space_name);
        };
        let id: IndexId = tuple.get(1).expect("index id should be present");
        Ok(id + 1)
    }

    pub fn parts(&self, storage: &Catalog) -> traft::Result<Vec<Part<String>>> {
        let table = self.table(storage)?;
        let mut parts = Vec::with_capacity(self.columns.len());

        for column_name in &self.columns {
            let found = table.format.iter().find(|c| &c.name == column_name);
            let Some(column) = found else {
                return Err(CreateIndexError::FieldUndefined {
                    name: column_name.clone(),
                }
                .into());
            };
            let index_field_type = try_space_field_type_to_index_field_type(column.field_type)
                .ok_or_else(|| CreateIndexError::IncompatibleIndexColumnType {
                    ty: self.ty.to_string(),
                    ctype: column.field_type.to_string(),
                })?;
            let part = Part {
                field: column_name.into(),
                r#type: Some(index_field_type),
                collation: None,
                is_nullable: Some(column.is_nullable),
                path: None,
            };
            parts.push(part);
        }
        Ok(parts)
    }

    pub fn into_ddl(&self, storage: &Catalog) -> Result<Ddl, Error> {
        let table = self.table(storage)?;
        let by_fields = self.parts(storage)?;
        let index_id = self.next_index_id()?;
        let ddl = Ddl::CreateIndex {
            space_id: table.id,
            index_id,
            name: self.name.clone(),
            ty: self.ty,
            by_fields,
            opts: self.opts.clone(),
            initiator: self.initiator,
        };
        Ok(ddl)
    }

    pub fn validate(&self, storage: &Catalog) -> traft::Result<()> {
        let table = self.table(storage)?;
        if !table.operable {
            return Err(CreateIndexError::TableNotOperable {
                table_name: self.space_name.clone(),
            })?;
        }
        let mut filed_names: AHashSet<&str> = AHashSet::with_capacity(table.format.len());
        for field in &table.format {
            filed_names.insert(field.name.as_str());
        }
        for column in &self.columns {
            if !filed_names.contains(column.as_str()) {
                return Err(CreateIndexError::FieldUndefined {
                    name: column.clone(),
                })?;
            }
        }

        for opt in &self.opts {
            if table.engine != SpaceEngineType::Vinyl && opt.is_vinyl() {
                return Err(CreateIndexError::IncompatibleTableEngineOption {
                    engine: table.engine.to_string(),
                    option: opt.type_name().into(),
                })?;
            }
            if self.ty != IndexType::Rtree && opt.is_rtree() {
                return Err(CreateIndexError::IncompatibleIndexTypeOption {
                    ty: self.ty.to_string(),
                    option: opt.type_name().into(),
                })?;
            }
            if self.ty != IndexType::Tree && opt.is_tree() {
                return Err(CreateIndexError::IncompatibleIndexTypeOption {
                    ty: self.ty.to_string(),
                    option: opt.type_name().into(),
                })?;
            }
            if let &IndexOption::Unique(false) = opt {
                if self.ty == IndexType::Hash {
                    return Err(CreateIndexError::NonUniqueIndexType {
                        ty: self.ty.to_string(),
                    })?;
                }
            }
            if let &IndexOption::Unique(true) = opt {
                if self.ty == IndexType::Rtree || self.ty == IndexType::Bitset {
                    return Err(CreateIndexError::UniqueIndexType {
                        ty: self.ty.to_string(),
                    })?;
                }
                // Unique index for the sharded table must duplicate its sharding key columns.
                if let Distribution::ShardedImplicitly { sharding_key, .. } = &table.distribution {
                    if sharding_key.len() != self.columns.len() {
                        return Err(CreateIndexError::IncompatibleUniqueIndexColumns)?;
                    }
                    for (sharding_key, column) in sharding_key.iter().zip(&self.columns) {
                        if sharding_key != column {
                            return Err(CreateIndexError::IncompatibleUniqueIndexColumns)?;
                        }
                    }
                }
            }
        }

        let check_multipart = |parts: &[Part<String>]| -> Result<(), CreateIndexError> {
            if parts.len() > 1 {
                return Err(CreateIndexError::IncompatibleIndexMultipleColumns {
                    ty: self.ty.to_string(),
                })?;
            }
            Ok(())
        };
        let check_part_nullability = |part: &Part<String>| -> Result<(), CreateIndexError> {
            if part.is_nullable == Some(true) {
                return Err(CreateIndexError::IncompatipleNullableColumn {
                    ty: self.ty.to_string(),
                })?;
            }
            Ok(())
        };
        let check_part_type = |part: &Part<String>,
                               eq: bool,
                               types: &[Option<IndexFieldType>]|
         -> Result<(), CreateIndexError> {
            let ctype = part
                .r#type
                .as_ref()
                .map(|t| t.to_string())
                .unwrap_or_else(|| "unknown".into());
            if eq {
                if !types.iter().any(|t| t == &part.r#type) {
                    return Err(CreateIndexError::IncompatibleIndexColumnType {
                        ty: self.ty.to_string(),
                        ctype,
                    })?;
                }
            } else if types.iter().any(|t| t == &part.r#type) {
                return Err(CreateIndexError::IncompatibleIndexColumnType {
                    ty: self.ty.to_string(),
                    ctype,
                })?;
            }
            Ok(())
        };
        let parts = self.parts(storage)?;
        match self.ty {
            IndexType::Bitset => {
                check_multipart(&parts)?;
                for part in &parts {
                    check_part_nullability(part)?;
                    check_part_type(
                        part,
                        true,
                        &[
                            Some(IndexFieldType::Unsigned),
                            Some(IndexFieldType::String),
                            Some(IndexFieldType::Varbinary),
                        ],
                    )?;
                }
            }
            IndexType::Rtree => {
                check_multipart(&parts)?;
                for part in &parts {
                    check_part_nullability(part)?;
                    check_part_type(part, true, &[Some(IndexFieldType::Array)])?;
                }
            }
            IndexType::Hash => {
                for part in &parts {
                    check_part_nullability(part)?;
                    check_part_type(
                        part,
                        false,
                        &[Some(IndexFieldType::Array), Some(IndexFieldType::Datetime)],
                    )?;
                }
            }
            IndexType::Tree => {
                for part in &parts {
                    check_part_type(part, false, &[Some(IndexFieldType::Array)])?;
                }
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
    pub(crate) tier: Option<String>,
    /// Timeout in seconds.
    ///
    /// Specifying the timeout identifies how long user is ready to wait for ddl to be applied.
    /// But it does not provide guarantees that a ddl will be aborted if wait for commit timeouts.
    pub timeout: Option<f64>,
}

impl CreateTableParams {
    /// Checks for the following conditions:
    /// 1) specified tier exists
    /// 2) specified tier contains at least one instance
    ///
    /// Checks occur only in case of a sharded table.
    pub fn check_tier_exists(&self, storage: &Catalog) -> traft::Result<()> {
        if self.distribution == DistributionParam::Sharded {
            let tier = self.tier.as_deref().unwrap_or(DEFAULT_TIER);

            if storage.tiers.by_name(tier)?.is_none() {
                return Err(CreateTableError::UnexistingTier {
                    tier_name: tier.to_string(),
                }
                .into());
            };

            let tier_is_not_empty = storage
                .instances
                .iter()?
                .any(|instance| instance.tier == tier);

            if !tier_is_not_empty {
                return Err(CreateTableError::EmptyTier {
                    tier_name: tier.to_string(),
                }
                .into());
            }
        }

        Ok(())
    }

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
                    return Err(CreateTableError::ConflictingShardingPolicy.into());
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
    pub fn test_create_space(&self, storage: &Catalog) -> traft::Result<()> {
        let id = self.id.expect("space id should've been chosen by now");
        let user = storage
            .users
            .by_id(self.owner)?
            .ok_or_else(|| Error::Other(format!("user with id {} not found", self.owner).into()))?
            .name;

        // TODO: This is needed because we do a dry-run of space creation to verify it's parameters,
        // which may fail if the operation is initiated on a read-only replica. For this reason we
        // temporarily switch off the read-only mode. This however will stop working once we add support
        // for synchronous transactions, because read-onlyness will be controlled by the internal raft machinery.
        // At that point we will need to rewrite this code and implement the explicit verification of the parameters.
        let lua = ::tarantool::lua_state();
        let was_read_only: bool = lua.eval(
            "local is_ro = box.cfg.read_only
            if is_ro then
                box.cfg { read_only = false }
            end
            return is_ro",
        )?;

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

        if was_read_only {
            lua.exec("box.cfg { read_only = true }")?;
        }

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
    pub fn choose_id_if_not_specified(
        &mut self,
        name: &str,
        governor_op_id: Option<u64>,
    ) -> traft::Result<()> {
        if self.id.is_some() {
            return Ok(());
        }
        if governor_op_id.is_some() {
            if let Some((table_def, _)) = system_table_definitions()
                .iter()
                .find(|(td, _)| td.name == name)
            {
                self.id = Some(table_def.id);
                return Ok(());
            }
        }

        let sys_space = Space::from(SystemSpace::Space);
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
                #[rustfmt::skip]
                    return Err(BoxError::new(TarantoolErrorCode::CreateSpace, "space id limit is reached").into());
            }
        }

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
                // Case when tier wasn't specified explicitly. On that stage we sure that specified tier exists and isn't empty.
                let tier = self.tier.unwrap_or_else(|| DEFAULT_TIER.into());
                if let Some(field) = self.by_field {
                    Distribution::ShardedByField { field, tier }
                } else {
                    Distribution::ShardedImplicitly {
                        sharding_key: self
                            .sharding_key
                            .expect("should be checked during `validate`"),
                        sharding_fn: self.sharding_fn.unwrap_or_default(),
                        tier,
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
    ddl_prepare_index: RaftIndex,
    timeout: Duration,
) -> traft::Result<RaftIndex> {
    let node = node::global()?;
    let raft_storage = &node.raft_storage;
    let deadline = fiber::clock().saturating_add(timeout);

    loop {
        // If DdlPrepare compacted, then it's finalizer maybe too.
        // Even if we find in raft log ddl finalizer, we can't be sure that it's ours.
        let compacted_index = raft_storage.compacted_index()?;
        if ddl_prepare_index <= compacted_index {
            tlog!(Warning, "DDL finalizer raft op probably was compacted");

            return Err(BoxError::new(
                ErrorCode::RaftLogCompacted,
                "DDL finalizer raft op probably was compacted",
            )
            .into());
        }

        let cur_applied = node.get_index();
        let new_entries = raft_storage.entries(ddl_prepare_index + 1, cur_applied + 1, None)?;
        for entry in new_entries {
            if entry.entry_type != raft::prelude::EntryType::EntryNormal {
                continue;
            }
            let index = entry.index;
            let op = entry.into_op().unwrap_or(Op::Nop);
            match op {
                Op::DdlCommit => return Ok(index),
                Op::DdlAbort { cause } => return Err(DdlError::Aborted(cause).into()),
                Op::DdlPrepare { .. } => unreachable!("Can't skip ddl finalizer with not compacted original DdlPrepare, otherwise completely broken."),
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
pub fn abort_ddl(deadline: Instant) -> traft::Result<RaftIndex> {
    let node = node::global()?;
    loop {
        if node.storage.properties.pending_schema_change()?.is_none() {
            return Err(DdlError::NoPendingDdl.into());
        }

        let instance_name = node
            .raft_storage
            .instance_name()?
            .expect("is persisted at boot");
        let cause = ErrorInfo {
            error_code: ErrorCode::Other as _,
            message: "explicit abort by user".into(),
            instance_name,
        };

        #[rustfmt::skip]
        let ranges = vec![
            cas::Range::new(Properties::TABLE_ID).eq([PropertyName::PendingSchemaChange]),
            cas::Range::new(Properties::TABLE_ID).eq([PropertyName::GlobalSchemaVersion]),
            cas::Range::new(Properties::TABLE_ID).eq([PropertyName::NextSchemaVersion]),
        ];
        let predicate = cas::Predicate::with_applied_index(ranges);
        let req = cas::Request::new(Op::DdlAbort { cause }, predicate, effective_user_id())?;
        let res = cas::compare_and_swap_and_wait(&req, deadline)?;
        match res {
            cas::CasResult::RetriableError(_) => continue,
            cas::CasResult::Ok((index, _, _)) => return Ok(index),
        }
    }
}

mod tests {
    use tarantool::{auth::AuthMethod, space::FieldType};

    use super::*;

    fn storage() -> Catalog {
        let storage = Catalog::for_tests();
        storage
            .users
            .insert(&UserDef {
                id: ADMIN_ID,
                name: String::from("admin"),
                schema_version: 0,
                auth: Some(AuthDef::new(AuthMethod::Md5, String::from(""))),
                owner: ADMIN_ID,
                ty: UserMetadataKind::User,
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
                    r#type: FieldType::Integer,
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
            tier: None,
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
            tier: None,
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
            tier: None,
        }
        .test_create_space(&storage)
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "box error: CreateSpace: Failed to create space 'friends_of_peppa': space id 0 is reserved"
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
            tier: None,
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
            tier: None,
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
            tier: None,
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
            tier: None,
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
            tier: None,
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
            tier: None,
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
            tier: None,
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
            tier: None,
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
            tier: None,
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
        use ::tarantool::msgpack;

        let i = TableDef::for_tests();
        let tuple_data = msgpack::encode(&i);
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
        assert_eq!(format[UserDef::FIELD_NAME].name, "name");
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
            let res = PrivilegeDef::new(1, 1, *privilege, object_type, object_id, 1);

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

    #[test]
    #[rustfmt::skip]
    fn plugin_def_matches_format() {
        let p = PluginDef::for_tests();
        let tuple_data = p.to_tuple_buffer().unwrap();
        let format = PluginDef::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "PluginDef::format");
        assert_eq!(format[PluginDef::FIELD_ENABLE].name, "enabled");
    }

    #[test]
    #[rustfmt::skip]
    fn service_def_matches_format() {
        let s = ServiceDef::for_tests();
        let tuple_data = s.to_tuple_buffer().unwrap();
        let format = ServiceDef::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "ServiceDef::format");
    }

    #[test]
    #[rustfmt::skip]
    fn service_route_item_matches_format() {
        let s = ServiceRouteItem::for_tests();
        let tuple_data = s.to_tuple_buffer().unwrap();
        let format = ServiceRouteItem::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "ServiceRouteItem::format");

        assert_eq!(format[ServiceRouteItem::FIELD_POISON as usize].name, "poison");
    }

    #[test]
    #[rustfmt::skip]
    fn plugin_migration_matches_format() {
        let p = PluginMigrationRecord::for_tests();
        let tuple_data = p.to_tuple_buffer().unwrap();
        let format = PluginMigrationRecord::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "PluginMigrationRecord::format");
    }

    #[test]
    #[rustfmt::skip]
    fn plugin_config_matches_format() {
        let s = PluginConfigRecord::for_tests();
        let tuple_data = s.to_tuple_buffer().unwrap();
        let format = PluginConfigRecord::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "PluginConfigRecord::format");
    }
}

use crate::column_name;
use crate::schema::{IndexDef, IndexOption, INITIAL_SCHEMA_VERSION};
use crate::storage::{space_by_id, SystemTable};

use tarantool::index::{FieldType as IndexFieldType, IndexType, IteratorType, Part};
use tarantool::space::{Field, FieldType, Space, SpaceId, SpaceType, UpdateOps};
use tarantool::tuple::Encode;

////////////////////////////////////////////////////////////////////////////////
/// Governor operation (definition from `_pico_governor_queue` table).
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GovernorOperationDef {
    /// Primary identifier.
    pub id: u64,

    /// ID used to group operations.
    pub batch_id: String,

    /// Operation (SQL statement or custom).
    pub op: String,

    /// Format of the operation.
    pub op_format: GovernorOpFormat,

    /// Current status of the operation.
    pub status: GovernorOpStatus,

    /// Detailed information about current status of the operation.
    /// Contains error message when `status = failed`.
    pub status_description: String,

    /// Type of the operation.
    pub kind: GovernorOpKind,

    /// Description of the operation.
    pub description: String,
}

impl Encode for GovernorOperationDef {}

impl GovernorOperationDef {
    /// Construct new upgrade operation.
    pub fn new_upgrade(version: &str, id: u64, op: &str, op_format: GovernorOpFormat) -> Self {
        Self {
            id,
            batch_id: version.to_string(),
            op: op.to_string(),
            op_format,
            description: format!("upgrade to catalog version {version}"),
            ..Self::default()
        }
    }

    /// Format of the `_pico_governor_queue` global table.
    #[inline(always)]
    pub fn format() -> Vec<::tarantool::space::Field> {
        vec![
            Field::from(("id", FieldType::Unsigned)).is_nullable(false),
            Field::from(("batch_id", FieldType::String)).is_nullable(false),
            Field::from(("op", FieldType::String)).is_nullable(false),
            Field::from(("op_format", FieldType::String)).is_nullable(false),
            Field::from(("status", FieldType::String)).is_nullable(false),
            Field::from(("status_description", FieldType::String)).is_nullable(false),
            Field::from(("kind", FieldType::String)).is_nullable(false),
            Field::from(("description", FieldType::String)).is_nullable(false),
        ]
    }
}

impl std::fmt::Display for GovernorOperationDef {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "(id: {}, batch_id: {}, op: {}, op_format: {}, status: {}, kind: {}, description: {})",
            self.id,
            self.batch_id,
            self.op,
            self.op_format,
            self.status,
            self.kind,
            self.description
        )
    }
}

::tarantool::define_str_enum! {
    /// Governor operation status.
    #[derive(Default)]
    pub enum GovernorOpStatus {
        /// Operation is pending.
        #[default]
        Pending = "pending",

        /// Operation is completed successfully.
        Done = "done",

        /// Operation is completed with error.
        Failed = "failed",
    }
}

::tarantool::define_str_enum! {
    /// Governor operation type.
    #[derive(Default)]
    pub enum GovernorOpKind {
        /// Upgrade operation.
        #[default]
        Upgrade = "upgrade",

        /// Custom operation.
        Custom = "custom",
    }
}

::tarantool::define_str_enum! {
    /// Format for `op` field from `_pico_governor_queue`.
    #[derive(Default)]
    pub enum GovernorOpFormat {
        /// SQL statement.
        #[default]
        Sql = "sql",

        /// Internal procedure name for creation.
        ProcName = "proc_name",

        /// Internal script name for executing.
        ExecScript = "exec_script",
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Governor operations queue
/// (struct for accessing of `_pico_governor_queue` table).
#[derive(Debug, Clone)]
pub struct GovernorQueue {
    space_id: SpaceId,
}

impl SystemTable for GovernorQueue {
    const TABLE_NAME: &'static str = "_pico_governor_queue";
    const TABLE_ID: SpaceId = 522;
    const DESCRIPTION: &'static str = "Stores governor operations.";

    fn format() -> Vec<tarantool::space::Field> {
        GovernorOperationDef::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            id: 0,
            name: "_pico_governor_queue_pkey".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![Part::from(("id", IndexFieldType::Unsigned)).is_nullable(false)],
            operable: true,
            schema_version: INITIAL_SCHEMA_VERSION,
        }]
    }
}

impl GovernorQueue {
    pub fn new() -> tarantool::Result<Self> {
        Ok(Self {
            space_id: Self::TABLE_ID,
        })
    }

    /// NOTE: this is new space since 25.3.1
    /// We need to create the new space only on masters
    /// to avoid duplicate key problem.
    /// That's why space creating logic is in separate function.
    pub fn create_space(&self) -> tarantool::Result<()> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;
        space
            .index_builder("_pico_governor_queue_pkey")
            .unique(true)
            .part("id")
            .if_not_exists(true)
            .create()?;

        Ok(())
    }

    /// Gets all operations from `_pico_governor_queue` table.
    /// If the table does not exist (because it has not been created yet),
    /// returns an empty vector.
    #[inline]
    pub fn all_operations(&self) -> tarantool::Result<Vec<GovernorOperationDef>> {
        let Ok(space) = space_by_id(self.space_id) else {
            return Ok(vec![]);
        };
        space
            .select(IteratorType::All, &())?
            .map(|tuple| tuple.decode())
            .collect()
    }

    /// Updates operation status.
    #[inline]
    pub fn update_status(&self, id: u64, status: GovernorOpStatus) -> tarantool::Result<()> {
        let space = space_by_id(self.space_id)?;
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(column_name!(GovernorOperationDef, status), status)?;
        space.update(&[id], ops)?;
        Ok(())
    }

    /// Gets next pending governor operation from `_pico_governor_queue`.
    #[inline]
    pub fn next_pending_operation(
        &self,
        batch_id: Option<String>,
    ) -> tarantool::Result<Option<GovernorOperationDef>> {
        let mut iter = self.all_operations()?.into_iter();
        let next_op;
        if let Some(batch_id) = batch_id {
            next_op = iter.find(|op| {
                op.kind == GovernorOpKind::Upgrade
                    && op.status == GovernorOpStatus::Pending
                    && op.batch_id <= batch_id
            });
        } else {
            next_op = iter.find(|op| {
                op.kind != GovernorOpKind::Upgrade && op.status == GovernorOpStatus::Pending
            });
        }
        Ok(next_op)
    }
}

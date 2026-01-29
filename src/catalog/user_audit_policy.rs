use crate::audit::policy::AuditPolicyId;
use crate::schema::{IndexDef, IndexOption, INITIAL_SCHEMA_VERSION};
use crate::storage::{space_by_id_unchecked, SystemTable};
use tarantool::index::{FieldType as IndexFieldType, IndexType, IteratorType, Part};
use tarantool::session::UserId;
use tarantool::space::{Field, FieldType, Space, SpaceId, SpaceType};
use tarantool::tuple::Encode;

////////////////////////////////////////////////////////////////////////////////
/// Represents an audit policy association for a specific user.
/// (definition from `_pico_user_audit_policy` table).
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UserAuditPolicyDef {
    /// User ID.
    pub user_id: UserId,

    /// Audit policy ID.
    pub policy_id: AuditPolicyId,
}

impl Encode for UserAuditPolicyDef {}

impl UserAuditPolicyDef {
    /// Format of the `_pico_user_audit_policy` global table.
    #[inline(always)]
    pub fn format() -> Vec<::tarantool::space::Field> {
        vec![
            Field::from(("user_id", FieldType::Unsigned)).is_nullable(false),
            Field::from(("policy_id", FieldType::Unsigned)).is_nullable(false),
        ]
    }
}

impl std::fmt::Display for UserAuditPolicyDef {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "(user_id: {}, policy_id: {})",
            self.user_id, self.policy_id
        )
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Represents the many-to-many relationship between users and audit policies.
/// (struct for accessing of `_pico_user_audit_policy` table).
///
/// # Indexes
/// - Primary key: (`user_id`, `policy_id`) composite
#[derive(Debug, Clone)]
pub struct PicoUserAuditPolicy {
    pub space: Space,
}

impl SystemTable for PicoUserAuditPolicy {
    const TABLE_NAME: &'static str = "_pico_user_audit_policy";
    const TABLE_ID: SpaceId = 525;
    const DESCRIPTION: &'static str =
        "Represents many-to-many relationship between users and audit policies.";

    fn format() -> Vec<tarantool::space::Field> {
        UserAuditPolicyDef::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            id: 0,
            name: "_pico_user_audit_policy_pkey".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![
                Part::from(("user_id", IndexFieldType::Unsigned)).is_nullable(false),
                Part::from(("policy_id", IndexFieldType::Unsigned)).is_nullable(false),
            ],
            operable: true,
            schema_version: INITIAL_SCHEMA_VERSION,
        }]
    }
}

impl PicoUserAuditPolicy {
    pub const fn new() -> Self {
        Self {
            space: space_by_id_unchecked(Self::TABLE_ID),
        }
    }

    /// NOTE: this is new space since 25.4.1
    /// We need to create the new space only on masters
    /// to avoid duplicate key problem.
    /// That's why space creating logic is in separate function.
    pub fn create(&self) -> tarantool::Result<()> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;
        space
            .index_builder("_pico_user_audit_policy_pkey")
            .unique(true)
            .part("user_id")
            .part("policy_id")
            .if_not_exists(true)
            .create()?;

        Ok(())
    }

    /// Retrieves the audit policy association for a specific user from the `_pico_user_audit_policy` table.
    ///
    /// # Returns
    ///   * `Ok(Some(UserAuditPolicyDef))` if the association exists
    ///   * `Ok(None)` if no association exists
    ///   * `Err` if error occurs.
    #[inline]
    pub fn get(
        &self,
        user_id: UserId,
        policy_id: AuditPolicyId,
    ) -> tarantool::Result<Option<UserAuditPolicyDef>> {
        let Some(tuple) = self.space.get(&(user_id, policy_id))? else {
            return Ok(None);
        };
        tuple.decode()
    }

    /// Enables the audit policy with ID `policy_id` for the user with ID `user_id`.
    ///
    /// This operation persists the association in the `_pico_user_audit_policy` table.
    #[inline]
    pub fn enable_audit_policy_for_user(
        &self,
        user_id: UserId,
        policy_id: AuditPolicyId,
    ) -> tarantool::Result<()> {
        self.space.replace(&(user_id, policy_id))?;
        Ok(())
    }

    /// Disables the audit policy with ID `policy_id` for the user with ID `user_id`.
    ///
    /// This operation deletes the association from the `_pico_user_audit_policy` table.
    #[inline]
    pub fn disable_audit_policy_for_user(
        &self,
        user_id: UserId,
        policy_id: AuditPolicyId,
    ) -> tarantool::Result<()> {
        self.space.delete(&(user_id, policy_id))?;
        Ok(())
    }

    /// Deletes all audit policy associations for a specific user.
    #[inline]
    pub fn delete_by_user(&self, user_id: UserId) -> tarantool::Result<()> {
        let user_policies: Vec<_> = self
            .space
            .select(IteratorType::Eq, &[user_id])?
            .map(|tuple| {
                tuple
                    .decode::<UserAuditPolicyDef>()
                    .expect("decoding of UserAuditPolicyDef should never fail")
            })
            .collect();
        for UserAuditPolicyDef { policy_id, .. } in user_policies {
            self.space.delete(&(user_id, policy_id))?;
        }
        Ok(())
    }
}

impl Default for PicoUserAuditPolicy {
    fn default() -> Self {
        Self::new()
    }
}

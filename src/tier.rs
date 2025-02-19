use ::tarantool::tlua;
use tarantool::{space::UpdateOps, tuple::Encode};

use crate::{
    column_name,
    schema::ADMIN_ID,
    sql,
    storage::{TClusterwideTable, Tiers},
    traft::{error::Error, op::Dml},
};

pub const DEFAULT_TIER: &str = "default";

#[derive(Debug, serde::Deserialize, serde::Serialize, PartialEq, tlua::Push, Clone)]
////////////////////////////////////////////////////////////////////////////////
/// Serializable struct representing a tier.
///
/// Can be used to store tier definition in the _pico_tier global table.
pub struct Tier {
    pub name: String,
    pub replication_factor: u8,
    pub can_vote: bool,
    pub current_vshard_config_version: u64,
    pub target_vshard_config_version: u64,
    pub vshard_bootstrapped: bool,
    pub bucket_count: u64,
}

impl Encode for Tier {}

impl Tier {
    /// Format of the _pico_tier global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::{Field, FieldType};
        vec![
            Field::from(("name", FieldType::String)),
            Field::from(("replication_factor", FieldType::Unsigned)),
            Field::from(("can_vote", FieldType::Boolean)),
            Field::from(("current_vshard_config_version", FieldType::Unsigned)),
            Field::from(("target_vshard_config_version", FieldType::Unsigned)),
            Field::from(("vshard_bootstrapped", FieldType::Boolean)),
            Field::from(("bucket_count", FieldType::Unsigned)),
        ]
    }

    /// Returns DML for updating `target_vshard_config_version` of corresponding tier record in '_pico_tier'
    pub fn get_vshard_config_version_bump_op_if_needed(tier: &Tier) -> Result<Option<Dml>, Error> {
        if tier.current_vshard_config_version == tier.target_vshard_config_version {
            let mut uops = UpdateOps::new();
            uops.assign(
                column_name!(Tier, target_vshard_config_version),
                tier.target_vshard_config_version + 1,
            )?;

            let dml = Dml::update(Tiers::TABLE_ID, &[&tier.name], uops, ADMIN_ID)?;

            return Ok(Some(dml));
        }

        Ok(None)
    }
}

impl Default for Tier {
    fn default() -> Self {
        Tier {
            name: DEFAULT_TIER.into(),
            replication_factor: 1,
            can_vote: true,
            bucket_count: sql::DEFAULT_BUCKET_COUNT,
            current_vshard_config_version: 0,
            target_vshard_config_version: 0,
            vshard_bootstrapped: false,
        }
    }
}

/// Tier definition struct which can be deserialized from the config file.
#[derive(
    PartialEq,
    Default,
    Debug,
    Clone,
    serde::Deserialize,
    serde::Serialize,
    tlua::Push,
    tlua::PushInto,
)]
#[serde(deny_unknown_fields)]
pub struct TierConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_factor: Option<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket_count: Option<u64>,

    #[serde(default = "default_can_vote")]
    pub can_vote: bool,
}

#[inline(always)]
fn default_can_vote() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use tarantool::tuple::ToTupleBuffer;

    #[test]
    #[rustfmt::skip]
    fn matches_format() {
        let i = Tier::default();
        let tuple_data = i.to_tuple_buffer().unwrap();
        let format = Tier::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "Tier::format");
    }
}

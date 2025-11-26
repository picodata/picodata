use ::tarantool::tlua;
use smol_str::SmolStr;
use tarantool::{space::UpdateOps, tuple::Encode};

use crate::{
    column_name,
    schema::ADMIN_ID,
    sql,
    storage::{SystemTable, Tiers},
    traft::{error::Error, op::Dml},
};

pub const DEFAULT_TIER: &str = "default";

#[derive(Debug, serde::Deserialize, serde::Serialize, PartialEq, tlua::Push, Clone)]
////////////////////////////////////////////////////////////////////////////////
/// Serializable struct representing a tier.
///
/// Can be used to store tier definition in the _pico_tier global table.
pub struct Tier {
    /// Tier name.
    pub name: SmolStr,

    /// The size of replicasets in this tier.
    ///
    /// When adding new instances to this tier picodata will try adding them to
    /// existing replicasets only if they don't already contain
    /// `replication_factor` of replicas.
    ///
    /// Also note that replicasets which aren't yet filled to the replication
    /// factor will not store any sharded data (cannot own buckets).
    pub replication_factor: u8,

    /// If `true` the instances of this tier can vote for and/or become the
    /// governor (raft leader of the cluster).
    pub can_vote: bool,

    /// Version number of the vshard configuration which was most recently applied to the cluster.
    /// If this is not equal to the value of `target_vshard_config_version`,
    /// then the actual runtime vsahrd configuration may be different on different instances.
    ///
    /// The replication config consists of:
    /// - `vshard.storage.cfg`
    /// - `pico.tier[*].cfg`
    pub current_vshard_config_version: u64,

    /// Version number of the vshard configuration which should be applied to the cluster.
    /// If this is equal to the value of `current_vshard_config_version`, then the configuration is up to date.
    pub target_vshard_config_version: u64,

    /// If `true` then the initial bucket distribution has been bootstrapped in
    /// this tier. Otherwise none of the replicasets in the tier own any buckets.
    ///
    /// Bucket distribution gets bootstrapped when the first replicaset in the
    /// tier gets filled up to the `replication_factor`.
    pub vshard_bootstrapped: bool,

    /// The number of buckets into which the data is sharded in this tier.
    pub bucket_count: u64,

    /// If `true` this is the default tier of the cluster.
    ///
    /// The default tier is used in some operations when the explicit tier
    /// parameter is not specified. For example CREATE TABLE will create a table
    /// with data distributed in this tier unless specified otherwise.
    /// NOTE: this feature is not yet implemented, see <https://git.picodata.io/core/picodata/-/issues/1437>
    #[serde(default)]
    pub is_default: Option<bool>,

    /// Version of the bucket distribution which is currently applied on this tier.
    ///
    /// If `current_bucket_state_version` != `target_bucket_state_version` then
    /// bucket rebalancing is currently in progress in this tier.
    #[serde(default)]
    pub current_bucket_state_version: u64,

    /// Version of the bucket distribution which should be applied on this tier.
    ///
    /// This value is increased everytime the bucket rebalancing process starts.
    #[serde(default)]
    pub target_bucket_state_version: u64,
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
            Field::from(("is_default", FieldType::Boolean)).is_nullable(true),
            Field::from(("current_bucket_state_version", FieldType::Unsigned)).is_nullable(true),
            Field::from(("target_bucket_state_version", FieldType::Unsigned)).is_nullable(true),
        ]
    }

    /// Returns DML for updating `target_vshard_config_version` of corresponding tier record in '_pico_tier'
    pub fn get_vshard_config_version_bump_op(tier: &Tier) -> Result<Dml, Error> {
        let mut uops = UpdateOps::new();
        uops.assign(
            column_name!(Tier, target_vshard_config_version),
            tier.target_vshard_config_version + 1,
        )?;
        let dml = Dml::update(Tiers::TABLE_ID, &[&tier.name], uops, ADMIN_ID)?;

        Ok(dml)
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
            is_default: Some(false),
            current_bucket_state_version: 0,
            target_bucket_state_version: 0,
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

use ::tarantool::tlua;
use tarantool::tuple::Encode;

pub const DEFAULT_TIER: &str = "default";

#[derive(Debug, serde::Deserialize, serde::Serialize, PartialEq, tlua::Push, Clone)]
////////////////////////////////////////////////////////////////////////////////
/// Serializable struct representing a tier.
///
/// Can be used to store tier definition in the _pico_tier global table.
pub struct Tier {
    pub name: String,
    pub replication_factor: u8,
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
        ]
    }
}

impl Default for Tier {
    fn default() -> Self {
        Tier {
            name: DEFAULT_TIER.into(),
            replication_factor: 1,
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

    /// TODO: This is not yet implemented, currently all tiers can vote
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

use ::tarantool::tlua;
use tarantool::tuple::Encode;

pub const DEFAULT_TIER: &str = "storage";

#[derive(Debug, serde::Deserialize, serde::Serialize, PartialEq, tlua::Push, Clone)]
////////////////////////////////////////////////////////////////////////////////
/// Serializable struct representing a tier.
pub struct Tier {
    pub name: String,
    pub replication_factor: u8,
}

impl Default for Tier {
    fn default() -> Self {
        Tier {
            name: DEFAULT_TIER.into(),
            replication_factor: 1,
        }
    }
}

impl Tier {
    pub fn with_replication_factor(replication_factor: u8) -> Self {
        Tier {
            name: DEFAULT_TIER.into(),
            replication_factor,
        }
    }
}

impl Encode for Tier {}

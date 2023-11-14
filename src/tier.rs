use ::tarantool::tlua;
use tarantool::tuple::Encode;

pub const DEFAULT_TIER: &str = "default";

#[derive(Debug, serde::Deserialize, serde::Serialize, PartialEq, tlua::Push, Clone)]
////////////////////////////////////////////////////////////////////////////////
/// Serializable struct representing a tier.
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

    pub fn with_replication_factor(replication_factor: u8) -> Self {
        Tier {
            name: DEFAULT_TIER.into(),
            replication_factor,
        }
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

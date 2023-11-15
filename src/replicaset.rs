use super::instance::InstanceId;
use ::tarantool::tlua;
use ::tarantool::tuple::Encode;

crate::define_string_newtype! {
    /// Unique id of a replicaset.
    ///
    /// This is a new-type style wrapper around String,
    /// to distinguish it from other strings.
    pub struct ReplicasetId(pub String);
}

pub type Weight = f64;

////////////////////////////////////////////////////////////////////////////////
/// Replicaset info
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Replicaset {
    /// Primary identifier.
    pub replicaset_id: ReplicasetId,

    /// UUID used to identify replicasets by tarantool's subsystems.
    pub replicaset_uuid: String,

    /// Instance id of the current replication leader.
    pub current_master_id: InstanceId,

    /// Id of instance which should become the replication leader.
    pub target_master_id: InstanceId,

    /// Name of the tier the replicaset belongs to.
    pub tier: String,

    /// Sharding weight of the replicaset.
    ///
    /// Determines the portion of the tier's buckets which will be stored in
    /// this replicaset.
    pub weight: Weight,

    /// Describes how the `weight` is chosen: either automatically by governor,
    /// or manually by the user.
    pub weight_origin: WeightOrigin,

    /// Current state of the replicaset. This is set to `NotReady` when the
    /// replicaset is not filled up to the tier's replication factor.
    pub state: ReplicasetState,
}
impl Encode for Replicaset {}

impl Replicaset {
    /// Format of the _pico_replicaset global table.
    #[inline(always)]
    pub fn format() -> Vec<::tarantool::space::Field> {
        use ::tarantool::space::{Field, FieldType};
        vec![
            Field::from(("replicaset_id", FieldType::String)),
            Field::from(("replicaset_uuid", FieldType::String)),
            Field::from(("current_master_id", FieldType::String)),
            Field::from(("target_master_id", FieldType::String)),
            Field::from(("tier", FieldType::String)),
            Field::from(("weight", FieldType::Number)),
            Field::from(("weight_origin", FieldType::String)),
            Field::from(("state", FieldType::String)),
        ]
    }

    /// A dummy instance of the type for use in tests.
    #[inline(always)]
    pub fn for_tests() -> Self {
        Self {
            replicaset_id: "r1".into(),
            replicaset_uuid: "r1-uuid".into(),
            current_master_id: "i".into(),
            target_master_id: "j".into(),
            tier: "storage".into(),
            weight: 13.37,
            weight_origin: WeightOrigin::Auto,
            state: ReplicasetState::Ready,
        }
    }
}

impl std::fmt::Display for Replicaset {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "({}, master: {}, tier: {}, weight: {}, weight_origin: {}, state: {})",
            self.replicaset_id,
            crate::util::Transition {
                from: &self.current_master_id,
                to: &self.target_master_id,
            },
            self.tier,
            self.weight,
            self.weight_origin,
            self.state,
        )
    }
}

::tarantool::define_str_enum! {
    /// Replicaset weight origin
    #[derive(Default)]
    pub enum WeightOrigin {
        /// Weight is determined by governor.
        #[default]
        Auto = "auto",

        /// Weight is specified by user.
        User = "user",
    }
}

::tarantool::define_str_enum! {
    /// Replicaset weight state
    #[derive(Default)]
    pub enum ReplicasetState {
        /// Replicaset is not filled up to the replication factor yet.
        #[default]
        NotReady = "not-ready",

        /// Replicaset is fully operable.
        Ready = "ready",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tarantool::tuple::ToTupleBuffer;

    #[test]
    fn matches_format() {
        let r = Replicaset::for_tests();
        let tuple_data = r.to_tuple_buffer().unwrap();
        let format = Replicaset::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "Replicaset::format");
    }
}

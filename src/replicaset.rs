use crate::traft::InstanceId;
use crate::util::Transition;
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
    pub master_id: InstanceId,

    /// Current sharding weight of the replicaset.
    pub current_weight: Weight,

    /// Target sharding weight of the replicaset.
    pub target_weight: Weight,

    /// Current schema version of the replicaset.
    pub current_schema_version: u64,
}
impl Encode for Replicaset {}

impl Replicaset {
    pub fn format() -> Vec<::tarantool::space::Field> {
        use ::tarantool::space::{Field, FieldType};
        vec![
            Field::from(("replicaset_id", FieldType::String)),
            Field::from(("replicaset_uuid", FieldType::String)),
            Field::from(("master_id", FieldType::String)),
            Field::from(("current_weight", FieldType::Double)),
            Field::from(("target_weight", FieldType::Double)),
            Field::from(("current_schema_version", FieldType::Unsigned)),
        ]
    }
}

impl std::fmt::Display for Replicaset {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "({}, master: {}, weight: {}, schema_version: {})",
            self.replicaset_id,
            self.master_id,
            Transition {
                from: self.current_weight,
                to: self.target_weight
            },
            self.current_schema_version,
        )
    }
}

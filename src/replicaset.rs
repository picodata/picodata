use super::instance::InstanceName;
use crate::instance::Instance;
use ::tarantool::tuple::Encode;
use ::tarantool::vclock::Vclock;
use smol_str::SmolStr;

// TODO: this redundant boilerplate needs to be removed
crate::define_smolstr_newtype! {
    /// Unique name of a replicaset.
    ///
    /// This is a new-type style wrapper around String,
    /// to distinguish it from other strings.
    pub struct ReplicasetName(pub SmolStr);
}

pub type Weight = f64;

////////////////////////////////////////////////////////////////////////////////
/// Replicaset info
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Replicaset {
    /// Primary identifier.
    pub name: ReplicasetName,

    /// UUID used to identify replicasets by tarantool's subsystems.
    pub uuid: SmolStr,

    /// Instance name of the current replication leader.
    pub current_master_name: InstanceName,

    /// Name of instance which should become the replication leader.
    pub target_master_name: InstanceName,

    /// Name of the tier the replicaset belongs to.
    pub tier: SmolStr,

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

    /// Version number of the replication configuration which was most recently applied to this replicaset.
    /// If this is not equal to the value of target_config_version,
    /// then the actual runtime replication configuration may be different on different instances.
    ///
    /// The replication config consists of:
    /// - box.cfg.replication
    /// - box.cfg.read_only
    pub current_config_version: u64,

    /// Version number of the replication configuration which should be applied to this replicaset.
    /// If this is equal to the value of current_config_version, then the configuration is up to date.
    pub target_config_version: u64,

    /// Vclock of the current master at the moment it was promoted.
    pub promotion_vclock: Vclock,
}
impl Encode for Replicaset {}

impl Replicaset {
    /// Index of field "uuid" in the table _pico_replicaset format.
    ///
    /// Index of first field is 0.
    pub const FIELD_REPLICASET_UUID: u32 = 1;

    /// Index of field "target_master_name" in the table _pico_replicaset format.
    pub const FIELD_TARGET_MASTER_NAME: u32 = 3;

    /// Index of field "tier" in the table _pico_replicaset format.
    pub const FIELD_TIER: u32 = 4;

    /// Index of field "state" in the table _pico_replicaset format.
    pub const FIELD_STATE: u32 = 7;

    #[inline]
    pub fn with_one_instance(master: &Instance) -> Replicaset {
        Replicaset {
            name: master.replicaset_name.clone(),
            uuid: master.replicaset_uuid.clone(),
            current_master_name: master.name.clone(),
            target_master_name: master.name.clone(),
            tier: master.tier.clone(),
            weight: 0.,
            weight_origin: WeightOrigin::Auto,
            state: ReplicasetState::NotReady,
            current_config_version: 0,
            target_config_version: 0,
            promotion_vclock: Vclock::from([]),
        }
    }

    /// Format of the _pico_replicaset global table.
    #[inline(always)]
    pub fn format() -> Vec<::tarantool::space::Field> {
        use ::tarantool::space::{Field, FieldType};
        vec![
            Field::from(("name", FieldType::String)),
            Field::from(("uuid", FieldType::String)),
            Field::from(("current_master_name", FieldType::String)),
            Field::from(("target_master_name", FieldType::String)),
            Field::from(("tier", FieldType::String)),
            Field::from(("weight", FieldType::Double)),
            Field::from(("weight_origin", FieldType::String)),
            Field::from(("state", FieldType::String)),
            Field::from(("current_config_version", FieldType::Unsigned)),
            Field::from(("target_config_version", FieldType::Unsigned)),
            Field::from(("promotion_vclock", FieldType::Map)),
        ]
    }

    #[inline(always)]
    pub fn master_is_transitioning(&self) -> bool {
        self.current_master_name != self.target_master_name
    }

    /// Returns the name of instance which should be the master replica at this
    /// moment.
    ///
    /// Returns `None` if the role of the master is transitioning from
    /// [`Self::current_master_name`] to [`Self::target_master_name`].
    ///
    /// NOTE if this function returns `None` then all replicas should be
    /// read-only.
    pub fn effective_master_name(&self) -> Option<&InstanceName> {
        if self.master_is_transitioning() {
            return None;
        }

        Some(&self.current_master_name)
    }

    /// A dummy instance of the type for use in tests.
    #[inline(always)]
    pub fn for_tests() -> Self {
        Self {
            name: "r1".into(),
            uuid: "r1-uuid".into(),
            current_master_name: "i".into(),
            target_master_name: "j".into(),
            tier: "storage".into(),
            weight: 13.37,
            weight_origin: WeightOrigin::Auto,
            state: ReplicasetState::Ready,
            current_config_version: 13,
            target_config_version: 13,
            promotion_vclock: Vclock::from([420, 69105]),
        }
    }
}

impl std::fmt::Display for Replicaset {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "({}, master: {}, tier: {}, weight: {}, weight_origin: {}, state: {})",
            self.name,
            crate::util::Transition {
                from: &self.current_master_name,
                to: &self.target_master_name,
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
        ///
        /// Instances cannot store any sharded data.
        #[default]
        NotReady = "not-ready",

        /// Replicaset is fully operable.
        Ready = "ready",

        /// Replicaset is in the process of being expelled.
        ///
        /// This means that governor is currently waiting until all buckets are
        /// transferred away from this replicaset.
        ///
        /// No more instances can join this replicaset.
        ToBeExpelled = "to-be-expelled",

        /// Replicaset has been expelled.
        ///
        /// All instances from this replicaset have been expelled.
        ///
        /// No more instances can join this replicaset.
        Expelled = "expelled",
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

        assert_eq!(
            format[Replicaset::FIELD_REPLICASET_UUID as usize].name,
            "uuid"
        );
        assert_eq!(
            format[Replicaset::FIELD_TARGET_MASTER_NAME as usize].name,
            "target_master_name"
        );
        assert_eq!(format[Replicaset::FIELD_TIER as usize].name, "tier");
        assert_eq!(format[Replicaset::FIELD_STATE as usize].name, "state");
    }
}

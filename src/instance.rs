use super::failure_domain::FailureDomain;
use super::replicaset::ReplicasetName;
use crate::has_states;
use crate::info::PICODATA_VERSION;
use crate::traft::RaftId;
use crate::util::Transition;
use crate::version::Version;
use ::serde::{Deserialize, Serialize};
use ::tarantool::tuple::Encode;
use smol_str::format_smolstr;
use smol_str::SmolStr;
use smol_str::ToSmolStr;
use tarantool::datetime::Datetime;

pub mod state;
pub use state::State;
pub use state::StateVariant;

crate::define_smolstr_newtype! {
    /// Unique id of a cluster instance.
    ///
    /// This is a new-type style wrapper around String,
    /// to distinguish it from other strings.
    pub struct InstanceName(pub SmolStr);
}

////////////////////////////////////////////////////////////////////////////////
/// Serializable struct representing a member of the raft group.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Instance {
    /// Instances are identified by name.
    pub name: InstanceName,
    pub uuid: SmolStr,

    /// Used for identifying raft nodes.
    /// Must be unique in the raft group.
    pub raft_id: RaftId,

    /// Name of a replicaset the instance belongs to.
    pub replicaset_name: ReplicasetName,
    pub replicaset_uuid: SmolStr,

    /// The cluster's mind about actual state of this instance's activity.
    pub current_state: State,
    /// The desired state of this instance
    pub target_state: State,

    /// Instance failure domains. Instances with overlapping failure domains
    /// must not be in the same replicaset.
    // TODO: raft_group space is kinda bloated, maybe we should store some data
    // in different spaces/not deserialize the whole tuple every time?
    pub failure_domain: FailureDomain,

    /// Instance tier. Each instance belongs to only one tier.
    pub tier: SmolStr,

    /// Version of picodata executable which running this instance.
    /// It should match the version returned by `.proc_version_info` on this instance.
    pub picodata_version: SmolStr,

    /// This value keeps track of the moment when this instance has synchronized
    /// with its replicaset's master. It is assigned `target_state.incarnation`
    /// when the synchronization takes place.
    #[serde(default)]
    pub sync_incarnation: u64,

    /// The explanation for `target_state`. If the instance was automatically
    /// made `Offline` then we will explain the reason for that in here.
    #[serde(default)]
    pub target_state_reason: SmolStr,

    /// The UNIX timestamp of the last time when `target_state` was changed
    /// (either state variant or state incarnation).
    #[serde(default)]
    pub target_state_change_time: Option<Datetime>,
}

impl Encode for Instance {}

impl Instance {
    /// Index of field "name" in the space _pico_instance format.
    ///
    /// Index of first field is 0.
    pub const FIELD_INSTANCE_NAME: u32 = 0;

    /// Index of field "raft_id" in the space _pico_instance format.
    ///
    /// Index of first field is 0.
    pub const FIELD_RAFT_ID: u32 = 2;

    /// Index of field "failure_domain" in the space _pico_instance format.
    ///
    /// Index of first field is 0.
    pub const FIELD_FAILURE_DOMAIN: u32 = 7;

    /// System catalog version in which `sync_incarnation` column was added to `_pico_instance`.
    pub const SYNC_INCARNATION_AVAILABLE_SINCE: Version = Version::new_clean(25, 5, 3);

    /// System catalog version in which `target_state_change_time` column was added to `_pico_instance`.
    pub const TARGET_STATE_CHANGE_TIME_AVAILABLE_SINCE: Version = Version::new_clean(25, 5, 3);

    /// Format of the _pico_instance global table.
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::{Field, FieldType};
        vec![
            Field::from(("name", FieldType::String)),
            Field::from(("uuid", FieldType::String)),
            Field::from(("raft_id", FieldType::Unsigned)),
            Field::from(("replicaset_name", FieldType::String)),
            Field::from(("replicaset_uuid", FieldType::String)),
            Field::from(("current_state", FieldType::Array)),
            Field::from(("target_state", FieldType::Array)),
            Field::from(("failure_domain", FieldType::Map)),
            Field::from(("tier", FieldType::String)),
            Field::from(("picodata_version", FieldType::String)),
            Field::from(("sync_incarnation", FieldType::Unsigned)).is_nullable(true),
            Field::from(("target_state_reason", FieldType::String)).is_nullable(true),
            Field::from(("target_state_change_time", FieldType::Datetime)).is_nullable(true),
        ]
    }

    /// Instance has a state that implies it may cooperate.
    /// Currently this means that
    /// - current_state is not Expelled
    /// - target_state is not Offline
    #[inline(always)]
    pub fn may_respond(&self) -> bool {
        // If instance is going offline ugracefully it will likely not respond
        has_states!(self, * -> not Offline) &&
        // If instance has already been expelled it will definitely not respond
        has_states!(self, not Expelled -> *) &&
        // If instance is currently offline and is being expelled, the above
        // rules don't work, but the instance is definitely not going to respond
        !has_states!(self, Offline -> Expelled)
    }

    #[inline]
    pub fn is_reincarnated(&self) -> bool {
        self.current_state.incarnation < self.target_state.incarnation
    }

    #[inline]
    pub fn replication_sync_needed(&self) -> bool {
        self.sync_incarnation < self.target_state.incarnation
    }

    /// Returns a dummy instance of the struct for use in tests
    pub fn for_tests() -> Self {
        Self {
            name: "default_13_37".into(),
            uuid: "ed7ba470-8e54-465e-825c-99712043e01c".into(),
            raft_id: 1,
            replicaset_name: "default_13".into(),
            replicaset_uuid: "deadbeef-cafe-babe-0123-456789abcdef".into(),
            current_state: State::new(StateVariant::Online, 13),
            target_state: State::new(StateVariant::Online, 13),
            failure_domain: FailureDomain::default(),
            tier: "default".into(),
            picodata_version: "22.07.0".into(),
            sync_incarnation: 13,
            target_state_reason: "wakeup".into(),
            target_state_change_time: Some(Datetime::now_utc()),
        }
    }
}

impl std::fmt::Display for Instance {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f,
            "({}, {}, {}, {}, {}, {}, {})",
            self.name,
            self.raft_id,
            self.replicaset_name,
            Transition { from: self.current_state, to: self.target_state },
            &self.failure_domain,
            self.tier,
            self.picodata_version,
        )
    }
}

#[rustfmt::skip]
mod tests {
    use std::collections::HashSet;
    use tarantool::space::UpdateOps;
    use crate::storage::SystemTable;
    use crate::tier::DEFAULT_TIER;
    use crate::failure_domain::FailureDomain;
    use crate::instance::state::State;
    use crate::instance::state::StateVariant::*;
    use crate::replicaset::Replicaset;
    use crate::replicaset::ReplicasetName;
    use crate::rpc::join::build_instance;
    use crate::storage::Catalog;
    use crate::rpc;
    use crate::rpc::update_instance::update_instance;
    use crate::tier::Tier;
    use crate::traft::op::Dml;
    use tarantool::tuple::TupleBuffer;

    use super::*;

    macro_rules! faildoms {
        ($(,)?) => { FailureDomain::default() };
        ($($k:tt : $v:tt),+ $(,)?) => {
            FailureDomain::from([$((stringify!($k), stringify!($v))),+])
        }
    }

    fn add_tier(storage: &Catalog, name: &str, replication_factor: u8, can_vote: bool) -> tarantool::Result<()> {
        let tier = Tier {
            name: name.into(),
            replication_factor,
            can_vote,
            ..Default::default()
        };
        storage.tiers.put(&tier)
    }

    fn dummy_instance(raft_id: RaftId, name: &str, replicaset_name: &str, state: &State) -> Instance {
        Instance {
            raft_id,
            name: name.into(),
            uuid: format_smolstr!("{name}-uuid"),
            replicaset_name: replicaset_name.into(),
            replicaset_uuid: format_smolstr!("{replicaset_name}-uuid"),
            current_state: *state,
            target_state: *state,
            failure_domain: FailureDomain::default(),
            tier: DEFAULT_TIER.into(),
            picodata_version: PICODATA_VERSION.into(),
            sync_incarnation: state.incarnation,
            target_state_reason: "".into(),
            target_state_change_time: None,
        }
    }

    fn add_instance(storage: &Catalog, instance: &Instance) -> tarantool::Result<()> {
        storage.instances.put(instance)?;
        // Ignore error in case replicaset already exists. Good enough for tests
        _ = storage.replicasets.put(&Replicaset::with_one_instance(instance));

        Ok(())
    }

    fn replication_names(replicaset_name: &ReplicasetName, storage: &Catalog) -> HashSet<RaftId> {
        storage
            .instances
            .replicaset_instances(replicaset_name)
            .expect("storage should not fail")
            .map(|i| i.raft_id).collect()
    }

    fn extract_instance(result: (Instance, bool)) -> Instance {
        result.0
    }

    #[::tarantool::test]
    fn test_simple() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 1, true).unwrap();

        let i1 = extract_instance(build_instance(None, None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-1").unwrap());
        assert_eq!(i1.raft_id, 1);
        assert_eq!(i1.name, "default_1_1");
        assert_eq!(i1.replicaset_name, "default_1");
        assert_eq!(i1.current_state, State::new(Offline, 0));
        assert_eq!(i1.target_state, State::new(Offline, 0));
        assert_eq!(i1.failure_domain, FailureDomain::default());
        assert_eq!(i1.tier, DEFAULT_TIER);
        add_instance(&storage, &i1).unwrap();

        let i2 = extract_instance(build_instance(None, None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-2").unwrap());
        assert_eq!(i2.raft_id, 2);
        assert_eq!(i2.name, "default_2_1");
        assert_eq!(i2.replicaset_name, "default_2");
        add_instance(&storage, &i2).unwrap();

        let i3 = extract_instance(build_instance(None, Some(&ReplicasetName::from("R3")), &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-3").unwrap());
        assert_eq!(i3.raft_id, 3);
        assert_eq!(i3.name, "R3_1");
        assert_eq!(i3.replicaset_name, "R3");
        add_instance(&storage, &i3).unwrap();

        let i4 = extract_instance(build_instance(Some(&InstanceName::from("I4")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-4").unwrap());
        assert_eq!(i4.raft_id, 4);
        assert_eq!(i4.name, "I4");
        assert_eq!(i4.replicaset_name, "default_3");
        add_instance(&storage, &i4).unwrap();
    }

    #[::tarantool::test]
    fn test_override() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 2, true).unwrap();
        add_instance(&storage, &dummy_instance(1, "i1", "r1", &State::new(Online, 1))).unwrap();
        add_instance(&storage, &dummy_instance(2, "i2", "r2-original", &State::new(Expelled, 0))).unwrap();

        // join::Request with a given instance_name online.
        // - It must be an impostor, return an error.
        // - Even if it's a fair rebootstrap, it will be marked as
        //   unreachable soon (when we implement failover) the error
        //   will be gone.
        let e = build_instance(Some(&InstanceName::from("i1")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-5").unwrap_err();
        assert_eq!(e.to_string(), "`i1` is already joined");

        // join::Request with a given instance_name offline (or unreachable).
        // - Presumably it's a rebootstrap.
        //   1. Perform auto-expel, unless it threatens data safety (TODO).
        //   2. Assign new raft_id.
        //   3. Assign new replicaset_name, unless specified explicitly. A
        //      new replicaset_name might be the same as before, since
        //      auto-expel provided a vacant place there. Or it might be
        //      not, if replication_factor / failure_domain were edited.
        // - Even if it's an impostor, rely on auto-expel policy.
        //   Disruption isn't destructive if auto-expel allows (TODO).
        let instance = extract_instance(build_instance(Some(&InstanceName::from("i2")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-6").unwrap());
        assert_eq!(instance.raft_id, 3);
        assert_eq!(instance.name, "i2");
        // Attention: generated replicaset_name differs from the original
        // one, as well as raft_id. That's a desired behavior.
        assert_eq!(instance.replicaset_name, "r1");
        assert_eq!(replication_names(&ReplicasetName::from("r1"), &storage), HashSet::from([1]));

        // TODO
        //
        // join::Request with a given instance_name bootstrapping.
        // - Presumably it's a retry after tarantool bootstrap failure.
        //   1. Perform auto-expel (it's always ok until bootstrap
        //      finishes).
        //   2. Assign a new raft_id.
        //   3. Assign new replicaset_name. Same as above.
        // - If it's actually an impostor (instance_name collision),
        //   original instance (that didn't report it has finished
        //   bootstrapping yet) will be disrupted.
    }

    #[::tarantool::test]
    fn test_instance_name_collision() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 2, true).unwrap();
        add_instance(&storage, &dummy_instance(1, "i1", "r1", &State::new(Online, 1))).unwrap();
        add_instance(&storage, &dummy_instance(2, "i3", "r3", &State::new(Online, 1))).unwrap();
        // Attention: i3 has raft_id=2

        let instance = extract_instance(build_instance(None, Some(&ReplicasetName::from("r2")), &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-7").unwrap());
        assert_eq!(instance.raft_id, 3);
        assert_eq!(instance.name, "r2_1");
        assert_eq!(instance.replicaset_name, "r2");
    }

    #[::tarantool::test]
    fn test_uuid_randomness() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 1, true).unwrap();
        let i1a = extract_instance(build_instance(None, None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-8").unwrap());
        let i1b = extract_instance(build_instance(None, None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-9").unwrap());
        assert_eq!(i1a.name, "default_1_1");
        assert_eq!(i1b.name, "default_1_1");

        assert_eq!(i1a.replicaset_name, "default_1");
        assert_eq!(i1b.replicaset_name, "default_1");
        // Attention: not equal
        assert_ne!(i1a.uuid, i1b.uuid);
    }

    #[::tarantool::test]
    fn test_replication_factor() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 2, true).unwrap();
        add_instance(&storage, &dummy_instance(9, "i9", "default_1", &State::new(Online, 1))).unwrap();
        add_instance(&storage, &dummy_instance(10, "i10", "default_1", &State::new(Online, 1))).unwrap();

        let i1 = extract_instance(build_instance(Some(&InstanceName::from("i1")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-10").unwrap());
        assert_eq!(i1.raft_id, 11);
        assert_eq!(i1.name, "i1");
        assert_eq!(i1.replicaset_name, "default_2");
        add_instance(&storage, &i1).unwrap();

        assert_eq!(replication_names(&ReplicasetName::from("default_2"), &storage), HashSet::from([11]));

        let i2 = extract_instance(build_instance(Some(&InstanceName::from("i2")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-11").unwrap());
        assert_eq!(i2.raft_id, 12);
        assert_eq!(i2.name, "i2");
        assert_eq!(i2.replicaset_name, "default_2");
        assert_eq!(i2.replicaset_uuid, i1.replicaset_uuid);
        add_instance(&storage, &i2).unwrap();
        assert_eq!(replication_names(&ReplicasetName::from("default_2"), &storage), HashSet::from([11, 12]));

        let i3 = extract_instance(build_instance(Some(&InstanceName::from("i3")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-12").unwrap());
        assert_eq!(i3.raft_id, 13);
        assert_eq!(i3.name, "i3");
        assert_eq!(i3.replicaset_name, "default_3");
        add_instance(&storage, &i3).unwrap();
        assert_eq!(replication_names(&ReplicasetName::from("default_3"), &storage), HashSet::from([13]));

        let i4 = extract_instance(build_instance(Some(&InstanceName::from("i4")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-13").unwrap());
        assert_eq!(i4.raft_id, 14);
        assert_eq!(i4.name, "i4");
        assert_eq!(i4.replicaset_name, "default_3");
        assert_eq!(i4.replicaset_uuid, i3.replicaset_uuid);
        add_instance(&storage, &i4).unwrap();
        assert_eq!(replication_names(&ReplicasetName::from("default_3"), &storage), HashSet::from([13, 14]));
    }

    #[track_caller]
    fn update_instance_dml(instance_name: impl Into<InstanceName>, ops: UpdateOps) -> Dml {
        Dml::update(
            crate::storage::Instances::TABLE_ID,
            &[&instance_name.into()],
            ops,
            crate::schema::ADMIN_ID,
        ).unwrap()
    }

    fn check_update_instance(dml: &Dml, expected_name: impl Into<InstanceName>) -> &[TupleBuffer] {
        let Dml::Update { table, key, ops, initiator } = dml else {
            panic!("expected Dml::Update, got {dml:?}");
        };

        assert_eq!(*table, crate::storage::Instances::TABLE_ID);
        assert_eq!(*initiator, crate::schema::ADMIN_ID);

        let [name]: [InstanceName; 1] = rmp_serde::from_slice(key.as_ref()).unwrap();
        assert_eq!(expected_name.into(), name);

        ops
    }

    fn check_field_assignment<'a, T>(op: &'a TupleBuffer, expected_field: &str) -> T
    where
        T: serde::Deserialize<'a>,
    {
        let v = match rmp_serde::from_slice(op.as_ref()) {
            Ok(v) => v,
            Err(e) => {
                let type_name = std::any::type_name::<T>();
                let actual_msgpack = picodata_plugin::util::DisplayAsHexBytesLimitted(op.as_ref());
                panic!(
                    "unexpected field assignment
expected ('=', '{expected_field}', {type_name})
got msgpack '{actual_msgpack}'
(aka {op:?})
rmp_serde error: {e}"
                );
            }
        };
        let (op, field, value): (&str, &str, T) = v;
        assert_eq!(op, "=");
        assert_eq!(field, expected_field);
        value
    }

    #[::tarantool::test]
    fn test_update_state() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 1, true).unwrap();
        let instance = dummy_instance(1, "i1", "r1", &State::new(Online, 1));
        add_instance(&storage, &instance).unwrap();
        let global_cluster_version = PICODATA_VERSION.to_string();
        let system_catalog_version = PICODATA_VERSION;

        //
        // Current state incarnation is allowed to go down,
        // governor has the authority over it
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "", "")
            .with_current_state(State::new(Offline, 0));
        let (dml, do_bump) = update_instance(&instance, &req, None, &global_cluster_version, system_catalog_version).unwrap().unwrap();

        let mut ops = UpdateOps::new();
        ops.assign("current_state", State::new(Offline, 0)).unwrap();
        assert_eq!(dml, update_instance_dml("i1", ops));
        assert_eq!(do_bump, false);

        storage.do_dml(&dml).unwrap();
        let instance = storage.instances.get(&InstanceName::from("i1")).unwrap();

        //
        // idempotency
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "", "")
            .with_current_state(State::new(Offline, 0));
        let dml = update_instance(&instance, &req, None, &global_cluster_version, system_catalog_version).unwrap();
        assert_eq!(dml, None);

        //
        // Offline takes incarnation from current state
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "", "")
            .with_target_state(Offline);
        let (dml, do_bump) = update_instance(&instance, &req, None, &global_cluster_version, system_catalog_version).unwrap().unwrap();

        assert_eq!(do_bump, true, "target state change requires replicaset config version bump");
        let ops = check_update_instance(&dml, "i1");
        assert_eq!(ops.len(), 2);
        let target_state: State = check_field_assignment(&ops[0], "target_state");
        assert_eq!(target_state, State::new(Offline, 0));
        let _time: Datetime = check_field_assignment(&ops[1], "target_state_change_time");
        // No need to check the actual value of `_time` because it is unstable

        storage.do_dml(&dml).unwrap();
        let instance = storage.instances.get(&InstanceName::from("i1")).unwrap();

        //
        // Online increases incarnation
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "", "")
            .with_target_state(Online);
        let (dml, do_bump) = update_instance(&instance, &req, None, &global_cluster_version, system_catalog_version).unwrap().unwrap();

        assert_eq!(do_bump, true, "target state change requires replicaset config version bump");
        let ops = check_update_instance(&dml, "i1");
        assert_eq!(ops.len(), 2);
        let target_state: State = check_field_assignment(&ops[0], "target_state");
        assert_eq!(target_state, State::new(Online, 1));
        // No need to check the actual value of `_time` because it is unstable
        let _time: Datetime = check_field_assignment(&ops[1], "target_state_change_time");

        storage.do_dml(&dml).unwrap();
        let instance = storage.instances.get(&InstanceName::from("i1")).unwrap();

        //
        // No idempotency, incarnation goes up
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "", "")
            .with_target_state(Online);
        let (dml, do_bump) = update_instance(&instance, &req, None, &global_cluster_version, system_catalog_version).unwrap().unwrap();

        assert_eq!(do_bump, true, "target state change requires replicaset config version bump");
        let ops = check_update_instance(&dml, "i1");
        assert_eq!(ops.len(), 2);
        let target_state: State = check_field_assignment(&ops[0], "target_state");
        assert_eq!(target_state, State::new(Online, 2));
        // No need to check the actual value of `_time` because it is unstable
        let _time: Datetime = check_field_assignment(&ops[1], "target_state_change_time");

        storage.do_dml(&dml).unwrap();
        let instance = storage.instances.get(&InstanceName::from("i1")).unwrap();

        //
        // State::Expelled takes incarnation from current state
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "", "")
            .with_target_state(Expelled);
        let (dml, do_bump) = update_instance(&instance, &req, None, &global_cluster_version, system_catalog_version).unwrap().unwrap();

        assert_eq!(do_bump, true, "target state change requires replicaset config version bump");
        let ops = check_update_instance(&dml, "i1");
        assert_eq!(ops.len(), 2);
        let target_state: State = check_field_assignment(&ops[0], "target_state");
        assert_eq!(target_state, State::new(Expelled, 0));
        // No need to check the actual value of `_time` because it is unstable
        let _time: Datetime = check_field_assignment(&ops[1], "target_state_change_time");


        storage.do_dml(&dml).unwrap();
        let instance = storage.instances.get(&InstanceName::from("i1")).unwrap();

        //
        // Instance gets expelled
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "", "")
            .with_current_state(State::new(Expelled, 69));
        let (dml, do_bump) = update_instance(&instance, &req, None, &global_cluster_version, system_catalog_version).unwrap().unwrap();

        let mut ops = UpdateOps::new();
        ops.assign("current_state", State::new(Expelled, 69)).unwrap();
        assert_eq!(dml, update_instance_dml("i1", ops));
        assert_eq!(do_bump, false);

        storage.do_dml(&dml).unwrap();
        let instance = storage.instances.get(&InstanceName::from("i1")).unwrap();

        //
        // Updating expelled instances isn't allowed
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "", "")
            .with_target_state(Online);
        let e = update_instance(&instance, &req, None, &global_cluster_version, system_catalog_version).unwrap_err();
        assert_eq!(e.to_string(), "cannot update expelled instance \"i1\"");
    }

    #[::tarantool::test]
    fn test_update_picodata_version() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 1, true).unwrap();
        let global_cluster_version = PICODATA_VERSION;
        let system_catalog_version = PICODATA_VERSION;
        let new_picodata_version = Version::try_from(global_cluster_version).expect("correct picodata version").next_by_minor().to_smolstr();

        let instance_name = "default_r1_1";
        let instance = dummy_instance(1, instance_name, "r1", &State::new(Online, 1));
        add_instance(&storage, &instance).unwrap();

        let req = rpc::update_instance::Request::new(instance.name.clone(), "", "")
            .with_picodata_version(new_picodata_version.clone());
        let (dml, do_bump) = update_instance(&instance, &req, None, global_cluster_version, system_catalog_version)
            .unwrap()
            .expect("expected update picodata version");

        let mut ops = UpdateOps::new();
        ops.assign("picodata_version", new_picodata_version.clone()).unwrap();
        assert_eq!(dml, update_instance_dml(instance_name, ops));
        assert_eq!(do_bump, false);

        storage.do_dml(&dml).unwrap();
        let instance = storage.instances.get(&InstanceName::from(instance_name)).unwrap();
        assert_eq!(instance.picodata_version, new_picodata_version);

        // Create an instance that is already expelled.
        let instance_name = "default_r1_expelled";
        let expelled_instance = Instance {
            name: instance_name.into(),
            uuid: format_smolstr!("{instance_name}-uuid"),
            raft_id: 2,
            replicaset_name: "r1".into(),
            replicaset_uuid: "r1-uuid".into(),
            current_state: State::new(Expelled, 69),
            target_state: State::new(Expelled, 0),
            failure_domain: FailureDomain::default(),
            tier: DEFAULT_TIER.into(),
            picodata_version: PICODATA_VERSION.into(),
            sync_incarnation: 0,
            target_state_reason: "".into(),
            target_state_change_time: None,
        };
        add_instance(&storage, &expelled_instance).unwrap();

        // Now try to update the picodata_version on the expelled instance.
        let req = rpc::update_instance::Request::new(expelled_instance.name.clone(), "", "")
            .with_picodata_version(new_picodata_version.clone());
        let err = update_instance(&expelled_instance, &req, None, global_cluster_version, system_catalog_version)
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            format!("cannot update expelled instance \"{}\"", expelled_instance.name)
        );
    }

    #[::tarantool::test]
    fn failure_domain() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 3, true).unwrap();

        let instance =
            extract_instance(build_instance(None, None, &faildoms! {planet: Earth}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-14")
                .unwrap());
        assert_eq!(instance.replicaset_name, "default_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            extract_instance(build_instance(None, None, &faildoms! {planet: Earth}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-15")
                .unwrap());
        assert_eq!(instance.replicaset_name, "default_2");
        add_instance(&storage, &instance).unwrap();

        let instance =
            extract_instance(build_instance(None, None, &faildoms! {planet: Mars}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-16")
                .unwrap());
        assert_eq!(instance.replicaset_name, "default_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            extract_instance(build_instance(None, None, &faildoms! {planet: Earth, os: BSD}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-17")
                .unwrap());
        assert_eq!(instance.replicaset_name, "default_3");
        add_instance(&storage, &instance).unwrap();

        let instance =
            extract_instance(build_instance(None, None, &faildoms! {planet: Mars, os: BSD}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-18")
                .unwrap());
        assert_eq!(instance.replicaset_name, "default_2");
        add_instance(&storage, &instance).unwrap();

        let e = build_instance(None, None, &faildoms! {os: Arch}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-19").unwrap_err();
        assert_eq!(e.to_string(), "missing failure domain names: PLANET");

        let instance =
            extract_instance(build_instance(None, None, &faildoms! {planet: Venus, os: Arch}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-20")
                .unwrap());
        assert_eq!(instance.replicaset_name, "default_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            extract_instance(build_instance(None, None, &faildoms! {planet: Venus, os: Mac}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-21")
                .unwrap());
        assert_eq!(instance.replicaset_name, "default_2");
        add_instance(&storage, &instance).unwrap();

        let instance =
            extract_instance(build_instance(None, None, &faildoms! {planet: Mars, os: Mac}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-22")
                .unwrap());
        assert_eq!(instance.replicaset_name, "default_3");
        add_instance(&storage, &instance).unwrap();

        let e = build_instance(None, None, &faildoms! {}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-23").unwrap_err();
        assert_eq!(e.to_string(), "missing failure domain names: OS, PLANET");
    }

    #[::tarantool::test]
    fn reconfigure_failure_domain() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 3, true).unwrap();
        let global_cluster_version = PICODATA_VERSION.to_string();
        let system_catalog_version = PICODATA_VERSION;

        //
        // first instance
        //
        let instance1 = extract_instance(build_instance(Some(&InstanceName::from("i1")), None, &faildoms! {planet: Earth}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-24").unwrap());
        add_instance(&storage, &instance1).unwrap();
        let existing_fds = storage.instances.failure_domain_names().unwrap();
        assert_eq!(instance1.failure_domain, faildoms! {planet: Earth});
        assert_eq!(instance1.replicaset_name, "default_1");

        //
        // reconfigure single instance, fail
        //
        let req = rpc::update_instance::Request::new(instance1.name.clone(), "", "")
            .with_failure_domain(faildoms! {owner: Ivan});
        let e = update_instance(&instance1, &req, Some(&existing_fds), &global_cluster_version, system_catalog_version).unwrap_err();
        assert_eq!(e.to_string(), "missing failure domain names: PLANET");

        //
        // reconfigure single instance, success
        //
        let fd = faildoms! {planet: Mars, owner: Ivan};
        let req = rpc::update_instance::Request::new(instance1.name.clone(), "", "")
            .with_failure_domain(fd.clone());
        let (dml, do_bump) = update_instance(&instance1, &req, Some(&existing_fds), &global_cluster_version, system_catalog_version).unwrap().unwrap();

        let mut ops = UpdateOps::new();
        ops.assign("failure_domain", fd).unwrap();
        assert_eq!(dml, update_instance_dml("i1", ops));
        assert_eq!(do_bump, false);

        storage.do_dml(&dml).unwrap();

        //
        // second instance won't be joined without the newly added required
        // failure domain subdivision of "OWNER"
        //
        let e = build_instance(Some(&InstanceName::from("i2")), None, &faildoms! {planet: Mars}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-25").unwrap_err();
        assert_eq!(e.to_string(), "missing failure domain names: OWNER");

        //
        // second instance
        //
        let fd = faildoms! {planet: Mars, owner: Mike};
        #[rustfmt::skip]
        let instance2 = extract_instance(build_instance(Some(&InstanceName::from("i2")), None, &fd, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-26").unwrap());
        add_instance(&storage, &instance2).unwrap();
        let existing_fds = storage.instances.failure_domain_names().unwrap();
        assert_eq!(instance2.failure_domain, fd);
        // doesn't fit into r1
        assert_eq!(instance2.replicaset_name, "default_2");

        //
        // reconfigure second instance, success
        //
        let fd = faildoms! {planet: Earth, owner: Mike};
        let req = rpc::update_instance::Request::new(instance2.name.clone(), "", "")
            .with_failure_domain(fd.clone());
        let (dml, do_bump) = update_instance(&instance2, &req, Some(&existing_fds), &global_cluster_version, system_catalog_version).unwrap().unwrap();

        let mut ops = UpdateOps::new();
        ops.assign("failure_domain", fd).unwrap();
        assert_eq!(dml, update_instance_dml("i2", ops));
        assert_eq!(do_bump, false);

        storage.do_dml(&dml).unwrap();

        //
        // add instance with new subdivision
        //
        #[rustfmt::skip]
        let instance3_v1 = extract_instance(build_instance(Some(&InstanceName::from("i3")), None, &faildoms! {planet: B, owner: V, dimension: C137}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-27")
            .unwrap());
        add_instance(&storage, &instance3_v1).unwrap();
        assert_eq!(
            instance3_v1.failure_domain,
            faildoms! {planet: B, owner: V, dimension: C137}
        );
        assert_eq!(instance3_v1.replicaset_name, "default_1");

        //
        // even though the only instance with failure domain subdivision of
        // `DIMENSION` is inactive, we can't add an instance without that
        // subdivision
        //
        let e = build_instance(Some(&InstanceName::from("i4")), None, &faildoms! {planet: Theia, owner: Me}, &storage, DEFAULT_TIER, PICODATA_VERSION, "test-uuid-28").unwrap_err();
        assert_eq!(e.to_string(), "missing failure domain names: DIMENSION");
    }

    #[::tarantool::test]
    fn replicaset_name_with_several_tiers() {
        let first_tier = "default";
        let second_tier = "compute";
        let third_tier = "trash";

        let storage = Catalog::for_tests();
        add_tier(&storage, first_tier, 3, true).unwrap();
        add_tier(&storage, second_tier, 2, true).unwrap();
        add_tier(&storage, third_tier, 2, true).unwrap();

        let instance =
            extract_instance(build_instance(None, None, &faildoms! {planet: Earth}, &storage, first_tier, PICODATA_VERSION, "test-uuid-29")
                .unwrap());
        assert_eq!(instance.replicaset_name, "default_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            extract_instance(build_instance(None, None, &faildoms! {planet: Mars}, &storage, second_tier, PICODATA_VERSION, "test-uuid-30")
                .unwrap());
        assert_eq!(instance.replicaset_name, "compute_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            extract_instance(build_instance(None, None, &faildoms! {planet: Mars}, &storage, first_tier, PICODATA_VERSION, "test-uuid-31")
                .unwrap());
        assert_eq!(instance.replicaset_name, "default_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            extract_instance(build_instance(None, None, &faildoms! {planet: Pluto}, &storage, third_tier, PICODATA_VERSION, "test-uuid-32")
                .unwrap());
        assert_eq!(instance.replicaset_name, "trash_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            extract_instance(build_instance(None, None, &faildoms! {planet: Venus}, &storage, third_tier, PICODATA_VERSION, "test-uuid-33")
                .unwrap());
        assert_eq!(instance.replicaset_name, "trash_1");
        add_instance(&storage, &instance).unwrap();

        let e = build_instance(None, None, &faildoms! {planet: 5}, &storage, "noexistent_tier", PICODATA_VERSION, "test-uuid-34").unwrap_err();
        assert_eq!(e.to_string(), r#"tier "noexistent_tier" doesn't exist"#);

        // gl589
        let e = build_instance(None, Some(&ReplicasetName::from("just to skip choose_replicaset function call")), &faildoms! {planet: 5}, &storage, "noexistent_tier", PICODATA_VERSION, "test-uuid-35") .unwrap_err();
        assert_eq!(e.to_string(), r#"tier "noexistent_tier" doesn't exist"#);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tarantool::tuple::ToTupleBuffer;

    #[test]
    #[rustfmt::skip]
    fn matches_format() {
        let i = Instance::for_tests();
        let tuple_data = i.to_tuple_buffer().unwrap();
        let format = Instance::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "Instance::format");

        assert_eq!(format[Instance::FIELD_INSTANCE_NAME as usize].name, "name");
        assert_eq!(format[Instance::FIELD_RAFT_ID as usize].name, "raft_id");
        assert_eq!(format[Instance::FIELD_FAILURE_DOMAIN as usize].name, "failure_domain");
    }
}

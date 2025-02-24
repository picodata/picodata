use super::failure_domain::FailureDomain;
use super::replicaset::ReplicasetName;
use crate::has_states;
use crate::info::PICODATA_VERSION;
use crate::traft::RaftId;
use crate::util::Transition;
use ::serde::{Deserialize, Serialize};
use ::tarantool::tlua;
use ::tarantool::tuple::Encode;

pub mod state;
pub use state::State;
pub use state::StateVariant;

crate::define_string_newtype! {
    /// Unique id of a cluster instance.
    ///
    /// This is a new-type style wrapper around String,
    /// to distinguish it from other strings.
    pub struct InstanceName(pub String);
}

////////////////////////////////////////////////////////////////////////////////
/// Serializable struct representing a member of the raft group.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Instance {
    /// Instances are identified by name.
    pub name: InstanceName,
    pub uuid: String,

    /// Used for identifying raft nodes.
    /// Must be unique in the raft group.
    pub raft_id: RaftId,

    /// Name of a replicaset the instance belongs to.
    pub replicaset_name: ReplicasetName,
    pub replicaset_uuid: String,

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
    pub tier: String,

    /// Version of picodata executable which running this instance.
    /// It should match the version returned by `.proc_version_info` on this instance.
    pub picodata_version: String,
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
    use crate::version::Version;

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
            uuid: format!("{name}-uuid"),
            replicaset_name: replicaset_name.into(),
            replicaset_uuid: format!("{replicaset_name}-uuid"),
            current_state: *state,
            target_state: *state,
            failure_domain: FailureDomain::default(),
            tier: DEFAULT_TIER.into(),
            picodata_version: PICODATA_VERSION.into(),
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

    #[::tarantool::test]
    fn test_simple() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 1, true).unwrap();

        let i1 = build_instance(None, None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
        assert_eq!(i1.raft_id, 1);
        assert_eq!(i1.name, "default_1_1");
        assert_eq!(i1.replicaset_name, "default_1");
        assert_eq!(i1.current_state, State::new(Offline, 0));
        assert_eq!(i1.target_state, State::new(Offline, 0));
        assert_eq!(i1.failure_domain, FailureDomain::default());
        assert_eq!(i1.tier, DEFAULT_TIER);
        add_instance(&storage, &i1).unwrap();

        let i2 = build_instance(None, None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
        assert_eq!(i2.raft_id, 2);
        assert_eq!(i2.name, "default_2_1");
        assert_eq!(i2.replicaset_name, "default_2");
        add_instance(&storage, &i2).unwrap();

        let i3 = build_instance(None, Some(&ReplicasetName::from("R3")), &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
        assert_eq!(i3.raft_id, 3);
        assert_eq!(i3.name, "R3_1");
        assert_eq!(i3.replicaset_name, "R3");
        add_instance(&storage, &i3).unwrap();

        let i4 = build_instance(Some(&InstanceName::from("I4")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
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
        let e = build_instance(Some(&InstanceName::from("i1")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap_err();
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
        let instance = build_instance(Some(&InstanceName::from("i2")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
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

        let instance = build_instance(None, Some(&ReplicasetName::from("r2")), &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
        assert_eq!(instance.raft_id, 3);
        assert_eq!(instance.name, "r2_1");
        assert_eq!(instance.replicaset_name, "r2");
    }

    #[::tarantool::test]
    fn test_uuid_randomness() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 1, true).unwrap();
        let i1a = build_instance(None, None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
        let i1b = build_instance(None, None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
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

        let i1 = build_instance(Some(&InstanceName::from("i1")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
        assert_eq!(i1.raft_id, 11);
        assert_eq!(i1.name, "i1");
        assert_eq!(i1.replicaset_name, "default_2");
        add_instance(&storage, &i1).unwrap();

        assert_eq!(replication_names(&ReplicasetName::from("default_2"), &storage), HashSet::from([11]));

        let i2 = build_instance(Some(&InstanceName::from("i2")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
        assert_eq!(i2.raft_id, 12);
        assert_eq!(i2.name, "i2");
        assert_eq!(i2.replicaset_name, "default_2");
        assert_eq!(i2.replicaset_uuid, i1.replicaset_uuid);
        add_instance(&storage, &i2).unwrap();
        assert_eq!(replication_names(&ReplicasetName::from("default_2"), &storage), HashSet::from([11, 12]));

        let i3 = build_instance(Some(&InstanceName::from("i3")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
        assert_eq!(i3.raft_id, 13);
        assert_eq!(i3.name, "i3");
        assert_eq!(i3.replicaset_name, "default_3");
        add_instance(&storage, &i3).unwrap();
        assert_eq!(replication_names(&ReplicasetName::from("default_3"), &storage), HashSet::from([13]));

        let i4 = build_instance(Some(&InstanceName::from("i4")), None, &FailureDomain::default(), &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
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

    #[::tarantool::test]
    fn test_update_state() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 1, true).unwrap();
        let instance = dummy_instance(1, "i1", "r1", &State::new(Online, 1));
        add_instance(&storage, &instance).unwrap();
        let existing_fds = HashSet::new();
        let global_cluster_version = PICODATA_VERSION.to_string();

        //
        // Current state incarnation is allowed to go down,
        // governor has the authority over it
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "".into())
            .with_current_state(State::new(Offline, 0));
        let (dml, do_bump) = update_instance(&instance, &req, &existing_fds, &global_cluster_version).unwrap().unwrap();

        let mut ops = UpdateOps::new();
        ops.assign("current_state", State::new(Offline, 0)).unwrap();
        assert_eq!(dml, update_instance_dml("i1", ops));
        assert_eq!(do_bump, false);

        storage.do_dml(&dml).unwrap();
        let instance = storage.instances.get(&InstanceName::from("i1")).unwrap();

        //
        // idempotency
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "".into())
            .with_current_state(State::new(Offline, 0));
        let dml = update_instance(&instance, &req, &existing_fds, &global_cluster_version).unwrap();
        assert_eq!(dml, None);

        //
        // Offline takes incarnation from current state
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "".into())
            .with_target_state(Offline);
        let (dml, do_bump) = update_instance(&instance, &req, &existing_fds, &global_cluster_version).unwrap().unwrap();

        let mut ops = UpdateOps::new();
        ops.assign("target_state", State::new(Offline, 0)).unwrap();
        assert_eq!(dml, update_instance_dml("i1", ops));
        assert_eq!(do_bump, true, "target state change requires replicaset config version bump");

        storage.do_dml(&dml).unwrap();
        let instance = storage.instances.get(&InstanceName::from("i1")).unwrap();

        //
        // Online increases incarnation
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "".into())
            .with_target_state(Online);
        let (dml, do_bump) = update_instance(&instance, &req, &existing_fds, &global_cluster_version).unwrap().unwrap();

        let mut ops = UpdateOps::new();
        ops.assign("target_state", State::new(Online, 1)).unwrap();
        assert_eq!(dml, update_instance_dml("i1", ops));
        assert_eq!(do_bump, true, "target state change requires replicaset config version bump");

        storage.do_dml(&dml).unwrap();
        let instance = storage.instances.get(&InstanceName::from("i1")).unwrap();

        //
        // No idempotency, incarnation goes up
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "".into())
            .with_target_state(Online);
        let (dml, do_bump) = update_instance(&instance, &req, &existing_fds, &global_cluster_version).unwrap().unwrap();

        let mut ops = UpdateOps::new();
        ops.assign("target_state", State::new(Online, 2)).unwrap();
        assert_eq!(dml, update_instance_dml("i1", ops));
        assert_eq!(do_bump, true, "target state change requires replicaset config version bump");

        storage.do_dml(&dml).unwrap();
        let instance = storage.instances.get(&InstanceName::from("i1")).unwrap();

        //
        // State::Expelled takes incarnation from current state
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "".into())
            .with_target_state(Expelled);
        let (dml, do_bump) = update_instance(&instance, &req, &existing_fds, &global_cluster_version).unwrap().unwrap();

        let mut ops = UpdateOps::new();
        ops.assign("target_state", State::new(Expelled, 0)).unwrap();
        assert_eq!(dml, update_instance_dml("i1", ops));
        assert_eq!(do_bump, true, "target state change requires replicaset config version bump");

        storage.do_dml(&dml).unwrap();
        let instance = storage.instances.get(&InstanceName::from("i1")).unwrap();

        //
        // Instance gets expelled
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "".into())
            .with_current_state(State::new(Expelled, 69));
        let (dml, do_bump) = update_instance(&instance, &req, &existing_fds, &global_cluster_version).unwrap().unwrap();

        let mut ops = UpdateOps::new();
        ops.assign("current_state", State::new(Expelled, 69)).unwrap();
        assert_eq!(dml, update_instance_dml("i1", ops));
        assert_eq!(do_bump, false);

        storage.do_dml(&dml).unwrap();
        let instance = storage.instances.get(&InstanceName::from("i1")).unwrap();

        //
        // Updating expelled instances isn't allowed
        //
        let req = rpc::update_instance::Request::new(instance.name.clone(), "".into())
            .with_target_state(Online);
        let e = update_instance(&instance, &req, &existing_fds, &global_cluster_version).unwrap_err();
        assert_eq!(e.to_string(), "cannot update expelled instance \"i1\"");
    }

    #[::tarantool::test]
    fn test_update_picodata_version() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 1, true).unwrap();
        let global_cluster_version = PICODATA_VERSION;
        let new_picodata_version = Version::try_from(global_cluster_version).expect("correct picodata version").next_by_minor().to_string();

        let instance_name = "default_r1_1";
        let instance = dummy_instance(1, instance_name, "r1", &State::new(Online, 1));
        add_instance(&storage, &instance).unwrap();
        let existing_fds = HashSet::new();

        let req = rpc::update_instance::Request::new(instance.name.clone(), "".into())
            .with_picodata_version(new_picodata_version.clone());
        let (dml, do_bump) = update_instance(&instance, &req, &existing_fds, global_cluster_version)
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
            uuid: format!("{instance_name}-uuid"),
            raft_id: 2,
            replicaset_name: "r1".into(),
            replicaset_uuid: "r1-uuid".into(),
            current_state: State::new(Expelled, 69),
            target_state: State::new(Expelled, 0),
            failure_domain: FailureDomain::default(),
            tier: DEFAULT_TIER.into(),
            picodata_version: PICODATA_VERSION.to_string(),
        };
        add_instance(&storage, &expelled_instance).unwrap();

        // Now try to update the picodata_version on the expelled instance.
        let req = rpc::update_instance::Request::new(expelled_instance.name.clone(), "".into())
            .with_picodata_version(new_picodata_version.clone());
        let err = update_instance(&expelled_instance, &req, &existing_fds, global_cluster_version)
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
            build_instance(None, None, &faildoms! {planet: Earth}, &storage, DEFAULT_TIER, PICODATA_VERSION)
                .unwrap();
        assert_eq!(instance.replicaset_name, "default_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Earth}, &storage, DEFAULT_TIER, PICODATA_VERSION)
                .unwrap();
        assert_eq!(instance.replicaset_name, "default_2");
        add_instance(&storage, &instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Mars}, &storage, DEFAULT_TIER, PICODATA_VERSION)
                .unwrap();
        assert_eq!(instance.replicaset_name, "default_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Earth, os: BSD}, &storage, DEFAULT_TIER, PICODATA_VERSION)
                .unwrap();
        assert_eq!(instance.replicaset_name, "default_3");
        add_instance(&storage, &instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Mars, os: BSD}, &storage, DEFAULT_TIER, PICODATA_VERSION)
                .unwrap();
        assert_eq!(instance.replicaset_name, "default_2");
        add_instance(&storage, &instance).unwrap();

        let e = build_instance(None, None, &faildoms! {os: Arch}, &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap_err();
        assert_eq!(e.to_string(), "missing failure domain names: PLANET");

        let instance =
            build_instance(None, None, &faildoms! {planet: Venus, os: Arch}, &storage, DEFAULT_TIER, PICODATA_VERSION)
                .unwrap();
        assert_eq!(instance.replicaset_name, "default_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Venus, os: Mac}, &storage, DEFAULT_TIER, PICODATA_VERSION)
                .unwrap();
        assert_eq!(instance.replicaset_name, "default_2");
        add_instance(&storage, &instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Mars, os: Mac}, &storage, DEFAULT_TIER, PICODATA_VERSION)
                .unwrap();
        assert_eq!(instance.replicaset_name, "default_3");
        add_instance(&storage, &instance).unwrap();

        let e = build_instance(None, None, &faildoms! {}, &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap_err();
        assert_eq!(e.to_string(), "missing failure domain names: OS, PLANET");
    }

    #[::tarantool::test]
    fn reconfigure_failure_domain() {
        let storage = Catalog::for_tests();
        add_tier(&storage, DEFAULT_TIER, 3, true).unwrap();
        let global_cluster_version = PICODATA_VERSION.to_string();

        //
        // first instance
        //
        let instance1 = build_instance(Some(&InstanceName::from("i1")), None, &faildoms! {planet: Earth}, &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
        add_instance(&storage, &instance1).unwrap();
        let existing_fds = storage.instances.failure_domain_names().unwrap();
        assert_eq!(instance1.failure_domain, faildoms! {planet: Earth});
        assert_eq!(instance1.replicaset_name, "default_1");

        //
        // reconfigure single instance, fail
        //
        let req = rpc::update_instance::Request::new(instance1.name.clone(), "".into())
            .with_failure_domain(faildoms! {owner: Ivan});
        let e = update_instance(&instance1, &req, &existing_fds, &global_cluster_version).unwrap_err();
        assert_eq!(e.to_string(), "missing failure domain names: PLANET");

        //
        // reconfigure single instance, success
        //
        let fd = faildoms! {planet: Mars, owner: Ivan};
        let req = rpc::update_instance::Request::new(instance1.name.clone(), "".into())
            .with_failure_domain(fd.clone());
        let (dml, do_bump) = update_instance(&instance1, &req, &existing_fds, &global_cluster_version).unwrap().unwrap();

        let mut ops = UpdateOps::new();
        ops.assign("failure_domain", fd).unwrap();
        assert_eq!(dml, update_instance_dml("i1", ops));
        assert_eq!(do_bump, false);

        storage.do_dml(&dml).unwrap();

        //
        // second instance won't be joined without the newly added required
        // failure domain subdivision of "OWNER"
        //
        let e = build_instance(Some(&InstanceName::from("i2")), None, &faildoms! {planet: Mars}, &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap_err();
        assert_eq!(e.to_string(), "missing failure domain names: OWNER");

        //
        // second instance
        //
        let fd = faildoms! {planet: Mars, owner: Mike};
        #[rustfmt::skip]
        let instance2 = build_instance(Some(&InstanceName::from("i2")), None, &fd, &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap();
        add_instance(&storage, &instance2).unwrap();
        let existing_fds = storage.instances.failure_domain_names().unwrap();
        assert_eq!(instance2.failure_domain, fd);
        // doesn't fit into r1
        assert_eq!(instance2.replicaset_name, "default_2");

        //
        // reconfigure second instance, success
        //
        let fd = faildoms! {planet: Earth, owner: Mike};
        let req = rpc::update_instance::Request::new(instance2.name.clone(), "".into())
            .with_failure_domain(fd.clone());
        let (dml, do_bump) = update_instance(&instance2, &req, &existing_fds, &global_cluster_version).unwrap().unwrap();

        let mut ops = UpdateOps::new();
        ops.assign("failure_domain", fd).unwrap();
        assert_eq!(dml, update_instance_dml("i2", ops));
        assert_eq!(do_bump, false);

        storage.do_dml(&dml).unwrap();

        //
        // add instance with new subdivision
        //
        #[rustfmt::skip]
        let instance3_v1 = build_instance(Some(&InstanceName::from("i3")), None, &faildoms! {planet: B, owner: V, dimension: C137}, &storage, DEFAULT_TIER, PICODATA_VERSION)
            .unwrap();
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
        let e = build_instance(Some(&InstanceName::from("i4")), None, &faildoms! {planet: Theia, owner: Me}, &storage, DEFAULT_TIER, PICODATA_VERSION).unwrap_err();
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
            build_instance(None, None, &faildoms! {planet: Earth}, &storage, first_tier, PICODATA_VERSION)
                .unwrap();
        assert_eq!(instance.replicaset_name, "default_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Mars}, &storage, second_tier, PICODATA_VERSION)
                .unwrap();
        assert_eq!(instance.replicaset_name, "compute_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Mars}, &storage, first_tier, PICODATA_VERSION)
                .unwrap();
        assert_eq!(instance.replicaset_name, "default_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Pluto}, &storage, third_tier, PICODATA_VERSION)
                .unwrap();
        assert_eq!(instance.replicaset_name, "trash_1");
        add_instance(&storage, &instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Venus}, &storage, third_tier, PICODATA_VERSION)
                .unwrap();
        assert_eq!(instance.replicaset_name, "trash_1");
        add_instance(&storage, &instance).unwrap();

        let e = build_instance(None, None, &faildoms! {planet: 5}, &storage, "noexistent_tier", PICODATA_VERSION).unwrap_err();
        assert_eq!(e.to_string(), r#"tier "noexistent_tier" doesn't exist"#);

        // gl589
        let e = build_instance(None, Some(&ReplicasetName::from("just to skip choose_replicaset function call")), &faildoms! {planet: 5}, &storage, "noexistent_tier", PICODATA_VERSION) .unwrap_err();
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
        let i = Instance::default();
        let tuple_data = i.to_tuple_buffer().unwrap();
        let format = Instance::format();
        crate::util::check_tuple_matches_format(tuple_data.as_ref(), &format, "Instance::format");

        assert_eq!(format[Instance::FIELD_INSTANCE_NAME as usize].name, "name");
        assert_eq!(format[Instance::FIELD_RAFT_ID as usize].name, "raft_id");
        assert_eq!(format[Instance::FIELD_FAILURE_DOMAIN as usize].name, "failure_domain");
    }
}

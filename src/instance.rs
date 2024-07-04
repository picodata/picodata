use super::failure_domain::FailureDomain;
use super::replicaset::ReplicasetId;
use crate::has_states;
use crate::traft::{instance_uuid, replicaset_uuid, RaftId};
use crate::util::Transition;
use ::serde::{Deserialize, Serialize};
use ::tarantool::tlua;
use ::tarantool::tuple::Encode;
use state::StateVariant::*;

pub mod state;
pub use state::State;
pub use state::StateVariant;

crate::define_string_newtype! {
    /// Unique id of a cluster instance.
    ///
    /// This is a new-type style wrapper around String,
    /// to distinguish it from other strings.
    pub struct InstanceId(pub String);
}

////////////////////////////////////////////////////////////////////////////////
/// Serializable struct representing a member of the raft group.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Instance {
    /// Instances are identified by name.
    pub instance_id: InstanceId,
    pub instance_uuid: String,

    /// Used for identifying raft nodes.
    /// Must be unique in the raft group.
    pub raft_id: RaftId,

    /// Name of a replicaset the instance belongs to.
    pub replicaset_id: ReplicasetId,
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
}

impl Encode for Instance {}

impl Instance {
    /// Index of field "instance_id" in the space _pico_instance format.
    ///
    /// Index of first field is 0.
    pub const FIELD_INSTANCE_ID: u32 = 0;

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
            Field::from(("instance_id", FieldType::String)),
            Field::from(("instance_uuid", FieldType::String)),
            Field::from(("raft_id", FieldType::Unsigned)),
            Field::from(("replicaset_id", FieldType::String)),
            Field::from(("replicaset_uuid", FieldType::String)),
            Field::from(("current_state", FieldType::Array)),
            Field::from(("target_state", FieldType::Array)),
            Field::from(("failure_domain", FieldType::Map)),
            Field::from(("tier", FieldType::String)),
        ]
    }

    /// Construct an instance.
    pub fn new(
        raft_id: Option<RaftId>,
        instance_id: Option<impl Into<InstanceId>>,
        replicaset_id: Option<impl Into<ReplicasetId>>,
        current_state: State,
        target_state: State,
        failure_domain: FailureDomain,
        tier: &str,
    ) -> Self {
        debug_assert!(
            matches!(target_state.variant, Online | Offline | Expelled),
            "target state can only be Online, Offline or Expelled"
        );
        let instance_id = instance_id.map(Into::into).unwrap_or_else(|| "i1".into());
        let replicaset_id = replicaset_id
            .map(Into::into)
            .unwrap_or_else(|| ReplicasetId::from("r1"));
        let raft_id = raft_id.unwrap_or(1);
        let instance_uuid = instance_uuid(&instance_id);
        let replicaset_uuid = replicaset_uuid(&replicaset_id);
        Self {
            instance_id,
            raft_id,
            replicaset_id,
            current_state,
            target_state,
            failure_domain,
            instance_uuid,
            replicaset_uuid,
            tier: tier.into(),
        }
    }

    /// Instance has a state that implies it may cooperate.
    /// Currently this means that target_state is neither Offline nor Expelled.
    #[inline]
    #[allow(clippy::nonminimal_bool)]
    pub fn may_respond(&self) -> bool {
        has_states!(self, * -> not Offline) && has_states!(self, * -> not Expelled)
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
            "({}, {}, {}, {}, {}, {})",
            self.instance_id,
            self.raft_id,
            self.replicaset_id,
            Transition { from: self.current_state, to: self.target_state },
            &self.failure_domain,
            self.tier,
        )
    }
}

#[rustfmt::skip]
mod tests {
    use std::collections::HashSet;

    use crate::tier::DEFAULT_TIER;
    use crate::failure_domain::FailureDomain;
    use crate::instance::state::State;
    use crate::instance::state::StateVariant::*;
    use crate::replicaset::ReplicasetId;
    use crate::rpc::join::build_instance;
    use crate::storage::Clusterwide;
    use crate::rpc;
    use crate::rpc::update_instance::update_instance;
    use crate::tier::Tier;

    use super::*;

    macro_rules! faildoms {
        ($(,)?) => { FailureDomain::default() };
        ($($k:tt : $v:tt),+ $(,)?) => {
            FailureDomain::from([$((stringify!($k), stringify!($v))),+])
        }
    }

    fn add_tier(storage: &Clusterwide, name: &str, replication_factor: u8, can_vote: bool) -> tarantool::Result<()> {
        storage.tiers.put(
                &Tier {
                    name: name.into(),
                    replication_factor,
                    can_vote,
                }
        )
    }

    fn setup_storage(storage: &Clusterwide, instances: Vec<Instance>, replication_factor: u8) {
        for instance in instances {
            storage.instances.put(&instance).unwrap();
        }

        add_tier(storage, DEFAULT_TIER, replication_factor, true).unwrap();
    }

    fn replication_ids(replicaset_id: &ReplicasetId, storage: &Clusterwide) -> HashSet<RaftId> {
        storage
            .instances
            .replicaset_instances(replicaset_id)
            .expect("storage should not fail")
            .map(|i| i.raft_id).collect()
    }

    #[::tarantool::test]
    fn test_simple() {
        let storage = Clusterwide::for_tests();
        setup_storage(&storage, vec![], 1);

        let instance = build_instance(None, None, &FailureDomain::default(), &storage, DEFAULT_TIER).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(1), Some("i1"), Some("r1"), State::new(Offline, 0), State::new(Offline, 0), FailureDomain::default(), DEFAULT_TIER),
        );
        storage.instances.put(&instance).unwrap();

        let instance = build_instance(None, None, &FailureDomain::default(), &storage, DEFAULT_TIER).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(2), Some("i2"), Some("r2"), State::new(Offline, 0), State::new(Offline, 0), FailureDomain::default(), DEFAULT_TIER),
        );
        storage.instances.put(&instance).unwrap();

        let instance = build_instance(None, Some(&ReplicasetId::from("R3")), &FailureDomain::default(), &storage, DEFAULT_TIER).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(3), Some("i3"), Some("R3"), State::new(Offline, 0), State::new(Offline, 0), FailureDomain::default(), DEFAULT_TIER),
        );
        storage.instances.put(&instance).unwrap();

        let instance = build_instance(Some(&InstanceId::from("I4")), None, &FailureDomain::default(), &storage, DEFAULT_TIER).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(4), Some("I4"), Some("r3"), State::new(Offline, 0), State::new(Offline, 0), FailureDomain::default(), DEFAULT_TIER),
        );
        storage.instances.put(&instance).unwrap();
    }

    #[::tarantool::test]
    fn test_override() {
        let storage = Clusterwide::for_tests();
        setup_storage(&storage, vec![
            Instance::new(Some(1), Some("i1"), Some("r1"), State::new(Online, 1), State::new(Online, 1), FailureDomain::default(), DEFAULT_TIER),
            Instance::new(Some(2), Some("i2"), Some("r2-original"), State::new(Expelled, 0), State::new(Expelled, 0), FailureDomain::default(), DEFAULT_TIER),
        ],
        2);

        // join::Request with a given instance_id online.
        // - It must be an impostor, return an error.
        // - Even if it's a fair rebootstrap, it will be marked as
        //   unreachable soon (when we implement failover) the error
        //   will be gone.
        assert_eq!(
            build_instance(Some(&InstanceId::from("i1")), None, &FailureDomain::default(), &storage, DEFAULT_TIER)
                .unwrap_err(),
            "`i1` is already joined",
        );

        // join::Request with a given instance_id offline (or unreachable).
        // - Presumably it's a rebootstrap.
        //   1. Perform auto-expel, unless it threatens data safety (TODO).
        //   2. Assign new raft_id.
        //   3. Assign new replicaset_id, unless specified explicitly. A
        //      new replicaset_id might be the same as before, since
        //      auto-expel provided a vacant place there. Or it might be
        //      not, if replication_factor / failure_domain were edited.
        // - Even if it's an impostor, rely on auto-expel policy.
        //   Disruption isn't destructive if auto-expel allows (TODO).
        assert_eq!(
            build_instance(Some(&InstanceId::from("i2")), None, &FailureDomain::default(), &storage, DEFAULT_TIER).unwrap(),
            (Instance::new(Some(3), Some("i2"), Some("r1"), State::new(Offline, 0), State::new(Offline, 0), FailureDomain::default(), DEFAULT_TIER)),
            // Attention: generated replicaset_id differs from the
            // original one, as well as raft_id.
            // That's a desired behavior.
        );
        assert_eq!(replication_ids(&ReplicasetId::from("r1"), &storage), HashSet::from([1]));

        // TODO
        //
        // join::Request with a given instance_id bootstrapping.
        // - Presumably it's a retry after tarantool bootstrap failure.
        //   1. Perform auto-expel (it's always ok until bootstrap
        //      finishes).
        //   2. Assign a new raft_id.
        //   3. Assign new replicaset_id. Same as above.
        // - If it's actually an impostor (instance_id collision),
        //   original instance (that didn't report it has finished
        //   bootstrapping yet) will be disrupted.
    }

    #[::tarantool::test]
    fn test_instance_id_collision() {
        let storage = Clusterwide::for_tests();
        setup_storage(&storage, vec![
            Instance::new(Some(1), Some("i1"), Some("r1"), State::new(Online, 1), State::new(Online, 1), FailureDomain::default(), DEFAULT_TIER),
            Instance::new(Some(2), Some("i3"), Some("r3"), State::new(Online, 1), State::new(Online, 1), FailureDomain::default(), DEFAULT_TIER),
            // Attention: i3 has raft_id=2
        ], 2);

        assert_eq!(
            build_instance(None, Some(&ReplicasetId::from("r2")), &FailureDomain::default(), &storage, DEFAULT_TIER).unwrap(),
            Instance::new(Some(3), Some("i3-2"), Some("r2"), State::new(Offline, 0), State::new(Offline, 0), FailureDomain::default(), DEFAULT_TIER),
        );
    }

    #[::tarantool::test]
    fn test_replication_factor() {
        let storage = Clusterwide::for_tests();
        setup_storage(&storage, vec![
            Instance::new(Some(9), Some("i9"), Some("r9"), State::new(Online, 1), State::new(Online, 1), FailureDomain::default(), DEFAULT_TIER),
            Instance::new(Some(10), Some("i10"), Some("r9"), State::new(Online, 1), State::new(Online, 1), FailureDomain::default(), DEFAULT_TIER),
        ],
        2);

        let instance = build_instance(Some(&InstanceId::from("i1")), None, &FailureDomain::default(), &storage, DEFAULT_TIER).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(11), Some("i1"), Some("r1"), State::new(Offline, 0), State::new(Offline, 0), FailureDomain::default(), DEFAULT_TIER),
        );
        storage.instances.put(&instance).unwrap();
        assert_eq!(replication_ids(&ReplicasetId::from("r1"), &storage), HashSet::from([11]));

        let instance = build_instance(Some(&InstanceId::from("i2")), None, &FailureDomain::default(), &storage, DEFAULT_TIER).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(12), Some("i2"), Some("r1"), State::new(Offline, 0), State::new(Offline, 0), FailureDomain::default(), DEFAULT_TIER),
        );
        storage.instances.put(&instance).unwrap();
        assert_eq!(replication_ids(&ReplicasetId::from("r1"), &storage), HashSet::from([11, 12]));

        let instance = build_instance(Some(&InstanceId::from("i3")), None, &FailureDomain::default(), &storage, DEFAULT_TIER).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(13), Some("i3"), Some("r2"), State::new(Offline, 0), State::new(Offline, 0), FailureDomain::default(), DEFAULT_TIER),
        );
        storage.instances.put(&instance).unwrap();
        assert_eq!(replication_ids(&ReplicasetId::from("r2"), &storage), HashSet::from([13]));

        let instance = build_instance(Some(&InstanceId::from("i4")), None, &FailureDomain::default(), &storage, DEFAULT_TIER).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(14), Some("i4"), Some("r2"), State::new(Offline, 0), State::new(Offline, 0), FailureDomain::default(), DEFAULT_TIER),
        );
        storage.instances.put(&instance).unwrap();
        assert_eq!(replication_ids(&ReplicasetId::from("r2"), &storage), HashSet::from([13, 14]));
    }

    #[::tarantool::test]
    fn test_update_state() {
        let storage = Clusterwide::for_tests();
        let mut instance = Instance::new(Some(1), Some("i1"), Some("r1"), State::new(Online, 1), State::new(Online, 1), FailureDomain::default(), DEFAULT_TIER);
        setup_storage(&storage, vec![instance.clone()], 1);

        // Current state incarnation is allowed to go down,
        // governor has the authority over it
        let req = rpc::update_instance::Request::new(instance.instance_id.clone(), "".into())
            .with_current_state(State::new(Offline, 0));
        update_instance(&mut instance, &req, &storage).unwrap();
        storage.instances.put(&instance).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(1), Some("i1"), Some("r1"), State::new(Offline, 0), State::new(Online, 1), FailureDomain::default(), DEFAULT_TIER)
        );

        // idempotency
        let req = rpc::update_instance::Request::new(instance.instance_id.clone(), "".into())
            .with_current_state(State::new(Offline, 0));
        update_instance(&mut instance, &req, &storage).unwrap();
        storage.instances.put(&instance).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(1), Some("i1"), Some("r1"), State::new(Offline, 0), State::new(Online, 1), FailureDomain::default(), DEFAULT_TIER)
        );

        // Offline takes incarnation from current state
        let req = rpc::update_instance::Request::new(instance.instance_id.clone(), "".into())
            .with_target_state(Offline);
        update_instance(&mut instance, &req, &storage).unwrap();
        storage.instances.put(&instance).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(1), Some("i1"), Some("r1"), State::new(Offline, 0), State::new(Offline, 0), FailureDomain::default(), DEFAULT_TIER),
        );

        // Online increases incarnation
        let req = rpc::update_instance::Request::new(instance.instance_id.clone(), "".into())
            .with_target_state(Online);
        update_instance(&mut instance, &req, &storage).unwrap();
        storage.instances.put(&instance).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(1), Some("i1"), Some("r1"), State::new(Offline, 0), State::new(Online, 1), FailureDomain::default(), DEFAULT_TIER)
        );

        // No idempotency, incarnation goes up
        let req = rpc::update_instance::Request::new(instance.instance_id.clone(), "".into())
            .with_target_state(Online);
        update_instance(&mut instance, &req, &storage).unwrap();
        storage.instances.put(&instance).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(1), Some("i1"), Some("r1"), State::new(Offline, 0), State::new(Online, 2), FailureDomain::default(), DEFAULT_TIER)
        );

        // Target state can only be Online, Offline or Expelled
        let mut req = rpc::update_instance::Request::new(instance.instance_id.clone(), "".into());
        // .with_target_state will just panic
        req.target_state = Some(Replicated);
        let e = update_instance(&mut instance, &req, &storage).unwrap_err();
        assert_eq!(e, "target state can only be Online, Offline or Expelled, not Replicated");

        // State::Expelled takes incarnation from current state
        let req = rpc::update_instance::Request::new(instance.instance_id.clone(), "".into())
            .with_target_state(Expelled);
        update_instance(&mut instance, &req, &storage).unwrap();
        storage.instances.put(&instance).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(1), Some("i1"), Some("r1"), State::new(Offline, 0), State::new(Expelled, 0), FailureDomain::default(), DEFAULT_TIER),
        );

        // Instance gets expelled
        let req = rpc::update_instance::Request::new(instance.instance_id.clone(), "".into())
            .with_current_state(State::new(Expelled, 69));
        update_instance(&mut instance, &req, &storage).unwrap();
        storage.instances.put(&instance).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(1), Some("i1"), Some("r1"), State::new(Expelled, 69), State::new(Expelled, 0), FailureDomain::default(), DEFAULT_TIER),
        );

        // Updating expelled instances isn't allowed
        let req = rpc::update_instance::Request::new(instance.instance_id.clone(), "".into())
            .with_target_state(Online);
        assert_eq!(
            update_instance(&mut instance, &req, &storage).unwrap_err(),
            "cannot update expelled instance \"i1\"",
        );
    }

    #[::tarantool::test]
    fn failure_domain() {
        let storage = Clusterwide::for_tests();
        setup_storage(&storage, vec![], 3);

        let instance =
            build_instance(None, None, &faildoms! {planet: Earth}, &storage, DEFAULT_TIER)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Earth}, &storage, DEFAULT_TIER)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r2");
        storage.instances.put(&instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Mars}, &storage, DEFAULT_TIER)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Earth, os: BSD}, &storage, DEFAULT_TIER)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r3");
        storage.instances.put(&instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Mars, os: BSD}, &storage, DEFAULT_TIER)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r2");
        storage.instances.put(&instance).unwrap();

        assert_eq!(
            build_instance(None, None, &faildoms! {os: Arch}, &storage, DEFAULT_TIER)
                .unwrap_err(),
            "missing failure domain names: PLANET",
        );

        let instance =
            build_instance(None, None, &faildoms! {planet: Venus, os: Arch}, &storage, DEFAULT_TIER)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Venus, os: Mac}, &storage, DEFAULT_TIER)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r2");
        storage.instances.put(&instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Mars, os: Mac}, &storage, DEFAULT_TIER)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r3");
        storage.instances.put(&instance).unwrap();

        assert_eq!(
            build_instance(None, None, &faildoms! {}, &storage, DEFAULT_TIER)
                .unwrap_err(),
            "missing failure domain names: OS, PLANET",
        );
    }

    #[::tarantool::test]
    fn reconfigure_failure_domain() {
        let storage = Clusterwide::for_tests();
        setup_storage(&storage, vec![], 3);

        // first instance
        let mut instance1 = build_instance(Some(&InstanceId::from("i1")), None, &faildoms! {planet: Earth}, &storage, DEFAULT_TIER).unwrap();
        storage.instances.put(&instance1).unwrap();
        assert_eq!(instance1.failure_domain, faildoms! {planet: Earth});
        assert_eq!(instance1.replicaset_id, "r1");

        // reconfigure single instance, fail
        let req = rpc::update_instance::Request::new(instance1.instance_id.clone(), "".into())
            .with_failure_domain(faildoms! {owner: Ivan});
        assert_eq!(
            update_instance(&mut instance1, &req, &storage).unwrap_err(),
            "missing failure domain names: PLANET",
        );

        // reconfigure single instance, success
        let req = rpc::update_instance::Request::new(instance1.instance_id.clone(), "".into())
            .with_failure_domain(faildoms! {planet: Mars, owner: Ivan});
        update_instance(&mut instance1, &req, &storage).unwrap();
        storage.instances.put(&instance1).unwrap();
        assert_eq!(instance1.failure_domain, faildoms! {planet: Mars, owner: Ivan});
        assert_eq!(instance1.replicaset_id, "r1"); // same replicaset

        // second instance won't be joined without the newly added required
        // failure domain subdivision of "OWNER"
        assert_eq!(
            build_instance(Some(&InstanceId::from("i2")), None, &faildoms! {planet: Mars}, &storage, DEFAULT_TIER)
                .unwrap_err(),
            "missing failure domain names: OWNER",
        );

        // second instance
        #[rustfmt::skip]
        let mut instance2 = build_instance(Some(&InstanceId::from("i2")), None, &faildoms! {planet: Mars, owner: Mike}, &storage, DEFAULT_TIER)
            .unwrap();
        storage.instances.put(&instance2).unwrap();
        assert_eq!(instance2.failure_domain, faildoms! {planet: Mars, owner: Mike});
        // doesn't fit into r1
        assert_eq!(instance2.replicaset_id, "r2");

        // reconfigure second instance, success
        let req = rpc::update_instance::Request::new(instance2.instance_id.clone(), "".into())
            .with_failure_domain(faildoms! {planet: Earth, owner: Mike});
        update_instance(&mut instance2, &req, &storage).unwrap();
        storage.instances.put(&instance2).unwrap();
        assert_eq!(instance2.failure_domain, faildoms! {planet: Earth, owner: Mike});
        // replicaset doesn't change automatically
        assert_eq!(instance2.replicaset_id, "r2");

        // add instance with new subdivision
        #[rustfmt::skip]
        let instance3_v1 = build_instance(Some(&InstanceId::from("i3")), None, &faildoms! {planet: B, owner: V, dimension: C137}, &storage, DEFAULT_TIER)
            .unwrap();
        storage.instances.put(&instance3_v1).unwrap();
        assert_eq!(
            instance3_v1.failure_domain,
            faildoms! {planet: B, owner: V, dimension: C137}
        );
        assert_eq!(instance3_v1.replicaset_id, "r1");

        // even though the only instance with failure domain subdivision of
        // `DIMENSION` is inactive, we can't add an instance without that
        // subdivision
        #[rustfmt::skip]
        assert_eq!(
            build_instance(Some(&InstanceId::from("i4")), None, &faildoms! {planet: Theia, owner: Me}, &storage, DEFAULT_TIER)
                .unwrap_err(),
            "missing failure domain names: DIMENSION",
        );
    }

    #[::tarantool::test]
    fn replicaset_id_with_several_tiers() {
        let first_tier = "default";
        let second_tier = "compute";
        let third_tier = "trash";

        let storage = Clusterwide::for_tests();
        setup_storage(&storage, vec![], 1);

        add_tier(&storage, first_tier, 3, true).unwrap();
        add_tier(&storage, second_tier, 2, true).unwrap();
        add_tier(&storage, third_tier, 2, true).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Earth}, &storage, first_tier)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Mars}, &storage, second_tier)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r2");
        storage.instances.put(&instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Mars}, &storage, first_tier)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Pluto}, &storage, third_tier)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r3");
        storage.instances.put(&instance).unwrap();

        let instance =
            build_instance(None, None, &faildoms! {planet: Venus}, &storage, third_tier)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r3");
        storage.instances.put(&instance).unwrap();


        assert_eq!(
            build_instance(None, None, &faildoms! {planet: 5}, &storage, "noexistent_tier")
                .unwrap_err(),
            r#"tier "noexistent_tier" doesn't exist"#,
        );

        // gl589
        assert_eq!(
            build_instance(None, Some(&ReplicasetId::from("just to skip choose_replicaset function call")), &faildoms! {planet: 5}, &storage, "noexistent_tier")
                .unwrap_err(),
            r#"tier "noexistent_tier" doesn't exist"#,
        );
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

        assert_eq!(format[Instance::FIELD_INSTANCE_ID as usize].name, "instance_id");
        assert_eq!(format[Instance::FIELD_RAFT_ID as usize].name, "raft_id");
        assert_eq!(format[Instance::FIELD_FAILURE_DOMAIN as usize].name, "failure_domain");
    }
}

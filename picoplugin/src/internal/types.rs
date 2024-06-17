use abi_stable::derive_macro_reexports::ROption;
use abi_stable::std_types::{RSome, RString, RVec};
use abi_stable::StableAbi;
use tarantool::session::UserId;
use tarantool::space::{SpaceId, UpdateOps};
use tarantool::tuple::{ToTupleBuffer, Tuple};

/// *For internal usage, don't use it in your code*.
#[derive(StableAbi, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum DmlInner {
    Insert {
        table: SpaceId,
        tuple: RVec<u8>,
        initiator: UserId,
    },
    Replace {
        table: SpaceId,
        tuple: RVec<u8>,
        initiator: UserId,
    },
    Update {
        table: SpaceId,
        key: RVec<u8>,
        ops: RVec<RVec<u8>>,
        initiator: UserId,
    },
    Delete {
        table: SpaceId,
        key: RVec<u8>,
        initiator: UserId,
    },
}

/// Cluster-wide data modification operation.
#[derive(StableAbi, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Dml(
    /// `DmlInner` is public just for internal purposes - implement
    /// `From<DmlInner>` trait for `picodata::traft::op::Dml` at `picodata` side.
    ///
    /// *You should not use it explicitly*.
    pub DmlInner,
);

impl Dml {
    /// Insert operation.
    pub fn insert(space_id: SpaceId, tuple: Tuple, initiator: UserId) -> Self {
        Dml(DmlInner::Insert {
            table: space_id,
            tuple: RVec::from(tuple.to_vec()),
            initiator,
        })
    }

    /// Replace operation.
    pub fn replace(space_id: SpaceId, tuple: Tuple, initiator: UserId) -> Self {
        Dml(DmlInner::Replace {
            table: space_id,
            tuple: RVec::from(tuple.to_vec()),
            initiator,
        })
    }

    /// Update operation.
    pub fn update(
        space_id: SpaceId,
        key: &impl ToTupleBuffer,
        ops: UpdateOps,
        initiator: UserId,
    ) -> tarantool::Result<Self> {
        let tb = key.to_tuple_buffer()?;
        let raw_key = Vec::from(tb);

        let ops: RVec<_> = ops
            .into_inner()
            .into_iter()
            .map(|tb| RVec::from(Vec::from(tb)))
            .collect();

        Ok(Dml(DmlInner::Update {
            table: space_id,
            key: RVec::from(raw_key),
            ops,
            initiator,
        }))
    }

    /// Delete operation.
    pub fn delete(
        space_id: SpaceId,
        key: &impl ToTupleBuffer,
        initiator: UserId,
    ) -> tarantool::Result<Self> {
        let tb = key.to_tuple_buffer()?;
        let raw_key = Vec::from(tb);

        Ok(Dml(DmlInner::Delete {
            table: space_id,
            key: RVec::from(raw_key),
            initiator,
        }))
    }
}

/// *For internal usage, don't use it in your code*.
#[derive(StableAbi, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OpInner {
    Nop,
    Dml(Dml),
    BatchDml(RVec<Dml>),
}

/// RAFT operation.
#[derive(StableAbi, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Op(
    /// `OpInner` is public just for internal purposes - implement
    /// `From<OpInner>` trait for `picodata::traft::op::Op` at `picodata` side.
    ///
    /// *You should not use it explicitly*.
    pub OpInner,
);

impl Op {
    /// Nop - empty raft record.
    pub fn nop() -> Self {
        Self(OpInner::Nop)
    }

    /// Dml request.
    pub fn dml(dml: Dml) -> Self {
        Self(OpInner::Dml(dml))
    }

    /// Batch Dml request.
    pub fn dml_batch(batch: Vec<Dml>) -> Self {
        Self(OpInner::BatchDml(RVec::from(batch)))
    }
}

#[derive(Default, StableAbi, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BoundKind {
    Included,
    Excluded,
    #[default]
    Unbounded,
}

#[derive(StableAbi, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Bound {
    pub kind: BoundKind,
    pub key: ROption<RVec<u8>>,
}

impl Bound {
    pub fn new(kind: BoundKind, key: &impl ToTupleBuffer) -> tarantool::Result<Self> {
        let tb = key.to_tuple_buffer()?;
        let raw_key = Vec::from(tb);

        Ok(Self {
            kind,
            key: RSome(RVec::from(raw_key)),
        })
    }
}

#[derive(StableAbi, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Range {
    pub table: SpaceId,
    pub key_min: Bound,
    pub key_max: Bound,
}

#[derive(StableAbi, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Predicate {
    pub index: u64,
    pub term: u64,
    pub ranges: RVec<Range>,
}

impl Predicate {
    pub fn new(index: u64, term: u64, ranges: Vec<Range>) -> Self {
        Self {
            index,
            term,
            ranges: RVec::from(ranges),
        }
    }
}

::tarantool::define_str_enum! {
    /// Activity state of an instance.
    #[derive(Default, StableAbi)]
    #[repr(C)]
    pub enum GradeVariant {
        /// Instance has gracefully shut down or has not been started yet.
        #[default]
        Offline = "Offline",
        /// Instance has configured replication.
        Replicated = "Replicated",
        /// Instance is active and is handling requests.
        Online = "Online",
        /// Instance has permanently removed from cluster.
        Expelled = "Expelled",
    }
}

#[derive(StableAbi, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Grade {
    variant: GradeVariant,
    incarnation: u64,
}

impl Grade {
    pub fn new(variant: GradeVariant, incarnation: u64) -> Self {
        Self {
            variant,
            incarnation,
        }
    }

    /// Grade name. Maybe one of:
    /// - Offline
    /// - Replicated
    /// - Online
    /// - Expelled
    pub fn name(&self) -> GradeVariant {
        self.variant
    }

    /// Monotonically increase counter.
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }
}

#[derive(StableAbi, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct InstanceInfo {
    raft_id: u64,
    advertise_address: RString,
    instance_id: RString,
    instance_uuid: RString,
    replicaset_id: RString,
    replicaset_uuid: RString,
    cluster_id: RString,
    current_grade: Grade,
    target_grade: Grade,
    tier: RString,
}

impl InstanceInfo {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        raft_id: u64,
        advertise_address: String,
        instance_id: String,
        instance_uuid: String,
        replicaset_id: String,
        replicaset_uuid: String,
        cluster_id: String,
        current_grade: Grade,
        target_grade: Grade,
        tier: String,
    ) -> Self {
        Self {
            raft_id,
            advertise_address: RString::from(advertise_address),
            instance_id: RString::from(instance_id),
            instance_uuid: RString::from(instance_uuid),
            replicaset_id: RString::from(replicaset_id),
            replicaset_uuid: RString::from(replicaset_uuid),
            cluster_id: RString::from(cluster_id),
            current_grade,
            target_grade,
            tier: RString::from(tier),
        }
    }

    /// Unique identifier of node in RAFT protocol.
    pub fn raft_id(&self) -> u64 {
        self.raft_id
    }

    /// Returns address where other instances can connect to this instance.
    pub fn advertise_address(&self) -> &str {
        self.advertise_address.as_str()
    }

    /// Returns the persisted `InstanceId` of the current instance.
    pub fn instance_id(&self) -> &str {
        self.instance_id.as_str()
    }

    /// Return current instance UUID.
    pub fn instance_uuid(&self) -> &str {
        self.instance_uuid.as_str()
    }

    /// ID of a replicaset the instance belongs to.
    pub fn replicaset_id(&self) -> &str {
        self.replicaset_id.as_str()
    }

    /// UUID of a replicaset the instance belongs to.
    pub fn replicaset_uuid(&self) -> &str {
        self.replicaset_uuid.as_str()
    }

    /// UUID of a cluster the instance belongs to.
    pub fn cluster_id(&self) -> &str {
        self.cluster_id.as_str()
    }

    /// Current grade (state) of a current instance.
    pub fn current_grade(&self) -> &Grade {
        &self.current_grade
    }

    /// Target grade (state) of a current instance.
    pub fn target_grade(&self) -> &Grade {
        &self.target_grade
    }

    /// Name of a tier the instance belongs to.
    pub fn tier(&self) -> &str {
        self.tier.as_str()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct RaftInfo {
    id: u64,
    term: u64,
    applied: u64,
    leader_id: u64,
    state: RaftState,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
    PreCandidate,
}

impl RaftInfo {
    pub fn new(id: u64, term: u64, applied: u64, leader_id: u64, state: RaftState) -> Self {
        Self {
            id,
            term,
            applied,
            leader_id,
            state,
        }
    }

    /// Unique identifier of node in RAFT protocol.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// RAFT term.
    pub fn term(&self) -> u64 {
        self.term
    }

    /// Last applied RAFT index.
    pub fn applied(&self) -> u64 {
        self.applied
    }

    /// Unique identifier of a RAFT leader node.
    pub fn leader_id(&self) -> u64 {
        self.leader_id
    }

    /// RAFT state of the current node. Maybe one of:
    /// - Follower
    /// - Candidate
    /// - Leader
    /// - PreCandidate
    pub fn state(&self) -> RaftState {
        self.state
    }
}

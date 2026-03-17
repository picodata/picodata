use abi_stable::std_types::{ROption, ROption::RNone, ROption::RSome, RString, RVec};
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

#[derive(StableAbi, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Bound {
    pub is_included: bool,
    pub key: RVec<u8>,
}

impl Bound {
    pub fn new(is_included: bool, key: &impl ToTupleBuffer) -> tarantool::Result<Self> {
        let tb = key.to_tuple_buffer()?;
        let raw_key = Vec::from(tb);

        Ok(Self {
            is_included,
            key: RVec::from(raw_key),
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
    pub enum StateVariant {
        /// Instance has gracefully shut down or has not been started yet.
        #[default]
        Offline = "Offline",
        /// Instance is active and is handling requests.
        Online = "Online",
        /// Instance has permanently removed from cluster.
        Expelled = "Expelled",
    }
}

#[derive(StableAbi, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct State {
    variant: StateVariant,
    incarnation: u64,
}

impl State {
    pub fn new(variant: StateVariant, incarnation: u64) -> Self {
        Self {
            variant,
            incarnation,
        }
    }

    /// State name. May be one of:
    /// - Offline
    /// - Online
    /// - Expelled
    pub fn name(&self) -> StateVariant {
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
    name: RString,
    uuid: RString,
    replicaset_name: RString,
    replicaset_uuid: RString,
    cluster_name: RString,
    current_state: State,
    target_state: State,
    tier: RString,
}

impl InstanceInfo {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        raft_id: u64,
        advertise_address: String,
        name: String,
        instance_uuid: String,
        replicaset_name: String,
        replicaset_uuid: String,
        cluster_name: String,
        current_state: State,
        target_state: State,
        tier: String,
    ) -> Self {
        Self {
            raft_id,
            advertise_address: RString::from(advertise_address),
            name: RString::from(name),
            uuid: RString::from(instance_uuid),
            replicaset_name: RString::from(replicaset_name),
            replicaset_uuid: RString::from(replicaset_uuid),
            cluster_name: RString::from(cluster_name),
            current_state,
            target_state,
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

    /// Returns the persisted `InstanceName` of the current instance.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return current instance UUID.
    pub fn uuid(&self) -> &str {
        self.uuid.as_str()
    }

    /// ID of a replicaset the instance belongs to.
    pub fn replicaset_name(&self) -> &str {
        self.replicaset_name.as_str()
    }

    /// UUID of a replicaset the instance belongs to.
    pub fn replicaset_uuid(&self) -> &str {
        self.replicaset_uuid.as_str()
    }

    /// Name of a cluster the instance belongs to.
    pub fn cluster_name(&self) -> &str {
        self.cluster_name.as_str()
    }

    /// Current state of a current instance.
    pub fn current_state(&self) -> &State {
        &self.current_state
    }

    /// Target state of a current instance.
    pub fn target_state(&self) -> &State {
        &self.target_state
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

////////////////////////////////////////////////////////////////////////////////
// ListenerConfig
////////////////////////////////////////////////////////////////////////////////

/// FFI-safe listener configuration passed from picodata to plugins.
///
/// Contains all the information needed to create a [`PicoListener`].
///
/// [`PicoListener`]: crate::transport::listener::PicoListener
#[derive(Clone, Debug, Default)]
#[repr(C)]
pub struct FfiListenerConfig {
    /// Whether the listener is enabled.
    pub enabled: bool,
    /// The address to listen on.
    pub listen: ROption<RString>,
    /// The address to advertise.
    pub advertise: ROption<RString>,
    /// Whether TLS is enabled.
    pub tls_enabled: bool,
    /// Path to certificate file.
    pub cert_file: ROption<RString>,
    /// Path to private key file.
    pub key_file: ROption<RString>,
    /// Path to CA certificate file for mTLS.
    pub ca_file: ROption<RString>,
    /// Path to password file for encrypted keys.
    pub password_file: ROption<RString>,
}

/// Builder for creating `FfiListenerConfig` instances.
///
/// This builder provides a safer and more readable way to construct
/// `FfiListenerConfig` compared to passing many arguments to a constructor.
///
/// # Example
///
/// ```ignore
/// let config = FfiListenerConfigBuilder::new()
///     .enabled(true)
///     .listen("127.0.0.1:8080")
///     .tls_enabled(true)
///     .cert_file("/path/to/cert.pem")
///     .key_file("/path/to/key.pem")
///     .build();
/// ```
#[derive(Default)]
pub struct FfiListenerConfigBuilder {
    enabled: bool,
    listen: Option<String>,
    advertise: Option<String>,
    tls_enabled: bool,
    cert_file: Option<String>,
    key_file: Option<String>,
    ca_file: Option<String>,
    password_file: Option<String>,
}

impl FfiListenerConfigBuilder {
    /// Creates a new builder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether the listener is enabled.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Sets the address to listen on.
    pub fn listen(mut self, listen: impl Into<String>) -> Self {
        self.listen = Some(listen.into());
        self
    }

    /// Sets the address to advertise.
    pub fn advertise(mut self, advertise: impl Into<String>) -> Self {
        self.advertise = Some(advertise.into());
        self
    }

    /// Sets whether TLS is enabled.
    pub fn tls_enabled(mut self, tls_enabled: bool) -> Self {
        self.tls_enabled = tls_enabled;
        self
    }

    /// Sets the path to the certificate file.
    pub fn cert_file(mut self, cert_file: impl Into<String>) -> Self {
        self.cert_file = Some(cert_file.into());
        self
    }

    /// Sets the path to the private key file.
    pub fn key_file(mut self, key_file: impl Into<String>) -> Self {
        self.key_file = Some(key_file.into());
        self
    }

    /// Sets the path to the CA certificate file for mTLS.
    pub fn ca_file(mut self, ca_file: impl Into<String>) -> Self {
        self.ca_file = Some(ca_file.into());
        self
    }

    /// Sets the path to the password file for encrypted keys.
    pub fn password_file(mut self, password_file: impl Into<String>) -> Self {
        self.password_file = Some(password_file.into());
        self
    }

    /// Builds the `FfiListenerConfig`.
    pub fn build(self) -> FfiListenerConfig {
        let to_roption = |opt: Option<String>| match opt {
            Some(s) => RSome(RString::from(s)),
            None => RNone,
        };
        FfiListenerConfig {
            enabled: self.enabled,
            listen: to_roption(self.listen),
            advertise: to_roption(self.advertise),
            tls_enabled: self.tls_enabled,
            cert_file: to_roption(self.cert_file),
            key_file: to_roption(self.key_file),
            ca_file: to_roption(self.ca_file),
            password_file: to_roption(self.password_file),
        }
    }
}

impl FfiListenerConfig {
    /// Returns a new builder for creating `FfiListenerConfig`.
    pub fn builder() -> FfiListenerConfigBuilder {
        FfiListenerConfigBuilder::new()
    }

    /// Returns true if listener is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the listen address, or None if not specified.
    pub fn listen(&self) -> Option<&str> {
        self.listen.as_ref().into_option().map(|s| s.as_str())
    }

    /// Returns the advertise address, or None if not specified.
    pub fn advertise(&self) -> Option<&str> {
        self.advertise.as_ref().into_option().map(|s| s.as_str())
    }

    /// Returns true if TLS is enabled.
    pub fn is_tls_enabled(&self) -> bool {
        self.tls_enabled
    }

    /// Returns the certificate file path, or None if not specified.
    pub fn cert_file(&self) -> Option<&str> {
        self.cert_file.as_ref().into_option().map(|s| s.as_str())
    }

    /// Returns the key file path, or None if not specified.
    pub fn key_file(&self) -> Option<&str> {
        self.key_file.as_ref().into_option().map(|s| s.as_str())
    }

    /// Returns the CA file path, or None if not specified.
    pub fn ca_file(&self) -> Option<&str> {
        self.ca_file.as_ref().into_option().map(|s| s.as_str())
    }

    /// Returns the password file path, or None if not specified.
    pub fn password_file(&self) -> Option<&str> {
        self.password_file
            .as_ref()
            .into_option()
            .map(|s| s.as_str())
    }
}

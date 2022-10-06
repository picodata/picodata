use ::raft::StorageError;
use ::raft::INVALID_ID;
use ::tarantool::index::{Index, IteratorType};
use ::tarantool::space::{FieldType, Space};
use ::tarantool::tuple::{DecodeOwned, ToTupleBuffer, Tuple};
use thiserror::Error;

use crate::define_str_enum;
use crate::traft;
use crate::traft::error::Error as TraftError;
use crate::traft::RaftId;
use crate::traft::RaftIndex;

use std::cell::RefCell;

////////////////////////////////////////////////////////////////////////////////
// ClusterSpace
////////////////////////////////////////////////////////////////////////////////

define_str_enum! {
    /// An enumeration of builtin cluster-wide spaces
    pub enum ClusterSpace {
        Group = "raft_group",
        State = "cluster_state",
    }

    FromStr::Err = UnknownClusterSpace;
}

#[derive(Error, Debug)]
#[error("unknown cluster space {0}")]
pub struct UnknownClusterSpace(pub String);

// TODO(gmoshkin): remove this
const RAFT_GROUP: &str = ClusterSpace::Group.as_str();

////////////////////////////////////////////////////////////////////////////////
// StateKey
////////////////////////////////////////////////////////////////////////////////

define_str_enum! {
    /// An enumeration of builtin raft spaces
    pub enum StateKey {
        ReplicationFactor = "replication_factor",
    }

    FromStr::Err = UnknownStateKey;
}

#[derive(Error, Debug)]
#[error("unknown state key {0}")]
pub struct UnknownStateKey(pub String);

////////////////////////////////////////////////////////////////////////////////
// Error
////////////////////////////////////////////////////////////////////////////////

// TODO: remove this type, use traft::error::Error instead
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
enum Error {
    #[error("no such space \"{0}\"")]
    NoSuchSpace(String),

    #[allow(dead_code)]
    #[error("no such index \"{1}\" in space \"{0}\"")]
    NoSuchIndex(String, String),
}

fn box_err(e: impl std::error::Error + Sync + Send + 'static) -> StorageError {
    StorageError::Other(Box::new(e))
}

////////////////////////////////////////////////////////////////////////////////
// Storage
////////////////////////////////////////////////////////////////////////////////

pub struct Storage;

// TODO: this should be a field in `Storage`. This is static for now, because
// the refactoring will block development.
static mut PEERS_ACCESS: Option<Peers> = None;

impl Storage {
    pub fn init_schema(peers: Peers) {
        ::tarantool::lua_state()
            .exec_with(
                r#"
            local STATE, GROUP = ...

            box.schema.space.create(STATE, {
                if_not_exists = true,
                is_local = true,
                format = {
                    {name = 'key', type = 'string', is_nullable = false},
                    {name = 'value', type = 'any', is_nullable = false},
                }
            })
            box.space[STATE]:create_index('pk', {
                if_not_exists = true,
                parts = {{'key'}},
                unique = true,
            })
        "#,
                ClusterSpace::State,
            )
            .unwrap();

        if unsafe { PEERS_ACCESS.is_some() } {
            crate::warn_or_panic!("schema reinitialized");
        }

        unsafe { PEERS_ACCESS = Some(peers) };
    }

    pub fn peers_access() -> &'static Peers {
        unsafe { PEERS_ACCESS.as_ref().unwrap() }
    }

    fn space(name: impl AsRef<str> + Into<String>) -> Result<Space, StorageError> {
        Space::find(name.as_ref())
            .ok_or_else(|| Error::NoSuchSpace(name.into()))
            .map_err(box_err)
    }

    fn cluster_state<T>(key: StateKey) -> Result<Option<T>, StorageError>
    where
        T: DecodeOwned,
    {
        let tuple: Option<Tuple> = Storage::space(ClusterSpace::State)?
            .get(&(key,))
            .map_err(box_err)?;

        match tuple {
            Some(t) => t.field(1).map_err(box_err),
            None => Ok(None),
        }
    }

    pub fn peer_by_raft_id(raft_id: RaftId) -> Result<Option<traft::Peer>, StorageError> {
        Self::peers_access()
            .peer_by_raft_id(raft_id)
            .map_err(box_err)
    }

    pub fn peer_by_instance_id(instance_id: &str) -> Result<Option<traft::Peer>, StorageError> {
        Self::peers_access()
            .peer_by_instance_id(instance_id)
            .map_err(box_err)
    }

    pub fn peers() -> Result<Vec<traft::Peer>, StorageError> {
        Self::peers_access().all_peers().map_err(box_err)
    }

    pub fn box_replication(
        replicaset_id: &str,
        max_index: Option<RaftIndex>,
    ) -> Result<Vec<String>, StorageError> {
        Self::peers_access()
            .replicaset_peer_addresses(replicaset_id, max_index)
            .map_err(box_err)
    }

    #[inline]
    pub fn replication_factor() -> Result<Option<u8>, StorageError> {
        Storage::cluster_state(StateKey::ReplicationFactor)
    }

    pub fn persist_peer(peer: &traft::Peer) -> Result<(), StorageError> {
        Self::peers_access().persist_peer(peer).map_err(box_err)
    }

    #[allow(dead_code)]
    pub fn delete_peer(instance_id: &str) -> Result<(), StorageError> {
        Self::peers_access()
            .delete_peer(instance_id)
            .map_err(box_err)
    }

    pub fn insert(space: ClusterSpace, tuple: &impl ToTupleBuffer) -> Result<Tuple, StorageError> {
        Storage::space(space.as_str())?
            .insert(tuple)
            .map_err(box_err)
    }

    pub fn replace(space: ClusterSpace, tuple: &impl ToTupleBuffer) -> Result<Tuple, StorageError> {
        Storage::space(space.as_str())?
            .replace(tuple)
            .map_err(box_err)
    }

    pub fn update(
        space: ClusterSpace,
        key: &impl ToTupleBuffer,
        ops: &[impl ToTupleBuffer],
    ) -> Result<Option<Tuple>, StorageError> {
        Storage::space(space.as_str())?
            .update(key, ops)
            .map_err(box_err)
    }

    #[rustfmt::skip]
    pub fn delete(
        space: ClusterSpace,
        key: &impl ToTupleBuffer,
    ) -> Result<Option<Tuple>, StorageError> {
        Storage::space(space.as_str())?
            .delete(key)
            .map_err(box_err)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Peers
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing storage of all the cluster peers
/// (currently raft_group).
#[derive(Clone, Debug)]
pub struct Peers {
    space_peers: RefCell<Space>,
    index_instance_id: Index,
    index_raft_id: Index,
    index_replicaset_id: Index,
}

impl Peers {
    const SPACE_NAME: &'static str = ClusterSpace::Group.as_str();
    const INDEX_INSTANCE_ID: &'static str = "instance_id";
    const INDEX_RAFT_ID: &'static str = "raft_id";
    const INDEX_REPLICASET_ID: &'static str = "replicaset_id";

    pub fn new() -> tarantool::Result<Self> {
        let space_peers = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .format(peer_format())
            .if_not_exists(true)
            .create()?;

        let index_instance_id = space_peers
            .index_builder(Self::INDEX_INSTANCE_ID)
            .unique(true)
            .part(peer_field::InstanceId)
            .if_not_exists(true)
            .create()?;

        let index_raft_id = space_peers
            .index_builder(Self::INDEX_RAFT_ID)
            .unique(true)
            .part(peer_field::RaftId)
            .if_not_exists(true)
            .create()?;

        let index_replicaset_id = space_peers
            .index_builder(Self::INDEX_REPLICASET_ID)
            .unique(false)
            .part(peer_field::ReplicasetId)
            .part(peer_field::CommitIndex)
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space_peers: RefCell::new(space_peers),
            index_instance_id,
            index_raft_id,
            index_replicaset_id,
        })
    }

    #[inline]
    pub fn persist_peer(&self, peer: &traft::Peer) -> tarantool::Result<()> {
        self.space_peers.borrow_mut().replace(peer)?;
        Ok(())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn delete_peer(&self, instance_id: &str) -> tarantool::Result<()> {
        self.space_peers.borrow_mut().delete(&[instance_id])?;
        Ok(())
    }

    #[inline]
    pub fn peer_by_raft_id(&self, raft_id: RaftId) -> tarantool::Result<Option<traft::Peer>> {
        if raft_id == INVALID_ID {
            unreachable!("peer_by_raft_id called with invalid id ({})", INVALID_ID);
        }

        let tuple = self.index_raft_id.get(&(raft_id,))?;
        match tuple {
            None => Ok(None),
            Some(v) => Ok(Some(v.decode()?)),
        }
    }

    /// Find a peer by `raft_id` and return a single field specified by `F`
    /// (see `PeerFieldDef` & `peer_field` module).
    #[inline(always)]
    #[allow(dead_code)]
    pub fn get(&self, id: &impl PeerId) -> Result<traft::Peer, TraftError> {
        let res = id.find_in(self)?.decode().expect("failed to decode peer");
        Ok(res)
    }

    /// Find a peer by `id` (see `PeerId`) and return a single field
    /// specified by `F` (see `PeerFieldDef` & `peer_field` module).
    #[inline(always)]
    pub fn peer_field<F>(&self, id: &impl PeerId) -> Result<F::Type, TraftError>
    where
        F: PeerFieldDef,
    {
        let tuple = id.find_in(self)?;
        let res = F::get_in(&tuple)?;
        Ok(res)
    }

    #[inline]
    pub fn peer_by_instance_id(&self, instance_id: &str) -> tarantool::Result<Option<traft::Peer>> {
        let tuple = self.index_instance_id.get(&(instance_id,))?;
        match tuple {
            None => Ok(None),
            Some(v) => Ok(Some(v.decode()?)),
        }
    }

    #[inline]
    pub fn all_peers(&self) -> tarantool::Result<Vec<traft::Peer>> {
        self.space_peers
            .borrow()
            .select(IteratorType::All, &())?
            .map(|tuple| tuple.decode())
            .collect()
    }

    pub fn replicaset_peer_addresses(
        &self,
        replicaset_id: &str,
        max_index: impl Into<Option<RaftIndex>>,
    ) -> tarantool::Result<Vec<String>> {
        let max_index = max_index.into();

        let mut ret = Vec::new();
        let iter = self
            .index_replicaset_id
            .select(IteratorType::GE, &[replicaset_id])?;
        for tuple in iter {
            let cur_replicaset_id: &str = tuple.get(peer_field::ReplicasetId).unwrap();
            if cur_replicaset_id != replicaset_id {
                // In Tarantool the iteration must be interrupted explicitly.
                break;
            }

            let commit_index: RaftIndex = tuple.get(peer_field::CommitIndex).unwrap();
            if matches!(max_index, Some(idx) if commit_index > idx) {
                break;
            }

            ret.push(tuple.get(peer_field::PeerAddress).unwrap());
        }
        Ok(ret)
    }

    pub fn replicaset_fields<T>(&self, replicaset_id: &str) -> tarantool::Result<Vec<T::Type>>
    where
        T: PeerFieldDef,
    {
        self.index_replicaset_id
            .select(IteratorType::Eq, &[replicaset_id])?
            .map(|tuple| T::get_in(&tuple))
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////
// PeerField
////////////////////////////////////////////////////////////////////////////////

macro_rules! define_peer_fields {
    ($($field:ident: $ty:ty = ($name:literal, $tt_ty:path))+) => {
        crate::define_str_enum! {
            /// An enumeration of raft_space field names
            pub enum PeerField {
                $($field = $name,)+
            }

            FromStr::Err = UnknownPeerField;
        }

        pub mod peer_field {
            use super::*;
            $(
                /// Helper struct that represents
                #[doc = stringify!($name)]
                /// field of [`Peer`].
                ///
                /// It's rust type is
                #[doc = concat!("`", stringify!($ty), "`")]
                /// and it's tarantool type is
                #[doc = concat!("`", stringify!($tt_ty), "`")]
                ///
                /// [`Peer`]: crate::traft::Peer
                pub struct $field;

                impl PeerFieldDef for $field {
                    type Type = $ty;

                    fn get_in(tuple: &Tuple) -> tarantool::Result<Self::Type> {
                        Ok(tuple.try_get($name)?.expect("peer fields aren't nullable"))
                    }
                }

                impl From<$field> for ::tarantool::index::Part {
                    #[inline(always)]
                    fn from(_: $field) -> ::tarantool::index::Part {
                        $name.into()
                    }
                }

                impl From<$field> for ::tarantool::space::Field {
                    #[inline(always)]
                    fn from(_: $field) -> ::tarantool::space::Field {
                        ($name, $tt_ty).into()
                    }
                }

                impl ::tarantool::tuple::TupleIndex for $field {
                    #[inline(always)]
                    fn get_field<'a, T>(self, tuple: &'a Tuple) -> ::tarantool::Result<Option<T>>
                    where
                        T: ::tarantool::tuple::Decode<'a>,
                    {
                        $name.get_field(tuple)
                    }
                }
            )+
        }

        fn peer_format() -> Vec<::tarantool::space::Field> {
            vec![
                $( ::tarantool::space::Field::from(($name, $tt_ty)), )+
            ]
        }
    };
}

define_peer_fields! {
    InstanceId     : traft::InstanceId    = ("instance_id",     FieldType::String)
    InstanceUuid   : String               = ("instance_uuid",   FieldType::String)
    RaftId         : traft::RaftId        = ("raft_id",         FieldType::Unsigned)
    PeerAddress    : String               = ("peer_address",    FieldType::String)
    ReplicasetId   : String               = ("replicaset_id",   FieldType::String)
    ReplicasetUuid : String               = ("replicaset_uuid", FieldType::String)
    CommitIndex    : RaftIndex            = ("commit_index",    FieldType::Unsigned)
    Grade          : traft::Grade         = ("grade",           FieldType::String)
    TargetGrade    : traft::TargetGrade   = ("target_grade",    FieldType::String)
    FailureDomain  : traft::FailureDomain = ("failure_domain",  FieldType::Map)
}

#[derive(Error, Debug)]
#[error(r#"unknown peer field "{0}""#)]
pub struct UnknownPeerField(pub String);

impl tarantool::tuple::TupleIndex for PeerField {
    fn get_field<'a, T>(self, tuple: &'a Tuple) -> tarantool::Result<Option<T>>
    where
        T: tarantool::tuple::Decode<'a>,
    {
        self.as_str().get_field(tuple)
    }
}

/// A helper trait for type-safe and efficient access to a Peer's fields
/// without deserializing the whole tuple.
///
/// This trait contains information needed to define and use a given tuple field.
pub trait PeerFieldDef {
    /// Rust type of the field.
    ///
    /// Used when decoding the field.
    type Type: tarantool::tuple::DecodeOwned;

    /// Get the field in `tuple`.
    fn get_in(tuple: &Tuple) -> tarantool::Result<Self::Type>;
}

macro_rules! define_peer_field_def_for_tuples {
    () => {};
    ($h:ident $($t:ident)*) => {
        impl<$h, $($t),*> PeerFieldDef for ($h, $($t),*)
        where
            $h: PeerFieldDef,
            $h::Type: serde::de::DeserializeOwned,
            $(
                $t: PeerFieldDef,
                $t::Type: serde::de::DeserializeOwned,
            )*
        {
            type Type = ($h::Type, $($t::Type),*);

            fn get_in(tuple: &Tuple) -> tarantool::Result<Self::Type> {
                Ok(($h::get_in(&tuple)?, $($t::get_in(&tuple)?,)*))
            }
        }

        define_peer_field_def_for_tuples!{ $($t)* }
    };
}

define_peer_field_def_for_tuples! {
    T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14 T15
}

////////////////////////////////////////////////////////////////////////////////
// PeerId
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait can be used to identify a `Peer` when
/// accessing storage.
pub trait PeerId: serde::Serialize {
    fn find_in(&self, peers: &Peers) -> Result<Tuple, TraftError>;
}

impl PeerId for RaftId {
    #[inline(always)]
    fn find_in(&self, peers: &Peers) -> Result<Tuple, TraftError> {
        peers
            .index_raft_id
            .get(&[self])?
            .ok_or(TraftError::NoPeerWithRaftId(*self))
    }
}

impl PeerId for traft::InstanceId {
    #[inline(always)]
    fn find_in(&self, peers: &Peers) -> Result<Tuple, TraftError> {
        peers
            .index_instance_id
            .get(&[self])?
            .ok_or_else(|| TraftError::NoPeerWithInstanceId(self.clone()))
    }
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

macro_rules! assert_err {
    ($expr:expr, $err:expr) => {
        assert_eq!($expr.map_err(|e| format!("{e}")), Err($err.into()))
    };
}

#[rustfmt::skip]
inventory::submit!(crate::InnerTest {
    name: "test_storage_peers",
    body: || {
        use traft::{Grade, TargetGrade};

        let mut raft_group = Storage::space(RAFT_GROUP).unwrap();

        let faildom = crate::traft::FailureDomain::from([("a", "b")]);

        for peer in vec![
            // r1
            ("i1", "i1-uuid", 1u64, "addr:1", "r1", "r1-uuid", 1u64, Grade::Online, TargetGrade::Online, &faildom,),
            ("i2", "i2-uuid", 2u64, "addr:2", "r1", "r1-uuid",    2, Grade::Online, TargetGrade::Online, &faildom,),
            // r2
            ("i3", "i3-uuid", 3u64, "addr:3", "r2", "r2-uuid",   10, Grade::Online, TargetGrade::Online, &faildom,),
            ("i4", "i4-uuid", 4u64, "addr:4", "r2", "r2-uuid",   10, Grade::Online, TargetGrade::Online, &faildom,),
            // r3
            ("i5", "i5-uuid", 5u64, "addr:5", "r3", "r3-uuid",   10, Grade::Online, TargetGrade::Online, &faildom,),
        ] {
            raft_group.put(&peer).unwrap();
        }

        let peers = Storage::peers().unwrap();
        assert_eq!(
            peers.iter().map(|p| &p.instance_id).collect::<Vec<_>>(),
            vec!["i1", "i2", "i3", "i4", "i5"]
        );

        assert_err!(
            Storage::persist_peer(&traft::Peer {
                raft_id: 1,
                instance_id: "i99".into(),
                ..Default::default()
            }),
            format!(
                concat!(
                    "unknown error",
                    " Tarantool error:",
                    " TupleFound: Duplicate key exists",
                    " in unique index \"raft_id\"",
                    " in space \"raft_group\"",
                    " with old tuple",
                    r#" - ["i1", "i1-uuid", 1, "addr:1", "r1", "r1-uuid", 1, "{gon}", "{tgon}", {{"A": "B"}}]"#,
                    " and new tuple",
                    r#" - ["i99", "", 1, "", "", "", 0, "{goff}", "{tgon}", {{}}]"#,
                ),
                gon = Grade::Online,
                goff = Grade::Offline,
                tgon = TargetGrade::Online,
            )
        );

        {
            // Ensure traft storage doesn't impose restrictions
            // on peer_address uniqueness.
            let peer = |id: RaftId, addr: &str| traft::Peer {
                raft_id: id,
                instance_id: format!("i{id}").into(),
                peer_address: addr.into(),
                ..Default::default()
            };

            Storage::persist_peer(&peer(10, "addr:collision")).unwrap();
            Storage::persist_peer(&peer(11, "addr:collision")).unwrap();
        }

        let peer_by_raft_id = |id: RaftId| Storage::peer_by_raft_id(id).unwrap().unwrap();
        {
            assert_eq!(peer_by_raft_id(1).instance_id, "i1");
            assert_eq!(peer_by_raft_id(2).instance_id, "i2");
            assert_eq!(peer_by_raft_id(3).instance_id, "i3");
            assert_eq!(peer_by_raft_id(4).instance_id, "i4");
            assert_eq!(peer_by_raft_id(5).instance_id, "i5");
            assert_eq!(Storage::peer_by_raft_id(6), Ok(None));
        }

        let peer_by_instance_id = |iid| Storage::peer_by_instance_id(iid).unwrap().unwrap();
        {
            assert_eq!(peer_by_instance_id("i1").peer_address, "addr:1");
            assert_eq!(peer_by_instance_id("i2").peer_address, "addr:2");
            assert_eq!(peer_by_instance_id("i3").peer_address, "addr:3");
            assert_eq!(peer_by_instance_id("i4").peer_address, "addr:4");
            assert_eq!(peer_by_instance_id("i5").peer_address, "addr:5");
            assert_eq!(
                peer_by_instance_id("i10").peer_address,
                peer_by_instance_id("i11").peer_address
            );
            assert_eq!(Storage::peer_by_instance_id("i6"), Ok(None));
        }

        let box_replication = |replicaset_id: &str, max_index: Option<RaftIndex>| {
            Storage::box_replication(replicaset_id, max_index).unwrap()
        };

        {
            assert_eq!(box_replication("r1", Some(0)), Vec::<&str>::new());
            assert_eq!(box_replication("XX", None), Vec::<&str>::new());

            assert_eq!(box_replication("r1", Some(1)), vec!["addr:1"]);
            assert_eq!(box_replication("r1", Some(2)), vec!["addr:1", "addr:2"]);
            assert_eq!(box_replication("r1", Some(99)), vec!["addr:1", "addr:2"]);
            assert_eq!(box_replication("r1", None), vec!["addr:1", "addr:2"]);

            assert_eq!(box_replication("r2", Some(10)), vec!["addr:3", "addr:4"]);
            assert_eq!(box_replication("r2", Some(10)), vec!["addr:3", "addr:4"]);
            assert_eq!(box_replication("r2", None), vec!["addr:3", "addr:4"]);

            assert_eq!(box_replication("r3", Some(10)), vec!["addr:5"]);
            assert_eq!(box_replication("r3", None), vec!["addr:5"]);
        }

        raft_group.index("raft_id").unwrap().drop().unwrap();

        assert_err!(
            Storage::peer_by_raft_id(1),
            concat!(
                "unknown error",
                " Tarantool error: NoSuchIndexID: No index #1 is defined",
                " in space 'raft_group'",
            )
        );

        raft_group.index("replicaset_id").unwrap().drop().unwrap();

        assert_err!(
            Storage::box_replication("", None),
            concat!(
                "unknown error",
                " Tarantool error: NoSuchIndexID: No index #2 is defined",
                " in space 'raft_group'",
            )
        );

        raft_group.primary_key().drop().unwrap();

        assert_err!(
            Storage::peer_by_instance_id("i1"),
            concat!(
                "unknown error",
                " Tarantool error: NoSuchIndexID: No index #0 is defined",
                " in space 'raft_group'",
            )
        );

        raft_group.drop().unwrap();

        assert_err!(
            Storage::peers(),
            format!(
                "unknown error Tarantool error: NoSuchSpace: Space '{}' does not exist",
                raft_group.id(),
            )
        );
    }
});

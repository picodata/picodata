use ::tarantool::index::{Index, IndexIterator, IteratorType};
use ::tarantool::space::{FieldType, Space};
use ::tarantool::tuple::{DecodeOwned, ToTupleBuffer, Tuple};

use crate::traft;
use crate::traft::error::Error;
use crate::traft::rpc::sharding::cfg::ReplicasetWeights;
use crate::traft::Migration;
use crate::traft::RaftId;
use crate::traft::Replicaset;
use crate::traft::Result;

use std::marker::PhantomData;

////////////////////////////////////////////////////////////////////////////////
// ClusterwideSpace
////////////////////////////////////////////////////////////////////////////////

::tarantool::define_str_enum! {
    /// An enumeration of builtin cluster-wide spaces
    pub enum ClusterwideSpace {
        Group = "_picodata_raft_group",
        Address = "_picodata_peer_address",
        State = "_picodata_cluster_state",
        Replicaset = "_picodata_replicaset",
        Migration = "_picodata_migration",
    }
}

impl ClusterwideSpace {
    #[inline]
    fn get(&self) -> tarantool::Result<Space> {
        Space::find(self.as_str()).ok_or_else(|| {
            tarantool::set_error!(
                tarantool::error::TarantoolErrorCode::NoSuchSpace,
                "no such space \"{self}\""
            );
            tarantool::error::TarantoolError::last().into()
        })
    }

    #[inline]
    pub fn insert(&self, tuple: &impl ToTupleBuffer) -> tarantool::Result<Tuple> {
        self.get()?.insert(tuple)
    }

    #[inline]
    pub fn replace(&self, tuple: &impl ToTupleBuffer) -> tarantool::Result<Tuple> {
        self.get()?.replace(tuple)
    }

    #[inline]
    pub fn update(
        &self,
        key: &impl ToTupleBuffer,
        ops: &[impl ToTupleBuffer],
    ) -> tarantool::Result<Option<Tuple>> {
        self.get()?.update(key, ops)
    }

    #[inline]
    pub fn delete(&self, key: &impl ToTupleBuffer) -> tarantool::Result<Option<Tuple>> {
        self.get()?.delete(key)
    }
}

////////////////////////////////////////////////////////////////////////////////
// StateKey
////////////////////////////////////////////////////////////////////////////////

::tarantool::define_str_enum! {
    /// An enumeration of builtin raft spaces
    pub enum StateKey {
        ReplicationFactor = "replication_factor",
        VshardBootstrapped = "vshard_bootstrapped",
        DesiredSchemaVersion = "desired_schema_version",
    }
}

////////////////////////////////////////////////////////////////////////////////
// Storage
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct Clusterwide {
    pub state: State,
    pub peers: Peers,
    pub peer_addresses: PeerAddresses,
    pub replicasets: Replicasets,
    pub migrations: Migrations,
}

impl Clusterwide {
    pub fn new() -> tarantool::Result<Self> {
        Ok(Self {
            state: State::new()?,
            peers: Peers::new()?,
            peer_addresses: PeerAddresses::new()?,
            replicasets: Replicasets::new()?,
            migrations: Migrations::new()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
// State
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing storage of the cluster-wide key-value state
/// (currently cluster_state).
#[derive(Clone, Debug)]
pub struct State {
    space: Space,
}

impl State {
    const SPACE_NAME: &'static str = ClusterwideSpace::State.as_str();
    const INDEX_PRIMARY: &'static str = "pk";

    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .field(("key", FieldType::String))
            .field(("value", FieldType::Any))
            .if_not_exists(true)
            .create()?;

        space
            .index_builder(Self::INDEX_PRIMARY)
            .unique(true)
            .part("key")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space })
    }

    #[inline]
    pub fn get<T>(&self, key: StateKey) -> tarantool::Result<Option<T>>
    where
        T: DecodeOwned,
    {
        match self.space.get(&[key])? {
            Some(t) => t.field(1),
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    #[inline]
    pub fn put(&self, key: StateKey, value: &impl serde::Serialize) -> tarantool::Result<()> {
        self.space.put(&(key, value))?;
        Ok(())
    }

    #[inline]
    pub fn vshard_bootstrapped(&self) -> tarantool::Result<bool> {
        Ok(self.get(StateKey::VshardBootstrapped)?.unwrap_or_default())
    }

    #[inline]
    pub fn replication_factor(&self) -> tarantool::Result<usize> {
        let res = self
            .get(StateKey::ReplicationFactor)?
            .expect("replication_factor must be set at boot");
        Ok(res)
    }

    #[inline]
    pub fn desired_schema_version(&self) -> tarantool::Result<u64> {
        let res = self
            .get(StateKey::DesiredSchemaVersion)?
            .unwrap_or_default();
        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Replicasets
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing replicaset info from storage
#[derive(Clone, Debug)]
pub struct Replicasets {
    space: Space,
}

impl Replicasets {
    const SPACE_NAME: &'static str = ClusterwideSpace::Replicaset.as_str();
    const INDEX_PRIMARY: &'static str = "pk";

    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .field(("replicaset_id", FieldType::String))
            .field(("replicaset_uuid", FieldType::String))
            .field(("master_id", FieldType::String))
            .field(("weight", FieldType::Double))
            .field(("current_schema_version", FieldType::Unsigned))
            .if_not_exists(true)
            .create()?;

        space
            .index_builder(Self::INDEX_PRIMARY)
            .unique(true)
            .part("replicaset_id")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space })
    }

    #[inline]
    pub fn weights(&self) -> Result<ReplicasetWeights> {
        Ok(self.iter()?.map(|r| (r.replicaset_id, r.weight)).collect())
    }

    #[inline]
    pub fn get(&self, replicaset_id: &str) -> tarantool::Result<Option<Replicaset>> {
        match self.space.get(&[replicaset_id])? {
            Some(tuple) => tuple.decode().map(Some),
            None => Ok(None),
        }
    }

    #[inline]
    pub fn iter(&self) -> Result<ReplicasetIter> {
        let iter = self.space.select(IteratorType::All, &())?;
        Ok(iter.into())
    }
}

////////////////////////////////////////////////////////////////////////////////
// ReplicasetIter
////////////////////////////////////////////////////////////////////////////////

pub struct ReplicasetIter {
    iter: IndexIterator,
}

impl From<IndexIterator> for ReplicasetIter {
    fn from(iter: IndexIterator) -> Self {
        Self { iter }
    }
}

impl Iterator for ReplicasetIter {
    type Item = traft::Replicaset;
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.next().as_ref().map(Tuple::decode);
        res.map(|res| res.expect("replicaset should decode correctly"))
    }
}

////////////////////////////////////////////////////////////////////////////////
// PeerAddresses
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing storage of peer addresses.
#[derive(Clone, Debug)]
pub struct PeerAddresses {
    space: Space,
    #[allow(dead_code)]
    index_raft_id: Index,
}

impl PeerAddresses {
    const SPACE_NAME: &'static str = ClusterwideSpace::Address.as_str();
    const INDEX_RAFT_ID: &'static str = "raft_id";

    pub fn new() -> tarantool::Result<Self> {
        let space_peers = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .field(("raft_id", FieldType::Unsigned))
            .field(("address", FieldType::String))
            .if_not_exists(true)
            .create()?;

        let index_raft_id = space_peers
            .index_builder(Self::INDEX_RAFT_ID)
            .unique(true)
            .part("raft_id")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space: space_peers,
            index_raft_id,
        })
    }

    #[inline]
    pub fn put(&self, raft_id: RaftId, address: &traft::Address) -> tarantool::Result<()> {
        self.space.replace(&(raft_id, address))?;
        Ok(())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn delete(&self, raft_id: RaftId) -> tarantool::Result<()> {
        self.space.delete(&[raft_id])?;
        Ok(())
    }

    #[inline(always)]
    pub fn get(&self, raft_id: RaftId) -> Result<Option<traft::Address>> {
        let Some(tuple) = self.space.get(&[raft_id])? else { return Ok(None) };
        tuple.field(1).map_err(Into::into)
    }

    #[inline(always)]
    pub fn try_get(&self, raft_id: RaftId) -> Result<traft::Address> {
        self.get(raft_id)?
            .ok_or(Error::AddressUnknownForRaftId(raft_id))
    }

    #[allow(dead_code)]
    #[inline]
    pub fn iter(&self) -> tarantool::Result<PeerAddressIter> {
        let iter = self.space.select(IteratorType::All, &())?;
        Ok(PeerAddressIter::new(iter))
    }
}

////////////////////////////////////////////////////////////////////////////////
// PeerAddressIter
////////////////////////////////////////////////////////////////////////////////

pub struct PeerAddressIter {
    iter: IndexIterator,
}

#[allow(dead_code)]
impl PeerAddressIter {
    fn new(iter: IndexIterator) -> Self {
        Self { iter }
    }
}

impl Iterator for PeerAddressIter {
    type Item = traft::PeerAddress;
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.next().as_ref().map(Tuple::decode);
        res.map(|res| res.expect("peer address should decode correctly"))
    }
}

////////////////////////////////////////////////////////////////////////////////
// Peers
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing storage of all the cluster peers
/// (currently raft_group).
#[derive(Clone, Debug)]
pub struct Peers {
    space: Space,
    index_instance_id: Index,
    index_raft_id: Index,
    index_replicaset_id: Index,
}

impl Peers {
    const SPACE_NAME: &'static str = ClusterwideSpace::Group.as_str();
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
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space: space_peers,
            index_instance_id,
            index_raft_id,
            index_replicaset_id,
        })
    }

    #[inline]
    pub fn put(&self, peer: &traft::Peer) -> tarantool::Result<()> {
        self.space.replace(peer)?;
        Ok(())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn delete(&self, instance_id: &str) -> tarantool::Result<()> {
        self.space.delete(&[instance_id])?;
        Ok(())
    }

    /// Find a peer by `raft_id` and return a single field specified by `F`
    /// (see `PeerFieldDef` & `peer_field` module).
    #[inline(always)]
    pub fn get(&self, id: &impl PeerId) -> Result<traft::Peer> {
        let res = id.find_in(self)?.decode().expect("failed to decode peer");
        Ok(res)
    }

    /// Find a peer by `id` (see `PeerId`) and return a single field
    /// specified by `F` (see `PeerFieldDef` & `peer_field` module).
    #[inline(always)]
    pub fn peer_field<F>(&self, id: &impl PeerId) -> Result<F::Type>
    where
        F: PeerFieldDef,
    {
        let tuple = id.find_in(self)?;
        let res = F::get_in(&tuple)?;
        Ok(res)
    }

    /// Return an iterator over all peers. Items of the iterator are
    /// specified by `F` (see `PeerFieldDef` & `peer_field` module).
    #[inline(always)]
    pub fn peers_fields<F>(&self) -> Result<PeersFields<F>>
    where
        F: PeerFieldDef,
    {
        let iter = self.space.select(IteratorType::All, &())?;
        Ok(PeersFields::new(iter))
    }

    #[inline]
    pub fn iter(&self) -> tarantool::Result<PeerIter> {
        let iter = self.space.select(IteratorType::All, &())?;
        Ok(PeerIter::new(iter))
    }

    #[inline]
    pub fn all_peers(&self) -> tarantool::Result<Vec<traft::Peer>> {
        self.space
            .select(IteratorType::All, &())?
            .map(|tuple| tuple.decode())
            .collect()
    }

    pub fn replicaset_peers(&self, replicaset_id: &str) -> tarantool::Result<PeerIter> {
        let iter = self
            .index_replicaset_id
            .select(IteratorType::Eq, &[replicaset_id])?;
        Ok(PeerIter::new(iter))
    }

    pub fn replicaset_fields<T>(
        &self,
        replicaset_id: &traft::ReplicasetId,
    ) -> tarantool::Result<Vec<T::Type>>
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
        ::tarantool::define_str_enum! {
            /// An enumeration of raft_space field names
            pub enum PeerField {
                $($field = $name,)+
            }
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
    ReplicasetId   : String               = ("replicaset_id",   FieldType::String)
    ReplicasetUuid : String               = ("replicaset_uuid", FieldType::String)
    CurrentGrade   : traft::CurrentGrade  = ("current_grade",   FieldType::Array)
    TargetGrade    : traft::TargetGrade   = ("target_grade",    FieldType::Array)
    FailureDomain  : traft::FailureDomain = ("failure_domain",  FieldType::Map)
}

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
    fn find_in(&self, peers: &Peers) -> Result<Tuple>;
}

impl PeerId for RaftId {
    #[inline(always)]
    fn find_in(&self, peers: &Peers) -> Result<Tuple> {
        peers
            .index_raft_id
            .get(&[self])?
            .ok_or(Error::NoPeerWithRaftId(*self))
    }
}

impl PeerId for traft::InstanceId {
    #[inline(always)]
    fn find_in(&self, peers: &Peers) -> Result<Tuple> {
        peers
            .index_instance_id
            .get(&[self])?
            .ok_or_else(|| Error::NoPeerWithInstanceId(self.clone()))
    }
}

////////////////////////////////////////////////////////////////////////////////
// PeerIter
////////////////////////////////////////////////////////////////////////////////

pub struct PeerIter {
    iter: IndexIterator,
}

impl PeerIter {
    fn new(iter: IndexIterator) -> Self {
        Self { iter }
    }
}

impl Iterator for PeerIter {
    type Item = traft::Peer;
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.next().as_ref().map(Tuple::decode);
        res.map(|res| res.expect("peer should decode correctly"))
    }
}

impl std::fmt::Debug for PeerIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerIter").finish_non_exhaustive()
    }
}

////////////////////////////////////////////////////////////////////////////////
// PeersFields
////////////////////////////////////////////////////////////////////////////////

pub struct PeersFields<F> {
    iter: IndexIterator,
    marker: PhantomData<F>,
}

impl<F> PeersFields<F> {
    fn new(iter: IndexIterator) -> Self {
        Self {
            iter,
            marker: PhantomData,
        }
    }
}

impl<F> Iterator for PeersFields<F>
where
    F: PeerFieldDef,
{
    type Item = F::Type;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.next().as_ref().map(F::get_in);
        res.map(|res| res.expect("peer should decode correctly"))
    }
}

////////////////////////////////////////////////////////////////////////////////
// Migrations
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct Migrations {
    #[allow(dead_code)]
    space: Space,
}

impl Migrations {
    const SPACE_NAME: &'static str = ClusterwideSpace::Migration.as_str();
    const INDEX_PRIMARY: &'static str = "pk";

    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .field(("id", FieldType::Unsigned))
            .field(("body", FieldType::String))
            .if_not_exists(true)
            .create()?;

        space
            .index_builder(Self::INDEX_PRIMARY)
            .unique(true)
            .part("id")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space })
    }

    #[inline]
    pub fn get(&self, id: u64) -> tarantool::Result<Option<Migration>> {
        match self.space.get(&[id])? {
            Some(tuple) => tuple.decode().map(Some),
            None => Ok(None),
        }
    }

    #[inline]
    pub fn get_latest(&self) -> tarantool::Result<Option<Migration>> {
        let iter = self.space.select(IteratorType::Req, &())?;
        let iter = MigrationIter::from(iter);
        let ms = iter.take(1).collect::<Vec<_>>();
        Ok(ms.first().cloned())
    }

    #[inline]
    pub fn iter(&self) -> Result<MigrationIter> {
        let iter = self.space.select(IteratorType::All, &())?;
        Ok(iter.into())
    }
}

////////////////////////////////////////////////////////////////////////////////
// MigrationtIter
////////////////////////////////////////////////////////////////////////////////

pub struct MigrationIter {
    iter: IndexIterator,
}

impl From<IndexIterator> for MigrationIter {
    fn from(iter: IndexIterator) -> Self {
        Self { iter }
    }
}

impl Iterator for MigrationIter {
    type Item = traft::Migration;
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.next().as_ref().map(Tuple::decode);
        res.map(|res| res.expect("migration should decode correctly"))
    }
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

macro_rules! assert_err {
    ($expr:expr, $err:expr) => {
        assert_eq!($expr.unwrap_err().to_string(), $err)
    };
}

#[rustfmt::skip]
inventory::submit!(crate::InnerTest {
    name: "test_storage_peers",
    body: || {
        use traft::{CurrentGradeVariant as CurrentGrade, TargetGradeVariant as TargetGrade, InstanceId};

        let storage_peers = Peers::new().unwrap();
        let raft_group = storage_peers.space.clone();
        let storage_peer_addresses = PeerAddresses::new().unwrap();
        let space_peer_addresses = storage_peer_addresses.space.clone();

        let faildom = crate::traft::FailureDomain::from([("a", "b")]);

        for peer in vec![
            // r1
            ("i1", "i1-uuid", 1u64, "r1", "r1-uuid", (CurrentGrade::Online, 0), (TargetGrade::Online, 0), &faildom,),
            ("i2", "i2-uuid", 2u64, "r1", "r1-uuid", (CurrentGrade::Online, 0), (TargetGrade::Online, 0), &faildom,),
            // r2
            ("i3", "i3-uuid", 3u64, "r2", "r2-uuid", (CurrentGrade::Online, 0), (TargetGrade::Online, 0), &faildom,),
            ("i4", "i4-uuid", 4u64, "r2", "r2-uuid", (CurrentGrade::Online, 0), (TargetGrade::Online, 0), &faildom,),
            // r3
            ("i5", "i5-uuid", 5u64, "r3", "r3-uuid", (CurrentGrade::Online, 0), (TargetGrade::Online, 0), &faildom,),
        ] {
            raft_group.put(&peer).unwrap();
            let (_, _, raft_id, ..) = peer;
            space_peer_addresses.put(&(raft_id, format!("addr:{raft_id}"))).unwrap();
        }

        let peers = storage_peers.all_peers().unwrap();
        assert_eq!(
            peers.iter().map(|p| &p.instance_id).collect::<Vec<_>>(),
            vec!["i1", "i2", "i3", "i4", "i5"]
        );

        assert_err!(
            storage_peers.put(&traft::Peer {
                raft_id: 1,
                instance_id: "i99".into(),
                ..traft::Peer::default()
            }),
            format!(
                concat!(
                    "Tarantool error:",
                    " TupleFound: Duplicate key exists",
                    " in unique index \"raft_id\"",
                    " in space \"_picodata_raft_group\"",
                    " with old tuple",
                    r#" - ["i1", "i1-uuid", 1, "r1", "r1-uuid", ["{gon}", 0], ["{tgon}", 0], {{"A": "B"}}]"#,
                    " and new tuple",
                    r#" - ["i99", "", 1, "", "", ["{goff}", 0], ["{tgoff}", 0], {{}}]"#,
                ),
                gon = CurrentGrade::Online,
                goff = CurrentGrade::Offline,
                tgon = TargetGrade::Online,
                tgoff = TargetGrade::Offline,
            )
        );

        {
            // Ensure traft storage doesn't impose restrictions
            // on peer_address uniqueness.
            storage_peer_addresses.put(10, &traft::Address::from("addr:collision")).unwrap();
            storage_peer_addresses.put(11, &traft::Address::from("addr:collision")).unwrap();
        }

        {
            // Check accessing peers by 'raft_id'
            assert_eq!(storage_peers.get(&1).unwrap().instance_id, "i1");
            assert_eq!(storage_peers.get(&2).unwrap().instance_id, "i2");
            assert_eq!(storage_peers.get(&3).unwrap().instance_id, "i3");
            assert_eq!(storage_peers.get(&4).unwrap().instance_id, "i4");
            assert_eq!(storage_peers.get(&5).unwrap().instance_id, "i5");
            assert_err!(storage_peers.get(&6), "peer with id 6 not found");
        }

        {
            // Check accessing peers by 'instance_id'
            assert_eq!(storage_peers.get(&InstanceId::from("i1")).unwrap().raft_id, 1);
            assert_eq!(storage_peers.get(&InstanceId::from("i2")).unwrap().raft_id, 2);
            assert_eq!(storage_peers.get(&InstanceId::from("i3")).unwrap().raft_id, 3);
            assert_eq!(storage_peers.get(&InstanceId::from("i4")).unwrap().raft_id, 4);
            assert_eq!(storage_peers.get(&InstanceId::from("i5")).unwrap().raft_id, 5);
            assert_err!(
                storage_peers.get(&InstanceId::from("i6")),
                "peer with id \"i6\" not found"
            );
        }

        let box_replication = |replicaset_id: &str| -> Vec<traft::Address> {
            storage_peers.replicaset_peers(replicaset_id).unwrap()
                .map(|peer| storage_peer_addresses.try_get(peer.raft_id).unwrap())
                .collect::<Vec<_>>()
        };

        {
            assert_eq!(box_replication("XX"), Vec::<&str>::new());
            assert_eq!(box_replication("r1"), ["addr:1", "addr:2"]);
            assert_eq!(box_replication("r2"), ["addr:3", "addr:4"]);
            assert_eq!(box_replication("r3"), ["addr:5"]);
        }

        raft_group.index("raft_id").unwrap().drop().unwrap();

        assert_err!(
            storage_peers.get(&1),
            concat!(
                "Tarantool error: NoSuchIndexID: No index #1 is defined",
                " in space '_picodata_raft_group'",
            )
        );

        raft_group.index("replicaset_id").unwrap().drop().unwrap();

        assert_err!(
            storage_peers.replicaset_peers(""),
            concat!(
                "Tarantool error: NoSuchIndexID: No index #2 is defined",
                " in space '_picodata_raft_group'",
            )
        );

        raft_group.primary_key().drop().unwrap();

        assert_err!(
            storage_peers.get(&InstanceId::from("i1")),
            concat!(
                "Tarantool error: NoSuchIndexID: No index #0 is defined",
                " in space '_picodata_raft_group'",
            )
        );

        raft_group.drop().unwrap();

        assert_err!(
            storage_peers.all_peers(),
            format!(
                "Tarantool error: NoSuchSpace: Space '{}' does not exist",
                raft_group.id(),
            )
        );
    }
});

#[rustfmt::skip]
inventory::submit!(crate::InnerTest {
    name: "test_storage_migrations",
    body: || {
        let migrations = Migrations::new().unwrap();

        assert_eq!(None, migrations.get_latest().unwrap());

        for m in &[
            (1, "first"),
            (3, "third"),
            (2, "second")
        ] {
            migrations.space.put(&m).unwrap();
        }

        assert_eq!(
            Some(Migration {id: 3, body: "third".to_string()}),
            migrations.get_latest().unwrap()
        );
    }
});

use ::tarantool::index::{Index, IndexIterator, IteratorType};
use ::tarantool::space::{FieldType, Space};
use ::tarantool::tuple::{DecodeOwned, ToTupleBuffer, Tuple};

use crate::failure_domain as fd;
use crate::instance::{self, grade, Instance};
use crate::replicaset::{Replicaset, ReplicasetId};
use crate::traft;
use crate::traft::error::Error;
use crate::traft::Migration;
use crate::traft::RaftId;
use crate::traft::Result;

use std::marker::PhantomData;

////////////////////////////////////////////////////////////////////////////////
// ClusterwideSpace
////////////////////////////////////////////////////////////////////////////////

::tarantool::define_str_enum! {
    /// An enumeration of builtin cluster-wide spaces
    pub enum ClusterwideSpace {
        Instance = "_picodata_instance",
        Address = "_picodata_peer_address",
        Property = "_picodata_property",
        Replicaset = "_picodata_replicaset",
        Migration = "_picodata_migration",
    }
}

impl ClusterwideSpace {
    #[inline]
    fn get(&self) -> tarantool::Result<Space> {
        Space::find_cached(self.as_str()).ok_or_else(|| {
            tarantool::set_error!(
                tarantool::error::TarantoolErrorCode::NoSuchSpace,
                "no such space \"{}\"",
                self
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
}

/// Types implementing this trait represent clusterwide spaces.
pub trait TClusterwideSpace {
    type Index: TClusterwideSpaceIndex;
    const SPACE_NAME: &'static str;

    #[inline(always)]
    fn primary_index() -> Self::Index {
        Self::Index::primary()
    }
}

////////////////////////////////////////////////////////////////////////////////
// ClusterwideSpaceIndex
////////////////////////////////////////////////////////////////////////////////

/// A type alias for getting the enumeration of indexes for a clusterwide space.
pub type IndexOf<T> = <T as TClusterwideSpace>::Index;

/// An index of a clusterwide space.
#[derive(Copy, Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum ClusterwideSpaceIndex {
    Property(IndexOf<Properties>),
    Instance(IndexOf<Instances>),
    Address(IndexOf<PeerAddresses>),
    Replicaset(IndexOf<Replicasets>),
    Migration(IndexOf<Migrations>),
}

impl ClusterwideSpaceIndex {
    #[inline]
    fn get(&self) -> tarantool::Result<Index> {
        let space = self.space().get()?;
        let index_name = self.index_name();
        let Some(index) = space.index_cached(index_name) else {
            tarantool::set_error!(
                tarantool::error::TarantoolErrorCode::NoSuchIndexName,
                "no such index \"{}\"", index_name
            );
            return Err(tarantool::error::TarantoolError::last().into());
        };
        Ok(index)
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

    pub const fn space(&self) -> ClusterwideSpace {
        match self {
            Self::Property(_) => ClusterwideSpace::Property,
            Self::Instance(_) => ClusterwideSpace::Instance,
            Self::Address(_) => ClusterwideSpace::Address,
            Self::Replicaset(_) => ClusterwideSpace::Replicaset,
            Self::Migration(_) => ClusterwideSpace::Migration,
        }
    }

    pub const fn index_name(&self) -> &'static str {
        match self {
            Self::Property(idx) => idx.as_str(),
            Self::Instance(idx) => idx.as_str(),
            Self::Address(idx) => idx.as_str(),
            Self::Replicaset(idx) => idx.as_str(),
            Self::Migration(idx) => idx.as_str(),
        }
    }

    pub fn is_primary(&self) -> bool {
        match self {
            Self::Property(idx) => idx.is_primary(),
            Self::Instance(idx) => idx.is_primary(),
            Self::Address(idx) => idx.is_primary(),
            Self::Replicaset(idx) => idx.is_primary(),
            Self::Migration(idx) => idx.is_primary(),
        }
    }
}

impl From<ClusterwideSpace> for ClusterwideSpaceIndex {
    fn from(space: ClusterwideSpace) -> Self {
        match space {
            ClusterwideSpace::Property => Self::Property(Properties::primary_index()),
            ClusterwideSpace::Instance => Self::Instance(Instances::primary_index()),
            ClusterwideSpace::Address => Self::Address(PeerAddresses::primary_index()),
            ClusterwideSpace::Replicaset => Self::Replicaset(Replicasets::primary_index()),
            ClusterwideSpace::Migration => Self::Migration(Migrations::primary_index()),
        }
    }
}

impl std::fmt::Display for ClusterwideSpaceIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_primary() {
            write!(f, "{}", self.space())
        } else {
            write!(f, "{}.index.{}", self.space(), self.index_name())
        }
    }
}

pub trait TClusterwideSpaceIndex {
    fn primary() -> Self;
    fn is_primary(&self) -> bool;
}

macro_rules! define_clusterwide_space_struct {
    (
        $space_name:path;

        $(#[$space_meta:meta])*
        pub struct $space:ident {
            $(
                $(#[$field_meta:meta])*
                $field:ident : $field_ty:ty,
            )+
        }

        $(#[$index_meta:meta])*
        pub enum $index:ident {
            #[primary]
            $primary_index_var:ident = $primary_index_name:expr,
            $( $index_var:ident = $index_name:expr, )*
        }
    ) => {
        $(#[$space_meta])*
        pub struct $space {
            $( $(#[$field_meta])* $field: $field_ty,)+
        }

        ::tarantool::define_str_enum! {
            $(#[$index_meta])*
            pub enum $index {
                $primary_index_var = $primary_index_name,
                $( $index_var = $index_name, )*
            }
        }

        impl TClusterwideSpace for $space {
            type Index = $index;
            const SPACE_NAME: &'static str = $space_name.as_str();
        }

        impl TClusterwideSpaceIndex for IndexOf<$space> {
            #[inline(always)]
            fn primary() -> Self {
                Self::$primary_index_var
            }

            #[inline(always)]
            fn is_primary(&self) -> bool {
                matches!(self, Self::$primary_index_var)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// PropertyName
////////////////////////////////////////////////////////////////////////////////

::tarantool::define_str_enum! {
    /// An enumeration of [`ClusterwideSpace::Property`] key names.
    pub enum PropertyName {
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
    pub properties: Properties,
    pub instances: Instances,
    pub peer_addresses: PeerAddresses,
    pub replicasets: Replicasets,
    pub migrations: Migrations,
}

impl Clusterwide {
    pub fn new() -> tarantool::Result<Self> {
        Ok(Self {
            properties: Properties::new()?,
            instances: Instances::new()?,
            peer_addresses: PeerAddresses::new()?,
            replicasets: Replicasets::new()?,
            migrations: Migrations::new()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
// Properties
////////////////////////////////////////////////////////////////////////////////

define_clusterwide_space_struct! {
    ClusterwideSpace::Property;

    /// A struct for accessing storage of the cluster-wide key-value properties
    #[derive(Clone, Debug)]
    pub struct Properties {
        space: Space,
    }

    /// An enumeration of indexes defined for property space.
    pub enum SpacePropertyIndex {
        #[primary]
        Key = "key",
    }
}

impl Properties {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .field(("key", FieldType::String))
            .field(("value", FieldType::Any))
            .if_not_exists(true)
            .create()?;

        space
            .index_builder(Self::primary_index().as_str())
            .unique(true)
            .part("key")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space })
    }

    #[inline]
    pub fn get<T>(&self, key: PropertyName) -> tarantool::Result<Option<T>>
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
    pub fn put(&self, key: PropertyName, value: &impl serde::Serialize) -> tarantool::Result<()> {
        self.space.put(&(key, value))?;
        Ok(())
    }

    #[inline]
    pub fn vshard_bootstrapped(&self) -> tarantool::Result<bool> {
        Ok(self
            .get(PropertyName::VshardBootstrapped)?
            .unwrap_or_default())
    }

    #[inline]
    pub fn replication_factor(&self) -> tarantool::Result<usize> {
        let res = self
            .get(PropertyName::ReplicationFactor)?
            .expect("replication_factor must be set at boot");
        Ok(res)
    }

    #[inline]
    pub fn desired_schema_version(&self) -> tarantool::Result<u64> {
        let res = self
            .get(PropertyName::DesiredSchemaVersion)?
            .unwrap_or_default();
        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Replicasets
////////////////////////////////////////////////////////////////////////////////

define_clusterwide_space_struct! {
    ClusterwideSpace::Replicaset;

    /// A struct for accessing replicaset info from storage
    #[derive(Clone, Debug)]
    pub struct Replicasets {
        space: Space,
    }


    /// An enumeration of indexes defined for replicaset space.
    pub enum SpaceReplicasetIndex {
        #[primary]
        ReplicasetId = "replicaset_id",
    }
}

impl Replicasets {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .format(Replicaset::format())
            .if_not_exists(true)
            .create()?;

        space
            .index_builder(Self::primary_index().as_str())
            .unique(true)
            .part("replicaset_id")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space })
    }

    #[allow(unused)]
    #[inline]
    pub fn get(&self, replicaset_id: &str) -> tarantool::Result<Option<Replicaset>> {
        match self.space.get(&[replicaset_id])? {
            Some(tuple) => tuple.decode().map(Some),
            None => Ok(None),
        }
    }
}

impl ToEntryIter for Replicasets {
    type Entry = Replicaset;

    #[inline(always)]
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
    }
}

////////////////////////////////////////////////////////////////////////////////
// PeerAddresses
////////////////////////////////////////////////////////////////////////////////

define_clusterwide_space_struct! {
    ClusterwideSpace::Address;

    /// A struct for accessing storage of peer addresses.
    #[derive(Clone, Debug)]
    pub struct PeerAddresses {
        space: Space,
    }

    /// An enumeration of indexes defined for peer address space.
    pub enum SpacePeerAddressIndex {
        #[primary]
        RaftId = "raft_id",
    }
}

impl PeerAddresses {
    pub fn new() -> tarantool::Result<Self> {
        let space_instances = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .field(("raft_id", FieldType::Unsigned))
            .field(("address", FieldType::String))
            .if_not_exists(true)
            .create()?;

        space_instances
            .index_builder(Self::primary_index().as_str())
            .unique(true)
            .part("raft_id")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space: space_instances,
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
}

impl ToEntryIter for PeerAddresses {
    type Entry = traft::PeerAddress;

    #[inline(always)]
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Instance
////////////////////////////////////////////////////////////////////////////////

define_clusterwide_space_struct! {
    ClusterwideSpace::Instance;

    /// A struct for accessing storage of all the cluster instances.
    #[derive(Clone, Debug)]
    pub struct Instances {
        space: Space,
        index_instance_id: Index,
        index_raft_id: Index,
        index_replicaset_id: Index,
    }

    /// An enumeration of indexes defined for instance space.
    #[allow(clippy::enum_variant_names)]
    pub enum SpaceInstanceIndex {
        #[primary]
        InstanceId = "instance_id",
        RaftId = "raft_id",
        ReplicasetId = "replicaset_id",
    }
}

impl Instances {
    pub fn new() -> tarantool::Result<Self> {
        let space_instances = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .format(instance_format())
            .if_not_exists(true)
            .create()?;

        let index_instance_id = space_instances
            .index_builder(IndexOf::<Self>::InstanceId.as_str())
            .unique(true)
            .part(instance_field::InstanceId)
            .if_not_exists(true)
            .create()?;

        let index_raft_id = space_instances
            .index_builder(IndexOf::<Self>::RaftId.as_str())
            .unique(true)
            .part(instance_field::RaftId)
            .if_not_exists(true)
            .create()?;

        let index_replicaset_id = space_instances
            .index_builder(IndexOf::<Self>::ReplicasetId.as_str())
            .unique(false)
            .part(instance_field::ReplicasetId)
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space: space_instances,
            index_instance_id,
            index_raft_id,
            index_replicaset_id,
        })
    }

    #[inline]
    pub fn put(&self, instance: &Instance) -> tarantool::Result<()> {
        self.space.replace(instance)?;
        Ok(())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn delete(&self, instance_id: &str) -> tarantool::Result<()> {
        self.space.delete(&[instance_id])?;
        Ok(())
    }

    /// Find a instance by `raft_id` and return a single field specified by `F`
    /// (see `InstanceFieldDef` & `instance_field` module).
    #[inline(always)]
    pub fn get(&self, id: &impl InstanceId) -> Result<Instance> {
        let res = id
            .find_in(self)?
            .decode()
            .expect("failed to decode instance");
        Ok(res)
    }

    /// Find a instance by `id` (see `InstanceId`) and return a single field
    /// specified by `F` (see `InstanceFieldDef` & `instance_field` module).
    #[inline(always)]
    pub fn field<F>(&self, id: &impl InstanceId) -> Result<F::Type>
    where
        F: InstanceFieldDef,
    {
        let tuple = id.find_in(self)?;
        let res = F::get_in(&tuple)?;
        Ok(res)
    }

    /// Return an iterator over all instances. Items of the iterator are
    /// specified by `F` (see `InstanceFieldDef` & `instance_field` module).
    #[inline(always)]
    pub fn instances_fields<F>(&self) -> Result<InstancesFields<F>>
    where
        F: InstanceFieldDef,
    {
        let iter = self.space.select(IteratorType::All, &())?;
        Ok(InstancesFields::new(iter))
    }

    #[inline]
    pub fn all_instances(&self) -> tarantool::Result<Vec<Instance>> {
        self.space
            .select(IteratorType::All, &())?
            .map(|tuple| tuple.decode())
            .collect()
    }

    pub fn replicaset_instances(
        &self,
        replicaset_id: &str,
    ) -> tarantool::Result<EntryIter<Instance>> {
        let iter = self
            .index_replicaset_id
            .select(IteratorType::Eq, &[replicaset_id])?;
        Ok(EntryIter::new(iter))
    }

    pub fn replicaset_fields<T>(
        &self,
        replicaset_id: &ReplicasetId,
    ) -> tarantool::Result<Vec<T::Type>>
    where
        T: InstanceFieldDef,
    {
        self.index_replicaset_id
            .select(IteratorType::Eq, &[replicaset_id])?
            .map(|tuple| T::get_in(&tuple))
            .collect()
    }
}

impl ToEntryIter for Instances {
    type Entry = Instance;

    #[inline(always)]
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
    }
}

////////////////////////////////////////////////////////////////////////////////
// InstanceField
////////////////////////////////////////////////////////////////////////////////

macro_rules! define_instance_fields {
    ($($field:ident: $ty:ty = ($name:literal, $tt_ty:path))+) => {
        ::tarantool::define_str_enum! {
            /// An enumeration of raft_space field names
            pub enum InstanceField {
                $($field = $name,)+
            }
        }

        pub mod instance_field {
            use super::*;
            $(
                /// Helper struct that represents
                #[doc = stringify!($name)]
                /// field of [`Instance`].
                ///
                /// It's rust type is
                #[doc = concat!("`", stringify!($ty), "`")]
                /// and it's tarantool type is
                #[doc = concat!("`", stringify!($tt_ty), "`")]
                ///
                /// [`Instance`]: crate::instance::Instance
                pub struct $field;

                impl InstanceFieldDef for $field {
                    type Type = $ty;

                    fn get_in(tuple: &Tuple) -> tarantool::Result<Self::Type> {
                        Ok(tuple.try_get($name)?.expect("instance fields aren't nullable"))
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

        fn instance_format() -> Vec<::tarantool::space::Field> {
            vec![
                $( ::tarantool::space::Field::from(($name, $tt_ty)), )+
            ]
        }
    };
}

define_instance_fields! {
    InstanceId     : instance::InstanceId = ("instance_id",     FieldType::String)
    InstanceUuid   : String               = ("instance_uuid",   FieldType::String)
    RaftId         : traft::RaftId        = ("raft_id",         FieldType::Unsigned)
    ReplicasetId   : String               = ("replicaset_id",   FieldType::String)
    ReplicasetUuid : String               = ("replicaset_uuid", FieldType::String)
    CurrentGrade   : grade::CurrentGrade  = ("current_grade",   FieldType::Array)
    TargetGrade    : grade::TargetGrade   = ("target_grade",    FieldType::Array)
    FailureDomain  : fd::FailureDomain    = ("failure_domain",  FieldType::Map)
}

impl tarantool::tuple::TupleIndex for InstanceField {
    fn get_field<'a, T>(self, tuple: &'a Tuple) -> tarantool::Result<Option<T>>
    where
        T: tarantool::tuple::Decode<'a>,
    {
        self.as_str().get_field(tuple)
    }
}

/// A helper trait for type-safe and efficient access to a Instance's fields
/// without deserializing the whole tuple.
///
/// This trait contains information needed to define and use a given tuple field.
pub trait InstanceFieldDef {
    /// Rust type of the field.
    ///
    /// Used when decoding the field.
    type Type: tarantool::tuple::DecodeOwned;

    /// Get the field in `tuple`.
    fn get_in(tuple: &Tuple) -> tarantool::Result<Self::Type>;
}

macro_rules! define_instance_field_def_for_tuples {
    () => {};
    ($h:ident $($t:ident)*) => {
        impl<$h, $($t),*> InstanceFieldDef for ($h, $($t),*)
        where
            $h: InstanceFieldDef,
            $h::Type: serde::de::DeserializeOwned,
            $(
                $t: InstanceFieldDef,
                $t::Type: serde::de::DeserializeOwned,
            )*
        {
            type Type = ($h::Type, $($t::Type),*);

            fn get_in(tuple: &Tuple) -> tarantool::Result<Self::Type> {
                Ok(($h::get_in(&tuple)?, $($t::get_in(&tuple)?,)*))
            }
        }

        define_instance_field_def_for_tuples!{ $($t)* }
    };
}

define_instance_field_def_for_tuples! {
    T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14 T15
}

////////////////////////////////////////////////////////////////////////////////
// InstanceId
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait can be used to identify a `Instance` when
/// accessing storage.
pub trait InstanceId: serde::Serialize {
    fn find_in(&self, instances: &Instances) -> Result<Tuple>;
}

impl InstanceId for RaftId {
    #[inline(always)]
    fn find_in(&self, instances: &Instances) -> Result<Tuple> {
        instances
            .index_raft_id
            .get(&[self])?
            .ok_or(Error::NoInstanceWithRaftId(*self))
    }
}

impl InstanceId for instance::InstanceId {
    #[inline(always)]
    fn find_in(&self, instances: &Instances) -> Result<Tuple> {
        instances
            .index_instance_id
            .get(&[self])?
            .ok_or_else(|| Error::NoInstanceWithInstanceId(self.clone()))
    }
}

////////////////////////////////////////////////////////////////////////////////
// InstancesFields
////////////////////////////////////////////////////////////////////////////////

pub struct InstancesFields<F> {
    iter: IndexIterator,
    marker: PhantomData<F>,
}

impl<F> InstancesFields<F> {
    fn new(iter: IndexIterator) -> Self {
        Self {
            iter,
            marker: PhantomData,
        }
    }
}

impl<F> Iterator for InstancesFields<F>
where
    F: InstanceFieldDef,
{
    type Item = F::Type;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.next().as_ref().map(F::get_in);
        res.map(|res| res.expect("instance should decode correctly"))
    }
}

////////////////////////////////////////////////////////////////////////////////
// Migrations
////////////////////////////////////////////////////////////////////////////////

define_clusterwide_space_struct! {
    ClusterwideSpace::Migration;

    #[derive(Clone, Debug)]
    pub struct Migrations {
        space: Space,
    }

    /// An enumeration of indexes defined for migration space.
    pub enum SpaceMigrationIndex {
        #[primary]
        Id = "id",
    }
}

impl Migrations {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .field(("id", FieldType::Unsigned))
            .field(("body", FieldType::String))
            .if_not_exists(true)
            .create()?;

        space
            .index_builder(Self::primary_index().as_str())
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
        let iter = EntryIter::new(iter);
        let ms = iter.take(1).collect::<Vec<_>>();
        Ok(ms.first().cloned())
    }
}

impl ToEntryIter for Migrations {
    type Entry = Migration;

    #[inline(always)]
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
    }
}

////////////////////////////////////////////////////////////////////////////////
// EntryIter
////////////////////////////////////////////////////////////////////////////////

/// This trait is implemented for storage structs for iterating over the entries
/// from that storage.
pub trait ToEntryIter {
    /// Target type for entry deserialization.
    type Entry;

    fn index_iter(&self) -> Result<IndexIterator>;

    #[inline(always)]
    fn iter(&self) -> Result<EntryIter<Self::Entry>> {
        Ok(EntryIter::new(self.index_iter()?))
    }
}

/// An iterator struct for automatically deserializing tuples into a given type.
///
/// # Panics
/// Will panic in case deserialization fails on a given iteration.
pub struct EntryIter<T> {
    iter: IndexIterator,
    marker: PhantomData<T>,
}

impl<T> EntryIter<T> {
    pub fn new(iter: IndexIterator) -> Self {
        Self {
            iter,
            marker: PhantomData,
        }
    }
}

impl<T> Iterator for EntryIter<T>
where
    T: DecodeOwned,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.next().as_ref().map(Tuple::decode);
        res.map(|res| res.expect("entry should decode correctly"))
    }
}

impl<T> std::fmt::Debug for EntryIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .finish_non_exhaustive()
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
    name: "test_storage_instances",
    body: || {
        use crate::instance::grade::{CurrentGradeVariant as CGV, TargetGradeVariant as TGV};
        use crate::instance::InstanceId;
        use crate::failure_domain::FailureDomain;

        let storage_instances = Instances::new().unwrap();
        let space_instances = storage_instances.space.clone();
        let storage_peer_addresses = PeerAddresses::new().unwrap();
        let space_peer_addresses = storage_peer_addresses.space.clone();

        let faildom = FailureDomain::from([("a", "b")]);

        for instance in vec![
            // r1
            ("i1", "i1-uuid", 1u64, "r1", "r1-uuid", (CGV::Online, 0), (TGV::Online, 0), &faildom,),
            ("i2", "i2-uuid", 2u64, "r1", "r1-uuid", (CGV::Online, 0), (TGV::Online, 0), &faildom,),
            // r2
            ("i3", "i3-uuid", 3u64, "r2", "r2-uuid", (CGV::Online, 0), (TGV::Online, 0), &faildom,),
            ("i4", "i4-uuid", 4u64, "r2", "r2-uuid", (CGV::Online, 0), (TGV::Online, 0), &faildom,),
            // r3
            ("i5", "i5-uuid", 5u64, "r3", "r3-uuid", (CGV::Online, 0), (TGV::Online, 0), &faildom,),
        ] {
            space_instances.put(&instance).unwrap();
            let (_, _, raft_id, ..) = instance;
            space_peer_addresses.put(&(raft_id, format!("addr:{raft_id}"))).unwrap();
        }

        let instance = storage_instances.all_instances().unwrap();
        assert_eq!(
            instance.iter().map(|p| &p.instance_id).collect::<Vec<_>>(),
            vec!["i1", "i2", "i3", "i4", "i5"]
        );

        assert_err!(
            storage_instances.put(&Instance {
                raft_id: 1,
                instance_id: "i99".into(),
                ..Instance::default()
            }),
            format!(
                concat!(
                    "Tarantool error:",
                    " TupleFound: Duplicate key exists",
                    " in unique index \"raft_id\"",
                    " in space \"_picodata_instance\"",
                    " with old tuple",
                    r#" - ["i1", "i1-uuid", 1, "r1", "r1-uuid", ["{gon}", 0], ["{tgon}", 0], {{"A": "B"}}]"#,
                    " and new tuple",
                    r#" - ["i99", "", 1, "", "", ["{goff}", 0], ["{tgoff}", 0], {{}}]"#,
                ),
                gon = CGV::Online,
                goff = CGV::Offline,
                tgon = TGV::Online,
                tgoff = TGV::Offline,
            )
        );

        {
            // Ensure traft storage doesn't impose restrictions
            // on peer_address uniqueness.
            storage_peer_addresses.put(10, &traft::Address::from("addr:collision")).unwrap();
            storage_peer_addresses.put(11, &traft::Address::from("addr:collision")).unwrap();
        }

        {
            // Check accessing instances by 'raft_id'
            assert_eq!(storage_instances.get(&1).unwrap().instance_id, "i1");
            assert_eq!(storage_instances.get(&2).unwrap().instance_id, "i2");
            assert_eq!(storage_instances.get(&3).unwrap().instance_id, "i3");
            assert_eq!(storage_instances.get(&4).unwrap().instance_id, "i4");
            assert_eq!(storage_instances.get(&5).unwrap().instance_id, "i5");
            assert_err!(storage_instances.get(&6), "instance with id 6 not found");
        }

        {
            // Check accessing instances by 'instance_id'
            assert_eq!(storage_instances.get(&InstanceId::from("i1")).unwrap().raft_id, 1);
            assert_eq!(storage_instances.get(&InstanceId::from("i2")).unwrap().raft_id, 2);
            assert_eq!(storage_instances.get(&InstanceId::from("i3")).unwrap().raft_id, 3);
            assert_eq!(storage_instances.get(&InstanceId::from("i4")).unwrap().raft_id, 4);
            assert_eq!(storage_instances.get(&InstanceId::from("i5")).unwrap().raft_id, 5);
            assert_err!(
                storage_instances.get(&InstanceId::from("i6")),
                "instance with id \"i6\" not found"
            );
        }

        let box_replication = |replicaset_id: &str| -> Vec<traft::Address> {
            storage_instances.replicaset_instances(replicaset_id).unwrap()
                .map(|instance| storage_peer_addresses.try_get(instance.raft_id).unwrap())
                .collect::<Vec<_>>()
        };

        {
            assert_eq!(box_replication("XX"), Vec::<&str>::new());
            assert_eq!(box_replication("r1"), ["addr:1", "addr:2"]);
            assert_eq!(box_replication("r2"), ["addr:3", "addr:4"]);
            assert_eq!(box_replication("r3"), ["addr:5"]);
        }

        space_instances.index("raft_id").unwrap().drop().unwrap();

        assert_err!(
            storage_instances.get(&1),
            concat!(
                "Tarantool error: NoSuchIndexID: No index #1 is defined",
                " in space '_picodata_instance'",
            )
        );

        space_instances.index("replicaset_id").unwrap().drop().unwrap();

        assert_err!(
            storage_instances.replicaset_instances(""),
            concat!(
                "Tarantool error: NoSuchIndexID: No index #2 is defined",
                " in space '_picodata_instance'",
            )
        );

        space_instances.primary_key().drop().unwrap();

        assert_err!(
            storage_instances.get(&InstanceId::from("i1")),
            concat!(
                "Tarantool error: NoSuchIndexID: No index #0 is defined",
                " in space '_picodata_instance'",
            )
        );

        space_instances.drop().unwrap();

        assert_err!(
            storage_instances.all_instances(),
            format!(
                "Tarantool error: NoSuchSpace: Space '{}' does not exist",
                space_instances.id(),
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

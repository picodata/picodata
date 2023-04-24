use ::tarantool::index::{Index, IndexId, IndexIterator, IteratorType};
use ::tarantool::msgpack::{ArrayWriter, ValueIter};
use ::tarantool::space::UpdateOps;
use ::tarantool::space::{FieldType, Space, SpaceId, SystemSpace};
use ::tarantool::tuple::KeyDef;
use ::tarantool::tuple::{Decode, DecodeOwned, Encode};
use ::tarantool::tuple::{RawBytes, ToTupleBuffer, Tuple, TupleBuffer};

use crate::failure_domain as fd;
use crate::instance::{self, grade, Instance};
use crate::replicaset::{Replicaset, ReplicasetId};
use crate::schema::{IndexDef, SpaceDef};
use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::op::Ddl;
use crate::traft::Migration;
use crate::traft::RaftId;
use crate::traft::Result;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::marker::PhantomData;

macro_rules! define_clusterwide_spaces {
    (
        $(#[$cw_struct_meta:meta])*
        pub struct $cw_struct:ident {
            pub #space_name_lower: #space_name_upper,
        }

        $(#[$cw_space_meta:meta])*
        pub enum $cw_space:ident {
            $(
                $(#[$cw_field_meta:meta])*
                $cw_space_var:ident = $cw_space_name:expr => {
                    $_cw_struct:ident :: $cw_field:ident;

                    $(#[$space_meta:meta])*
                    pub struct $space:ident {
                        $space_field:ident: $space_ty:ty,
                        #[primary]
                        $index_field_pk:ident: $index_ty_pk:ty
                            => $index_var_pk:ident = $index_name_pk:expr,
                        $(
                            $index_field:ident: $index_ty:ty
                                => $index_var:ident = $index_name:expr,
                        )*
                    }

                    $(#[$index_meta:meta])*
                    pub enum $index:ident;
                }
            )+
        }

        $(#[$cw_index_meta:meta])*
        pub enum $cw_index:ident {
            #space_name( $index_of:ident <#space_struct_name>),
        }
    ) => {
        ////////////////////////////////////////////////////////////////////////
        // ClusterwideSpace
        ::tarantool::define_str_enum! {
            $(#[$cw_space_meta])*
            pub enum $cw_space {
                $(
                    $(#[$cw_field_meta])*
                    $cw_space_var = $cw_space_name,
                )+
            }
        }

        impl $cw_space {
            pub fn primary_index(&self) -> $cw_index {
                match self {
                    $(
                        $cw_space::$cw_space_var => $cw_index::$cw_space_var(<$space>::primary_index()),
                    )+
                }
            }
        }

        $( const _: $crate::util::CheckIsSameType<$_cw_struct, $cw_struct> = (); )+

        ////////////////////////////////////////////////////////////////////////
        // ClusterwideSpaceIndex
        $(#[$cw_index_meta])*
        #[derive(Copy, Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
        pub enum $cw_index {
            $( $cw_space_var($index_of<$space>),)+
        }

        impl From<$cw_space> for $cw_index {
            fn from(space: $cw_space) -> Self {
                space.primary_index()
            }
        }

        impl $cw_index {
            pub const fn space(&self) -> $cw_space {
                match self {
                    $(
                        Self::$cw_space_var(_) => $cw_space::$cw_space_var,
                    )+
                }
            }

            pub const fn index_name(&self) -> &'static str {
                match self {
                    $( Self::$cw_space_var(idx) => idx.as_str(), )+
                }
            }

            pub fn is_primary(&self) -> bool {
                match self {
                    $( Self::$cw_space_var(idx) => idx.is_primary(), )+
                }
            }
        }

        impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::LuaRead<L> for $cw_index {
            fn lua_read_at_position(lua: L, index: ::std::num::NonZeroI32) -> ::tarantool::tlua::ReadResult<Self, L> {
                use ::tarantool::tlua;

                #[derive(tlua::LuaRead)]
                struct ClusterwideSpaceIndexInfo {
                    name: Option<String>,
                    index_name: Option<String>,
                    space_id: Option<u32>,
                    space_name: Option<String>,
                }

                let when = "reading cluster-wide space index info";
                let info = $crate::unwrap_ok_or! {
                    ClusterwideSpaceIndexInfo::lua_read_at_position(&lua, index),
                    Err((_, err)) => {
                        let err = err.when(when).expected("Lua table");
                        return Err((lua, err));
                    }
                };

                let index_name = if let Some(index_name) = info.name {
                    index_name
                } else if let Some(index_name) = info.index_name {
                    index_name
                } else {
                    let err = tlua::WrongType::info(when)
                        .expected("field 'name' or 'index_name'")
                        .actual("table with none of them");
                    return Err((lua, err));
                };

                let space_name = if let Some(space_name) = info.space_name {
                    space_name
                } else if let Some(space_id) = info.space_id {
                    let space = unsafe { Space::from_id_unchecked(space_id) };
                    let meta = $crate::unwrap_ok_or! { space.meta(),
                        Err(err) => {
                            let err = tlua::WrongType::info(when)
                                .expected("valid space id")
                                .actual(format!("error: {err}"));
                            return Err((lua, err));
                        }
                    };
                    meta.name.into()
                } else {
                    let err = tlua::WrongType::info(when)
                        .expected("field 'space_name' or 'space_id'")
                        .actual("table with none of them");
                    return Err((lua, err));
                };

                match &*space_name {
                    $(
                        $cw_space_name => {
                            let index = match &*index_name {
                                $index_name_pk => $index::$index_var_pk,
                                $( $index_name => $index::$index_var, )*
                                unknown_index => {
                                    let err = tlua::WrongType::info(when)
                                        .expected(format!("one of {:?}", $index_of::<$space>::values()))
                                        .actual(unknown_index);
                                    return Err((lua, err))
                                }
                            };
                            Ok(Self::$cw_space_var(index))
                        }
                    )+
                    unknown_space => {
                        let err = tlua::WrongType::info(when)
                            .expected(format!("one of {:?}", $cw_space::values()))
                            .actual(unknown_space);
                        return Err((lua, err))
                    }
                }
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // Clusterwide
        $(#[$cw_struct_meta])*
        #[derive(Clone, Debug)]
        pub struct $cw_struct {
            $( pub $cw_field: $space, )+
        }

        impl $cw_struct {
            #[inline(always)]
            pub fn new() -> tarantool::Result<Self> {
                Ok(Self { $( $cw_field: $space::new()?, )+ })
            }

            #[inline(always)]
            fn space(&self, space: $cw_space) -> &Space {
                match space.into() {
                    $( $cw_index::$cw_space_var(_) => &self.$cw_field.space, )+
                }
            }

            #[inline(always)]
            fn index(&self, index: impl Into<$cw_index>) -> &Index {
                match index.into() {
                    $(
                        $cw_index::$cw_space_var(idx) => match idx {
                            $index_of::<$space>::$index_var_pk => &self.$cw_field.$index_field_pk,
                            $( $index_of::<$space>::$index_var => &self.$cw_field.$index_field, )*
                        }
                    )+
                }
            }

            /// Apply `cb` to each clusterwide space.
            #[inline(always)]
            pub fn for_each_space(
                &self,
                mut cb: impl FnMut(&Space) -> tarantool::Result<()>,
            ) -> tarantool::Result<()>
            {
                $( cb(&self.$cw_field.space)?; )+
                Ok(())
            }

            pub fn snapshot_data() -> tarantool::Result<SnapshotData> {
                let space_dumps = vec![
                    $( $cw_space::$cw_space_var.dump()?, )+
                ];
                Ok(SnapshotData { space_dumps })
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // Instances, Replicasets, etc.
        $(
            $(#[$space_meta])*
            #[derive(Clone, Debug)]
            pub struct $space {
                $space_field: $space_ty,
                #[allow(unused)]
                $index_field_pk: $index_ty_pk,
                $( $index_field: $index_ty, )*
            }

            ::tarantool::define_str_enum! {
                $(#[$index_meta])*
                pub enum $index {
                    $index_var_pk = $index_name_pk,
                    $( $index_var = $index_name, )*
                }
            }

            impl From<$index> for $cw_index {
                fn from(index: $index) -> Self {
                    Self::$cw_space_var(index)
                }
            }

            impl TClusterwideSpace for $space {
                type Index = $index;
                const SPACE_NAME: &'static str = $cw_space_name;
            }

            impl TClusterwideSpaceIndex for IndexOf<$space> {
                #[inline(always)]
                fn primary() -> Self {
                    Self::$index_var_pk
                }

                #[inline(always)]
                fn is_primary(&self) -> bool {
                    matches!(self, Self::$index_var_pk)
                }
            }
        )+
    }
}

////////////////////////////////////////////////////////////////////////////////
// Clusterwide
////////////////////////////////////////////////////////////////////////////////

define_clusterwide_spaces! {
    pub struct Clusterwide {
        pub #space_name_lower: #space_name_upper,
    }

    /// An enumeration of builtin cluster-wide spaces
    pub enum ClusterwideSpace {
        Instance = "_pico_instance" => {
            Clusterwide::instances;

            /// A struct for accessing storage of all the cluster instances.
            pub struct Instances {
                space: Space,
                #[primary]
                index_instance_id:   Index => InstanceId   = "instance_id",
                index_raft_id:       Index => RaftId       = "raft_id",
                index_replicaset_id: Index => ReplicasetId = "replicaset_id",
            }

            /// An enumeration of indexes defined for instance space.
            #[allow(clippy::enum_variant_names)]
            pub enum SpaceInstanceIndex;
        }
        Address = "_pico_peer_address" => {
            Clusterwide::peer_addresses;

            /// A struct for accessing storage of peer addresses.
            pub struct PeerAddresses {
                space: Space,
                #[primary]
                index: Index => RaftId = "raft_id",
            }

            /// An enumeration of indexes defined for peer address space.
            pub enum SpacePeerAddressIndex;
        }
        Property = "_pico_property" => {
            Clusterwide::properties;

            /// A struct for accessing storage of the cluster-wide key-value properties
            pub struct Properties {
                space: Space,
                #[primary]
                index: Index => Key = "key",
            }

            /// An enumeration of indexes defined for property space.
            pub enum SpacePropertyIndex;
        }
        Replicaset = "_pico_replicaset" => {
            Clusterwide::replicasets;

            /// A struct for accessing replicaset info from storage
            pub struct Replicasets {
                space: Space,
                #[primary]
                index: Index => ReplicasetId = "replicaset_id",
            }

            /// An enumeration of indexes defined for replicaset space.
            pub enum SpaceReplicasetIndex;
        }
        Migration = "_pico_migration" => {
            Clusterwide::migrations;

            pub struct Migrations {
                space: Space,
                #[primary]
                index: Index => Id = "id",
            }

            /// An enumeration of indexes defined for migration space.
            pub enum SpaceMigrationIndex;
        }
        Space = "_pico_space" => {
            Clusterwide::spaces;

            /// A struct for accessing definitions of all the user-defined spaces.
            pub struct Spaces {
                space: Space,
                #[primary]
                index_id: Index => Id = "id",
                index_name: Index => Name = "name",
            }

            /// An enumeration of indexes defined for "_pico_space".
            #[allow(clippy::enum_variant_names)]
            pub enum SpaceSpaceIndex;
        }
        Index = "_pico_index" => {
            Clusterwide::indexes;

            /// A struct for accessing definitions of all the user-defined indexes.
            pub struct Indexes {
                space: Space,
                #[primary]
                index_id: Index => Id = "id",
                index_name: Index => Name = "name",
            }

            /// An enumeration of indexes defined for "_pico_index".
            #[allow(clippy::enum_variant_names)]
            pub enum SpaceIndexIndex;
        }
    }

    /// An index of a clusterwide space.
    pub enum ClusterwideSpaceIndex {
        #space_name(IndexOf<#space_struct_name>),
    }
}

impl Clusterwide {
    pub fn apply_snapshot_data(&self, raw_data: &[u8]) -> tarantool::Result<()> {
        let data = SnapshotData::decode(raw_data)?;
        for space_dump in &data.space_dumps {
            let space = self.space(space_dump.space);
            space.truncate()?;
            let tuples = space_dump.tuples.as_ref();
            for tuple in ValueIter::from_array(tuples)? {
                space.insert(RawBytes::new(tuple))?;
            }
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////
// ClusterwideSpace
////////////////////////////////////////////////////////////////////////////////

impl ClusterwideSpace {
    #[inline]
    pub(crate) fn get(&self) -> tarantool::Result<Space> {
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

    pub fn dump(&self) -> tarantool::Result<SpaceDump> {
        let space = self.get()?;
        let mut array_writer = ArrayWriter::from_vec(Vec::with_capacity(space.bsize()?));
        for t in self.get()?.select(IteratorType::All, &())? {
            array_writer.push_tuple(&t)?;
        }
        let tuples = array_writer.finish()?.into_inner();
        // SAFETY: ArrayWriter guarantees the result is a valid msgpack array
        let tuples = unsafe { TupleBuffer::from_vec_unchecked(tuples) };
        tlog!(
            Info,
            "generated snapshot for clusterwide space '{self}': {} bytes",
            tuples.len()
        );
        Ok(SpaceDump {
            space: *self,
            tuples,
        })
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

impl ClusterwideSpaceIndex {
    #[inline]
    pub(crate) fn get(&self) -> tarantool::Result<Index> {
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

    /// Return reference to a cached instance of `KeyDef` to be used for
    /// comparing **tuples** of the corresponding cluster-wide space.
    pub(crate) fn key_def(&self) -> tarantool::Result<&'static KeyDef> {
        static mut KEY_DEF: Option<HashMap<ClusterwideSpaceIndex, KeyDef>> = None;
        let key_defs = unsafe { KEY_DEF.get_or_insert_with(HashMap::new) };
        let key_def = match key_defs.entry(*self) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let key_def = self.get()?.meta()?.to_key_def();
                v.insert(key_def)
            }
        };
        Ok(key_def)
    }

    /// Return reference to a cached instance of `KeyDef` to be used for
    /// comparing **keys** of the corresponding cluster-wide space.
    pub(crate) fn key_def_for_key(&self) -> tarantool::Result<&'static KeyDef> {
        static mut KEY_DEF: Option<HashMap<ClusterwideSpaceIndex, KeyDef>> = None;
        let key_defs = unsafe { KEY_DEF.get_or_insert_with(HashMap::new) };
        let key_def = match key_defs.entry(*self) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let key_def = self.get()?.meta()?.to_key_def_for_key();
                v.insert(key_def)
            }
        };
        Ok(key_def)
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

////////////////////////////////////////////////////////////////////////////////
// PropertyName
////////////////////////////////////////////////////////////////////////////////

::tarantool::define_str_enum! {
    /// An enumeration of [`ClusterwideSpace::Property`] key names.
    pub enum PropertyName {
        ReplicationFactor = "replication_factor",
        VshardBootstrapped = "vshard_bootstrapped",
        DesiredSchemaVersion = "desired_schema_version",
        PendingSchemaChange = "pending_schema_change",
        PendingSchemaVersion = "pending_schema_version",
        CurrentSchemaVersion = "current_schema_version",
    }
}

////////////////////////////////////////////////////////////////////////////////
// Properties
////////////////////////////////////////////////////////////////////////////////

impl Properties {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .field(("key", FieldType::String))
            .field(("value", FieldType::Any))
            .if_not_exists(true)
            .create()?;

        let index = space
            .index_builder(Self::primary_index().as_str())
            .unique(true)
            .part("key")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, index })
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

    #[inline]
    pub fn put(&self, key: PropertyName, value: &impl serde::Serialize) -> tarantool::Result<()> {
        self.space.put(&(key, value))?;
        Ok(())
    }

    #[inline]
    pub fn delete(&self, key: PropertyName) -> tarantool::Result<()> {
        self.space.delete(&[key])?;
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

    #[inline]
    pub fn pending_schema_change(&self) -> tarantool::Result<Option<Ddl>> {
        self.get(PropertyName::PendingSchemaChange)
    }

    #[inline]
    pub fn pending_schema_version(&self) -> tarantool::Result<Option<u64>> {
        self.get(PropertyName::PendingSchemaVersion)
    }

    #[inline]
    pub fn current_schema_version(&self) -> tarantool::Result<u64> {
        let res = self
            .get(PropertyName::CurrentSchemaVersion)?
            .unwrap_or_default();
        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Replicasets
////////////////////////////////////////////////////////////////////////////////

impl Replicasets {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .format(Replicaset::format())
            .if_not_exists(true)
            .create()?;

        let index = space
            .index_builder(Self::primary_index().as_str())
            .unique(true)
            .part("replicaset_id")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, index })
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

impl PeerAddresses {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .field(("raft_id", FieldType::Unsigned))
            .field(("address", FieldType::String))
            .if_not_exists(true)
            .create()?;

        let index = space
            .index_builder(Self::primary_index().as_str())
            .unique(true)
            .part("raft_id")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, index })
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

    /// Find a instance by `id` (see trait [`InstanceId`]).
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

impl Migrations {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .field(("id", FieldType::Unsigned))
            .field(("body", FieldType::String))
            .if_not_exists(true)
            .create()?;

        let index = space
            .index_builder(Self::primary_index().as_str())
            .unique(true)
            .part("id")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, index })
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
// SnapshotData
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct SnapshotData {
    pub space_dumps: Vec<SpaceDump>,
}
impl Encode for SnapshotData {}

#[derive(Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct SpaceDump {
    pub space: ClusterwideSpace,
    #[serde(with = "serde_bytes")]
    pub tuples: TupleBuffer,
}
impl Encode for SpaceDump {}

////////////////////////////////////////////////////////////////////////////////
// Spaces
////////////////////////////////////////////////////////////////////////////////

impl Spaces {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .field(("id", FieldType::Unsigned))
            .field(("name", FieldType::String))
            .field(("distribution", FieldType::Array))
            .field(("format", FieldType::Array))
            .field(("schema_version", FieldType::Unsigned))
            .field(("operable", FieldType::Boolean))
            .if_not_exists(true)
            .create()?;

        let index_id = space
            .index_builder(IndexOf::<Self>::Id.as_str())
            .unique(true)
            .part("id")
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder(IndexOf::<Self>::Name.as_str())
            .unique(true)
            .part("name")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space,
            index_id,
            index_name,
        })
    }

    #[inline]
    pub fn get(&self, id: SpaceId) -> tarantool::Result<Option<SpaceDef>> {
        match self.space.get(&[id])? {
            Some(tuple) => tuple.decode().map(Some),
            None => Ok(None),
        }
    }

    #[inline]
    pub fn put(&self, space_def: &SpaceDef) -> tarantool::Result<()> {
        self.space.replace(space_def)?;
        Ok(())
    }

    #[inline]
    pub fn insert(&self, space_def: &SpaceDef) -> tarantool::Result<()> {
        self.space.insert(space_def)?;
        Ok(())
    }

    #[inline]
    pub fn update_operable(&self, id: SpaceId, operable: bool) -> tarantool::Result<()> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(SpaceDef::FIELD_OPERABLE, operable)?;
        self.space.update(&[id], ops)?;
        Ok(())
    }

    #[inline]
    pub fn delete(&self, id: SpaceId) -> tarantool::Result<Option<Tuple>> {
        self.space.delete(&[id])
    }

    #[inline]
    pub fn by_name(&self, name: String) -> tarantool::Result<Option<SpaceDef>> {
        match self.index_name.get(&[name])? {
            Some(tuple) => tuple.decode().map(Some),
            None => Ok(None),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Indexes
////////////////////////////////////////////////////////////////////////////////

impl Indexes {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .is_local(true)
            .is_temporary(false)
            .field(("space_id", FieldType::Unsigned))
            .field(("id", FieldType::Unsigned))
            .field(("name", FieldType::String))
            .field(("local", FieldType::Boolean))
            .field(("parts", FieldType::Array))
            .field(("schema_version", FieldType::Unsigned))
            .field(("operable", FieldType::Boolean))
            .if_not_exists(true)
            .create()?;

        let index_id = space
            .index_builder(IndexOf::<Self>::Id.as_str())
            .unique(true)
            .part("space_id")
            .part("id")
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder(IndexOf::<Self>::Name.as_str())
            .unique(true)
            .part("space_id")
            .part("name")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space,
            index_id,
            index_name,
        })
    }

    #[inline]
    pub fn get(&self, space_id: SpaceId, index_id: IndexId) -> tarantool::Result<Option<IndexDef>> {
        match self.space.get(&(space_id, index_id))? {
            Some(tuple) => tuple.decode().map(Some),
            None => Ok(None),
        }
    }

    #[inline]
    pub fn put(&self, index_def: &IndexDef) -> tarantool::Result<()> {
        self.space.replace(index_def)?;
        Ok(())
    }

    #[inline]
    pub fn insert(&self, index_def: &IndexDef) -> tarantool::Result<()> {
        self.space.insert(index_def)?;
        Ok(())
    }

    #[inline]
    pub fn update_operable(
        &self,
        space_id: SpaceId,
        index_id: IndexId,
        operable: bool,
    ) -> tarantool::Result<()> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(IndexDef::FIELD_OPERABLE, operable)?;
        self.space.update(&(space_id, index_id), ops)?;
        Ok(())
    }

    #[inline]
    pub fn delete(&self, space_id: SpaceId, index_id: IndexId) -> tarantool::Result<Option<Tuple>> {
        self.space.delete(&[space_id, index_id])
    }

    #[inline]
    pub fn by_name(&self, space_id: SpaceId, name: String) -> tarantool::Result<Option<IndexDef>> {
        match self.index_name.get(&(space_id, name))? {
            Some(tuple) => tuple.decode().map(Some),
            None => Ok(None),
        }
    }
}

pub fn pico_schema_version() -> tarantool::Result<u64> {
    let space_schema = Space::from(SystemSpace::Schema);
    let tuple = space_schema.get(&["pico_schema_version"])?;
    let mut res = 0;
    if let Some(tuple) = tuple {
        if let Some(v) = tuple.field(1)? {
            res = v;
        }
    }
    Ok(res)
}

pub fn set_pico_schema_version(v: u64) -> tarantool::Result<()> {
    let space_schema = Space::from(SystemSpace::Schema);
    space_schema.insert(&("pico_schema_version", v))?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

macro_rules! assert_err {
    ($expr:expr, $err:expr) => {
        assert_eq!($expr.unwrap_err().to_string(), $err)
    };
}

mod tests {
    use super::*;

    #[rustfmt::skip]
    #[::tarantool::test]
    fn test_storage_instances() {
        use crate::instance::grade::{CurrentGradeVariant as CGV, TargetGradeVariant as TGV};
        use crate::instance::InstanceId;
        use crate::failure_domain::FailureDomain;

        let storage = Clusterwide::new().unwrap();
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
            storage.space(ClusterwideSpace::Instance).put(&instance).unwrap();
            let (_, _, raft_id, ..) = instance;
            space_peer_addresses.put(&(raft_id, format!("addr:{raft_id}"))).unwrap();
        }

        let instance = storage.instances.all_instances().unwrap();
        assert_eq!(
            instance.iter().map(|p| &p.instance_id).collect::<Vec<_>>(),
            vec!["i1", "i2", "i3", "i4", "i5"]
        );

        assert_err!(
            storage.instances.put(&Instance {
                raft_id: 1,
                instance_id: "i99".into(),
                ..Instance::default()
            }),
            format!(
                concat!(
                    "Tarantool error:",
                    " TupleFound: Duplicate key exists",
                    " in unique index \"raft_id\"",
                    " in space \"_pico_instance\"",
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
            assert_eq!(storage.instances.get(&1).unwrap().instance_id, "i1");
            assert_eq!(storage.instances.get(&2).unwrap().instance_id, "i2");
            assert_eq!(storage.instances.get(&3).unwrap().instance_id, "i3");
            assert_eq!(storage.instances.get(&4).unwrap().instance_id, "i4");
            assert_eq!(storage.instances.get(&5).unwrap().instance_id, "i5");
            assert_err!(storage.instances.get(&6), "instance with id 6 not found");
        }

        {
            // Check accessing instances by 'instance_id'
            assert_eq!(storage.instances.get(&InstanceId::from("i1")).unwrap().raft_id, 1);
            assert_eq!(storage.instances.get(&InstanceId::from("i2")).unwrap().raft_id, 2);
            assert_eq!(storage.instances.get(&InstanceId::from("i3")).unwrap().raft_id, 3);
            assert_eq!(storage.instances.get(&InstanceId::from("i4")).unwrap().raft_id, 4);
            assert_eq!(storage.instances.get(&InstanceId::from("i5")).unwrap().raft_id, 5);
            assert_err!(
                storage.instances.get(&InstanceId::from("i6")),
                "instance with id \"i6\" not found"
            );
        }

        let box_replication = |replicaset_id: &str| -> Vec<traft::Address> {
            storage.instances.replicaset_instances(replicaset_id).unwrap()
                .map(|instance| storage_peer_addresses.try_get(instance.raft_id).unwrap())
                .collect::<Vec<_>>()
        };

        {
            assert_eq!(box_replication("XX"), Vec::<&str>::new());
            assert_eq!(box_replication("r1"), ["addr:1", "addr:2"]);
            assert_eq!(box_replication("r2"), ["addr:3", "addr:4"]);
            assert_eq!(box_replication("r3"), ["addr:5"]);
        }

        storage.index(IndexOf::<Instances>::RaftId).drop().unwrap();

        assert_err!(
            storage.instances.get(&1),
            concat!(
                "Tarantool error: NoSuchIndexID: No index #1 is defined",
                " in space '_pico_instance'",
            )
        );

        storage.index(IndexOf::<Instances>::ReplicasetId).drop().unwrap();

        assert_err!(
            storage.instances.replicaset_instances(""),
            concat!(
                "Tarantool error: NoSuchIndexID: No index #2 is defined",
                " in space '_pico_instance'",
            )
        );

        storage.index(IndexOf::<Instances>::InstanceId).drop().unwrap();

        assert_err!(
            storage.instances.get(&InstanceId::from("i1")),
            concat!(
                "Tarantool error: NoSuchIndexID: No index #0 is defined",
                " in space '_pico_instance'",
            )
        );

        let space = storage.space(ClusterwideSpace::Instance);
        space.drop().unwrap();

        assert_err!(
            storage.instances.all_instances(),
            format!(
                "Tarantool error: NoSuchSpace: Space '{}' does not exist",
                space.id(),
            )
        );
    }

    #[rustfmt::skip]
    #[::tarantool::test]
    fn test_storage_migrations() {
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

    #[::tarantool::test]
    fn clusterwide_space_index() {
        let storage = Clusterwide::new().unwrap();

        storage
            .space(ClusterwideSpace::Address)
            .insert(&(1, "foo"))
            .unwrap();
        storage
            .space(ClusterwideSpace::Address)
            .insert(&(2, "bar"))
            .unwrap();

        assert_eq!(storage.peer_addresses.get(1).unwrap().unwrap(), "foo");
        assert_eq!(storage.peer_addresses.get(2).unwrap().unwrap(), "bar");

        let inst = |raft_id, instance_id: &str| Instance {
            raft_id,
            instance_id: instance_id.into(),
            ..Instance::default()
        };
        storage.instances.put(&inst(1, "bob")).unwrap();

        let t = storage
            .index(IndexOf::<Instances>::RaftId)
            .get(&[1])
            .unwrap()
            .unwrap();
        let i: Instance = t.decode().unwrap();
        assert_eq!(i.raft_id, 1);
        assert_eq!(i.instance_id, "bob");
    }

    #[::tarantool::test]
    fn snapshot_data() {
        let storage = Clusterwide::new().unwrap();
        storage.for_each_space(|s| s.truncate()).unwrap();

        let i = Instance {
            raft_id: 1,
            instance_id: "i".into(),
            ..Instance::default()
        };
        storage.instances.put(&i).unwrap();

        storage
            .peer_addresses
            .space
            .insert(&(1, "google.com"))
            .unwrap();
        storage.peer_addresses.space.insert(&(2, "ya.ru")).unwrap();

        storage.properties.space.insert(&("foo", "bar")).unwrap();

        let r = Replicaset {
            replicaset_id: "r1".into(),
            replicaset_uuid: "r1-uuid".into(),
            master_id: "i".into(),
            weight: crate::replicaset::weight::Info::default(),
            current_schema_version: 0,
        };
        storage.replicasets.space.insert(&r).unwrap();

        storage
            .migrations
            .space
            .insert(&(1, "drop table BANK_ACCOUNTS"))
            .unwrap();

        let snapshot_data = Clusterwide::snapshot_data().unwrap();
        let space_dumps = snapshot_data.space_dumps;

        assert_eq!(space_dumps.len(), 7);

        for space_dump in &space_dumps {
            match space_dump.space {
                ClusterwideSpace::Instance => {
                    let [instance]: [Instance; 1] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(instance, i);
                }

                ClusterwideSpace::Address => {
                    let addrs: [(i32, String); 2] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(
                        addrs,
                        [(1, "google.com".to_string()), (2, "ya.ru".to_string())]
                    );
                }

                ClusterwideSpace::Property => {
                    let [property]: [(String, String); 1] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(property, ("foo".to_owned(), "bar".to_owned()));
                }

                ClusterwideSpace::Replicaset => {
                    let [replicaset]: [Replicaset; 1] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(replicaset, r);
                }

                ClusterwideSpace::Migration => {
                    let [migration]: [(i32, String); 1] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(migration, (1, "drop table BANK_ACCOUNTS".to_owned()));
                }

                ClusterwideSpace::Space => {
                    let []: [(); 0] = Decode::decode(space_dump.tuples.as_ref()).unwrap();
                }

                ClusterwideSpace::Index => {
                    let []: [(); 0] = Decode::decode(space_dump.tuples.as_ref()).unwrap();
                }
            }
        }
    }

    #[::tarantool::test]
    fn apply_snapshot_data() {
        let storage = Clusterwide::new().unwrap();

        let mut data = SnapshotData {
            space_dumps: vec![],
        };

        let i = Instance {
            raft_id: 1,
            instance_id: "i".into(),
            ..Instance::default()
        };
        let tuples = [&i].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space: ClusterwideSpace::Instance,
            tuples,
        });

        let tuples = [(1, "google.com"), (2, "ya.ru")].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space: ClusterwideSpace::Address,
            tuples,
        });

        let tuples = [("foo", "bar")].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space: ClusterwideSpace::Property,
            tuples,
        });

        let r = Replicaset {
            replicaset_id: "r1".into(),
            replicaset_uuid: "r1-uuid".into(),
            master_id: "i".into(),
            weight: crate::replicaset::weight::Info::default(),
            current_schema_version: 0,
        };
        let tuples = [&r].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space: ClusterwideSpace::Replicaset,
            tuples,
        });

        let m = Migration {
            id: 1,
            body: "drop table BANK_ACCOUNTS".into(),
        };
        let tuples = [&m].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space: ClusterwideSpace::Migration,
            tuples,
        });

        let raw_data = data.to_tuple_buffer().unwrap();

        storage.for_each_space(|s| s.truncate()).unwrap();
        if let Err(e) = storage.apply_snapshot_data(raw_data.as_ref()) {
            println!("{e}");
            panic!();
        }

        assert_eq!(storage.instances.space.len().unwrap(), 1);
        let instance = storage.instances.get(&1).unwrap();
        assert_eq!(instance, i);

        assert_eq!(storage.peer_addresses.space.len().unwrap(), 2);
        let addr = storage.peer_addresses.get(1).unwrap().unwrap();
        assert_eq!(addr, "google.com");
        let addr = storage.peer_addresses.get(2).unwrap().unwrap();
        assert_eq!(addr, "ya.ru");

        assert_eq!(storage.replicasets.space.len().unwrap(), 1);
        let replicaset = storage.replicasets.get("r1").unwrap().unwrap();
        assert_eq!(replicaset, r);

        assert_eq!(storage.migrations.space.len().unwrap(), 1);
        let migration = storage.migrations.get(1).unwrap().unwrap();
        assert_eq!(migration, m);
    }
}

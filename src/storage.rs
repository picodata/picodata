use ::tarantool::error::Error as TntError;
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
use crate::schema::{Distribution, IndexDef, SpaceDef};
use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::op::Ddl;
use crate::traft::RaftId;
use crate::traft::Result;

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::rc::Rc;

macro_rules! define_clusterwide_spaces {
    (
        $(#[$Clusterwide_meta:meta])*
        pub struct $Clusterwide:ident {
            pub #space_name_lower: #space_name_upper,
        }

        $(#[$ClusterwideSpaceId_meta:meta])*
        pub enum $ClusterwideSpaceId:ident {
            #space_name_upper = #space_id,
        }

        $(#[$ClusterwideSpace_meta:meta])*
        pub enum $ClusterwideSpace:ident {
            $(
                $(#[$cw_field_meta:meta])*
                $cw_space_var:ident = $cw_space_id_value:expr, $cw_space_name:expr => {
                    $_Clusterwide:ident :: $Clusterwide_field:ident;

                    $(#[$space_struct_meta:meta])*
                    pub struct $space_struct:ident {
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

        $(#[$ClusterwideSpaceIndex_meta:meta])*
        pub enum $ClusterwideSpaceIndex:ident {
            #space_name( $index_of:ident <#space_struct_name>),
        }
    ) => {
        ////////////////////////////////////////////////////////////////////////
        // ClusterwideSpace
        ::tarantool::define_str_enum! {
            $(#[$ClusterwideSpace_meta])*
            pub enum $ClusterwideSpace {
                $(
                    $(#[$cw_field_meta])*
                    $cw_space_var = $cw_space_name,
                )+
            }
        }

        $(#[$ClusterwideSpaceId_meta])*
        pub enum $ClusterwideSpaceId {
            $( $cw_space_var = $cw_space_id_value, )+
        }

        impl From<$ClusterwideSpaceId> for $ClusterwideSpace {
            fn from(space_id: $ClusterwideSpaceId) -> Self {
                match space_id {
                    $( $ClusterwideSpaceId::$cw_space_var => Self::$cw_space_var, )+
                }
            }
        }

        impl From<$ClusterwideSpace> for $ClusterwideSpaceId {
            fn from(space_name: $ClusterwideSpace) -> Self {
                match space_name {
                    $(
                        $ClusterwideSpace::$cw_space_var => Self::$cw_space_var,
                    )+
                }
            }
        }

        impl $ClusterwideSpaceId {
            #[inline(always)]
            pub const fn name(&self) -> &'static str {
                match self {
                    $( Self::$cw_space_var => $cw_space_name, )+
                }
            }
        }

        impl TryFrom<SpaceId> for $ClusterwideSpaceId {
            // TODO: conform to bureaucracy
            type Error = ();

            #[inline(always)]
            fn try_from(id: SpaceId) -> ::std::result::Result<$ClusterwideSpaceId, Self::Error> {
                match id {
                    $( $cw_space_id_value => Ok(Self::$cw_space_var), )+
                    _ => Err(()),
                }
            }
        }

        impl $ClusterwideSpace {
            pub fn primary_index(&self) -> $ClusterwideSpaceIndex {
                match self {
                    $(
                        $ClusterwideSpace::$cw_space_var => $ClusterwideSpaceIndex::$cw_space_var(<$space_struct>::primary_index()),
                    )+
                }
            }
        }

        $( const _: $crate::util::CheckIsSameType<$_Clusterwide, $Clusterwide> = (); )+

        ////////////////////////////////////////////////////////////////////////
        // ClusterwideSpaceIndex
        $(#[$ClusterwideSpaceIndex_meta])*
        #[derive(Copy, Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
        pub enum $ClusterwideSpaceIndex {
            $( $cw_space_var($index_of<$space_struct>),)+
        }

        impl From<$ClusterwideSpace> for $ClusterwideSpaceIndex {
            fn from(space: $ClusterwideSpace) -> Self {
                space.primary_index()
            }
        }

        impl $ClusterwideSpaceIndex {
            pub const fn space(&self) -> $ClusterwideSpace {
                match self {
                    $(
                        Self::$cw_space_var(_) => $ClusterwideSpace::$cw_space_var,
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

        impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::LuaRead<L> for $ClusterwideSpaceIndex {
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
                                        .expected(format!("one of {:?}", $index_of::<$space_struct>::values()))
                                        .actual(unknown_index);
                                    return Err((lua, err))
                                }
                            };
                            Ok(Self::$cw_space_var(index))
                        }
                    )+
                    unknown_space => {
                        let err = tlua::WrongType::info(when)
                            .expected(format!("one of {:?}", $ClusterwideSpace::values()))
                            .actual(unknown_space);
                        return Err((lua, err))
                    }
                }
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // Clusterwide
        $(#[$Clusterwide_meta])*
        #[derive(Clone, Debug)]
        pub struct $Clusterwide {
            $( pub $Clusterwide_field: $space_struct, )+
        }

        impl $Clusterwide {
            #[inline(always)]
            pub fn new() -> tarantool::Result<Self> {
                Ok(Self { $( $Clusterwide_field: $space_struct::new()?, )+ })
            }

            #[inline(always)]
            fn space_by_name(&self, space: impl AsRef<str>) -> tarantool::Result<Space> {
                let space = space.as_ref();
                match space {
                    $( $cw_space_name => Ok(self.$Clusterwide_field.space.clone()), )+
                    _ => space_by_name(space),

                }
            }

            #[inline(always)]
            fn index(&self, index: impl Into<$ClusterwideSpaceIndex>) -> &Index {
                match index.into() {
                    $(
                        $ClusterwideSpaceIndex::$cw_space_var(idx) => match idx {
                            $index_of::<$space_struct>::$index_var_pk => &self.$Clusterwide_field.$index_field_pk,
                            $( $index_of::<$space_struct>::$index_var => &self.$Clusterwide_field.$index_field, )*
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
                $( cb(&self.$Clusterwide_field.space)?; )+
                Ok(())
            }

            #[inline(always)]
            pub fn internal_space_dumps() -> tarantool::Result<Vec<SpaceDump>> {
                let res = vec![
                    $( Self::space_dump(&$ClusterwideSpace::$cw_space_var)?, )+
                ];
                Ok(res)
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // Instances, Replicasets, etc.
        $(
            $(#[$space_struct_meta])*
            #[derive(Clone, Debug)]
            pub struct $space_struct {
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

            impl From<$index> for $ClusterwideSpaceIndex {
                fn from(index: $index) -> Self {
                    Self::$cw_space_var(index)
                }
            }

            impl TClusterwideSpace for $space_struct {
                type Index = $index;
                const SPACE_NAME: &'static str = $cw_space_name;
                const SPACE_ID: SpaceId = $cw_space_id_value;
            }

            impl TClusterwideSpaceIndex for IndexOf<$space_struct> {
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

fn space_by_id(space_id: SpaceId) -> tarantool::Result<Space> {
    let space = unsafe { Space::from_id_unchecked(space_id) };
    // TODO: maybe we should verify the space exists, but it's not a big deal
    // currently, because first of all tarantool api will just return a no such
    // space id error from any box_* function, and secondly this function is
    // only used internally for now anyway.
    Ok(space)
}

pub fn space_by_name(space_name: &str) -> tarantool::Result<Space> {
    let space = Space::find(space_name).ok_or_else(|| {
        tarantool::set_error!(
            tarantool::error::TarantoolErrorCode::NoSuchSpace,
            "no such space \"{}\"",
            space_name
        );
        tarantool::error::TarantoolError::last()
    })?;
    Ok(space)
}

////////////////////////////////////////////////////////////////////////////////
// Clusterwide
////////////////////////////////////////////////////////////////////////////////

define_clusterwide_spaces! {
    pub struct Clusterwide {
        pub #space_name_lower: #space_name_upper,
    }

    /// An enumeration of system clusterwide spaces' ids.
    #[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
    pub enum ClusterwideSpaceId {
        #space_name_upper = #space_id,
    }

    /// An enumeration of builtin cluster-wide spaces
    // TODO: maybe rename ClusterwideSpaceName
    pub enum ClusterwideSpace {
        Instance = 515, "_pico_instance" => {
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
        Address = 514, "_pico_peer_address" => {
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
        Property = 516, "_pico_property" => {
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
        Replicaset = 517, "_pico_replicaset" => {
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
        Space = 512, "_pico_space" => {
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
        Index = 513, "_pico_index" => {
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
    // This doesn't have `&self` parameter, because it is called from
    // RaftSpaceAccess, which doesn't have a reference to Node at least
    // for now.
    pub fn snapshot_data() -> tarantool::Result<SnapshotData> {
        let mut space_dumps = Self::internal_space_dumps()?;

        let pico_space = space_by_name(&ClusterwideSpace::Space)?;
        let iter = pico_space.select(IteratorType::All, &())?;
        for tuple in iter {
            let space_def: SpaceDef = tuple.decode()?;
            if !matches!(space_def.distribution, Distribution::Global) {
                continue;
            }
            space_dumps.push(Self::space_dump(&space_def.name)?);
        }

        let pico_property = space_by_id(ClusterwideSpaceId::Property.value())?;
        let tuple = pico_property.get(&[PropertyName::GlobalSchemaVersion.as_str()])?;
        let mut schema_version = 0;
        if let Some(tuple) = tuple {
            if let Some(v) = tuple.field(1)? {
                schema_version = v;
            }
        }

        Ok(SnapshotData {
            schema_version,
            space_dumps,
        })
    }

    fn space_dump(space_name: &str) -> tarantool::Result<SpaceDump> {
        let space = space_by_name(space_name)?;
        let mut array_writer = ArrayWriter::from_vec(Vec::with_capacity(space.bsize()?));
        for t in space.select(IteratorType::All, &())? {
            array_writer.push_tuple(&t)?;
        }
        let tuples = array_writer.finish()?.into_inner();
        // SAFETY: ArrayWriter guarantees the result is a valid msgpack array
        let tuples = unsafe { TupleBuffer::from_vec_unchecked(tuples) };
        tlog!(
            Info,
            "generated snapshot for clusterwide space '{space_name}': {} bytes",
            tuples.len()
        );
        Ok(SpaceDump {
            space_name: space_name.into(),
            tuples,
        })
    }

    /// Applies contents of the raft snapshot. This includes
    /// * Contents of internal clusterwide spaces.
    /// * If `is_master` is `true`, definitions of user-defined global spaces.
    /// * If `is_master` is `true`, contents of user-defined global spaces.
    ///
    /// This function is called from [`NodeImpl::advance`] when a snapshot was
    /// received from the current raft leader and it was determined that the
    /// snapshot should be applied.
    ///
    /// This should be called within a transaction.
    ///
    /// [`NodeImpl::advance`]: crate::traft::node::NodeImpl::advance
    pub fn apply_snapshot_data(&self, data: &SnapshotData, is_master: bool) -> Result<()> {
        debug_assert!(unsafe { ::tarantool::ffi::tarantool::box_txn() });

        // We need to save these before truncating _pico_space.
        let mut old_space_versions: HashMap<SpaceId, u64> = HashMap::new();
        for space_def in self.spaces.iter()? {
            old_space_versions.insert(space_def.id, space_def.schema_version);
        }

        let mut dont_exist_yet = Vec::new();
        for space_dump in &data.space_dumps {
            let space_name = &space_dump.space_name;
            let Ok(space) = self.space_by_name(space_name) else {
                dont_exist_yet.push(space_dump);
                continue;
            };
            space.truncate()?;
            let tuples = space_dump.tuples.as_ref();
            for tuple in ValueIter::from_array(tuples).map_err(TntError::from)? {
                space.insert(RawBytes::new(tuple))?;
            }
        }

        // If we're not the replication master, the rest of the data will come
        // via tarantool replication.
        if is_master {
            self.apply_ddl_changes_on_replicaset_master(&old_space_versions)?;
            set_local_schema_version(data.schema_version)?;
        }

        // These are likely globally distributed user-defined spaces, which
        // just got defined from the metadata which arrived in the _pico_space
        // dump.
        for space_dump in &dont_exist_yet {
            let space_name = &space_dump.space_name;
            let Ok(space) = self.space_by_name(space_name) else {
                crate::warn_or_panic!("a dump for a non existent space '{}' arrived via snapshot", space_name);
                continue;
            };
            space.truncate()?;
            let tuples = space_dump.tuples.as_ref();
            for tuple in ValueIter::from_array(tuples).map_err(TntError::from)? {
                space.insert(RawBytes::new(tuple))?;
            }
        }

        Ok(())
    }

    pub fn apply_ddl_changes_on_replicaset_master(
        &self,
        old_space_versions: &HashMap<SpaceId, u64>,
    ) -> traft::Result<()> {
        debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

        let mut space_defs = Vec::new();
        let mut new_space_ids = HashSet::new();
        for space_def in self.spaces.iter()? {
            new_space_ids.insert(space_def.id);
            space_defs.push(space_def);
        }

        // First we drop all the spaces which have been dropped.
        for &old_space_id in old_space_versions.keys() {
            if new_space_ids.contains(&old_space_id) {
                // Will be handled later.
                continue;
            }

            // Space was dropped.
            let abort_reason = ddl_drop_space_on_master(old_space_id)?;
            if let Some(e) = abort_reason {
                return Err(Error::other(format!(
                    "failed to drop space {old_space_id}: {e}"
                )));
            }
        }

        // Now create any new spaces, or replace ones that changed.
        for space_def in &space_defs {
            let space_id = space_def.id;

            if let Some(&v_old) = old_space_versions.get(&space_id) {
                let v_new = space_def.schema_version;
                assert!(v_old <= v_new);

                if v_old == v_new {
                    // Space is up to date.
                    continue;
                }

                // Space was updated, need to drop it and recreate.
                let abort_reason = ddl_drop_space_on_master(space_def.id)?;
                if let Some(e) = abort_reason {
                    return Err(Error::other(format!(
                        "failed to drop space {space_id}: {e}"
                    )));
                }
            } else {
                // New space.
            }

            if !space_def.operable {
                // If it so happens, that we receive an unfinished schema change via snapshot,
                // which will somehow get finished without the governor sending us
                // a proc_apply_schema_change rpc, it will be a very sad day.
                continue;
            }

            let abort_reason = ddl_create_space_on_master(self, space_def.id)?;
            if let Some(e) = abort_reason {
                return Err(Error::other(format!(
                    "failed to create space {}: {e}",
                    space_def.id
                )));
            }
        }

        // TODO: secondary indexes

        Ok(())
    }

    /// Return a `KeyDef` to be used for comparing **tuples** of the
    /// corresponding global space.
    pub(crate) fn key_def(
        &self,
        space_id: SpaceId,
        index_id: IndexId,
    ) -> tarantool::Result<Rc<KeyDef>> {
        static mut KEY_DEF: Option<HashMap<(ClusterwideSpaceId, IndexId), Rc<KeyDef>>> = None;
        let key_defs = unsafe { KEY_DEF.get_or_insert_with(HashMap::new) };
        if let Ok(sys_space_id) = ClusterwideSpaceId::try_from(space_id) {
            let key_def = match key_defs.entry((sys_space_id, index_id)) {
                Entry::Occupied(o) => o.into_mut(),
                Entry::Vacant(v) => {
                    let index = unsafe { Index::from_ids_unchecked(space_id, index_id) };
                    let key_def = index.meta()?.to_key_def();
                    // System space definition's never change during a single
                    // execution, so it's safe to cache these
                    v.insert(Rc::new(key_def))
                }
            };
            return Ok(key_def.clone());
        }

        let index = unsafe { Index::from_ids_unchecked(space_id, index_id) };
        let key_def = index.meta()?.to_key_def();
        Ok(Rc::new(key_def))
    }

    /// Return a `KeyDef` to be used for comparing **keys** of the
    /// corresponding global space.
    pub(crate) fn key_def_for_key(
        &self,
        space_id: SpaceId,
        index_id: IndexId,
    ) -> tarantool::Result<Rc<KeyDef>> {
        static mut KEY_DEF: Option<HashMap<(ClusterwideSpaceId, IndexId), Rc<KeyDef>>> = None;
        let key_defs = unsafe { KEY_DEF.get_or_insert_with(HashMap::new) };
        if let Ok(sys_space_id) = ClusterwideSpaceId::try_from(space_id) {
            let key_def = match key_defs.entry((sys_space_id, index_id)) {
                Entry::Occupied(o) => o.into_mut(),
                Entry::Vacant(v) => {
                    let index = unsafe { Index::from_ids_unchecked(space_id, index_id) };
                    let key_def = index.meta()?.to_key_def_for_key();
                    // System space definition's never change during a single
                    // execution, so it's safe to cache these
                    v.insert(Rc::new(key_def))
                }
            };
            return Ok(key_def.clone());
        }

        let index = unsafe { Index::from_ids_unchecked(space_id, index_id) };
        let key_def = index.meta()?.to_key_def_for_key();
        Ok(Rc::new(key_def))
    }

    pub(crate) fn insert(
        &self,
        space_id: SpaceId,
        tuple: &TupleBuffer,
    ) -> tarantool::Result<Tuple> {
        let space = space_by_id(space_id)?;
        let res = space.insert(tuple)?;
        Ok(res)
    }

    pub(crate) fn replace(
        &self,
        space_id: SpaceId,
        tuple: &TupleBuffer,
    ) -> tarantool::Result<Tuple> {
        let space = space_by_id(space_id)?;
        let res = space.replace(tuple)?;
        Ok(res)
    }

    pub(crate) fn update(
        &self,
        space_id: SpaceId,
        key: &TupleBuffer,
        ops: &[TupleBuffer],
    ) -> tarantool::Result<Option<Tuple>> {
        let space = space_by_id(space_id)?;
        let res = space.update(key, ops)?;
        Ok(res)
    }

    pub(crate) fn delete(
        &self,
        space_id: SpaceId,
        key: &TupleBuffer,
    ) -> tarantool::Result<Option<Tuple>> {
        let space = space_by_id(space_id)?;
        let res = space.delete(key)?;
        Ok(res)
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
}

/// Types implementing this trait represent clusterwide spaces.
pub trait TClusterwideSpace {
    type Index: TClusterwideSpaceIndex;
    const SPACE_NAME: &'static str;
    const SPACE_ID: SpaceId;

    #[inline(always)]
    fn primary_index() -> Self::Index {
        Self::Index::primary()
    }
}

impl ClusterwideSpaceId {
    #[inline(always)]
    pub const fn value(&self) -> SpaceId {
        *self as _
    }
}

impl From<ClusterwideSpaceId> for SpaceId {
    #[inline(always)]
    fn from(id: ClusterwideSpaceId) -> SpaceId {
        id as _
    }
}

////////////////////////////////////////////////////////////////////////////////
// ClusterwideSpaceIndex
////////////////////////////////////////////////////////////////////////////////

/// A type alias for getting the enumeration of indexes for a clusterwide space.
pub type IndexOf<T> = <T as TClusterwideSpace>::Index;

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

        /// Pending ddl operation which is to be either committed or aborted.
        ///
        /// Is only present during the time between the last ddl prepare
        /// operation and the corresponding ddl commit or abort operation.
        PendingSchemaChange = "pending_schema_change",

        /// Schema version of the pending ddl prepare operation.
        /// This will be the current schema version if the next entry is a ddl
        /// commit operation.
        ///
        /// Is only present during the time between the last ddl prepare
        /// operation and the corresponding ddl commit or abort operation.
        PendingSchemaVersion = "pending_schema_version",

        /// Current schema version. Increases with every ddl commit operation.
        ///
        /// This is equal to [`local_schema_version`] during the time between
        /// the last ddl commit or abort operation and the next ddl prepare
        /// operation.
        GlobalSchemaVersion = "global_schema_version",

        /// Schema version which should be used for the next ddl prepare
        /// operation. This increases with every ddl prepare operation in the
        /// log no matter if it is committed or aborted.
        /// This guards us from some painfull corner cases.
        NextSchemaVersion = "next_schema_version",
    }
}

////////////////////////////////////////////////////////////////////////////////
// Properties
////////////////////////////////////////////////////////////////////////////////

impl Properties {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .id(Self::SPACE_ID)
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
    pub fn pending_schema_change(&self) -> tarantool::Result<Option<Ddl>> {
        self.get(PropertyName::PendingSchemaChange)
    }

    #[inline]
    pub fn pending_schema_version(&self) -> tarantool::Result<Option<u64>> {
        self.get(PropertyName::PendingSchemaVersion)
    }

    #[inline]
    pub fn global_schema_version(&self) -> tarantool::Result<u64> {
        let res = self
            .get(PropertyName::GlobalSchemaVersion)?
            .unwrap_or_default();
        Ok(res)
    }

    #[inline]
    pub fn next_schema_version(&self) -> tarantool::Result<u64> {
        let res = if let Some(version) = self.get(PropertyName::NextSchemaVersion)? {
            version
        } else {
            let current = self.global_schema_version()?;
            current + 1
        };
        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Replicasets
////////////////////////////////////////////////////////////////////////////////

impl Replicasets {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .id(Self::SPACE_ID)
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
            .id(Self::SPACE_ID)
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
            .id(Self::SPACE_ID)
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
    pub schema_version: u64,
    pub space_dumps: Vec<SpaceDump>,
}
impl Encode for SnapshotData {}

#[derive(Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct SpaceDump {
    pub space_name: String,
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
            .id(Self::SPACE_ID)
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
    pub fn by_name(&self, name: &str) -> tarantool::Result<Option<SpaceDef>> {
        match self.index_name.get(&[name])? {
            Some(tuple) => tuple.decode().map(Some),
            None => Ok(None),
        }
    }
}

impl ToEntryIter for Spaces {
    type Entry = SpaceDef;

    #[inline(always)]
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Indexes
////////////////////////////////////////////////////////////////////////////////

impl Indexes {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .id(Self::SPACE_ID)
            .is_local(true)
            .is_temporary(false)
            .field(("space_id", FieldType::Unsigned))
            .field(("id", FieldType::Unsigned))
            .field(("name", FieldType::String))
            .field(("local", FieldType::Boolean))
            .field(("parts", FieldType::Array))
            .field(("schema_version", FieldType::Unsigned))
            .field(("operable", FieldType::Boolean))
            .field(("unique", FieldType::Boolean))
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

    #[inline]
    pub fn by_space_id(&self, space_id: SpaceId) -> tarantool::Result<EntryIter<IndexDef>> {
        let iter = self.space.select(IteratorType::Eq, &[space_id])?;
        Ok(EntryIter::new(iter))
    }
}

impl ToEntryIter for Indexes {
    type Entry = IndexDef;

    #[inline(always)]
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
    }
}

////////////////////////////////////////////////////////////////////////////////
// ddl meta
////////////////////////////////////////////////////////////////////////////////

/// Updates the field `"operable"` for a space with id `space_id` and any
/// necessary entities (currently all existing indexes).
///
/// This function is called when applying the different ddl operations.
pub fn ddl_meta_space_update_operable(
    storage: &Clusterwide,
    space_id: SpaceId,
    operable: bool,
) -> traft::Result<()> {
    storage.spaces.update_operable(space_id, operable)?;
    let iter = storage.indexes.by_space_id(space_id)?;
    for index in iter {
        storage
            .indexes
            .update_operable(index.space_id, index.id, operable)?;
    }
    Ok(())
}

/// Deletes the picodata internal metadata for a space with id `space_id`.
///
/// This function is called when applying the different ddl operations.
pub fn ddl_meta_drop_space(storage: &Clusterwide, space_id: SpaceId) -> traft::Result<()> {
    storage.spaces.delete(space_id)?;
    let iter = storage.indexes.by_space_id(space_id)?;
    for index in iter {
        storage.indexes.delete(index.space_id, index.id)?;
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// ddl
////////////////////////////////////////////////////////////////////////////////

pub fn ddl_abort_on_master(ddl: &Ddl, version: u64) -> traft::Result<()> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_space = Space::from(SystemSpace::Space);
    let sys_index = Space::from(SystemSpace::Index);

    match *ddl {
        Ddl::CreateSpace { id, .. } => {
            sys_index.delete(&[id, 1])?;
            sys_index.delete(&[id, 0])?;
            sys_space.delete(&[id])?;
            set_local_schema_version(version)?;
        }

        Ddl::DropSpace { .. } => {
            // Actual drop happens only on commit, so there's nothing to abort.
        }

        _ => {
            todo!();
        }
    }

    Ok(())
}

/// Create tarantool space and any required indexes. Currently it creates a
/// primary index and a `bucket_id` index if it's a sharded space.
///
/// Return values:
/// * `Ok(None)` in case of success.
/// * `Ok(Some(abort_reason))` in case of error which should result in a ddl abort.
/// * `Err(e)` in case of retryable errors.
///
// FIXME: this function returns 2 kinds of errors: retryable and non-retryable.
// Currently this is impelemnted by returning one kind of errors as Err(e) and
// the other as Ok(Some(e)). This was the simplest solution at the time this
// function was implemented, as it requires the least amount of boilerplate and
// error forwarding code. But this signature is not intuitive, so maybe there's
// room for improvement.
pub fn ddl_create_space_on_master(
    storage: &Clusterwide,
    space_id: SpaceId,
) -> traft::Result<Option<TntError>> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_space = Space::from(SystemSpace::Space);
    let sys_index = Space::from(SystemSpace::Index);

    let pico_space_def = storage
        .spaces
        .get(space_id)?
        .ok_or_else(|| Error::other(format!("space with id {space_id} not found")))?;
    // TODO: set defaults
    let tt_space_def = pico_space_def.to_space_metadata()?;

    let pico_pk_def = storage.indexes.get(space_id, 0)?.ok_or_else(|| {
        Error::other(format!(
            "primary index for space {} not found",
            pico_space_def.name
        ))
    })?;
    let tt_pk_def = pico_pk_def.to_index_metadata();

    // For now we just assume that during space creation index with id 1
    // exists if and only if it is a bucket_id index.
    let mut tt_bucket_id_def = None;
    let pico_bucket_id_def = storage.indexes.get(space_id, 1)?;
    if let Some(def) = &pico_bucket_id_def {
        tt_bucket_id_def = Some(def.to_index_metadata());
    }

    let res = (|| -> tarantool::Result<()> {
        if tt_pk_def.parts.is_empty() {
            return Err(tarantool::set_and_get_error!(
                tarantool::error::TarantoolErrorCode::ModifyIndex,
                "can't create index '{}' in space '{}': parts list cannot be empty",
                tt_pk_def.name,
                tt_space_def.name,
            )
            .into());
        }
        sys_space.insert(&tt_space_def)?;
        sys_index.insert(&tt_pk_def)?;
        if let Some(def) = tt_bucket_id_def {
            sys_index.insert(&def)?;
        }

        Ok(())
    })();
    Ok(res.err())
}

/// Drop tarantool space and any entities which depend on it (currently just indexes).
///
/// Return values:
/// * `Ok(None)` in case of success.
/// * `Ok(Some(abort_reason))` in case of error which should result in a ddl abort.
/// * `Err(e)` in case of retryable errors.
///
// FIXME: this function returns 2 kinds of errors: retryable and non-retryable.
// Currently this is impelemnted by returning one kind of errors as Err(e) and
// the other as Ok(Some(e)). This was the simplest solution at the time this
// function was implemented, as it requires the least amount of boilerplate and
// error forwarding code. But this signature is not intuitive, so maybe there's
// room for improvement.
pub fn ddl_drop_space_on_master(space_id: SpaceId) -> traft::Result<Option<TntError>> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_space = Space::from(SystemSpace::Space);
    let sys_index = Space::from(SystemSpace::Index);
    let sys_truncate = Space::from(SystemSpace::Truncate);

    let iter = sys_index.select(IteratorType::Eq, &[space_id])?;
    let mut index_ids = Vec::with_capacity(4);
    for tuple in iter {
        let index_id: IndexId = tuple
            .field(1)?
            .expect("decoding metadata should never fail");
        // Primary key is handled explicitly.
        if index_id != 0 {
            index_ids.push(index_id);
        }
    }
    let res = (|| -> tarantool::Result<()> {
        // TODO: delete it from _truncate, delete grants
        for iid in index_ids.iter().rev() {
            sys_index.delete(&(space_id, iid))?;
        }
        // Primary key must be dropped last.
        sys_index.delete(&(space_id, 0))?;
        sys_truncate.delete(&[space_id])?;
        sys_space.delete(&[space_id])?;

        Ok(())
    })();
    Ok(res.err())
}

////////////////////////////////////////////////////////////////////////////////
// local schema version
////////////////////////////////////////////////////////////////////////////////

pub fn local_schema_version() -> tarantool::Result<u64> {
    let space_schema = Space::from(SystemSpace::Schema);
    let tuple = space_schema.get(&["local_schema_version"])?;
    let mut res = 0;
    if let Some(tuple) = tuple {
        if let Some(v) = tuple.field(1)? {
            res = v;
        }
    }
    Ok(res)
}

pub fn set_local_schema_version(v: u64) -> tarantool::Result<()> {
    let space_schema = Space::from(SystemSpace::Schema);
    space_schema.replace(&("local_schema_version", v))?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// max space id
////////////////////////////////////////////////////////////////////////////////

pub const SPACE_ID_INTERNAL_MAX: u32 = 1024;

/// Updates box.space._schema max_id to start outside the reserved internal
/// space id range.
pub fn tweak_max_space_id() -> tarantool::Result<()> {
    let sys_schema = Space::from(SystemSpace::Schema);
    let tuple = sys_schema
        .get(&["max_id"])?
        .expect("max_id is always there");
    let max_id: u32 = tuple.field(1)?.expect("always has value");
    if max_id < SPACE_ID_INTERNAL_MAX {
        sys_schema.put(&("max_id", SPACE_ID_INTERNAL_MAX))?;
    }
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
    use ::tarantool::transaction::transaction;

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
            storage.space_by_name(ClusterwideSpace::Instance).unwrap().put(&instance).unwrap();
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
                    "tarantool error:",
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
                "tarantool error: NoSuchIndexID: No index #1 is defined",
                " in space '_pico_instance'",
            )
        );

        storage.index(IndexOf::<Instances>::ReplicasetId).drop().unwrap();

        assert_err!(
            storage.instances.replicaset_instances(""),
            concat!(
                "tarantool error: NoSuchIndexID: No index #2 is defined",
                " in space '_pico_instance'",
            )
        );

        storage.index(IndexOf::<Instances>::InstanceId).drop().unwrap();

        assert_err!(
            storage.instances.get(&InstanceId::from("i1")),
            concat!(
                "tarantool error: NoSuchIndexID: No index #0 is defined",
                " in space '_pico_instance'",
            )
        );

        let space = storage.space_by_name(ClusterwideSpace::Instance).unwrap();
        space.drop().unwrap();

        assert_err!(
            storage.instances.all_instances(),
            format!(
                "tarantool error: NoSuchSpace: Space '{}' does not exist",
                space.id(),
            )
        );
    }

    #[::tarantool::test]
    fn clusterwide_space_index() {
        let storage = Clusterwide::new().unwrap();

        storage
            .space_by_name(ClusterwideSpace::Address)
            .unwrap()
            .insert(&(1, "foo"))
            .unwrap();
        storage
            .space_by_name(ClusterwideSpace::Address)
            .unwrap()
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
        };
        storage.replicasets.space.insert(&r).unwrap();

        let snapshot_data = Clusterwide::snapshot_data().unwrap();
        let space_dumps = snapshot_data.space_dumps;

        assert_eq!(space_dumps.len(), 6);

        for space_dump in &space_dumps {
            match &space_dump.space_name {
                s if s == &*ClusterwideSpace::Instance => {
                    let [instance]: [Instance; 1] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(instance, i);
                }

                s if s == &*ClusterwideSpace::Address => {
                    let addrs: [(i32, String); 2] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(
                        addrs,
                        [(1, "google.com".to_string()), (2, "ya.ru".to_string())]
                    );
                }

                s if s == &*ClusterwideSpace::Property => {
                    let [property]: [(String, String); 1] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(property, ("foo".to_owned(), "bar".to_owned()));
                }

                s if s == &*ClusterwideSpace::Replicaset => {
                    let [replicaset]: [Replicaset; 1] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(replicaset, r);
                }

                s if s == &*ClusterwideSpace::Space => {
                    let []: [(); 0] = Decode::decode(space_dump.tuples.as_ref()).unwrap();
                }

                s if s == &*ClusterwideSpace::Index => {
                    let []: [(); 0] = Decode::decode(space_dump.tuples.as_ref()).unwrap();
                }

                _ => {
                    unreachable!();
                }
            }
        }
    }

    #[::tarantool::test]
    fn apply_snapshot_data() {
        let storage = Clusterwide::new().unwrap();

        let mut data = SnapshotData {
            schema_version: 0,
            space_dumps: vec![],
        };

        let i = Instance {
            raft_id: 1,
            instance_id: "i".into(),
            ..Instance::default()
        };
        let tuples = [&i].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space_name: ClusterwideSpace::Instance.into(),
            tuples,
        });

        let tuples = [(1, "google.com"), (2, "ya.ru")].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space_name: ClusterwideSpace::Address.into(),
            tuples,
        });

        let tuples = [("foo", "bar")].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space_name: ClusterwideSpace::Property.into(),
            tuples,
        });

        let r = Replicaset {
            replicaset_id: "r1".into(),
            replicaset_uuid: "r1-uuid".into(),
            master_id: "i".into(),
            weight: crate::replicaset::weight::Info::default(),
        };
        let tuples = [&r].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space_name: ClusterwideSpace::Replicaset.into(),
            tuples,
        });

        storage.for_each_space(|s| s.truncate()).unwrap();
        if let Err(e) =
            transaction(|| -> traft::Result<()> { storage.apply_snapshot_data(&data, true) })
        {
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
    }
}

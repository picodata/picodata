use rmp::decode::{read_array_len, Bytes, RmpRead};
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr};
use std::collections::HashMap;
use tarantool::tlua::{self, AsLua, Push, PushGuard, PushInto, PushOne, PushOneInto, Void};
use tarantool::tuple::{Tuple, TupleBuilder};

use crate::backend::sql::tree::OrderedSyntaxNodes;
use crate::errors::{Action, Entity, SbroadError};
use crate::executor::ir::{ExecutionPlan, QueryType};
use crate::ir::value::Value;
use crate::ir::Options;

use crate::executor::engine::TableVersionMap;
use crate::ir::node::NodeId;

use super::engine::helpers::vshard::CacheInfo;
use super::vtable::VirtualTableMeta;

pub type VTablesMeta = HashMap<NodeId, VirtualTableMeta>;

pub fn rust_allocated_tuple_from_bincode<T>(value: &T) -> Result<Tuple, SmolStr>
where
    T: ?Sized + serde::Serialize,
{
    let type_name = std::any::type_name::<T>();

    let res = bincode::serialized_size(value);
    let bincode_size = match res {
        Ok(v) => v,
        Err(e) => {
            let msg = format_smolstr!("failed getting serialized size for {type_name}: {e}");
            tarantool::say_warn!("{msg}");
            return Err(msg);
        }
    };
    if bincode_size > u32::MAX as u64 {
        let msg = format_smolstr!(
            "serialized value of {type_name} is too big: {bincode_size} > {}",
            u32::MAX
        );
        tarantool::say_warn!("{msg}");
        return Err(msg);
    }

    let msgpack_header = msgpack_header_for_data(bincode_size as u32);
    let capacity = msgpack_header.len() + bincode_size as usize;
    let mut builder = TupleBuilder::rust_allocated();
    builder.reserve(capacity);
    builder.append(&msgpack_header);

    let res = bincode::serialize_into(&mut builder, value);
    match res {
        Ok(()) => {}
        Err(e) => {
            let msg = format_smolstr!("failed serializing value of {type_name}: {e}");
            tarantool::say_warn!("{msg}");
            return Err(msg);
        }
    }

    let tuple = builder.into_tuple();
    let tuple = match tuple {
        Ok(v) => v,
        Err(e) => {
            let msg = format_smolstr!(
                "failed creating a tuple from serialized data for {type_name}: {e}"
            );
            tarantool::say_warn!("{msg}");
            return Err(msg);
        }
    };

    Ok(tuple)
}

fn msgpack_header_for_data(data_len: u32) -> [u8; 6] {
    let mut msgpack_header = [0_u8; 6];
    // array of len 1
    msgpack_header[0] = b'\x91';
    // string with 32bit length
    msgpack_header[1] = b'\xdb';
    // 32bit length of string
    msgpack_header[2..].copy_from_slice(&data_len.to_be_bytes());
    msgpack_header
}

pub fn rust_allocated_tuple_from_bytes(data: &[u8]) -> Tuple {
    assert!(data.len() <= u32::MAX as usize);
    let msgpack_header = msgpack_header_for_data(data.len() as u32);
    let capacity = msgpack_header.len() + data.len();
    let mut builder = TupleBuilder::rust_allocated();
    builder.reserve(capacity);
    builder.append(&msgpack_header);

    builder.append(data);

    let tuple = builder.into_tuple();
    match tuple {
        Ok(v) => v,
        Err(e) => {
            unreachable!("can't fail msgpack validation, msgpack header is valid: {e}");
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Binary(#[serde(with = "serde_bytes")] Tuple);

impl From<Vec<u8>> for Binary {
    #[inline(always)]
    fn from(value: Vec<u8>) -> Self {
        let tuple = rust_allocated_tuple_from_bytes(&value);
        Binary(tuple)
    }
}

impl From<Tuple> for Binary {
    #[inline(always)]
    fn from(tuple: tarantool::tuple::Tuple) -> Self {
        Binary(tuple)
    }
}

impl<L> PushInto<L> for Binary
where
    L: AsLua,
{
    type Err = Void;

    fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
        self.0.push_into_lua(lua)
    }
}

impl<L> Push<L> for Binary
where
    L: AsLua,
{
    type Err = Void;

    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        self.0.push_to_lua(lua)
    }
}

impl<L> PushOne<L> for Binary where L: AsLua {}
impl<L> PushOneInto<L> for Binary where L: AsLua {}

#[derive(PushInto, Push, Debug, Deserialize, Serialize, PartialEq)]
pub struct RequiredMessage {
    pub required: Binary,
    pub cache_hint: CacheInfo,
}

impl From<Binary> for RequiredMessage {
    fn from(value: Binary) -> Self {
        RequiredMessage {
            required: value,
            cache_hint: CacheInfo::CacheableFirstRequest,
        }
    }
}

#[derive(PushInto, Push, Debug, Deserialize, Serialize, PartialEq)]
pub struct FullMessage {
    pub required: Binary,
    pub optional: Binary,
    pub cache_hint: CacheInfo,
}

impl FullMessage {
    #[must_use]
    pub fn new(required: Binary, optional: Binary) -> Self {
        FullMessage {
            required,
            optional,
            cache_hint: CacheInfo::CacheableSecondRequest,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Default)]
pub struct SchemaInfo {
    pub router_version_map: TableVersionMap,
}

impl SchemaInfo {
    #[must_use]
    pub fn new(router_version_map: TableVersionMap) -> Self {
        SchemaInfo { router_version_map }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EncodedRows {
    /// Lengths of encoded rows.
    pub marking: Vec<usize>,
    /// Encoded rows as msgpack array.
    pub encoded: Binary,
}

impl EncodedRows {
    #[must_use]
    pub fn new(marking: Vec<usize>, encoded: Binary) -> Self {
        EncodedRows { marking, encoded }
    }

    #[must_use]
    pub fn iter(&self) -> EncodedRowsIter {
        <&Self as IntoIterator>::into_iter(self)
    }
}

impl<'e> IntoIterator for &'e EncodedRows {
    type Item = Tuple;
    type IntoIter = EncodedRowsIter<'e>;

    fn into_iter(self) -> Self::IntoIter {
        EncodedRowsIter {
            stream: Bytes::from(self.encoded.0.data()),
            marking: &self.marking,
            position: 0,
        }
    }
}

pub struct EncodedRowsIter<'e> {
    /// Encoded tuples as msgpack array stream.
    stream: Bytes<'e>,
    /// Lengths of encoded rows.
    marking: &'e [usize],
    /// Current stream position.
    position: usize,
}

impl<'e> Iterator for EncodedRowsIter<'e> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        let cur_pos = self.position;
        self.position += 1;
        if cur_pos == 0 {
            let array_len = read_array_len(&mut self.stream).expect("encoded rows length");
            assert_eq!(array_len as usize, self.marking.len());
        }
        let row_len = self.marking.get(cur_pos)?;
        assert!(*row_len <= u32::MAX as usize);
        let mut builder = TupleBuilder::rust_allocated();
        builder.reserve(*row_len);
        for _ in 0..*row_len {
            let byte = self.stream.read_u8().expect("encoded tuple");
            builder.append(&[byte]);
        }
        let tuple = builder
            .into_tuple()
            .expect("failed to create rust-allocated tuple");
        Some(tuple)
    }
}

/// Encoded virtual tables data.
pub type EncodedTables = HashMap<NodeId, EncodedRows>;

/// Query data used for executing of the cached statements.
/// Note that it contains only meta-information about SQL query (
/// e.g. cached plan id and params).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RequiredData {
    /// A unique ID of the IR subtree that is used as a key for prepared statements
    /// in tarantool SQL cache.
    pub plan_id: SmolStr,
    pub parameters: Vec<Value>,
    pub query_type: QueryType,
    pub options: Options,
    pub schema_info: SchemaInfo,
    pub tables: EncodedTables,
}

impl Default for RequiredData {
    fn default() -> Self {
        RequiredData {
            plan_id: SmolStr::default(),
            parameters: vec![],
            query_type: QueryType::DQL,
            options: Options::default(),
            schema_info: SchemaInfo::default(),
            tables: EncodedTables::default(),
        }
    }
}

impl TryFrom<RequiredData> for Vec<u8> {
    type Error = SbroadError;

    fn try_from(value: RequiredData) -> Result<Self, Self::Error> {
        bincode::serialize(&value).map_err(|e| {
            SbroadError::FailedTo(
                Action::Serialize,
                Some(Entity::RequiredData),
                format_smolstr!("to binary: {e:?}"),
            )
        })
    }
}

impl TryFrom<&[u8]> for RequiredData {
    type Error = SbroadError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        bincode::deserialize(value).map_err(|e| {
            SbroadError::FailedTo(
                Action::Deserialize,
                Some(Entity::RequiredData),
                format_smolstr!("{e:?}"),
            )
        })
    }
}

impl RequiredData {
    const ENTITY: Entity = Entity::RequiredData;

    /// Construct a tuple, i.e. msgpack array of one binary string
    /// containing the bincode encoding of `self`.
    #[inline(always)]
    pub fn to_tuple(&self) -> Result<tarantool::tuple::Tuple, SbroadError> {
        rust_allocated_tuple_from_bincode(self)
            .map_err(|msg| SbroadError::FailedTo(Action::Serialize, Some(Self::ENTITY), msg))
    }

    #[must_use]
    pub fn new(
        plan_id: SmolStr,
        parameters: Vec<Value>,
        query_type: QueryType,
        options: Options,
        schema_info: SchemaInfo,
        tables: EncodedTables,
    ) -> Self {
        RequiredData {
            plan_id,
            parameters,
            query_type,
            options,
            schema_info,
            tables,
        }
    }

    #[must_use]
    pub fn plan_id(&self) -> &str {
        &self.plan_id
    }
}

pub struct EncodedRequiredData(Vec<u8>);

impl From<Vec<u8>> for EncodedRequiredData {
    fn from(value: Vec<u8>) -> Self {
        EncodedRequiredData(value)
    }
}

impl From<EncodedRequiredData> for Vec<u8> {
    fn from(value: EncodedRequiredData) -> Self {
        value.0
    }
}

impl TryFrom<RequiredData> for EncodedRequiredData {
    type Error = SbroadError;

    fn try_from(value: RequiredData) -> Result<Self, Self::Error> {
        let bytes: Vec<u8> = value.try_into()?;
        Ok(EncodedRequiredData(bytes))
    }
}

impl TryFrom<EncodedRequiredData> for RequiredData {
    type Error = SbroadError;

    fn try_from(value: EncodedRequiredData) -> Result<Self, Self::Error> {
        let ir: RequiredData = value.0.as_slice().try_into()?;
        Ok(ir)
    }
}

/// Query data used for executing non-cachable queries.
/// Note that it contains a full plan with all needed vtables.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct OptionalData {
    pub exec_plan: ExecutionPlan,
    pub ordered: OrderedSyntaxNodes,
    pub vtables_meta: VTablesMeta,
}

impl TryFrom<OptionalData> for Vec<u8> {
    type Error = SbroadError;

    fn try_from(value: OptionalData) -> Result<Self, Self::Error> {
        bincode::serialize(&value).map_err(|e| {
            SbroadError::FailedTo(
                Action::Serialize,
                Some(Entity::OptionalData),
                format_smolstr!("to binary: {e:?}"),
            )
        })
    }
}

impl TryFrom<&[u8]> for OptionalData {
    type Error = SbroadError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        bincode::deserialize(value).map_err(|e| {
            SbroadError::FailedTo(
                Action::Deserialize,
                Some(Entity::OptionalData),
                format_smolstr!("{e:?}"),
            )
        })
    }
}

impl OptionalData {
    const ENTITY: Entity = Entity::OptionalData;

    /// Construct a tuple, i.e. msgpack array of one binary string
    /// containing the bincode encoding of `self`.
    #[inline(always)]
    pub fn to_tuple(&self) -> Result<tarantool::tuple::Tuple, SbroadError> {
        rust_allocated_tuple_from_bincode(self)
            .map_err(|msg| SbroadError::FailedTo(Action::Serialize, Some(Self::ENTITY), msg))
    }

    #[must_use]
    pub fn new(
        exec_plan: ExecutionPlan,
        ordered: OrderedSyntaxNodes,
        vtables_meta: VTablesMeta,
    ) -> Self {
        OptionalData {
            exec_plan,
            ordered,
            vtables_meta,
        }
    }

    /// Serialize to bytes
    ///
    /// # Errors
    /// - If the `OptionalData` cannot be serialized to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, SbroadError> {
        bincode::serialize(self).map_err(|e| {
            SbroadError::FailedTo(
                Action::Serialize,
                Some(Entity::RequiredData),
                format_smolstr!("to binary: {e:?}"),
            )
        })
    }

    /// Try to deserialize from bytes
    ///
    /// # Errors
    /// - If the bytes cannot be deserialized to an `OptionalData`.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, SbroadError> {
        bincode::deserialize(bytes).map_err(|e| {
            SbroadError::FailedTo(
                Action::Deserialize,
                Some(Entity::OptionalData),
                format_smolstr!("{e:?}"),
            )
        })
    }
}

pub struct EncodedOptionalData(Vec<u8>);

impl From<Vec<u8>> for EncodedOptionalData {
    fn from(value: Vec<u8>) -> Self {
        EncodedOptionalData(value)
    }
}

impl From<EncodedOptionalData> for Vec<u8> {
    fn from(value: EncodedOptionalData) -> Self {
        value.0
    }
}

impl TryFrom<OptionalData> for EncodedOptionalData {
    type Error = SbroadError;

    fn try_from(value: OptionalData) -> Result<Self, Self::Error> {
        let bytes: Vec<u8> = value.try_into()?;
        Ok(EncodedOptionalData(bytes))
    }
}

impl TryFrom<EncodedOptionalData> for OptionalData {
    type Error = SbroadError;

    fn try_from(value: EncodedOptionalData) -> Result<Self, Self::Error> {
        let ir: OptionalData = value.0.as_slice().try_into()?;
        Ok(ir)
    }
}

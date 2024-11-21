use ahash::AHashSet;
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr};
use std::any::Any;
use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::rc::Rc;
use std::{iter, vec};
use tarantool::tuple::TupleBuilder;

use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::{TupleBuilderCommand, TupleBuilderPattern};
use crate::executor::protocol::{Binary, EncodedRows, EncodedTables};
use crate::executor::{bucket::Buckets, Vshard};
use crate::ir::helpers::RepeatableState;
use crate::ir::node::NodeId;
use crate::ir::relation::{Column, ColumnRole, Type};
use crate::ir::transformation::redistribution::{ColumnPosition, MotionKey, Target};
use crate::ir::value::{EncodedValue, LuaValue, MsgPackValue, Value};
use crate::utils::{write_u32_array_len, ByteCounter};

use super::ir::ExecutionPlan;
use super::result::{ExecutorTuple, MetadataColumn, ProducerResult};

#[cfg(not(feature = "mock"))]
use tarantool::tuple::Tuple;

type ShardingKey = Vec<Value>;
pub type VTableTuple = Vec<Value>;

/// Helper struct to group tuples by buckets.
/// key:   bucket id.
/// value: list of positions in the `tuples` list (see `VirtualTable`) corresponding to the bucket.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct VTableIndex {
    value: HashMap<u64, Vec<usize>, RepeatableState>,
}

impl VTableIndex {
    fn new() -> Self {
        Self {
            value: HashMap::with_hasher(RepeatableState),
        }
    }

    pub fn add_entry(&mut self, bucket_id: u64, position: usize) {
        match self.value.entry(bucket_id) {
            Entry::Vacant(entry) => {
                entry.insert(vec![position]);
            }
            Entry::Occupied(entry) => {
                entry.into_mut().push(position);
            }
        }
    }
}

impl From<HashMap<u64, Vec<usize>, RepeatableState>> for VTableIndex {
    fn from(value: HashMap<u64, Vec<usize>, RepeatableState>) -> Self {
        Self { value }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct ShardingRecord(ShardingKey, usize);

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct VirtualTableMeta {
    /// List of the columns.
    pub columns: Vec<Column>,
    /// Unique table name (we need to generate it ourselves).
    pub name: Option<SmolStr>,
    /// Column positions that form a primary key.
    pub primary_key: Option<Vec<ColumnPosition>>,
}

/// Result tuple storage, created by the executor. All tuples
/// have a distribution key.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct VirtualTable {
    /// List of the columns.
    /// TODO: Make it `VTableColumn` (not containing `name` field) instead of a `Column`.
    columns: Vec<Column>,
    /// "Raw" tuples (list of values)
    tuples: Vec<VTableTuple>,
    /// Unique table name (we need to generate it ourselves).
    name: Option<SmolStr>,
    /// Column positions that form a primary key.
    primary_key: Option<Vec<ColumnPosition>>,
    /// Index groups tuples by the buckets:
    /// the key is a bucket id, the value is a list of positions
    /// in the `tuples` list corresponding to the bucket.
    bucket_index: VTableIndex,
}

/// Facade for `Column` class.
/// Idea is to restrict caller's ability to add a custom name to a column
/// (as soon as it's generated automatically).
#[derive(PartialEq, Debug, Eq, Clone)]
pub struct VTableColumn {
    pub r#type: Type,
    pub role: ColumnRole,
    pub is_nullable: bool,
}

impl Default for VirtualTable {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for VirtualTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for col in &self.columns {
            write!(f, "{col:?}, ")?;
        }
        writeln!(f)?;
        for row in &self.tuples {
            writeln!(f, "{row:?}")?;
        }
        writeln!(f)
    }
}

#[must_use]
#[inline]
#[allow(clippy::module_name_repetitions)]
pub fn vtable_indexed_column_name(index: usize) -> SmolStr {
    format_smolstr!("COL_{index}")
}

impl VirtualTable {
    #[must_use]
    pub fn new() -> Self {
        VirtualTable {
            columns: vec![],
            tuples: vec![],
            name: None,
            primary_key: None,
            bucket_index: VTableIndex::new(),
        }
    }

    #[must_use]
    pub fn metadata(&self) -> VirtualTableMeta {
        VirtualTableMeta {
            columns: self.columns.clone(),
            name: self.name.clone(),
            primary_key: self.primary_key.clone(),
        }
    }

    /// Add column to virtual table
    pub fn add_column(&mut self, vtable_col: VTableColumn) {
        let col = Column {
            name: vtable_indexed_column_name(self.columns.len() + 1),
            r#type: vtable_col.r#type,
            role: vtable_col.role,
            is_nullable: vtable_col.is_nullable,
        };
        self.columns.push(col);
    }

    /// Adds a tuple of values to virtual table
    ///
    /// # Errors
    /// Returns IR `Value` transformation error
    pub fn add_tuple(&mut self, tuple: VTableTuple) {
        self.tuples.push(tuple);
    }

    /// Gets a virtual table tuples list
    #[must_use]
    pub fn get_tuples(&self) -> &[VTableTuple] {
        &self.tuples
    }

    /// Gets a mutable virtual table tuples list
    #[must_use]
    pub fn get_mut_tuples(&mut self) -> &mut Vec<VTableTuple> {
        &mut self.tuples
    }

    /// Given a vec of [(`is_nullable`, `correct_type`)], fix metadata of each value in the tuples.
    ///
    /// # Errors
    /// - Unable to apply values cast.
    ///
    /// # Panics
    /// - Unacceptable type met.
    pub fn cast_values(&mut self, fixed_types: &[(bool, Type)]) -> Result<(), SbroadError> {
        for tuple in self.get_mut_tuples() {
            for (i, v) in tuple.iter_mut().enumerate() {
                let (_, ty) = fixed_types.get(i).expect("Type expected.");
                let cast_value = v.cast(ty)?;
                match cast_value {
                    EncodedValue::Ref(_) => {
                        // Value type is already ok.
                    }
                    EncodedValue::Owned(v_o) => *v = v_o.into(),
                }
            }
        }
        for (i, (is_nullable, ty)) in fixed_types.iter().enumerate() {
            let current_column_mut = self
                .get_mut_columns()
                .get_mut(i)
                .expect("Vtable column not found.");
            current_column_mut.is_nullable = *is_nullable;
            current_column_mut.r#type = *ty;
        }
        Ok(())
    }

    /// Gets virtual table columns list
    #[must_use]
    pub fn get_columns(&self) -> &[Column] {
        &self.columns
    }

    /// Gets virtual table columns list
    #[must_use]
    pub fn get_mut_columns(&mut self) -> &mut Vec<Column> {
        &mut self.columns
    }

    /// Gets virtual table's buket index
    #[must_use]
    pub fn get_bucket_index(&self) -> &HashMap<u64, Vec<usize>, RepeatableState> {
        &self.bucket_index.value
    }

    /// Gets virtual table mutable bucket index
    #[must_use]
    pub fn get_mut_bucket_index(&mut self) -> &mut HashMap<u64, Vec<usize>, RepeatableState> {
        &mut self.bucket_index.value
    }

    /// Set vtable index
    pub fn set_bucket_index(&mut self, index: HashMap<u64, Vec<usize>, RepeatableState>) {
        self.bucket_index = index.into();
    }

    /// Get vtable's tuples corresponding to the buckets.
    #[must_use]
    pub fn get_tuples_with_buckets(&self, buckets: &Buckets) -> Vec<&VTableTuple> {
        let tuples: Vec<&VTableTuple> = match buckets {
            Buckets::All | Buckets::Any => self.get_tuples().iter().collect(),
            Buckets::Filtered(bucket_ids) => {
                if self.get_bucket_index().is_empty() {
                    // TODO: Implement selection push-down (join_linker3_test).
                    self.get_tuples().iter().collect()
                } else {
                    bucket_ids
                        .iter()
                        .filter_map(|bucket_id| self.get_bucket_index().get(bucket_id))
                        .flatten()
                        .filter_map(|pos| self.get_tuples().get(*pos))
                        .collect()
                }
            }
        };
        tuples
    }

    /// Set vtable alias name
    ///
    /// # Errors
    /// - Try to set an empty alias name to the virtual table.
    pub fn set_alias(&mut self, name: &str) {
        self.name = Some(SmolStr::from(name));
    }

    /// Get vtable alias name
    #[must_use]
    pub fn get_alias(&self) -> Option<&SmolStr> {
        self.name.as_ref()
    }

    /// Create a new virtual table from the original one with
    /// a list of tuples corresponding only to the passed buckets.
    ///
    /// # Errors
    /// - bucket index is corrupted
    pub fn new_with_buckets(&self, bucket_ids: &[u64]) -> Result<Self, SbroadError> {
        let mut result = Self::new();
        result.columns.clone_from(&self.columns);
        result.name.clone_from(&self.name);

        result.primary_key.clone_from(&self.primary_key);
        for bucket_id in bucket_ids {
            // If bucket_id is met among those that are present in self.
            if let Some(pointers) = self.get_bucket_index().get(bucket_id) {
                let mut new_pointers: Vec<usize> = Vec::with_capacity(pointers.len());
                for pointer in pointers {
                    let tuple = self.tuples.get(*pointer).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::VirtualTable,
                            Some(format_smolstr!(
                                "Tuple with position {pointer} in the bucket index not found"
                            )),
                        )
                    })?;
                    result.tuples.push(tuple.clone());
                    new_pointers.push(result.tuples.len() - 1);
                }
                result
                    .get_mut_bucket_index()
                    .insert(*bucket_id, new_pointers);
            }
        }
        Ok(result)
    }

    /// Reshard a virtual table (i.e. build a bucket index).
    ///
    /// # Errors
    /// - Motion key is invalid.
    pub fn reshard(
        &mut self,
        motion_key: &MotionKey,
        runtime: &impl Vshard,
    ) -> Result<(), SbroadError> {
        let mut index = HashMap::with_hasher(RepeatableState);
        for (pos, tuple) in self.get_tuples().iter().enumerate() {
            let mut shard_key_tuple: Vec<&Value> = Vec::new();
            for target in &motion_key.targets {
                match target {
                    Target::Reference(col_idx) => {
                        let part = tuple.get(*col_idx).ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::DistributionKey,
                                format_smolstr!(
                                "failed to find a distribution key column {pos} in the tuple {tuple:?}."
                            ),
                            )
                        })?;
                        shard_key_tuple.push(part);
                    }
                    Target::Value(ref value) => {
                        shard_key_tuple.push(value);
                    }
                }
            }
            let bucket_id = runtime.determine_bucket_id(&shard_key_tuple)?;
            match index.entry(bucket_id) {
                Entry::Vacant(entry) => {
                    entry.insert(vec![pos]);
                }
                Entry::Occupied(entry) => {
                    entry.into_mut().push(pos);
                }
            }
        }

        self.set_bucket_index(index);
        Ok(())
    }

    /// Set primary key in the virtual table.
    ///
    /// # Errors
    /// - primary key refers invalid column positions
    pub fn set_primary_key(&mut self, pk: &[ColumnPosition]) -> Result<(), SbroadError> {
        for pos in pk {
            if pos >= &self.columns.len() {
                return Err(SbroadError::NotFound(
                    Entity::Column,
                    format_smolstr!(
                        "primary key in the virtual table {:?} contains invalid position {pos}.",
                        self.name
                    ),
                ));
            }
        }
        self.primary_key = Some(pk.to_vec());
        Ok(())
    }

    /// Get primary key in the virtual table.
    ///
    /// # Errors
    /// - primary key refers invalid column positions
    pub fn get_primary_key(&self) -> Result<&[ColumnPosition], SbroadError> {
        if let Some(cols) = &self.primary_key {
            return Ok(cols);
        }
        Err(SbroadError::Invalid(
            Entity::VirtualTable,
            Some("expected to have primary key!".into()),
        ))
    }

    /// Helper logic of `rearrange_for_update` related to creation of delete tuples.
    /// For more details see `rearrange_for_update`.
    fn create_delete_tuples(
        &mut self,
        runtime: &(impl Vshard + Sized),
        old_shard_columns_len: usize,
    ) -> Result<(Vec<VTableTuple>, VTableIndex), SbroadError> {
        let mut index = VTableIndex::new();
        let delete_tuple_pattern: TupleBuilderPattern = {
            let pk_positions = self.get_primary_key()?;
            let mut res = Vec::with_capacity(old_shard_columns_len + pk_positions.len());
            for pos in pk_positions {
                res.push(TupleBuilderCommand::TakePosition(*pos));
            }
            res
        };
        let mut delete_tuple: VTableTuple = vec![Value::Null; delete_tuple_pattern.len()];
        let mut delete_tuples: Vec<VTableTuple> = Vec::with_capacity(self.get_tuples().len());
        let tuples_len = self.get_tuples().len();
        for (pos, insert_tuple) in self.get_mut_tuples().iter_mut().enumerate() {
            for (idx, c) in delete_tuple_pattern.iter().enumerate() {
                if let TupleBuilderCommand::TakePosition(pos) = c {
                    let value = insert_tuple.get(*pos).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::TupleBuilderCommand,
                            Some(format_smolstr!(
                                "expected position {pos} with tuple len: {}",
                                insert_tuple.len()
                            )),
                        )
                    })?;
                    if let Some(elem) = delete_tuple.get_mut(idx) {
                        *elem = value.clone();
                    }
                };
            }
            let mut old_shard_key: VTableTuple = Vec::with_capacity(old_shard_columns_len);
            for _ in 0..old_shard_columns_len {
                // Note that we are getting values from the end of `insert_tuple` using `pop` method
                // as soon as `old_shard_key_columns` are located in the end of `insert_tuple`
                let Some(v) = insert_tuple.pop() else {
                    return Err(SbroadError::Invalid(
                        Entity::MotionOpcode,
                        Some(format_smolstr!(
                            "invalid number of old shard columns: {old_shard_columns_len}"
                        )),
                    ));
                };
                old_shard_key.push(v);
            }

            // When popping we got these keys in reverse order. That's why we need to use `reverse`.
            old_shard_key.reverse();
            let bucket_id =
                runtime.determine_bucket_id(&old_shard_key.iter().collect::<Vec<&Value>>())?;
            index.add_entry(bucket_id, tuples_len + pos);
            delete_tuples.push(delete_tuple.clone());
        }

        Ok((delete_tuples, index))
    }

    /// Rearrange virtual table under sharded `Update`.
    ///
    /// For more details, see `UpdateStrategy`.
    ///
    /// # Tuple format
    /// Each tuple in table will be used to create delete tuple
    /// and will be transformed to insert tuple itself.
    ///
    /// Original tuple format:
    /// ```text
    /// table_columns, old_shard_key_columns
    /// ```
    ///
    /// Insert tuple:
    /// ```text
    /// table_columns
    /// ```
    /// Bucket is calculated using `new_shard_columns_positions` and
    /// values from `table_columns`.
    ///
    /// Delete tuple:
    /// ```text
    /// pk_columns
    /// ```
    /// Values of `pk_columns` are taken from `table_columns`.
    /// Bucket is calculated using `old_shard_key_columns`.
    ///
    /// # Errors
    /// - invalid len of old shard key
    /// - invalid new shard key positions
    pub fn rearrange_for_update(
        &mut self,
        runtime: &(impl Vshard + Sized),
        old_shard_columns_len: usize,
        new_shard_columns_positions: &Vec<ColumnPosition>,
    ) -> Result<Option<usize>, SbroadError> {
        if new_shard_columns_positions.is_empty() {
            return Err(SbroadError::Invalid(
                Entity::Update,
                Some("No positions for new shard key!".into()),
            ));
        }
        if old_shard_columns_len == 0 {
            return Err(SbroadError::Invalid(
                Entity::Update,
                Some("Invalid len of old shard key: 0".into()),
            ));
        }
        if self.tuples.is_empty() {
            return Ok(None);
        };
        let (delete_tuples, mut index) =
            self.create_delete_tuples(runtime, old_shard_columns_len)?;

        // Index insert tuple, using new shard key values.
        for (pointer, update_tuple) in self.get_mut_tuples().iter_mut().enumerate() {
            let mut update_tuple_shard_key = Vec::with_capacity(new_shard_columns_positions.len());
            for pos in new_shard_columns_positions {
                let value = update_tuple.get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::TupleBuilderCommand,
                        Some(format_smolstr!(
                            "invalid pos: {pos} for update tuple with len: {}",
                            update_tuple.len()
                        )),
                    )
                })?;
                update_tuple_shard_key.push(value);
            }
            let bucket_id = runtime.determine_bucket_id(&update_tuple_shard_key)?;
            index.add_entry(bucket_id, pointer);
        }
        let delete_tuple_len = delete_tuples.first().map(Vec::len);
        self.set_bucket_index(index.value);
        self.get_mut_tuples().extend(delete_tuples);
        Ok(delete_tuple_len)
    }

    /// Adds rows that are not present in `Self`
    /// from another virtual table.
    ///
    /// Assumptions:
    /// 1. All columns from `from_vtable` are located in
    ///    a row from the beginning of the current vtable's columns.
    ///
    /// # Errors
    /// - invalid arguments
    pub fn add_missing_rows(&mut self, from_vtable: &Rc<VirtualTable>) -> Result<(), SbroadError> {
        if from_vtable.columns.len() >= self.columns.len() {
            return Err(SbroadError::UnexpectedNumberOfValues(
                "from vtable must have less columns then self vtable!".into(),
            ));
        }
        let mut current_tuples: AHashSet<&[Value]> = AHashSet::with_capacity(self.tuples.len());

        let key_tuple_len = from_vtable.columns.len();
        for tuple in &self.tuples {
            let key_tuple = &tuple[..key_tuple_len];
            current_tuples.insert(key_tuple);
        }
        current_tuples.shrink_to_fit();

        let estimated_capacity = from_vtable.tuples.len().saturating_sub(self.tuples.len());
        let mut missing_tuples: HashMap<VTableTuple, usize, RepeatableState> =
            HashMap::with_capacity_and_hasher(estimated_capacity, RepeatableState);
        let mut missing_tuples_cnt: usize = 0;
        for tuple in &from_vtable.tuples {
            if !current_tuples.contains(&tuple[..]) {
                if let Some(cnt) = missing_tuples.get_mut(tuple) {
                    *cnt += 1;
                } else {
                    missing_tuples.insert(tuple.clone(), 1);
                }
            }
            missing_tuples_cnt += 1;
        }

        let move_to_slice = |dst: &mut [Value], src: Vec<Value>| {
            for (to, from) in dst.iter_mut().zip(src) {
                *to = from;
            }
        };

        self.tuples.reserve(missing_tuples_cnt);
        for (key_tuple, count) in missing_tuples {
            let mut joined_tuple = vec![Value::Null; self.columns.len()];
            move_to_slice(&mut joined_tuple[0..key_tuple_len], key_tuple);
            for _ in 0..count - 1 {
                self.tuples.push(joined_tuple.clone());
            }
            self.tuples.push(joined_tuple);
        }

        Ok(())
    }

    /// Removes duplicates from virtual table, the order
    /// of rows is changed.
    pub fn remove_duplicates(&mut self) {
        // O(1) extra space and O(n*log n) time implementation

        self.tuples.sort_unstable();
        let mut unique_cnt = 0;
        let len = self.tuples.len();
        for idx in 1..len {
            if self.tuples[idx] != self.tuples[idx - 1] {
                self.tuples.swap(unique_cnt, idx - 1);
                unique_cnt += 1;
            }
        }
        if len > 0 {
            self.tuples.swap(unique_cnt, len - 1);
            unique_cnt += 1;
        }
        self.tuples.truncate(unique_cnt);
    }

    /// Convert vtable to output tuple for returning
    /// result from stored procedure.
    ///
    /// # Errors
    /// - Failed to create tuple
    pub fn to_output(&mut self, motion_aliases: &[SmolStr]) -> Result<Box<dyn Any>, SbroadError> {
        let mut metadata = Vec::with_capacity(self.columns.len());
        for (col, alias) in self.columns.iter().zip(motion_aliases.iter()) {
            let meta_column = MetadataColumn::new(alias.to_string(), col.r#type.to_string());
            metadata.push(meta_column);
        }

        let tuples = std::mem::take(&mut self.tuples);
        let mut rows: Vec<ExecutorTuple> = Vec::with_capacity(tuples.len());
        for tuple in tuples {
            let mut row = Vec::with_capacity(self.columns.len());
            for value in tuple {
                row.push(value.into());
            }
            rows.push(row);
        }

        let res = vec![ProducerResult {
            metadata,
            rows,
            // cache_miss: None,
        }];
        #[cfg(feature = "mock")]
        {
            Ok(Box::new(res))
        }
        #[cfg(not(feature = "mock"))]
        {
            Ok(Box::new(Tuple::new(&res).map_err(|e| {
                SbroadError::Invalid(
                    Entity::VirtualTable,
                    Some(format_smolstr!("failed to create tuple from vtable: {e}")),
                )
            })?))
        }
    }
}

struct TupleIterator<'t> {
    vtable: &'t VirtualTable,
    buf: Vec<EncodedValue<'t>>,
    row_id: usize,
}

impl<'t> TupleIterator<'t> {
    fn new(vtable: &'t VirtualTable) -> Self {
        let buf = Vec::with_capacity(vtable.get_columns().len() + 1);
        TupleIterator {
            vtable,
            buf,
            row_id: 0,
        }
    }

    fn next<'a>(&'a mut self) -> Option<&'a [EncodedValue<'t>]> {
        let pk = self.row_id as u64;

        let row_id = self.row_id;

        let vt_tuple = self.vtable.get_tuples().get(row_id)?;

        self.buf.clear();
        for value in vt_tuple {
            self.buf.push(MsgPackValue::from(value).into());
        }
        self.buf.push(LuaValue::Unsigned(pk).into());

        self.row_id += 1;
        Some(&self.buf)
    }
}

fn write_vtable_as_msgpack(vtable: &VirtualTable, stream: &mut impl Write) {
    let array_len =
        u32::try_from(vtable.get_tuples().len()).expect("expected u32 tuples in virtual table");
    write_u32_array_len(stream, array_len).expect("failed to write array len");

    let mut ser = Serializer::new(stream);
    let mut tuple_iter = TupleIterator::new(vtable);

    while let Some(tuple) = tuple_iter.next() {
        tuple
            .serialize(&mut ser)
            .expect("failed to serialize tuple");
    }
}

fn vtable_marking(vtable: &VirtualTable) -> Vec<usize> {
    let mut marking: Vec<usize> = Vec::with_capacity(vtable.get_tuples().len());
    let mut tuple_iter = TupleIterator::new(vtable);
    while let Some(tuple) = tuple_iter.next() {
        let mut byte_counter = ByteCounter::default();
        let mut ser = Serializer::new(&mut byte_counter);
        tuple
            .serialize(&mut ser)
            .expect("temporary table serialization failed");
        marking.push(byte_counter.bytes());
    }
    marking
}

impl ExecutionPlan {
    pub fn encode_vtables(&self) -> EncodedTables {
        let Some(vtables) = self.get_vtables() else {
            return EncodedTables::default();
        };
        let mut encoded_tables = EncodedTables::with_capacity(vtables.len());

        for (id, vtable) in vtables {
            let marking = vtable_marking(vtable);
            // Array marker (1 byte) + array length (4 bytes) + tuples.
            let data_len = 5 + marking.iter().sum::<usize>();
            assert!(data_len <= u32::MAX as usize);
            let mut builder = TupleBuilder::rust_allocated();
            builder.reserve(data_len);
            write_vtable_as_msgpack(vtable, &mut builder);
            let Ok(tuple) = builder.into_tuple() else {
                unreachable!("failed to build binary table")
            };
            let binary_table = Binary::from(tuple);
            encoded_tables.insert(*id, EncodedRows::new(marking, binary_table));
        }
        encoded_tables
    }
}

/// In case Vtable tuples have values of different types, we try to
/// unify them:
/// * Some types (like String or Boolean) support only values of the same type. In case we met inconsistency,
///   we throw an error.
/// * Numerical values can be cast according to the following order:
///   Decimal > Double > Integer > Unsigned > Null.
///
/// # Errors
/// - Contains values of inconsistent types.
///
/// # Panics
/// - Internal error.
#[allow(clippy::too_many_lines)]
pub fn calculate_vtable_unified_types(
    vtable: &VirtualTable,
) -> Result<Vec<(bool, Type)>, SbroadError> {
    // Map of { type -> types_which_can_be_upcasted_to_given_one }.
    let get_types_less = |ty: &Type| -> &[Type] {
        match ty {
            Type::Scalar
            | Type::Any
            | Type::Map
            | Type::Array
            | Type::Boolean
            | Type::String
            | Type::Datetime => &[],
            Type::Uuid => &[Type::String],
            Type::Unsigned => &[Type::Scalar],
            Type::Integer => &[Type::Scalar, Type::Unsigned],
            Type::Double => &[Type::Scalar, Type::Unsigned, Type::Integer],
            Type::Decimal => &[Type::Scalar, Type::Unsigned, Type::Integer, Type::Double],
            Type::Number => &[
                Type::Scalar,
                Type::Unsigned,
                Type::Integer,
                Type::Double,
                Type::Decimal,
            ],
        }
    };

    let columns_len = vtable.columns.len();
    let mut nullable_column_indices = HashSet::with_capacity(columns_len);
    let fix_type = |current_type_unified: &mut Option<Type>, given_type: &Type| {
        if let Some(current_type_unified) = current_type_unified {
            if get_types_less(given_type).contains(current_type_unified) {
                *current_type_unified = *given_type;
            } else if *given_type != *current_type_unified
                && !get_types_less(current_type_unified).contains(given_type)
            {
                return Err(SbroadError::Invalid(
                    Entity::Type,
                    Some(format_smolstr!("Virtual table contains values of inconsistent types: {current_type_unified:?} and {given_type:?}.")),
                ));
            }
        } else {
            *current_type_unified = Some(*given_type);
        }
        Ok(())
    };

    let mut unified_types: Vec<Option<Type>> = iter::repeat(None).take(columns_len).collect();

    for tuple in vtable.get_tuples() {
        for (i, value) in tuple.iter().enumerate() {
            let current_type_unified = unified_types
                .get_mut(i)
                .expect("Unified types vec isn't initialized.");
            match value {
                Value::Boolean(_) => {
                    fix_type(current_type_unified, &Type::Boolean)?;
                }
                Value::String(_) => {
                    fix_type(current_type_unified, &Type::String)?;
                }
                Value::Uuid(_) => {
                    fix_type(current_type_unified, &Type::Uuid)?;
                }
                Value::Datetime(_) => {
                    fix_type(current_type_unified, &Type::Datetime)?;
                }
                Value::Null => {
                    nullable_column_indices.insert(i);
                }
                Value::Unsigned(_) => {
                    fix_type(current_type_unified, &Type::Unsigned)?;
                }
                Value::Integer(_) => {
                    fix_type(current_type_unified, &Type::Integer)?;
                }
                Value::Double(_) => {
                    fix_type(current_type_unified, &Type::Double)?;
                }
                Value::Decimal(_) => {
                    fix_type(current_type_unified, &Type::Decimal)?;
                }
                Value::Tuple(_) => panic!("Unexpected tuple under values."),
            }
        }
    }

    if unified_types
        .first()
        .expect("Unified types vec is empty.")
        .is_none()
    {
        // This is possible in case we deal with VALUES like
        // `values ((select a from t where false), (select a from t where false), ...)`
        // where there are no tuples to traverse so that we just infer types returned to us from
        // local SQL execution.
        Ok(vtable
            .get_columns()
            .iter()
            .map(|c| (c.is_nullable, c.r#type))
            .collect())
    } else {
        Ok(unified_types
            .into_iter()
            .zip(vtable.get_columns().iter())
            .enumerate()
            .map(|(i, (t, c))| {
                let t = if let Some(t) = t { t } else { c.r#type };
                (nullable_column_indices.contains(&i), t)
            })
            .collect())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct VirtualTableMap(HashMap<NodeId, Rc<VirtualTable>>);

impl VirtualTableMap {
    #[must_use]
    pub fn new(map: HashMap<NodeId, Rc<VirtualTable>>) -> Self {
        Self(map)
    }

    #[must_use]
    pub fn map(&self) -> &HashMap<NodeId, Rc<VirtualTable>> {
        &self.0
    }

    pub fn mut_map(&mut self) -> &mut HashMap<NodeId, Rc<VirtualTable>> {
        &mut self.0
    }
}

#[cfg(test)]
mod tests;

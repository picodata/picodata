use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::errors::SbroadError;
use crate::executor::engine::helpers::{new_table_name, write_insert_args, TupleBuilderPattern};
use crate::executor::engine::VersionMap;
use crate::executor::ir::{ExecutionPlan, QueryType};
use crate::executor::protocol::SchemaInfo;
use crate::executor::vtable::{VTableTuple, VirtualTable, VirtualTableTupleEncoder};
use crate::ir::helpers::RepeatableState;
use crate::ir::node::relational::Relational;
use crate::ir::node::Motion;
use crate::ir::relation::Column;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::tree::Snapshot;
use rmp::encode::write_array_len;
use smol_str::SmolStr;
use sql_protocol::dml::delete::{
    CoreDeleteDataSource, DeleteFilteredDataSource, DeleteFullDataSource,
};
use sql_protocol::dml::insert::ConflictPolicy;
use sql_protocol::dml::insert::{
    CoreInsertDataSource, InsertDataSource, InsertMaterializedDataSource,
};
use sql_protocol::dml::update::UpdateType;
use sql_protocol::dml::update::{
    CoreUpdateDataSource, UpdateDataSource, UpdateSharedKeyDataSource,
};
use sql_protocol::dql::DQLCacheMissResult::{Sql, VtablesMetadata};
use sql_protocol::dql::DQLResult::{Options, Params, Vtables};
use sql_protocol::dql::{DQLCacheMissResult, DQLResult};
use sql_protocol::dql_encoder::{ColumnType, DQLCacheMissDataSource, DQLDataSource, MsgpackEncode};
use sql_protocol::error::ProtocolError;
use sql_protocol::iterators::MsgpackMapIterator;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::marker::PhantomData;
use std::ops::Not;
use std::rc::Rc;
use tarantool::msgpack::{Context, Encode};

/// Adapter for a slice to satisfy protocol trait MsgpackEncode
struct ArrayMsgpackEncoder<'e, V>
where
    V: Encode,
{
    data: &'e [V],
}

impl<'e, V> ArrayMsgpackEncoder<'e, V>
where
    V: Encode,
{
    fn new(data: &'e [V]) -> Self {
        Self { data }
    }
}

impl<V: Encode> MsgpackEncode for ArrayMsgpackEncoder<'_, V> {
    fn encode_into(&self, w: &mut impl Write) -> std::io::Result<()> {
        self.data
            .encode(w, &Context::DEFAULT)
            .map_err(std::io::Error::other)
    }
}

/// Datasource for DQL queries
pub struct ExecutionData {
    plan_id: u64,
    sender_id: u64,
    vtables: HashMap<SmolStr, Rc<VirtualTable>>,
    plan: ExecutionPlan, // TODO: maybe Rc<> + top_id is better
}

impl DQLDataSource for ExecutionData {
    fn get_table_schema_info(&self) -> impl ExactSizeIterator<Item = (u32, u64)> {
        self.plan
            .get_ir_plan()
            .table_version_map
            .iter()
            .map(|(k, v)| (*k, *v))
    }

    fn get_index_schema_info(&self) -> impl ExactSizeIterator<Item = ([u32; 2], u64)> {
        self.plan
            .get_ir_plan()
            .index_version_map
            .iter()
            .map(|(k, v)| (*k, *v))
    }

    fn get_plan_id(&self) -> u64 {
        self.plan_id
    }

    fn get_sender_id(&self) -> u64 {
        self.sender_id
    }

    fn get_request_id(&self) -> &str {
        self.plan.get_request_id()
    }

    fn get_vtables(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, impl ExactSizeIterator<Item = impl MsgpackEncode>)>
    {
        self.vtables.iter().map(|(name, table)| {
            (
                name.as_str(),
                table
                    .get_tuples()
                    .iter()
                    .enumerate()
                    .map(|(pk, tuple)| VirtualTableTupleEncoder::new(tuple, pk as u64)),
            )
        })
    }

    fn get_options(&self) -> [u64; 2] {
        let options = &self.plan.get_ir_plan().effective_options;
        [
            options.sql_motion_row_max as u64,
            options.sql_vdbe_opcode_max as u64,
        ]
    }

    fn get_params(&self) -> impl MsgpackEncode {
        ArrayMsgpackEncoder::new(self.plan.get_ir_plan().get_params().as_slice())
    }
}

pub struct DeleteCoreData {
    pub request_id: SmolStr,
    pub space_id: u32,
    pub space_version: u64,
}

pub struct FullDeleteData {
    core: DeleteCoreData,
    plan_id: u64,
    options: (u64, u64),
}

impl FullDeleteData {
    pub fn new(core: DeleteCoreData, plan_id: u64, options: (u64, u64)) -> Self {
        Self {
            core,
            plan_id,
            options,
        }
    }
}

impl CoreDeleteDataSource for FullDeleteData {
    fn get_request_id(&self) -> &str {
        &self.core.request_id
    }
    fn get_target_table_id(&self) -> u32 {
        self.core.space_id
    }
    fn get_target_table_version(&self) -> u64 {
        self.core.space_version
    }
}

impl DeleteFullDataSource for FullDeleteData {
    fn get_plan_id(&self) -> u64 {
        self.plan_id
    }

    fn get_options(&self) -> [u64; 2] {
        [self.options.0, self.options.1]
    }
}

pub struct FilteredDeleteData<'a> {
    core: DeleteCoreData,
    types: Vec<Column>,
    pattern: TupleBuilderPattern,
    dql_data: &'a ExecutionData,
}

impl<'a> FilteredDeleteData<'a> {
    pub fn new(
        core: DeleteCoreData,
        types: Vec<Column>,
        pattern: TupleBuilderPattern,
        dql_data: &'a ExecutionData,
    ) -> Self {
        Self {
            core,
            types,
            pattern,
            dql_data,
        }
    }
}

impl CoreDeleteDataSource for FilteredDeleteData<'_> {
    fn get_request_id(&self) -> &str {
        &self.core.request_id
    }
    fn get_target_table_id(&self) -> u32 {
        self.core.space_id
    }
    fn get_target_table_version(&self) -> u64 {
        self.core.space_version
    }
}

impl DeleteFilteredDataSource for FilteredDeleteData<'_> {
    fn get_column_types(&self) -> impl ExactSizeIterator<Item = ColumnType> {
        self.types.iter().map(|x| x.r#type.into())
    }

    fn get_builder(&self) -> impl MsgpackEncode {
        ArrayMsgpackEncoder::new(self.pattern.as_slice())
    }

    fn get_dql_data_source(&self) -> &impl DQLDataSource {
        self.dql_data
    }
}

pub struct InsertCoreData {
    pub request_id: SmolStr,
    pub space_id: u32,
    pub space_version: u64,
    pub conflict_policy: ConflictPolicy,
}

pub struct TupleInsertData {
    core: InsertCoreData,
    pattern: TupleBuilderPattern,
    vtable: Rc<VirtualTable>,
}

impl TupleInsertData {
    pub fn new(
        core: InsertCoreData,
        vtable: Rc<VirtualTable>,
        pattern: TupleBuilderPattern,
    ) -> Self {
        Self {
            core,
            vtable,
            pattern,
        }
    }
}

impl CoreInsertDataSource for TupleInsertData {
    fn get_request_id(&self) -> &str {
        &self.core.request_id
    }
    fn get_target_table_id(&self) -> u32 {
        self.core.space_id
    }
    fn get_target_table_version(&self) -> u64 {
        self.core.space_version
    }
    fn get_conflict_policy(&self) -> ConflictPolicy {
        self.core.conflict_policy
    }
}

struct VTableTupleIterator<I, V>
where
    I: Iterator<Item = V>,
    V: MsgpackEncode,
{
    iter: I,
    index: usize,
    size: usize,
}

impl<I, V> VTableTupleIterator<I, V>
where
    I: Iterator<Item = V>,
    V: MsgpackEncode,
{
    fn new(iter: I, size: usize) -> Self {
        Self {
            iter,
            index: 0,
            size,
        }
    }
}

impl<I, V> Iterator for VTableTupleIterator<I, V>
where
    I: Iterator<Item = V>,
    V: MsgpackEncode,
{
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        let tuple = self.iter.next()?;
        self.index += 1;
        Some(tuple)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.size.saturating_sub(self.index);
        (len, Some(len))
    }
}

impl<I, V> ExactSizeIterator for VTableTupleIterator<I, V>
where
    I: Iterator<Item = V>,
    V: MsgpackEncode,
{
}

impl InsertDataSource for TupleInsertData {
    fn get_tuples(&self) -> impl ExactSizeIterator<Item = impl MsgpackEncode> {
        let iter = self
            .vtable
            .get_bucket_index()
            .iter()
            .flat_map(|(bid, tuples)| {
                tuples.iter().map(|idx| {
                    InsertTupleEncoder::new(
                        self.vtable
                            .get_tuples()
                            .get(*idx)
                            .expect("tuple must be in vtable"),
                        &self.pattern,
                        Some(*bid),
                    )
                })
            });
        VTableTupleIterator::new(iter, self.vtable.get_tuples().len())
    }
}

/// Helper for encoding tuples into msgpack for insert operation
struct InsertTupleEncoder<'t> {
    tuple: &'t VTableTuple,
    pattern: &'t TupleBuilderPattern,
    bucket_id: Option<u64>,
}

impl<'t> InsertTupleEncoder<'t> {
    fn new(
        tuple: &'t VTableTuple,
        pattern: &'t TupleBuilderPattern,
        bucket_id: Option<u64>,
    ) -> Self {
        Self {
            tuple,
            pattern,
            bucket_id,
        }
    }
}

impl MsgpackEncode for InsertTupleEncoder<'_> {
    fn encode_into(&self, w: &mut impl Write) -> std::io::Result<()> {
        write_array_len(w, self.pattern.len() as u32)?;
        write_insert_args(self.tuple, self.pattern, self.bucket_id.as_ref(), w)
            .map_err(|e| std::io::Error::other(format!("failed to build insert args: {e}")))?;

        Ok(())
    }
}

pub struct LocalInsertData<'a> {
    core: InsertCoreData,
    types: Vec<Column>,
    pattern: TupleBuilderPattern,
    dql_data: &'a ExecutionData,
}

impl<'a> LocalInsertData<'a> {
    pub fn new(
        core: InsertCoreData,
        types: Vec<Column>,
        pattern: TupleBuilderPattern,
        dql_data: &'a ExecutionData,
    ) -> Self {
        Self {
            core,
            types,
            pattern,
            dql_data,
        }
    }
}

impl CoreInsertDataSource for LocalInsertData<'_> {
    fn get_request_id(&self) -> &str {
        &self.core.request_id
    }
    fn get_target_table_id(&self) -> u32 {
        self.core.space_id
    }
    fn get_target_table_version(&self) -> u64 {
        self.core.space_version
    }
    fn get_conflict_policy(&self) -> ConflictPolicy {
        self.core.conflict_policy
    }
}

impl InsertMaterializedDataSource for LocalInsertData<'_> {
    fn get_column_types(&self) -> impl ExactSizeIterator<Item = ColumnType> {
        self.types.iter().map(|x| x.r#type.into())
    }

    fn get_builder(&self) -> impl MsgpackEncode {
        ArrayMsgpackEncoder::new(self.pattern.as_slice())
    }

    fn get_dql_data_source(&self) -> &impl DQLDataSource {
        self.dql_data
    }
}

pub struct UpdateCoreData {
    pub request_id: SmolStr,
    pub space_id: u32,
    pub space_version: u64,
    pub update_type: UpdateType,
}

/// SharedUpdateData is used for shared update.
/// Contains the `vtable` with tuples to delete and tuples to update.
/// The `del_index` field tracks which tuples should be deleted.
pub struct SharedUpdateData {
    core: UpdateCoreData,
    pattern: TupleBuilderPattern,
    del_index: HashSet<usize>,
    vtable: Rc<VirtualTable>,
}

impl SharedUpdateData {
    pub fn new(
        core: UpdateCoreData,
        del_tuple_size: usize,
        vtable: Rc<VirtualTable>,
        pattern: TupleBuilderPattern,
    ) -> Self {
        // if a tuple size is del_tuple_size, then it should be deleted
        let del_size = vtable
            .get_tuples()
            .iter()
            .filter(|t| t.len() == del_tuple_size)
            .count();

        let mut del_index = HashSet::with_capacity(del_size);
        vtable
            .get_tuples()
            .iter()
            .enumerate()
            .filter(|(_, tuple)| tuple.len() == del_tuple_size)
            .for_each(|(i, _)| {
                del_index.insert(i);
            });

        Self {
            core,
            del_index,
            vtable,
            pattern,
        }
    }
}

impl CoreUpdateDataSource for SharedUpdateData {
    fn get_request_id(&self) -> &str {
        &self.core.request_id
    }
    fn get_target_table_id(&self) -> u32 {
        self.core.space_id
    }
    fn get_target_table_version(&self) -> u64 {
        self.core.space_version
    }
    fn get_update_type(&self) -> UpdateType {
        self.core.update_type
    }
}

impl UpdateSharedKeyDataSource for SharedUpdateData {
    fn get_del_tuples(&self) -> impl ExactSizeIterator<Item = impl MsgpackEncode> {
        self.del_index
            .iter()
            .map(|i| PureTupleEncoder(self.vtable.get_tuples().get(*i).expect("should be present")))
    }
    fn get_tuples(&self) -> impl ExactSizeIterator<Item = impl MsgpackEncode> {
        let iter = self
            .vtable
            .get_bucket_index()
            .iter()
            .flat_map(|(bid, tuples)| {
                tuples
                    .iter()
                    .filter(|i| self.del_index.contains(i).not())
                    .map(|i| {
                        UpdateTupleEncoder::new(
                            self.vtable
                                .get_tuples()
                                .get(*i)
                                .expect("tuple must be in vtable"),
                            &self.pattern,
                            *bid,
                        )
                    })
            });

        // we know that the size of tuples is equal to self.vtable.get_tuples().len() - self.del_index.len()
        // so we use adapter to satisfy the ExactSizeIterator trait
        VTableTupleIterator::new(iter, self.vtable.get_tuples().len() - self.del_index.len())
    }
}

/// Adapter for encoding tuple into msgpack to satisfy protocol MsgpackEncode trait
struct PureTupleEncoder<'a>(&'a VTableTuple);

impl MsgpackEncode for PureTupleEncoder<'_> {
    fn encode_into(&self, w: &mut impl Write) -> std::io::Result<()> {
        self.0
            .encode(w, &Context::DEFAULT)
            .map_err(std::io::Error::other)?;

        Ok(())
    }
}

/// Helper for encoding tuples into msgpack for update operation
struct UpdateTupleEncoder<'a> {
    tuple: &'a VTableTuple,
    pattern: &'a TupleBuilderPattern,
    bucket_id: u64,
}

impl<'a> UpdateTupleEncoder<'a> {
    fn new(tuple: &'a VTableTuple, pattern: &'a TupleBuilderPattern, bucket_id: u64) -> Self {
        Self {
            tuple,
            pattern,
            bucket_id,
        }
    }
}

impl MsgpackEncode for UpdateTupleEncoder<'_> {
    fn encode_into(&self, w: &mut impl Write) -> std::io::Result<()> {
        write_array_len(w, self.pattern.len() as u32)?;
        crate::executor::engine::helpers::write_shared_update_args(
            self.tuple,
            self.pattern,
            self.bucket_id,
            w,
        )
        .map_err(std::io::Error::other)?;

        Ok(())
    }
}
pub struct LocalUpdateData<'a> {
    core: UpdateCoreData,
    types: Vec<Column>,
    pattern: TupleBuilderPattern,
    dql_data: &'a ExecutionData,
}

impl<'a> LocalUpdateData<'a> {
    pub fn new(
        core: UpdateCoreData,
        types: Vec<Column>,
        pattern: TupleBuilderPattern,
        dql_data: &'a ExecutionData,
    ) -> Self {
        Self {
            core,
            types,
            pattern,
            dql_data,
        }
    }
}

impl CoreUpdateDataSource for LocalUpdateData<'_> {
    fn get_request_id(&self) -> &str {
        &self.core.request_id
    }
    fn get_target_table_id(&self) -> u32 {
        self.core.space_id
    }
    fn get_target_table_version(&self) -> u64 {
        self.core.space_version
    }
    fn get_update_type(&self) -> UpdateType {
        self.core.update_type
    }
}

impl UpdateDataSource for LocalUpdateData<'_> {
    fn get_column_types(&self) -> impl ExactSizeIterator<Item = ColumnType> {
        self.types.iter().map(|x| x.r#type.into())
    }
    fn get_builder(&self) -> impl MsgpackEncode {
        ArrayMsgpackEncoder::new(self.pattern.as_slice())
    }
    fn get_dql_data_source(&self) -> &impl DQLDataSource {
        self.dql_data
    }
}

#[derive(Default)]
pub struct PlanVersion {
    pub table_version_map: VersionMap,
    pub index_version_map: HashMap<[u32; 2], u64, RepeatableState>,
}

/// Data for handle dql cache miss
#[derive(Default)]
pub struct ExecutionCacheMissData {
    pub schema_info: PlanVersion,
    pub vtables_meta: HashMap<SmolStr, Vec<(SmolStr, ColumnType)>>,
    pub sql: String,
}

impl TryFrom<ExecutionData> for ExecutionCacheMissData {
    type Error = SbroadError;

    fn try_from(value: ExecutionData) -> Result<Self, Self::Error> {
        let sql = {
            let plan_id = value.get_plan_id();
            let plan = value.plan.get_ir_plan();
            let top_id = if plan.is_dml()? {
                let child_id = plan.children(plan.get_top()?)[0];
                value.plan.get_motion_child(child_id)?
            } else {
                plan.get_top()?
            };

            let sp = SyntaxPlan::new(&value.plan, top_id, Snapshot::Oldest)?;
            let on = OrderedSyntaxNodes::try_from(sp)?;
            let a = on.to_syntax_data()?;
            let (sql, _) = value
                .plan
                .generate_sql(a.as_slice(), plan_id, None, new_table_name)?;
            sql
        };

        let vtables_meta = {
            let plan_id = value.get_plan_id();
            value
                .plan
                .get_vtables()
                .iter()
                .map(|(k, v)| {
                    let columns = v
                        .get_columns()
                        .iter()
                        .map(|column| (column.name.clone(), column.r#type.into()));
                    (new_table_name(plan_id, *k), columns.collect::<Vec<_>>())
                })
                .collect::<HashMap<_, _>>()
        };

        let index_version_map = value.plan.plan.index_version_map;
        let schema_info = PlanVersion {
            table_version_map: value.plan.plan.table_version_map,
            index_version_map,
        };
        Ok(Self {
            schema_info,
            vtables_meta,
            sql,
        })
    }
}

impl DQLCacheMissDataSource for ExecutionCacheMissData {
    fn get_table_schema_info(&self) -> impl ExactSizeIterator<Item = (u32, u64)> {
        self.schema_info
            .table_version_map
            .iter()
            .map(|(k, v)| (*k, *v))
    }
    fn get_index_schema_info(&self) -> impl ExactSizeIterator<Item = ([u32; 2], u64)> {
        self.schema_info
            .index_version_map
            .iter()
            .map(|(k, v)| (*k, *v))
    }
    fn get_vtables_metadata(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, impl ExactSizeIterator<Item = (&str, ColumnType)>)>
    {
        self.vtables_meta
            .iter()
            .map(|(k, v)| (k.as_str(), v.iter().map(|(name, ty)| (name.as_str(), *ty))))
    }
    fn get_sql(&self) -> &str {
        self.sql.as_str()
    }
}

pub fn build_dql_data_source(
    exec_plan: ExecutionPlan,
    sender_id: u64,
) -> Result<ExecutionData, SbroadError> {
    let query_type = exec_plan.query_type()?;
    let mut sub_plan_id = None;
    {
        let ir = exec_plan.get_ir_plan();
        let top_id = ir.get_top()?;
        match query_type {
            QueryType::DQL => {
                sub_plan_id = Some(ir.new_pattern_id(top_id)?);
            }
            QueryType::DML => {
                let top = ir.get_relation_node(top_id)?;
                let top_children = ir.children(top_id);
                if matches!(top, Relational::Delete(_)) && top_children.is_empty() {
                    sub_plan_id = Some(ir.new_pattern_id(top_id)?);
                } else {
                    let child_id = top_children[0];
                    let is_cacheable = matches!(
                        ir.get_relation_node(child_id)?,
                        Relational::Motion(Motion {
                            policy: MotionPolicy::Local | MotionPolicy::LocalSegment { .. },
                            ..
                        })
                    );
                    if is_cacheable {
                        let cacheable_subtree_root_id =
                            exec_plan.get_motion_subtree_root(child_id)?;
                        sub_plan_id = Some(ir.new_pattern_id(cacheable_subtree_root_id)?);
                    }
                }
            }
        };
    }
    let plan_id = sub_plan_id.expect("should be initialized");
    let vtables = exec_plan
        .get_vtables()
        .iter()
        .map(|(k, t)| (new_table_name(plan_id, *k), t.clone()))
        .collect();

    Ok(ExecutionData {
        plan_id,
        sender_id,
        vtables,
        plan: exec_plan,
    })
}

pub trait PlanInfo {
    fn schema_info(&self) -> &SchemaInfo;

    fn plan_id(&self) -> u64;

    fn get_cache_hit_iter(&self) -> impl Iterator<Item = Result<DQLResult<'_>, ProtocolError>>;

    fn get_cache_miss_iter(
        &self,
    ) -> impl Iterator<Item = Result<DQLCacheMissResult<'_>, ProtocolError>>;
}

pub struct FullDeletePlanInfo {
    plan_id: u64,
    schema_info: SchemaInfo,
    options: (u64, u64),
    sql: String,
}

impl FullDeletePlanInfo {
    pub fn new(plan_id: u64, table_name: &str, options: (u64, u64)) -> Self {
        Self {
            plan_id,
            options,
            schema_info: SchemaInfo::default(),
            sql: format!("DELETE FROM \"{}\"", table_name),
        }
    }
}

impl PlanInfo for FullDeletePlanInfo {
    fn schema_info(&self) -> &SchemaInfo {
        &self.schema_info
    }

    fn plan_id(&self) -> u64 {
        self.plan_id
    }

    fn get_cache_hit_iter(&self) -> impl Iterator<Item = Result<DQLResult<'_>, ProtocolError>> {
        FullDeleteCacheHitIter::new(self.options)
    }

    fn get_cache_miss_iter(
        &self,
    ) -> impl Iterator<Item = Result<DQLCacheMissResult<'_>, ProtocolError>> {
        FullDeleteCacheMissIter::new(&self.sql)
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum FullDeleteState {
    Vtables = 0,
    Options,
    Params,
    End,
}

struct FullDeleteCacheHitIter<'a> {
    state: FullDeleteState,
    options: (u64, u64),
    phantom: PhantomData<&'a str>,
}

impl FullDeleteCacheHitIter<'_> {
    fn new(options: (u64, u64)) -> Self {
        Self {
            state: FullDeleteState::Vtables,
            options,
            phantom: PhantomData,
        }
    }
}

impl<'a> Iterator for FullDeleteCacheHitIter<'a> {
    type Item = Result<DQLResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            FullDeleteState::Vtables => {
                self.state = FullDeleteState::Options;
                Some(Ok(Vtables(MsgpackMapIterator::empty())))
            }
            FullDeleteState::Options => {
                self.state = FullDeleteState::Params;
                Some(Ok(Options(self.options)))
            }
            FullDeleteState::Params => {
                self.state = FullDeleteState::End;
                Some(Ok(Params(&[0x90]))) // empty array
            }
            FullDeleteState::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = (FullDeleteState::End as usize).saturating_sub(self.state as usize);
        (len, Some(len))
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum FullDeleteMissState {
    VtablesMetadata = 0,
    Sql,
    End,
}

struct FullDeleteCacheMissIter<'a> {
    state: FullDeleteMissState,
    sql: &'a str,
}

impl<'a> FullDeleteCacheMissIter<'a> {
    fn new(sql: &'a str) -> Self {
        Self {
            state: FullDeleteMissState::VtablesMetadata,
            sql,
        }
    }
}

impl<'a> Iterator for FullDeleteCacheMissIter<'a> {
    type Item = Result<DQLCacheMissResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            FullDeleteMissState::VtablesMetadata => {
                self.state = FullDeleteMissState::Sql;
                Some(Ok(VtablesMetadata(MsgpackMapIterator::empty())))
            }
            FullDeleteMissState::Sql => {
                self.state = FullDeleteMissState::End;
                Some(Ok(Sql(self.sql)))
            }
            FullDeleteMissState::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = (FullDeleteMissState::End as usize).saturating_sub(self.state as usize);
        (len, Some(len))
    }
}

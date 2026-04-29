use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::{table_name, write_insert_args, TupleBuilderPattern};
use crate::executor::engine::VersionMap;
use crate::executor::ir::{DqlSubtree, ExecutionPlan, SubtreeViewBuilder};
use crate::executor::vtable::{
    VTableTuple, VirtualTable, VirtualTableMap, VirtualTableTupleEncoder,
};
use crate::ir::helpers::RepeatableState;
use crate::ir::node::NodeId;
use crate::ir::relation::Column;
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;
use rmp::encode::write_array_len;
use smol_str::{format_smolstr, SmolStr};
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
use sql_protocol::dql_encoder::{
    ColumnType, DQLCacheMissDataSource, DQLDataSource, DQLOptions, MsgpackEncode,
};
use std::collections::{HashMap, HashSet};
use std::io::Write;
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

/// Datasource for DQL queries backed by an owned execution plan.
pub struct ExecutionData {
    plan_id: u64,
    sender_id: u64,
    vtables: HashMap<SmolStr, Rc<VirtualTable>>,
    sql_top_id: NodeId,
    constant_ids: Vec<NodeId>,
    params: Vec<Value>,
    plan: ExecutionPlan, // TODO: maybe Rc<> + top_id is better
}

/// Datasource for DQL queries over an immutable execution plan.
pub struct DqlProtocol {
    plan_id: u64,
    sender_id: u64,
    vtables: HashMap<SmolStr, Rc<VirtualTable>>,
    sql_top_id: NodeId,
    constant_ids: Vec<NodeId>,
    params: Vec<Value>,
    plan: Rc<ExecutionPlan>,
}

impl ExecutionData {
    /// Returns the plan id used by the remote SQL cache.
    pub fn plan_id(&self) -> u64 {
        self.plan_id
    }

    /// Returns virtual tables that must be sent with this query.
    pub fn vtables(&self) -> &HashMap<SmolStr, Rc<VirtualTable>> {
        &self.vtables
    }

    /// Returns effective DQL options encoded into the protocol message.
    pub fn options(&self) -> DQLOptions {
        self.plan
            .get_ir_plan()
            .effective_options
            .to_protocol_options()
    }

    /// Encodes local SQL parameter values as msgpack.
    pub fn encoded_params(&self) -> Vec<u8> {
        tarantool::msgpack::encode(&self.params)
    }
}

impl DqlProtocol {
    /// Returns the plan id used by the remote SQL cache.
    pub fn plan_id(&self) -> u64 {
        self.plan_id
    }

    /// Returns virtual tables visible inside this DQL subtree.
    pub fn vtables(&self) -> &HashMap<SmolStr, Rc<VirtualTable>> {
        &self.vtables
    }

    /// Returns effective DQL options encoded into the protocol message.
    pub fn options(&self) -> DQLOptions {
        self.plan
            .get_ir_plan()
            .effective_options
            .to_protocol_options()
    }

    /// Encodes local SQL parameter values as msgpack.
    pub fn encoded_params(&self) -> Vec<u8> {
        tarantool::msgpack::encode(&self.params)
    }

    fn dql_subtree(&self) -> Result<DqlSubtree<'_>, SbroadError> {
        self.plan
            .freeze()
            .execution_view()
            .dql_subtree(self.sql_top_id)
    }
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

    fn get_options(&self) -> DQLOptions {
        self.plan
            .get_ir_plan()
            .effective_options
            .to_protocol_options()
    }

    fn get_params(&self) -> impl MsgpackEncode {
        ArrayMsgpackEncoder::new(self.params.as_slice())
    }
}

impl DQLDataSource for DqlProtocol {
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

    fn get_options(&self) -> DQLOptions {
        self.plan
            .get_ir_plan()
            .effective_options
            .to_protocol_options()
    }

    fn get_params(&self) -> impl MsgpackEncode {
        ArrayMsgpackEncoder::new(self.params.as_slice())
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
    options: DQLOptions,
}

impl FullDeleteData {
    pub fn new(core: DeleteCoreData, plan_id: u64, options: DQLOptions) -> Self {
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

    fn get_options(&self) -> DQLOptions {
        self.options
    }
}

/// Protocol data for a delete whose target rows come from a DQL data source.
pub struct FilteredDeleteData<'a, D: DQLDataSource> {
    core: DeleteCoreData,
    types: Vec<Column>,
    pattern: TupleBuilderPattern,
    dql_data: &'a D,
}

impl<'a, D: DQLDataSource> FilteredDeleteData<'a, D> {
    /// Creates filtered delete protocol data.
    pub fn new(
        core: DeleteCoreData,
        types: Vec<Column>,
        pattern: TupleBuilderPattern,
        dql_data: &'a D,
    ) -> Self {
        Self {
            core,
            types,
            pattern,
            dql_data,
        }
    }
}

impl<D: DQLDataSource> CoreDeleteDataSource for FilteredDeleteData<'_, D> {
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

impl<D: DQLDataSource> DeleteFilteredDataSource for FilteredDeleteData<'_, D> {
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

/// Protocol data for an insert that materializes tuples from a DQL source.
pub struct LocalInsertData<'a, D: DQLDataSource> {
    core: InsertCoreData,
    types: Vec<Column>,
    pattern: TupleBuilderPattern,
    dql_data: &'a D,
}

impl<'a, D: DQLDataSource> LocalInsertData<'a, D> {
    /// Creates local insert protocol data.
    pub fn new(
        core: InsertCoreData,
        types: Vec<Column>,
        pattern: TupleBuilderPattern,
        dql_data: &'a D,
    ) -> Self {
        Self {
            core,
            types,
            pattern,
            dql_data,
        }
    }
}

impl<D: DQLDataSource> CoreInsertDataSource for LocalInsertData<'_, D> {
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

impl<D: DQLDataSource> InsertMaterializedDataSource for LocalInsertData<'_, D> {
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

/// Protocol data for an update that materializes tuples from a DQL source.
pub struct LocalUpdateData<'a, D: DQLDataSource> {
    core: UpdateCoreData,
    types: Vec<Column>,
    pattern: TupleBuilderPattern,
    dql_data: &'a D,
}

impl<'a, D: DQLDataSource> LocalUpdateData<'a, D> {
    /// Creates local update protocol data.
    pub fn new(
        core: UpdateCoreData,
        types: Vec<Column>,
        pattern: TupleBuilderPattern,
        dql_data: &'a D,
    ) -> Self {
        Self {
            core,
            types,
            pattern,
            dql_data,
        }
    }
}

impl<D: DQLDataSource> CoreUpdateDataSource for LocalUpdateData<'_, D> {
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

impl<D: DQLDataSource> UpdateDataSource for LocalUpdateData<'_, D> {
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

/// Schema versions included in a DQL cache-miss response.
#[derive(Default)]
pub struct PlanVersion {
    /// Table version map observed when the SQL was generated.
    pub table_version_map: VersionMap,
    /// Index version map observed when the SQL was generated.
    pub index_version_map: HashMap<[u32; 2], u64, RepeatableState>,
}

/// Data used to populate a DQL cache-miss response.
#[derive(Default)]
pub struct ExecutionCacheMissData {
    /// Schema versions required to validate the cached SQL.
    pub schema_info: PlanVersion,
    /// Metadata for virtual tables referenced by the generated SQL.
    pub vtables_meta: HashMap<SmolStr, Vec<(SmolStr, ColumnType)>>,
    /// Local SQL rendered for the cache miss.
    pub sql: String,
}

fn vtables_metadata(
    plan_id: u64,
    vtables: &VirtualTableMap,
) -> HashMap<SmolStr, Vec<(SmolStr, ColumnType)>> {
    vtables
        .iter()
        .map(|(k, v)| {
            let columns = v
                .get_columns()
                .iter()
                .map(|column| (column.name.clone(), column.r#type.into()));
            (table_name(plan_id, *k), columns.collect::<Vec<_>>())
        })
        .collect::<HashMap<_, _>>()
}

impl TryFrom<&ExecutionData> for ExecutionCacheMissData {
    type Error = SbroadError;

    fn try_from(value: &ExecutionData) -> Result<Self, Self::Error> {
        let sql = {
            let plan_id = value.get_plan_id();
            let sp = SyntaxPlan::new(&value.plan, value.sql_top_id, Snapshot::Oldest, false)?;
            let on = OrderedSyntaxNodes::try_from(sp)?;
            let a = on.to_syntax_data()?;
            value
                .plan
                .generate_sql(&a, plan_id, table_name, Some(value.constant_ids.as_slice()))?
        };

        let schema_info = PlanVersion {
            table_version_map: value.plan.plan.table_version_map.clone(),
            index_version_map: value.plan.plan.index_version_map.clone(),
        };
        Ok(Self {
            schema_info,
            vtables_meta: vtables_metadata(value.get_plan_id(), value.plan.get_vtables()),
            sql,
        })
    }
}

impl TryFrom<&DqlProtocol> for ExecutionCacheMissData {
    type Error = SbroadError;

    fn try_from(value: &DqlProtocol) -> Result<Self, Self::Error> {
        let sql = {
            let plan_id = value.get_plan_id();
            let subtree = value.dql_subtree()?;
            let sp = SyntaxPlan::new_for_dql_subtree(&subtree, Snapshot::Oldest, false)?;
            let on = OrderedSyntaxNodes::try_from(sp)?;
            let a = on.to_syntax_data()?;
            subtree.generate_sql(&a, plan_id, table_name, Some(value.constant_ids.as_slice()))?
        };

        let plan = value.plan.get_ir_plan();
        let schema_info = PlanVersion {
            table_version_map: plan.table_version_map.clone(),
            index_version_map: plan.index_version_map.clone(),
        };
        Ok(Self {
            schema_info,
            vtables_meta: vtables_metadata(value.get_plan_id(), value.plan.get_vtables()),
            sql,
        })
    }
}

impl TryFrom<ExecutionData> for ExecutionCacheMissData {
    type Error = SbroadError;

    fn try_from(mut value: ExecutionData) -> Result<Self, Self::Error> {
        // Take version maps before borrowing to avoid unnecessary cloning.
        let schema_info = PlanVersion {
            table_version_map: std::mem::take(&mut value.plan.plan.table_version_map),
            index_version_map: std::mem::take(&mut value.plan.plan.index_version_map),
        };
        let mut result = Self::try_from(&value)?;
        result.schema_info = schema_info;
        Ok(result)
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

fn dql_sql_top_id(exec_plan: &ExecutionPlan) -> Result<NodeId, SbroadError> {
    let top_id = exec_plan.get_ir_plan().get_top()?;
    dql_sql_top_id_for_top(exec_plan, top_id)
}

fn dql_sql_top_id_for_top(
    exec_plan: &ExecutionPlan,
    top_id: NodeId,
) -> Result<NodeId, SbroadError> {
    let plan = exec_plan.get_ir_plan();
    if plan.get_relation_node(top_id)?.is_dml() {
        let child_id = plan.children(top_id)[0];
        exec_plan
            .effective_motion_subtree_root(child_id)?
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Motion,
                    Some(format_smolstr!(
                        "motion {child_id:?} has no effective subtree"
                    )),
                )
            })
    } else {
        Ok(top_id)
    }
}

/// Builds DQL protocol data for the top query of an execution plan.
///
/// # Errors
/// - If the plan id is not set.
/// - If the top query cannot be converted into a DQL subtree protocol view.
pub fn build_dql_protocol(
    exec_plan: ExecutionPlan,
    sender_id: u64,
) -> Result<DqlProtocol, SbroadError> {
    let top_id = exec_plan.get_ir_plan().get_top()?;
    build_dql_protocol_for_top(Rc::new(exec_plan), top_id, sender_id)
}

/// Builds DQL protocol data for an effective subtree rooted at `top_id`.
///
/// Only virtual tables visible from the selected subtree are included.
///
/// # Errors
/// - If the plan id is not set.
/// - If the selected subtree cannot be inspected or parameterized.
pub fn build_dql_protocol_for_top(
    exec_plan: Rc<ExecutionPlan>,
    top_id: NodeId,
    sender_id: u64,
) -> Result<DqlProtocol, SbroadError> {
    let plan_id = exec_plan.get_plan_id()?;
    let sql_top_id = dql_sql_top_id_for_top(&exec_plan, top_id)?;
    let subtree = SubtreeViewBuilder::new(&exec_plan, sql_top_id)?;
    let sql_params = exec_plan.local_sql_params(sql_top_id, Snapshot::Oldest)?;
    let subtree = subtree.node_ids().iter().copied().collect::<HashSet<_>>();
    let vtables = exec_plan
        .get_vtables()
        .iter()
        .filter(|(node_id, _)| subtree.contains(node_id))
        .map(|(k, t)| (table_name(plan_id, *k), t.clone()))
        .collect();
    let (constant_ids, params) = sql_params.into_parts();

    Ok(DqlProtocol {
        plan_id,
        sender_id,
        vtables,
        sql_top_id,
        constant_ids,
        params,
        plan: exec_plan,
    })
}

/// Builds the legacy owned-plan DQL data source.
///
/// # Errors
/// - If the plan id is not set.
/// - If the top query cannot be inspected or parameterized.
pub fn build_dql_data_source(
    exec_plan: ExecutionPlan,
    sender_id: u64,
) -> Result<ExecutionData, SbroadError> {
    let plan_id = exec_plan.get_plan_id()?;
    let sql_top_id = dql_sql_top_id(&exec_plan)?;
    let sql_params = exec_plan.local_sql_params(sql_top_id, Snapshot::Oldest)?;
    let vtables = exec_plan
        .get_vtables()
        .iter()
        .map(|(k, t)| (table_name(plan_id, *k), t.clone()))
        .collect();
    let (constant_ids, params) = sql_params.into_parts();

    Ok(ExecutionData {
        plan_id,
        sender_id,
        vtables,
        sql_top_id,
        constant_ids,
        params,
        plan: exec_plan,
    })
}

#[cfg(all(test, feature = "mock"))]
mod tests {
    use super::*;
    use crate::ir::transformation::helpers::sql_to_optimized_ir;
    use pretty_assertions::assert_eq;

    #[test]
    fn dql_protocol_cache_miss_matches_execution_data() {
        let mut exec_plan = ExecutionPlan::new(sql_to_optimized_ir(
            r#"select "id" from "test_space" where "id" = 1"#,
            vec![],
        ));
        let top_id = exec_plan.get_ir_plan().get_top().unwrap();
        exec_plan.set_plan_id(top_id).unwrap();

        let execution_data = build_dql_data_source(exec_plan.clone(), 42).unwrap();
        let dql_protocol = build_dql_protocol(exec_plan.clone(), 42).unwrap();

        let mut protocol_params = Vec::new();
        dql_protocol
            .get_params()
            .encode_into(&mut protocol_params)
            .unwrap();

        assert_eq!(execution_data.get_plan_id(), dql_protocol.get_plan_id());
        assert_eq!(execution_data.encoded_params(), protocol_params);
        assert_eq!(execution_data.get_options(), dql_protocol.get_options());

        let execution_miss = ExecutionCacheMissData::try_from(&execution_data).unwrap();
        let protocol_miss = ExecutionCacheMissData::try_from(&dql_protocol).unwrap();

        assert_eq!(execution_miss.sql, protocol_miss.sql);
        assert_eq!(execution_miss.vtables_meta, protocol_miss.vtables_meta);
        assert_eq!(
            execution_miss.schema_info.table_version_map,
            protocol_miss.schema_info.table_version_map
        );
        assert_eq!(
            execution_miss.schema_info.index_version_map,
            protocol_miss.schema_info.index_version_map
        );
    }
}

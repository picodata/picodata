use smol_str::{SmolStr, ToSmolStr};
use std::any::Any;

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;

use std::rc::Rc;
use tarantool::decimal;
use tarantool::decimal::Decimal;

use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::cbo::histogram::normalization::DEFAULT_HISTOGRAM_BUCKETS_NUMBER;
use crate::cbo::histogram::HistogramBuckets;
use crate::cbo::histogram::{Histogram, Mcv, McvSet, Scalar};
use crate::cbo::tests::construct_i64_buckets;
use crate::cbo::{ColumnStats, TableColumnPair, TableStats};
use crate::errors::{Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::engine::{
    helpers::{sharding_key_from_map, sharding_key_from_tuple, vshard::get_random_bucket},
    Router, Statistics, Vshard,
};
use crate::executor::hash::bucket_id_by_tuple;
use crate::executor::ir::ExecutionPlan;
use crate::executor::lru::{LRUCache, DEFAULT_CAPACITY};
use crate::executor::result::ProducerResult;
use crate::executor::vtable::VirtualTable;
use crate::executor::Cache;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::ir::function::Function;
use crate::ir::node::NodeId;
use crate::ir::relation::{Column, ColumnRole, SpaceEngine, Table, Type};
use crate::ir::tree::Snapshot;
use crate::ir::value::{LuaValue, Value};
use crate::ir::Plan;
use crate::utils::MutexLike;

use super::helpers::vshard::{prepare_rs_to_ir_map, GroupedBuckets};
use super::helpers::{dispatch_by_buckets, normalize_name_from_sql};
use super::{get_builtin_functions, DispatchReturnFormat, Metadata, QueryCache};

pub const TEMPLATE: &str = "test";

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct RouterConfigurationMock {
    functions: HashMap<SmolStr, Function>,
    tables: HashMap<SmolStr, Table>,
    bucket_count: u64,
    sharding_column: SmolStr,
}

impl Metadata for RouterConfigurationMock {
    fn table(&self, table_name: &str) -> Result<Table, SbroadError> {
        match self.tables.get(table_name) {
            Some(v) => Ok(v.clone()),
            None => Err(SbroadError::NotFound(
                Entity::Space,
                table_name.to_smolstr(),
            )),
        }
    }

    fn function(&self, fn_name: &str) -> Result<&Function, SbroadError> {
        let name = normalize_name_from_sql(fn_name);
        match self.functions.get(&name) {
            Some(v) => Ok(v),
            None => Err(SbroadError::NotFound(Entity::SQLFunction, name)),
        }
    }

    fn waiting_timeout(&self) -> u64 {
        0
    }

    fn sharding_column(&self) -> &str {
        self.sharding_column.as_str()
    }

    fn sharding_key_by_space(&self, space: &str) -> Result<Vec<SmolStr>, SbroadError> {
        let table = self.table(space)?;
        table.get_sharding_column_names()
    }

    fn sharding_positions_by_space(&self, space: &str) -> Result<Vec<usize>, SbroadError> {
        let table = self.table(space)?;
        Ok(table.get_sk()?.to_vec())
    }
}

impl Default for RouterConfigurationMock {
    fn default() -> Self {
        Self::new()
    }
}

impl RouterConfigurationMock {
    /// Mock engine constructor.
    ///
    /// # Panics
    /// - If schema is invalid.
    #[allow(clippy::too_many_lines)]
    #[must_use]
    pub fn new() -> Self {
        let name_func = normalize_name_from_sql("func");
        let fn_func = Function::new_stable(name_func.clone(), Type::Integer, false);
        let name_trim = normalize_name_from_sql("trim");
        let trim_func = Function::new_stable(name_trim.clone(), Type::String, false);
        let mut functions = HashMap::new();
        functions.insert(name_func, fn_func);
        functions.insert(name_trim, trim_func);
        for f in get_builtin_functions() {
            functions.insert(f.name.clone(), f.clone());
        }

        let mut tables = HashMap::new();

        let columns = vec![
            Column::new(
                "identification_number",
                Type::Integer,
                ColumnRole::User,
                false,
            ),
            Column::new("product_code", Type::String, ColumnRole::User, false),
            Column::new("product_units", Type::Boolean, ColumnRole::User, true),
            Column::new("sys_op", Type::Unsigned, ColumnRole::User, true),
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding, true),
        ];
        let sharding_key = &["identification_number", "product_code"];
        let primary_key = &["product_code", "identification_number"];
        tables.insert(
            "hash_testing".to_smolstr(),
            Table::new_sharded(
                "hash_testing",
                columns.clone(),
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        tables.insert(
            "hash_testing_hist".to_smolstr(),
            Table::new_sharded(
                "hash_testing_hist",
                columns.clone(),
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let sharding_key = &["identification_number"];
        tables.insert(
            "hash_single_testing".to_smolstr(),
            Table::new_sharded(
                "hash_single_testing",
                columns.clone(),
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        tables.insert(
            "hash_single_testing_hist".to_smolstr(),
            Table::new_sharded(
                "hash_single_testing_hist",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("id", Type::Unsigned, ColumnRole::User, false),
            Column::new("sysFrom", Type::Unsigned, ColumnRole::User, true),
            Column::new("FIRST_NAME", Type::String, ColumnRole::User, true),
            Column::new("sys_op", Type::Unsigned, ColumnRole::User, true),
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding, true),
        ];
        let sharding_key = &["id"];
        let primary_key = &["id"];

        tables.insert(
            "test_space".to_smolstr(),
            Table::new_sharded(
                "test_space",
                columns.clone(),
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        tables.insert(
            "test_space_hist".to_smolstr(),
            Table::new_sharded(
                "test_space_hist",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("id", Type::Unsigned, ColumnRole::User, false),
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding, true),
        ];
        let sharding_key: &[&str] = &["id"];
        let primary_key: &[&str] = &["id"];
        tables.insert(
            "history".to_smolstr(),
            Table::new_sharded(
                "history",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("A", Type::Unsigned, ColumnRole::User, true),
            Column::new("B", Type::Unsigned, ColumnRole::User, false),
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding, true),
        ];
        let sharding_key: &[&str] = &["A", "B"];
        let primary_key: &[&str] = &["B"];
        tables.insert(
            "TBL".to_smolstr(),
            Table::new_sharded(
                "TBL",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("a", Type::Unsigned, ColumnRole::User, true),
            Column::new("b", Type::Unsigned, ColumnRole::User, false),
            Column::new("c", Type::Unsigned, ColumnRole::User, true),
            Column::new("d", Type::Unsigned, ColumnRole::User, true),
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding, true),
        ];
        let sharding_key: &[&str] = &["a", "b"];
        let primary_key: &[&str] = &["b"];
        tables.insert(
            "t".to_smolstr(),
            Table::new_sharded("t", columns, sharding_key, primary_key, SpaceEngine::Memtx)
                .unwrap(),
        );

        let columns = vec![
            Column::new("a", Type::String, ColumnRole::User, false),
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding, true),
            Column::new("b", Type::Integer, ColumnRole::User, false),
        ];
        let sharding_key: &[&str] = &["a", "b"];
        let primary_key: &[&str] = &["a", "b"];
        tables.insert(
            "t1".to_smolstr(),
            Table::new_sharded("t1", columns, sharding_key, primary_key, SpaceEngine::Memtx)
                .unwrap(),
        );

        let columns = vec![
            Column::new("e", Type::Unsigned, ColumnRole::User, false),
            Column::new("f", Type::Unsigned, ColumnRole::User, false),
            Column::new("g", Type::Unsigned, ColumnRole::User, false),
            Column::new("h", Type::Unsigned, ColumnRole::User, false),
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding, true),
        ];
        let sharding_key: &[&str] = &["e", "f"];
        let primary_key: &[&str] = &["g", "h"];
        tables.insert(
            "t2".to_smolstr(),
            Table::new_sharded("t2", columns, sharding_key, primary_key, SpaceEngine::Memtx)
                .unwrap(),
        );

        let columns = vec![
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding, true),
            Column::new("a", Type::String, ColumnRole::User, false),
            Column::new("b", Type::Integer, ColumnRole::User, false),
        ];
        let sharding_key: &[&str] = &["a"];
        let primary_key: &[&str] = &["a"];
        tables.insert(
            "t3".to_smolstr(),
            Table::new_sharded("t3", columns, sharding_key, primary_key, SpaceEngine::Memtx)
                .unwrap(),
        );

        let columns = vec![
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding, true),
            Column::new("c", Type::String, ColumnRole::User, false),
            Column::new("d", Type::Integer, ColumnRole::User, false),
        ];
        let sharding_key: &[&str] = &["c"];
        let primary_key: &[&str] = &["d"];
        tables.insert(
            "t4".to_smolstr(),
            Table::new_sharded("t4", columns, sharding_key, primary_key, SpaceEngine::Memtx)
                .unwrap(),
        );

        let columns = vec![
            Column::new("a", Type::Integer, ColumnRole::User, false),
            Column::new("b", Type::Integer, ColumnRole::User, false),
        ];
        let primary_key: &[&str] = &["a"];
        tables.insert(
            "global_t".to_smolstr(),
            Table::new_global("global_t", columns, primary_key).unwrap(),
        );

        // Table for sbroad-benches
        let columns = vec![
            Column::new("vehicleguid", Type::Unsigned, ColumnRole::User, false),
            Column::new("reestrid", Type::Unsigned, ColumnRole::User, false),
            Column::new("reestrstatus", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehicleregno", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehiclevin", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehiclevin2", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehiclechassisnum", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "vehiclereleaseyear",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "operationregdoctypename",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("operationregdoc", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "operationregdocissuedate",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "operationregdoccomments",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleptstypename",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("vehicleptsnum", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "vehicleptsissuedate",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("vehicleptsissuer", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "vehicleptscomments",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("vehiclebodycolor", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehiclebrand", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehiclemodel", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehiclebrandmodel", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehiclebodynum", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehiclecost", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehiclegasequip", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "vehicleproducername",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("vehiclegrossmass", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehiclemass", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "vehiclesteeringwheeltypeid",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("vehiclekpptype", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "vehicletransmissiontype",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("vehicletypename", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehiclecategory", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehicletypeunit", Type::Unsigned, ColumnRole::User, false),
            Column::new("vehicleecoclass", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "vehiclespecfuncname",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleenclosedvolume",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleenginemodel",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("vehicleenginenum", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "vehicleenginepower",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleenginepowerkw",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("vehicleenginetype", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "holdrestrictiondate",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("approvalnum", Type::Unsigned, ColumnRole::User, false),
            Column::new("approvaldate", Type::Unsigned, ColumnRole::User, false),
            Column::new("approvaltype", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "utilizationfeename",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("customsdoc", Type::Unsigned, ColumnRole::User, false),
            Column::new("customsdocdate", Type::Unsigned, ColumnRole::User, false),
            Column::new("customsdocissue", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "customsdocrestriction",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "customscountryremovalid",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "customscountryremovalname",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("ownerorgname", Type::Unsigned, ColumnRole::User, false),
            Column::new("ownerinn", Type::Unsigned, ColumnRole::User, false),
            Column::new("ownerogrn", Type::Unsigned, ColumnRole::User, false),
            Column::new("ownerkpp", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "ownerpersonlastname",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerpersonfirstname",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerpersonmiddlename",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerpersonbirthdate",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("ownerbirthplace", Type::Unsigned, ColumnRole::User, false),
            Column::new("ownerpersonogrnip", Type::Unsigned, ColumnRole::User, false),
            Column::new("owneraddressindex", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "owneraddressmundistrict",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "owneraddresssettlement",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "owneraddressstreet",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("ownerpersoninn", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "ownerpersondoccode",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("ownerpersondocnum", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "ownerpersondocdate",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("operationname", Type::Unsigned, ColumnRole::User, false),
            Column::new("operationdate", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "operationdepartmentname",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("operationattorney", Type::Unsigned, ColumnRole::User, false),
            Column::new("operationlising", Type::Unsigned, ColumnRole::User, false),
            Column::new("holdertypeid", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "holderpersondoccode",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersondocnum",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersondocdate",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersondocissuer",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonlastname",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonfirstname",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonmiddlename",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonbirthdate",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonbirthregionid",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("holderpersonsex", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "holderpersonbirthplace",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("holderpersoninn", Type::Unsigned, ColumnRole::User, false),
            Column::new("holderpersonsnils", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "holderpersonogrnip",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("holderaddressguid", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "holderaddressregionid",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressregionname",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressdistrict",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressmundistrict",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddresssettlement",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressstreet",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressbuilding",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressstructureid",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressstructurename",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressstructure",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("sys_from", Type::Unsigned, ColumnRole::User, false),
            Column::new("sys_to", Type::Unsigned, ColumnRole::User, false),
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding, true),
        ];
        let sharding_key: &[&str] = &["reestrid"];
        let primary_key: &[&str] = &["reestrid"];
        tables.insert(
            "test__gibdd_db__vehicle_reg_and_res100_actual".to_smolstr(),
            Table::new_sharded(
                "test__gibdd_db__vehicle_reg_and_res100_actual",
                columns.clone(),
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );
        tables.insert(
            "test__gibdd_db__vehicle_reg_and_res100_history".to_smolstr(),
            Table::new_sharded(
                "test__gibdd_db__vehicle_reg_and_res100_history",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        RouterConfigurationMock {
            functions,
            tables,
            bucket_count: 10000,
            sharding_column: "bucket_id".into(),
        }
    }
}

/// Helper struct to group buckets by replicasets.
/// Assumes that all buckets are uniformly distributed
/// between replicasets: first rs holds p buckets,
/// second rs holds p buckets, .., last rs holds p + r
/// buckets.
/// Where: `p = bucket_cnt / rs_cnt, r = bucket_cnt % rs_cnt`
#[allow(clippy::module_name_repetitions)]
#[derive(Clone)]
pub struct VshardMock {
    // Holds boundaries of replicaset buckets: [start, end)
    blocks: Vec<(u64, u64)>,
}

impl VshardMock {
    #[must_use]
    pub fn new(rs_count: usize, bucket_count: u64) -> Self {
        let mut blocks = Vec::new();
        let rs_count: u64 = rs_count as u64;
        let buckets_per_rs = bucket_count / rs_count;
        let remainder = bucket_count % rs_count;
        for rs_idx in 0..rs_count {
            let start = rs_idx * buckets_per_rs;
            let end = start + buckets_per_rs;
            blocks.push((start, end));
        }
        if let Some(last_block) = blocks.last_mut() {
            last_block.1 += remainder + 1;
        }
        Self { blocks }
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn group(&self, buckets: &Buckets) -> GroupedBuckets {
        let mut res: GroupedBuckets = HashMap::new();
        match buckets {
            Buckets::All => {
                for (idx, (start, end)) in self.blocks.iter().enumerate() {
                    let name = Self::generate_rs_name(idx);
                    res.insert(name, ((*start)..(*end)).collect());
                }
            }
            Buckets::Filtered(buckets_set) => {
                for bucket_id in buckets_set {
                    let comparator = |block: &(u64, u64)| -> Ordering {
                        let start = block.0;
                        let end = block.1;
                        if *bucket_id < start {
                            Ordering::Greater
                        } else if *bucket_id >= end {
                            Ordering::Less
                        } else {
                            Ordering::Equal
                        }
                    };
                    let block_idx = match self.blocks.binary_search_by(comparator) {
                        Ok(idx) => idx,
                        Err(idx) => {
                            panic!("bucket_id: {bucket_id}, err_idx: {idx}");
                        }
                    };
                    let name = Self::generate_rs_name(block_idx);
                    match res.entry(name) {
                        Entry::Occupied(mut e) => {
                            e.get_mut().push(*bucket_id);
                        }
                        Entry::Vacant(e) => {
                            e.insert(vec![*bucket_id]);
                        }
                    }
                }
            }
            Buckets::Any => {
                res.insert(Self::generate_rs_name(0), vec![0]);
            }
        }

        res
    }

    #[must_use]
    pub fn generate_rs_name(idx: usize) -> String {
        format!("replicaset_{idx}")
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn get_id(name: &str) -> usize {
        name[name.find('_').unwrap() + 1..]
            .parse::<usize>()
            .unwrap()
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone)]
pub struct RouterRuntimeMock {
    // It's based on the RefCells instead of tarantool mutexes,
    // so it could be used in unit tests - they won't compile otherwise due to missing tarantool symbols.
    metadata: RefCell<RouterConfigurationMock>,
    virtual_tables: RefCell<HashMap<NodeId, VirtualTable>>,
    ir_cache: Rc<RefCell<LRUCache<SmolStr, Plan>>>,
    table_statistics_cache: RefCell<HashMap<SmolStr, Rc<TableStats>>>,
    initial_column_statistics_cache: RefCell<HashMap<TableColumnPair, Rc<Box<dyn Any>>>>,
    pub vshard_mock: VshardMock,
}

impl std::fmt::Debug for RouterRuntimeMock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("")
            .field(&self.metadata)
            .field(&self.virtual_tables)
            .field(&self.table_statistics_cache)
            .field(&self.initial_column_statistics_cache)
            .finish()
    }
}

impl ProducerResult {
    /// Merge two record sets. If current result is empty function sets metadata and appends result rows.
    /// If the current result is not empty compare its metadata with the one from the new result.
    /// If they differ return error.
    ///
    ///  # Errors
    ///  - metadata isn't equal.
    fn extend(&mut self, result: ProducerResult) -> Result<(), SbroadError> {
        if self.metadata.is_empty() {
            self.metadata = result.clone().metadata;
        }

        if self.metadata != result.metadata {
            return Err(SbroadError::Invalid(
                Entity::Metadata,
                Some("Metadata mismatch. Producer results can't be extended".into()),
            ));
        }
        self.rows.extend(result.rows);
        Ok(())
    }
}

impl QueryCache for RouterRuntimeMock {
    type Cache = LRUCache<SmolStr, Plan>;
    type Mutex = RefCell<Self::Cache>;

    fn clear_cache(&self) -> Result<(), SbroadError> {
        *self.ir_cache.borrow_mut() = LRUCache::new(self.cache_capacity()?, None)?;
        Ok(())
    }

    fn cache(&self) -> &Self::Mutex {
        &self.ir_cache
    }

    fn cache_capacity(&self) -> Result<usize, SbroadError> {
        Ok(self.cache().lock().capacity())
    }

    fn provides_versions(&self) -> bool {
        false
    }

    fn get_table_version(&self, _: &str) -> Result<u64, SbroadError> {
        Err(SbroadError::DoSkip)
    }
}

impl Vshard for RouterRuntimeMock {
    fn exec_ir_on_any_node(
        &self,
        _sub_plan: ExecutionPlan,
        _return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_locally is not supported for the mock runtime".to_smolstr()),
        ))
    }

    fn bucket_count(&self) -> u64 {
        self.metadata().lock().bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError> {
        Ok(bucket_id_by_tuple(s, self.bucket_count()))
    }

    fn exec_ir_on_buckets(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
        _return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError> {
        mock_exec_ir_on_buckets(&self.vshard_mock, buckets, sub_plan)
    }
}

impl Vshard for &RouterRuntimeMock {
    fn bucket_count(&self) -> u64 {
        self.metadata().lock().bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError> {
        Ok(bucket_id_by_tuple(s, self.bucket_count()))
    }

    fn exec_ir_on_any_node(
        &self,
        _sub_plan: ExecutionPlan,
        _return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_locally is not supported for the mock runtime".to_smolstr()),
        ))
    }

    fn exec_ir_on_buckets(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
        _return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError> {
        mock_exec_ir_on_buckets(&self.vshard_mock, buckets, sub_plan)
    }
}

fn mock_exec_ir_on_buckets(
    vshard_mock: &VshardMock,
    buckets: &Buckets,
    sub_plan: ExecutionPlan,
) -> Result<Box<dyn Any>, SbroadError> {
    let mut rs_bucket_vec: Vec<(String, Vec<u64>)> = vshard_mock.group(buckets).drain().collect();
    // sort to get deterministic test results
    rs_bucket_vec.sort_by_key(|(rs_name, _)| rs_name.clone());
    let rs_ir = prepare_rs_to_ir_map(&rs_bucket_vec, sub_plan)?;
    let mut dispatch_vec: Vec<ReplicasetDispatchInfo> = Vec::new();
    for (rs_name, exec_plan) in rs_ir {
        let id = VshardMock::get_id(&rs_name);
        let dispatch = ReplicasetDispatchInfo::new(id, &exec_plan);
        dispatch_vec.push(dispatch);
    }
    dispatch_vec.sort_by_key(|d| d.rs_id);
    Ok(Box::new(dispatch_vec))
}

impl Default for RouterRuntimeMock {
    fn default() -> Self {
        Self::new()
    }
}

impl RouterRuntimeMock {
    #[allow(dead_code, clippy::missing_panics_doc, clippy::too_many_lines)]
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    #[must_use]
    pub fn new() -> Self {
        let cache: LRUCache<SmolStr, Plan> = LRUCache::new(DEFAULT_CAPACITY, None).unwrap();

        let mut table_statistics_cache = HashMap::new();
        let hash_testing_hist_rows_number = 1000.0;
        table_statistics_cache.insert(
            "hash_testing_hist".to_smolstr(),
            Rc::new(TableStats::new(hash_testing_hist_rows_number as u64)),
        );
        let hash_testing_rows_number = 10000.0;
        table_statistics_cache.insert(
            "hash_testing".to_smolstr(),
            Rc::new(TableStats::new(hash_testing_rows_number as u64)),
        );
        let test_space_rows_number = 25000.0;
        table_statistics_cache.insert(
            "test_space".to_smolstr(),
            Rc::new(TableStats::new(test_space_rows_number as u64)),
        );

        let mut column_statistics_cache: HashMap<TableColumnPair, Rc<Box<dyn Any>>> =
            HashMap::new();
        // Column statistics with empty histogram.
        //
        // * rows_number: 1000
        // * min_value: 1
        // * max_value: 50
        // * avg_size: 4
        // * histogram:
        //   - null_fraction: 0.0
        //   - most_common: []
        //   - ndv (absolute value): 0
        //   - buckets_count: 0
        //   - buckets_frequency: 0
        //   - buckets_boundaries: []
        let boxed_column_stats = Box::new(ColumnStats::new(
            1i64,
            50i64,
            4,
            Some(Histogram::new(
                McvSet::default(),
                HistogramBuckets::default(),
                decimal!(0.0),
                decimal!(0.0),
            )),
        ));
        column_statistics_cache.insert(
            TableColumnPair::new("hash_testing_hist".to_smolstr(), 0),
            Rc::new(boxed_column_stats),
        );

        // Casual column statistics.
        //
        // Values `min_value` and `max_value` of `ColumnStats` structure are in fact
        // displaying MIN and MAX values of `Histogram` structure (that is seen from its
        // `most_common` and `buckets_boundaries` fields).
        // An example of statistics, where general column statistics and histogram statistics conform.
        //
        // * rows_number: 10000
        // * min_value: 0
        // * max_value: 1005
        // * avg_size: 8
        // * histogram:
        //   - null_fraction: 0.1 (1000)
        //   - most_common:
        //     [0 -> 1000,
        //      1 -> 1000,
        //      2 -> 500,
        //      3 -> 500,
        //      4 -> 1000]
        //   - ndv (absolute value): 15 (only 10 in buckets)
        //   - buckets_count: 100
        //   - buckets_frequency: 50 (as only 5000 elements are stored in buckets)
        //   - buckets_boundaries: equally splitted [5; 1005] range
        let mcv_vec = vec![
            Mcv::new(
                0i64,
                Decimal::try_from(1000.0 / hash_testing_rows_number).unwrap(),
            ),
            Mcv::new(
                1i64,
                Decimal::try_from(1000.0 / hash_testing_rows_number).unwrap(),
            ),
            Mcv::new(
                2i64,
                Decimal::try_from(500.0 / hash_testing_rows_number).unwrap(),
            ),
            Mcv::new(
                3i64,
                Decimal::try_from(500.0 / hash_testing_rows_number).unwrap(),
            ),
            Mcv::new(
                4i64,
                Decimal::try_from(1000.0 / hash_testing_rows_number).unwrap(),
            ),
        ];
        let buckets = construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, 5, 1005).unwrap();
        let boxed_column_stats = Box::new(ColumnStats::new(
            0i64,
            1005i64,
            8,
            Some(Histogram::new(
                McvSet::from_vec(&mcv_vec),
                buckets,
                decimal!(0.1),
                // 15 / (10000 * (1 -  0.1))
                decimal!(0.001666),
            )),
        ));
        column_statistics_cache.insert(
            TableColumnPair::new("hash_testing".to_smolstr(), 0),
            Rc::new(boxed_column_stats),
        );

        // Column statistics with unique values.
        // Note that it's also a column statistics with no `most_common` values.
        //
        // * rows_number: 10000
        // * min_value: 1
        // * max_value: 90
        // * avg_size: 4
        // * histogram:
        //   - null_fraction: 0.1 (1000)
        //   - most_common: []
        //   - ndv (absolute value): 900
        //   - buckets_count: 3
        //   - buckets_frequency: 3000 (as all 9000 left elements are stored in buckets)
        //   - buckets_boundaries: [1, 40, 65, 90]
        let mut boundaries = vec![1i64, 40i64, 65i64, 90i64];
        let buckets = HistogramBuckets::try_from(&mut boundaries).unwrap();
        let boxed_column_stats = Box::new(ColumnStats::new(
            1i64,
            900i64,
            4,
            Some(Histogram::new(
                McvSet::new(),
                buckets,
                decimal!(0.1),
                // 900 / (10000 * (1 -  0.1))
                decimal!(0.1),
            )),
        ));
        column_statistics_cache.insert(
            TableColumnPair::new("hash_testing".to_smolstr(), 3),
            Rc::new(boxed_column_stats),
        );

        // Casual column statistics, but for different column in different table.
        //
        // Values `min_value` and `max_value` of `ColumnStats` structure differ
        // from MIN and MAX values that we can get from `Histogram` structure (that is seen from its
        // `most_common` and `buckets_boundaries` fields).
        // An example of statistics, where general column statistics and histogram statistics
        // do NOT conform. That means histogram was gathered before updates to the corresponding table were made.
        //
        // * rows_number: 25000
        // * min_value: 1
        // * max_value: 2000
        // * avg_size: 8
        // * histogram:
        //   - null_fraction: 0.2 (5000)
        //   - most_common:
        //     [3 -> 2500,
        //      4 -> 500,
        //      5 -> 500,
        //      6 -> 1500]
        //   - ndv: 104 (only 100 in buckets)
        //   - buckets_count: 4
        //   - buckets_frequency: 3750 (as only 15000 elements are stored in buckets)
        //   - buckets_boundaries: [0, 78, 200, 780, 1800]
        let mcv_vec = vec![
            Mcv::new(
                3u64,
                Decimal::try_from(2500.0 / test_space_rows_number).unwrap(),
            ),
            Mcv::new(
                4u64,
                Decimal::try_from(500.0 / test_space_rows_number).unwrap(),
            ),
            Mcv::new(
                5u64,
                Decimal::try_from(500.0 / test_space_rows_number).unwrap(),
            ),
            Mcv::new(
                6u64,
                Decimal::try_from(1500.0 / test_space_rows_number).unwrap(),
            ),
        ];
        let mut boundaries = vec![0u64, 78u64, 200u64, 780u64, 1800u64];
        let boxed_column_stats = Box::new(ColumnStats::new(
            1u64,
            2000u64,
            8,
            Some(Histogram::new(
                McvSet::from_vec(&mcv_vec),
                HistogramBuckets::try_from(&mut boundaries).unwrap(),
                decimal!(0.2),
                // 104 / (25000 * (1 -  0.2))
                decimal!(0.0052),
            )),
        ));
        column_statistics_cache.insert(
            TableColumnPair::new("test_space".to_smolstr(), 0),
            Rc::new(boxed_column_stats),
        );

        // Casual column statistics (one more column with adequate histogram buckets):
        //
        // * rows_number: 25000
        // * min_value: 100
        // * max_value: 400
        // * avg_size: 8
        // * histogram:
        //   - null_fraction: 0.2 (5000)
        //   - most_common: []
        //   - ndv (absolute value): 10 (10 in buckets)
        //   - buckets_count: 100
        //   - buckets_frequency: 200 (as only 20000 elements are stored in buckets)
        //   - buckets_boundaries: equally splitted [100; 400] range
        let buckets = construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, 100, 400).unwrap();
        let boxed_column_stats = Box::new(ColumnStats::new(
            100i64,
            400i64,
            8,
            Some(Histogram::new(
                McvSet::new(),
                buckets,
                decimal!(0.2),
                // 10 / (25000 * (1 -  0.2))
                decimal!(0.0005),
            )),
        ));
        column_statistics_cache.insert(
            TableColumnPair::new("test_space".to_smolstr(), 1),
            Rc::new(boxed_column_stats),
        );

        // String column statistics.
        //
        // * rows_number: 25000
        // * min_value: "a"
        // * max_value: "z"
        // * avg_size: 1
        // * histogram:
        //   - null_fraction: 0.2 (5000)
        //   - most_common:
        //     ["a" -> 2500,
        //      "b" -> 500,
        //      "c" -> 500,
        //      "d" -> 1500]
        //   - ndv: 104 (only 100 in buckets)
        //   - buckets_count: 4
        //   - buckets_frequency: 3750 (as only 15000 elements are stored in buckets)
        //   - buckets_boundaries: ["e", "j", "q", "s", "z"]
        let mcv_vec = vec![
            Mcv::new(
                String::from("a"),
                Decimal::try_from(2500.0 / test_space_rows_number).unwrap(),
            ),
            Mcv::new(
                String::from("b"),
                Decimal::try_from(500.0 / test_space_rows_number).unwrap(),
            ),
            Mcv::new(
                String::from("c"),
                Decimal::try_from(500.0 / test_space_rows_number).unwrap(),
            ),
            Mcv::new(
                String::from("d"),
                Decimal::try_from(1500.0 / test_space_rows_number).unwrap(),
            ),
        ];
        let mut boundaries = vec![
            String::from("e"),
            String::from("j"),
            String::from("q"),
            String::from("s"),
            String::from("z"),
        ];
        let boxed_column_stats = Box::new(ColumnStats::new(
            String::from("a"),
            String::from("z"),
            1,
            Some(Histogram::new(
                McvSet::from_vec(&mcv_vec),
                HistogramBuckets::try_from(&mut boundaries).unwrap(),
                decimal!(0.2),
                // 104 / (25000 * (1 -  0.2))
                decimal!(0.0052),
            )),
        ));
        column_statistics_cache.insert(
            TableColumnPair::new("test_space".to_smolstr(), 2),
            Rc::new(boxed_column_stats),
        );

        let meta = RouterConfigurationMock::new();
        let bucket_cnt = meta.bucket_count;
        RouterRuntimeMock {
            metadata: RefCell::new(meta),
            virtual_tables: RefCell::new(HashMap::new()),
            ir_cache: Rc::new(RefCell::new(cache)),
            table_statistics_cache: RefCell::new(table_statistics_cache),
            initial_column_statistics_cache: RefCell::new(column_statistics_cache),
            vshard_mock: VshardMock::new(2, bucket_cnt),
        }
    }

    #[allow(dead_code)]
    pub fn add_virtual_table(&self, id: NodeId, table: VirtualTable) {
        self.virtual_tables.borrow_mut().insert(id, table);
    }
}

impl Router for RouterRuntimeMock {
    type ParseTree = AbstractSyntaxTree;
    type MetadataProvider = RouterConfigurationMock;
    type VshardImplementor = Self;

    fn metadata(&self) -> &impl MutexLike<Self::MetadataProvider> {
        &self.metadata
    }

    fn materialize_motion(
        &self,
        _plan: &mut ExecutionPlan,
        motion_node_id: &NodeId,
        _buckets: &Buckets,
    ) -> Result<VirtualTable, SbroadError> {
        Ok(self
            .virtual_tables
            .borrow()
            .get(motion_node_id)
            .expect("Virtual table for motion with id {motion_node_id} not found.")
            .clone())
    }

    fn materialize_values(
        &self,
        _exec_plan: &mut ExecutionPlan,
        values_id: NodeId,
    ) -> Result<VirtualTable, SbroadError> {
        Ok(self
            .virtual_tables
            .borrow()
            .get(&values_id)
            .expect("Virtual table for values with id {values_id} not found.")
            .clone())
    }

    fn dispatch(
        &self,
        plan: &mut ExecutionPlan,
        top_id: NodeId,
        buckets: &Buckets,
        _return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let mut result = ProducerResult::new();
        let sp = SyntaxPlan::new(plan, top_id, Snapshot::Oldest)?;
        let ordered = OrderedSyntaxNodes::try_from(sp)?;
        let nodes = ordered.to_syntax_data()?;

        match buckets {
            Buckets::All => {
                let (sql, _) = plan.to_sql(&nodes, TEMPLATE, None)?;
                result.extend(exec_on_all(String::from(sql).as_str()))?;
            }
            Buckets::Any => {
                let (sql, _) = plan.to_sql(&nodes, TEMPLATE, None)?;
                result.extend(exec_locally(String::from(sql).as_str()))?;
            }
            Buckets::Filtered(list) => {
                for bucket in list {
                    let (sql, _) = plan.to_sql(&nodes, TEMPLATE, None)?;
                    let temp_result = exec_on_some(*bucket, String::from(sql).as_str());
                    result.extend(temp_result)?;
                }
            }
        }

        // Sort results to make tests reproducible.
        result.rows.sort_by_key(|k| k[0].to_smolstr());
        Ok(Box::new(result))
    }

    fn explain_format(&self, explain: SmolStr) -> Result<Box<dyn Any>, SbroadError> {
        Ok(Box::new(explain))
    }

    fn extract_sharding_key_from_map<'rec>(
        &self,
        space: SmolStr,
        args: &'rec HashMap<SmolStr, Value>,
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_key_from_map(&*self.metadata().lock(), &space, args)
    }

    fn extract_sharding_key_from_tuple<'rec>(
        &self,
        space: SmolStr,
        rec: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_key_from_tuple(&*self.metadata().lock(), &space, rec)
    }

    fn get_current_tier_name(&self) -> Result<Option<SmolStr>, SbroadError> {
        Ok(None)
    }

    fn get_vshard_object_by_tier(
        &self,
        _tier_name: Option<&SmolStr>,
    ) -> Result<Self::VshardImplementor, SbroadError> {
        Ok(self.clone())
    }
}

impl Statistics for RouterRuntimeMock {
    fn get_table_stats(&self, table_name: &str) -> Result<Option<Rc<TableStats>>, SbroadError> {
        let stats = self.table_statistics_cache.borrow_mut();
        if let Some(value) = stats.get(table_name) {
            Ok(Some(value.clone()))
        } else {
            Ok(None)
        }
    }

    fn get_column_stats(
        &self,
        table_column_pair: &TableColumnPair,
    ) -> Result<Option<Rc<Box<dyn Any>>>, SbroadError> {
        let stats = self.initial_column_statistics_cache.borrow_mut();
        if let Some(value) = stats.get(table_column_pair) {
            Ok(Some(value.clone()))
        } else {
            Ok(None)
        }
    }

    fn update_table_stats(
        &mut self,
        table_name: SmolStr,
        table_stats: TableStats,
    ) -> Result<(), SbroadError> {
        let mut stats = self.table_statistics_cache.borrow_mut();
        let value = stats.get_mut(table_name.as_str());
        if let Some(value) = value {
            *value = Rc::new(table_stats);
        } else {
            stats.insert(table_name, Rc::new(table_stats));
        }
        Ok(())
    }

    fn update_column_stats<T: Scalar>(
        &self,
        table_column_pair: TableColumnPair,
        column_stats: ColumnStats<T>,
    ) -> Result<(), SbroadError> {
        let mut stats = self.initial_column_statistics_cache.borrow_mut();
        let boxed = Box::new(column_stats);
        let value = stats.get_mut(&table_column_pair);
        if let Some(value) = value {
            *value = Rc::new(boxed);
        } else {
            stats.insert(table_column_pair, Rc::new(boxed));
        }
        Ok(())
    }
}

fn exec_on_some(bucket: u64, query: &str) -> ProducerResult {
    let mut result = ProducerResult::new();

    result.rows.push(vec![
        LuaValue::String(format!("Execute query on a bucket [{bucket}]")),
        LuaValue::String(String::from(query)),
    ]);

    result
}

fn exec_locally(query: &str) -> ProducerResult {
    let mut result = ProducerResult::new();

    result.rows.push(vec![
        LuaValue::String("Execute query locally".into()),
        LuaValue::String(String::from(query)),
    ]);

    result
}

fn exec_on_all(query: &str) -> ProducerResult {
    let mut result = ProducerResult::new();

    result.rows.push(vec![
        LuaValue::String(String::from("Execute query on all buckets")),
        LuaValue::String(String::from(query)),
    ]);

    result
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReplicasetDispatchInfo {
    pub rs_id: usize,
    pub pattern: String,
    pub params: Vec<Value>,
    pub vtables_map: HashMap<NodeId, Rc<VirtualTable>>,
}

impl ReplicasetDispatchInfo {
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(rs_id: usize, exec_plan: &ExecutionPlan) -> Self {
        let top = exec_plan.get_ir_plan().get_top().unwrap();
        let sp = SyntaxPlan::new(exec_plan, top, Snapshot::Oldest).unwrap();
        let ordered_sn = OrderedSyntaxNodes::try_from(sp).unwrap();
        let syntax_data_nodes = ordered_sn.to_syntax_data().unwrap();
        let (pattern_with_params, _) = exec_plan
            .to_sql(&syntax_data_nodes, TEMPLATE, None)
            .unwrap();
        let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
        if let Some(vtables_map) = exec_plan.get_vtables() {
            vtables.clone_from(vtables_map);
        }
        Self {
            rs_id,
            pattern: pattern_with_params.pattern,
            params: pattern_with_params.params,
            vtables_map: vtables,
        }
    }
}

impl RouterRuntimeMock {
    pub fn set_vshard_mock(&mut self, rs_count: usize) {
        self.vshard_mock = VshardMock::new(rs_count, self.bucket_count());
    }

    /// Imitates the real pipeline of dispatching plan subtree
    /// on the given buckets. But does not encode plan into
    /// message for sending, instead it evalutes what sql
    /// query will be executed on each replicaset, and what vtables
    /// will be send to that replicaset.
    #[allow(clippy::missing_panics_doc)]
    pub fn detailed_dispatch(
        &self,
        plan: ExecutionPlan,
        buckets: &Buckets,
    ) -> Vec<ReplicasetDispatchInfo> {
        *dispatch_by_buckets(plan, buckets, self, DispatchReturnFormat::Tuple)
            .unwrap()
            .downcast::<Vec<ReplicasetDispatchInfo>>()
            .unwrap()
    }
}

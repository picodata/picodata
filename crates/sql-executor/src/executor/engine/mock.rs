use smol_str::{SmolStr, ToSmolStr};

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::errors::{Entity, SbroadError};
use crate::executor::engine::{
    helpers::{sharding_key_from_map, sharding_key_from_tuple, vshard::get_random_bucket},
    Router, Vshard,
};
use crate::executor::hash::bucket_id_by_tuple;
use crate::executor::ir::ExecutionPlan;
use crate::executor::lru::{Cache as _, LRUCache, DEFAULT_CAPACITY};
use crate::executor::preemption::{SchedulerMetrics, SchedulerOptions};
use crate::executor::vtable::VirtualTable;
use crate::executor::{ExecutingQuery, ExplainQueryLocation, MotionInfo};
use crate::executor::{Port, PortType};
use crate::ir::bucket::{BucketSet, Buckets};
use crate::ir::function::Function;
use crate::ir::node::NodeId;
use crate::ir::options::Forward;
use crate::ir::relation::{Column, ColumnRole, SpaceEngine, Table};
use crate::ir::tree::Snapshot;
use crate::ir::types::{DerivedType, NestedType, UnrestrictedType};
use crate::ir::value::Value;
use crate::ir::Plan;
use crate::utils::MutexLike;
use rand::random;
use serde::{Deserialize, Serialize};
use std::io::{Result as IoResult, Write};
use std::rc::Rc;
use tarantool::space::SpaceId;

use super::helpers::vshard::prepare_rs_to_ir_map;
use super::helpers::{dispatch_impl, normalize_name_from_sql, table_name};
use super::{get_builtin_functions, BlockExecData, Metadata, QueryCache};
use crate::executor::result::MetadataColumn;
use crate::executor::vdbe::{ExecutionInsight, SqlError, SqlStmt};

pub const TEMPLATE: u64 = 0;

pub struct PortMocked {
    tuples: Vec<Vec<u8>>,
    port_type: PortType,
}

impl Default for PortMocked {
    fn default() -> Self {
        Self::new()
    }
}

impl PortMocked {
    pub fn new() -> Self {
        Self {
            tuples: Vec::new(),
            port_type: PortType::DispatchExplain,
        }
    }

    pub fn decode(&self) -> Vec<DispatchInfo> {
        let mut result = Vec::with_capacity(self.size() as usize);
        for mp in self.iter() {
            let info: DispatchInfo = rmp_serde::from_slice(mp).unwrap();
            result.push(info);
        }
        result
    }
}

impl Port<'_> for PortMocked {
    fn add_mp(&mut self, data: &[u8]) {
        self.tuples.push(data.to_vec());
    }

    fn process_stmt(
        &mut self,
        _stmt: &mut SqlStmt,
        _params: &[Value],
        _max_vdbe: u64,
    ) -> Result<ExecutionInsight, SqlError>
    where
        Self: Sized,
    {
        unreachable!();
    }

    fn process_stmt_with_raw_params(
        &mut self,
        _stmt: &mut SqlStmt,
        _params: &[u8],
        _max_vdbe: u64,
    ) -> Result<ExecutionInsight, SqlError> {
        unreachable!();
    }

    fn process_txn(
        &mut self,
        _stmt: &mut SqlStmt,
        _params: &[&Value],
        _vdbe_max_steps: u64,
    ) -> Result<ExecutionInsight, SbroadError>
    where
        Self: Sized,
    {
        unreachable!("there is no mock tests for txns")
    }

    fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.tuples.iter().map(|t| t.as_slice())
    }

    fn set_type(&mut self, port_type: PortType) {
        self.port_type = port_type;
    }

    fn size(&self) -> u32 {
        self.tuples.len() as u32
    }
}

impl Write for PortMocked {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.add_mp(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

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

    fn get_index_id(&self, _index_name: &str, _table_name: &str) -> Result<u32, SbroadError> {
        Err(SbroadError::DoSkip)
    }

    fn function(&self, fn_name: &str) -> Result<&Function, SbroadError> {
        let name = normalize_name_from_sql(fn_name);
        match self.functions.get(&name) {
            Some(v) => Ok(v),
            None => Err(SbroadError::NotFound(Entity::SQLFunction, name)),
        }
    }

    fn waiting_timeout(&self) -> Duration {
        Duration::default()
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

/// The `test__gibdd_db__vehicle_*` bench fixture: table names and user-column
/// names, shared with sql-frontend's bench/profiling case generators
/// (`sql-frontend/benches/bench_cases.rs`) so the generated bench SQL and the
/// mocked schema cannot drift apart.
pub const VEHICLE_ACTUAL_TABLE: &str = "test__gibdd_db__vehicle_reg_and_res100_actual";
pub const VEHICLE_HISTORY_TABLE: &str = "test__gibdd_db__vehicle_reg_and_res100_history";
pub const VEHICLE_COLUMNS: &[&str] = &[
    "vehicleguid",
    "reestrid",
    "reestrstatus",
    "vehicleregno",
    "vehiclevin",
    "vehiclevin2",
    "vehiclechassisnum",
    "vehiclereleaseyear",
    "operationregdoctypename",
    "operationregdoc",
    "operationregdocissuedate",
    "operationregdoccomments",
    "vehicleptstypename",
    "vehicleptsnum",
    "vehicleptsissuedate",
    "vehicleptsissuer",
    "vehicleptscomments",
    "vehiclebodycolor",
    "vehiclebrand",
    "vehiclemodel",
    "vehiclebrandmodel",
    "vehiclebodynum",
    "vehiclecost",
    "vehiclegasequip",
    "vehicleproducername",
    "vehiclegrossmass",
    "vehiclemass",
    "vehiclesteeringwheeltypeid",
    "vehiclekpptype",
    "vehicletransmissiontype",
    "vehicletypename",
    "vehiclecategory",
    "vehicletypeunit",
    "vehicleecoclass",
    "vehiclespecfuncname",
    "vehicleenclosedvolume",
    "vehicleenginemodel",
    "vehicleenginenum",
    "vehicleenginepower",
    "vehicleenginepowerkw",
    "vehicleenginetype",
    "holdrestrictiondate",
    "approvalnum",
    "approvaldate",
    "approvaltype",
    "utilizationfeename",
    "customsdoc",
    "customsdocdate",
    "customsdocissue",
    "customsdocrestriction",
    "customscountryremovalid",
    "customscountryremovalname",
    "ownerorgname",
    "ownerinn",
    "ownerogrn",
    "ownerkpp",
    "ownerpersonlastname",
    "ownerpersonfirstname",
    "ownerpersonmiddlename",
    "ownerpersonbirthdate",
    "ownerbirthplace",
    "ownerpersonogrnip",
    "owneraddressindex",
    "owneraddressmundistrict",
    "owneraddresssettlement",
    "owneraddressstreet",
    "ownerpersoninn",
    "ownerpersondoccode",
    "ownerpersondocnum",
    "ownerpersondocdate",
    "operationname",
    "operationdate",
    "operationdepartmentname",
    "operationattorney",
    "operationlising",
    "holdertypeid",
    "holderpersondoccode",
    "holderpersondocnum",
    "holderpersondocdate",
    "holderpersondocissuer",
    "holderpersonlastname",
    "holderpersonfirstname",
    "holderpersonmiddlename",
    "holderpersonbirthdate",
    "holderpersonbirthregionid",
    "holderpersonsex",
    "holderpersonbirthplace",
    "holderpersoninn",
    "holderpersonsnils",
    "holderpersonogrnip",
    "holderaddressguid",
    "holderaddressregionid",
    "holderaddressregionname",
    "holderaddressdistrict",
    "holderaddressmundistrict",
    "holderaddresssettlement",
    "holderaddressstreet",
    "holderaddressbuilding",
    "holderaddressstructureid",
    "holderaddressstructurename",
    "holderaddressstructure",
    "sys_from",
    "sys_to",
];

impl RouterConfigurationMock {
    /// Mock engine constructor.
    ///
    /// # Panics
    /// - If schema is invalid.
    #[allow(clippy::too_many_lines)]
    #[must_use]
    pub fn new() -> Self {
        let name_func = normalize_name_from_sql("func");
        let fn_func = Function::new_stable(
            name_func.clone(),
            DerivedType::new(UnrestrictedType::Integer),
            false,
        );
        let name_trim = normalize_name_from_sql("trim");
        let trim_func = Function::new_stable(
            name_trim.clone(),
            DerivedType::new(UnrestrictedType::String),
            false,
        );
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
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "product_code",
                DerivedType::new(UnrestrictedType::String),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "product_units",
                DerivedType::new(UnrestrictedType::Boolean),
                ColumnRole::User,
                true,
            ),
            Column::new(
                "sys_op",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                true,
            ),
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
        ];
        let sharding_key = &["identification_number", "product_code"];
        let primary_key = &["product_code", "identification_number"];
        tables.insert(
            "hash_testing".to_smolstr(),
            Table::new_sharded(
                random(),
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
                random(),
                "hash_testing_hist",
                columns.clone(),
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns2 = vec![
            Column::new(
                "identification_number",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "product_code",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "product_units",
                DerivedType::new(UnrestrictedType::Boolean),
                ColumnRole::User,
                true,
            ),
            Column::new(
                "sys_op",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                true,
            ),
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
        ];
        let sharding_key = &["identification_number", "product_code"];
        let primary_key = &["product_code", "identification_number"];
        tables.insert(
            "hash_testing2".to_smolstr(),
            Table::new_sharded(
                random(),
                "hash_testing2",
                columns2.clone(),
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        tables.insert(
            "hash_testing_hist2".to_smolstr(),
            Table::new_sharded(
                random(),
                "hash_testing_hist2",
                columns2,
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
                random(),
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
                random(),
                "hash_single_testing_hist",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let bool_sharded_columns = vec![
            Column::new(
                "b",
                DerivedType::new(UnrestrictedType::Boolean),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "payload",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                true,
            ),
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
        ];
        let bool_sharded_key: &[&str] = &["b"];
        let bool_sharded_pk: &[&str] = &["b"];
        tables.insert(
            "bool_sharded".to_smolstr(),
            Table::new_sharded(
                random(),
                "bool_sharded",
                bool_sharded_columns,
                bool_sharded_key,
                bool_sharded_pk,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "sysFrom",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                true,
            ),
            Column::new(
                "FIRST_NAME",
                DerivedType::new(UnrestrictedType::String),
                ColumnRole::User,
                true,
            ),
            Column::new(
                "sys_op",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                true,
            ),
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
        ];
        let sharding_key = &["id"];
        let primary_key = &["id"];

        tables.insert(
            "test_space".to_smolstr(),
            Table::new_sharded(
                random(),
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
                random(),
                "test_space_hist",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
        ];
        let sharding_key: &[&str] = &["id"];
        let primary_key: &[&str] = &["id"];
        tables.insert(
            "history".to_smolstr(),
            Table::new_sharded(
                random(),
                "history",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "A",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                true,
            ),
            Column::new(
                "B",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
        ];
        let sharding_key: &[&str] = &["A", "B"];
        let primary_key: &[&str] = &["B"];
        tables.insert(
            "TBL".to_smolstr(),
            Table::new_sharded(
                random(),
                "TBL",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "a",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                true,
            ),
            Column::new(
                "b",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "c",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                true,
            ),
            Column::new(
                "d",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                true,
            ),
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
        ];
        let sharding_key: &[&str] = &["a", "b"];
        let primary_key: &[&str] = &["b"];
        tables.insert(
            "t".to_smolstr(),
            Table::new_sharded(
                random(),
                "t",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "a",
                DerivedType::new(UnrestrictedType::String),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
            Column::new(
                "b",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
        ];
        let sharding_key: &[&str] = &["a", "b"];
        let primary_key: &[&str] = &["a", "b"];
        tables.insert(
            "t1".to_smolstr(),
            Table::new_sharded(
                random(),
                "t1",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "a",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
            Column::new(
                "b",
                DerivedType::new(UnrestrictedType::Array(NestedType::Integer)),
                ColumnRole::User,
                true,
            ),
        ];
        let sharding_key: &[&str] = &["a"];
        let primary_key: &[&str] = &["a"];
        tables.insert(
            "arr_t".to_smolstr(),
            Table::new_sharded(
                random(),
                "arr_t",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "a",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
            Column::new(
                "b",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
        ];
        let sharding_key: &[&str] = &["a", "b"];
        let primary_key: &[&str] = &["a", "b"];
        tables.insert(
            "t1_2".to_smolstr(),
            Table::new_sharded(
                random(),
                "t1_2",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "e",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "f",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "g",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "h",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
        ];
        let sharding_key: &[&str] = &["e", "f"];
        let primary_key: &[&str] = &["g", "h"];
        tables.insert(
            "t2".to_smolstr(),
            Table::new_sharded(
                random(),
                "t2",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
            Column::new(
                "a",
                DerivedType::new(UnrestrictedType::String),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "b",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
        ];
        let sharding_key: &[&str] = &["a"];
        let primary_key: &[&str] = &["a"];
        tables.insert(
            "t3".to_smolstr(),
            Table::new_sharded(
                random(),
                "t3",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
            Column::new(
                "a",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "b",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
        ];
        let sharding_key: &[&str] = &["a"];
        let primary_key: &[&str] = &["a"];
        tables.insert(
            "t3_2".to_smolstr(),
            Table::new_sharded(
                random(),
                "t3_2",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
            Column::new(
                "c",
                DerivedType::new(UnrestrictedType::String),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "d",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
        ];
        let sharding_key: &[&str] = &["c"];
        let primary_key: &[&str] = &["d"];
        tables.insert(
            "t4".to_smolstr(),
            Table::new_sharded(
                random(),
                "t4",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
            Column::new(
                "a",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "b",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
        ];

        let sharding_key: &[&str] = &["a"];
        let primary_key: &[&str] = &["a"];
        tables.insert(
            "t5".to_smolstr(),
            Table::new_sharded(
                random(),
                "t5",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "a",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "b",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
        ];

        let primary_key: &[&str] = &["a"];
        tables.insert(
            "global_t".to_smolstr(),
            Table::new_global(random(), "global_t", columns, primary_key).unwrap(),
        );

        // Table for sql-benches
        let mut columns: Vec<Column> = VEHICLE_COLUMNS
            .iter()
            .map(|&name| {
                Column::new(
                    name,
                    DerivedType::new(UnrestrictedType::Integer),
                    ColumnRole::User,
                    false,
                )
            })
            .collect();
        columns.push(Column::new(
            "bucket_id",
            DerivedType::new(UnrestrictedType::Integer),
            ColumnRole::Sharding,
            true,
        ));
        let sharding_key: &[&str] = &["reestrid"];
        let primary_key: &[&str] = &["reestrid"];
        tables.insert(
            VEHICLE_ACTUAL_TABLE.to_smolstr(),
            Table::new_sharded(
                random(),
                VEHICLE_ACTUAL_TABLE,
                columns.clone(),
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );
        tables.insert(
            VEHICLE_HISTORY_TABLE.to_smolstr(),
            Table::new_sharded(
                random(),
                VEHICLE_HISTORY_TABLE,
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
            Column::new(
                "a",
                DerivedType::new(UnrestrictedType::String),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "b",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
        ];
        let sharding_key: &[&str] = &["a", "b"];
        let primary_key: &[&str] = &["a", "b"];
        tables.insert(
            "t6".to_smolstr(),
            Table::new_sharded(
                random(),
                "t6",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new(
                "bucket_id",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::Sharding,
                true,
            ),
            Column::new(
                "a",
                DerivedType::new(UnrestrictedType::String),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "b",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
        ];
        let sharding_key: &[&str] = &["a"];
        let primary_key: &[&str] = &["a", "b"];
        tables.insert(
            "t7".to_smolstr(),
            Table::new_sharded(
                random(),
                "t7",
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
    pub fn group(&self, buckets: &Buckets) -> HashMap<String, Vec<u64>> {
        let mut res: HashMap<String, Vec<u64>> = HashMap::new();
        match buckets {
            Buckets::All => {
                for (idx, (start, end)) in self.blocks.iter().enumerate() {
                    let name = Self::generate_rs_name(idx);
                    res.insert(name, ((*start)..(*end)).collect());
                }
            }
            Buckets::Filtered(BucketSet::Exact(buckets_set)) => {
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
            Buckets::Filtered(_) => panic!("buckets are not discovered"),
            Buckets::Any => {
                res.insert(Self::generate_rs_name(0), vec![0]);
            }
        }

        res
    }

    fn generate_rs_name(idx: usize) -> String {
        format!("replicaset_{idx}")
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone)]
pub struct RouterRuntimeMock {
    // It's based on the RefCells instead of tarantool mutexes,
    // so it could be used in unit tests - they won't compile otherwise due to missing tarantool symbols.
    metadata: RefCell<RouterConfigurationMock>,
    virtual_tables: RefCell<HashMap<NodeId, VirtualTable>>,
    ir_cache: Rc<RefCell<LRUCache<SmolStr, Rc<Plan>>>>,
    pub vshard_mock: VshardMock,
}

impl std::fmt::Debug for RouterRuntimeMock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("")
            .field(&self.metadata)
            .field(&self.virtual_tables)
            .finish()
    }
}

impl QueryCache for RouterRuntimeMock {
    type Cache = LRUCache<SmolStr, Rc<Plan>>;
    type Mutex = RefCell<Self::Cache>;

    fn cache(&self) -> &Self::Mutex {
        &self.ir_cache
    }

    fn provides_versions(&self) -> bool {
        false
    }

    fn get_table_version(&self, _: &str) -> Result<u64, SbroadError> {
        Err(SbroadError::DoSkip)
    }

    fn get_index_version_by_pk(&self, _: u32, _: u32) -> Result<u64, SbroadError> {
        Err(SbroadError::DoSkip)
    }

    fn get_table_version_by_id(&self, _: SpaceId) -> Result<u64, SbroadError> {
        Err(SbroadError::DoSkip)
    }

    fn get_table_name_and_version(&self, _: SpaceId) -> Result<(SmolStr, u64), SbroadError> {
        Err(SbroadError::DoSkip)
    }
}

impl Vshard for RouterRuntimeMock {
    fn exec_ir_on_any_node<'p>(
        &self,
        sub_plan: Rc<ExecutionPlan>,
        top_id: NodeId,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        mock_dispatch(self, sub_plan, top_id, buckets, port)?;
        Ok(())
    }

    fn bucket_count(&self) -> u64 {
        self.metadata().lock().bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id_with_buf(
        &self,
        s: &[&Value],
        _buf: &mut Vec<u8>,
    ) -> Result<u64, SbroadError> {
        Ok(bucket_id_by_tuple(s, self.bucket_count()))
    }

    fn exec_ir_on_buckets<'p>(
        &self,
        sub_plan: Rc<ExecutionPlan>,
        top_id: NodeId,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        mock_dispatch(self, sub_plan, top_id, buckets, port)?;
        Ok(())
    }

    fn exec_block_on_buckets<'p>(
        &self,
        _metadata: Vec<MetadataColumn>,
        _block: BlockExecData,
        _buckets: &Buckets,
        _request_id: &str,
        _port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        todo!()
    }
}

impl Vshard for &RouterRuntimeMock {
    fn bucket_count(&self) -> u64 {
        self.metadata().lock().bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id_with_buf(
        &self,
        s: &[&Value],
        _buf: &mut Vec<u8>,
    ) -> Result<u64, SbroadError> {
        Ok(bucket_id_by_tuple(s, self.bucket_count()))
    }

    fn exec_ir_on_any_node<'p>(
        &self,
        sub_plan: Rc<ExecutionPlan>,
        top_id: NodeId,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        mock_dispatch(self, sub_plan, top_id, buckets, port)?;
        Ok(())
    }

    fn exec_ir_on_buckets<'p>(
        &self,
        sub_plan: Rc<ExecutionPlan>,
        top_id: NodeId,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        mock_dispatch(self, sub_plan, top_id, buckets, port)?;
        Ok(())
    }

    fn exec_block_on_buckets<'p>(
        &self,
        _metadata: Vec<MetadataColumn>,
        _block: BlockExecData,
        _buckets: &Buckets,
        _request_id: &str,
        _port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        todo!()
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum DispatchInfo {
    /// (sql, parameters)
    All(String, Vec<Value>),
    /// (sql, parameters, replicaset)
    Any(String, Vec<Value>),
    /// [(sql, parameters, replicaset, buckets)]
    Filtered(Vec<(String, Vec<Value>, String, Vec<u64>)>),
}

fn mock_dispatch<'p>(
    runtime: &RouterRuntimeMock,
    plan: Rc<ExecutionPlan>,
    top_id: NodeId,
    buckets: &Buckets,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError> {
    let flags = plan.subtree_dispatch_flags_at(top_id)?;
    let is_single = !flags.has_segmented_tables && !flags.has_customization_opcodes;

    match buckets {
        Buckets::All if is_single => {
            let (pattern, params) = to_sql(&plan, top_id);
            let mp = rmp_serde::to_vec(&DispatchInfo::All(pattern, params)).unwrap();
            port.add_mp(mp.as_slice());
        }
        Buckets::Any if !flags.has_customization_opcodes => {
            let (pattern, params) = to_sql(&plan, top_id);
            let mp = rmp_serde::to_vec(&DispatchInfo::Any(pattern, params)).unwrap();
            port.add_mp(mp.as_slice());
        }
        _ => {
            let info = custom_plan_dispatch(runtime, &plan, top_id, buckets);
            let mp = rmp_serde::to_vec(&DispatchInfo::Filtered(info)).unwrap();
            port.add_mp(mp.as_slice());
        }
    }

    Ok(())
}

fn to_sql(plan: &ExecutionPlan, top_id: NodeId) -> (String, Vec<Value>) {
    let subtree = plan
        .freeze()
        .execution_view()
        .dql_subtree(top_id)
        .expect("dql subtree");
    let sp = SyntaxPlan::new_for_dql_subtree(&subtree, Snapshot::Oldest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let params = plan
        .local_sql_params(top_id, Snapshot::Oldest)
        .expect("local sql params");
    let (constant_ids, params) = params.into_parts();
    let sql = subtree
        .generate_sql(&nodes, TEMPLATE, table_name, Some(constant_ids.as_slice()))
        .unwrap();
    (sql, params)
}

fn custom_plan_dispatch(
    runtime: &RouterRuntimeMock,
    plan: &ExecutionPlan,
    top_id: NodeId,
    buckets: &Buckets,
) -> Vec<(String, Vec<Value>, String, Vec<u64>)> {
    let mut info = Vec::new();
    let mut rs_bucket_vec: Vec<(String, Vec<u64>)> =
        runtime.vshard_mock.group(buckets).drain().collect();
    rs_bucket_vec.sort_by_key(|(rs, _)| rs.clone());
    let (rs_ir, _) = prepare_rs_to_ir_map(&rs_bucket_vec, plan.clone(), top_id).unwrap();
    for (rs, ex_plan) in rs_ir {
        let (pattern, params) = to_sql(&ex_plan, top_id);
        let buckets = rs_bucket_vec
            .iter()
            .find_map(|(name, buckets)| {
                if *name == rs {
                    Some(buckets.clone())
                } else {
                    None
                }
            })
            .unwrap();
        info.push((pattern, params, rs, buckets));
    }
    // Sort to get deterministic test results.
    info.sort_by_key(|(_, _, rs, _)| rs.clone());
    info
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
        let cache: LRUCache<SmolStr, Rc<Plan>> = LRUCache::new(DEFAULT_CAPACITY, None).unwrap();
        let meta = RouterConfigurationMock::new();
        let bucket_cnt = meta.bucket_count;

        RouterRuntimeMock {
            metadata: RefCell::new(meta),
            virtual_tables: RefCell::new(HashMap::new()),
            ir_cache: Rc::new(RefCell::new(cache)),
            vshard_mock: VshardMock::new(2, bucket_cnt),
        }
    }

    #[allow(dead_code)]
    pub fn add_virtual_table(&self, id: NodeId, table: VirtualTable) {
        self.virtual_tables.borrow_mut().insert(id, table);
    }

    pub fn add_table(&mut self, table: Table) {
        self.metadata
            .borrow_mut()
            .tables
            .insert(table.name.clone(), table);
    }

    pub fn set_vshard_mock(&mut self, rs_count: usize) {
        self.vshard_mock = VshardMock::new(rs_count, self.bucket_count());
    }
}

impl Router for RouterRuntimeMock {
    type MetadataProvider = RouterConfigurationMock;
    type VshardImplementor = Self;

    fn metadata(&self) -> &impl MutexLike<Self::MetadataProvider> {
        &self.metadata
    }

    fn with_admin_su<T>(&self, f: impl FnOnce() -> T) -> Result<T, SbroadError> {
        Ok(f())
    }

    fn new_port<'p>(&self) -> impl Port<'p> {
        PortMocked::new()
    }

    fn materialize_motion(
        &self,
        plan: &mut ExecutionPlan,
        motion_node_id: &NodeId,
        _buckets: &Buckets,
    ) -> Result<VirtualTable, SbroadError> {
        plan.mark_motion_subtree_unlinked(*motion_node_id)?;
        Ok(self
            .virtual_tables
            .borrow()
            .get(motion_node_id)
            .expect(&format!(
                "Virtual table for motion with id {motion_node_id} not found."
            ))
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
            .expect(&format!(
                "Virtual table for values with id {values_id} not found."
            ))
            .clone())
    }

    fn dispatch<'p>(
        &self,
        plan: &mut ExecutionPlan,
        top_id: NodeId,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        dispatch_impl(self, plan, top_id, buckets, port)?;
        Ok(())
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

    fn is_audit_enabled(&self, _plan: &Plan) -> Result<bool, SbroadError> {
        Ok(false)
    }

    fn is_sql_log_enabled(&self, _plan: &Plan) -> Result<bool, SbroadError> {
        Ok(false)
    }

    fn get_scheduler_options(&self) -> SchedulerOptions {
        SchedulerOptions {
            enabled: false,
            yield_interval_us: 500,
            yield_vdbe_opcodes: 1024,
            yield_impl: || {},
            metrics: SchedulerMetrics::noop(),
        }
    }

    fn enforce_forward_option(
        &self,
        _forward_option: Forward,
        _buckets: &Buckets,
        _target_replicaset: &mut Option<String>,
    ) -> Result<(), SbroadError> {
        Ok(())
    }

    fn get_possible_forward_option(
        &self,
        _buckets: &Buckets,
        _target_replicaset: &mut Option<String>,
    ) -> Result<Forward, SbroadError> {
        Ok(Forward::On)
    }

    fn build_explain_query_location(
        _buckets: &Buckets,
        _motion_info: &MotionInfo,
    ) -> ExplainQueryLocation {
        ExplainQueryLocation::Whole
    }
}

impl<C: Router> ExecutingQuery<'_, C> {
    pub fn explain(&mut self) -> Result<String, SbroadError> {
        let mut explain = Vec::new();
        if self.is_logical_explain() {
            let logical_explain = self.explain_logical()?;
            explain.push(logical_explain);
        }

        if self.is_raw_explain() {
            return Err(SbroadError::Other(
                "RAW mode of EXPLAIN is not supported for mocks".to_smolstr(),
            ));
        }

        if self.is_explain_forward() {
            return Err(SbroadError::Other(
                "FORWARD mode of EXPLAIN is not supported for mocks".to_smolstr(),
            ));
        }

        if self.is_buckets_explain() {
            let buckets_explain = self.explain_buckets()?;
            explain.push(buckets_explain);
        }

        // Each entry in `explain` is a plain line without a trailing '\n',
        // so we join them with "\n\n" to separate each entry with a blank line.
        let explain = explain.join("\n\n");

        Ok(explain.into())
    }
}

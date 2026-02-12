use smol_str::{SmolStr, ToSmolStr};
use std::any::Any;

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;

use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::errors::{Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::engine::{
    helpers::{sharding_key_from_map, sharding_key_from_tuple, vshard::get_random_bucket},
    Router, Vshard,
};
use crate::executor::hash::bucket_id_by_tuple;
use crate::executor::ir::ExecutionPlan;
use crate::executor::lru::{Cache as _, LRUCache, DEFAULT_CAPACITY};
use crate::executor::preemption::{SchedulerMetrics, SchedulerOptions};
use crate::executor::vtable::VirtualTable;
use crate::executor::{Port, PortType};
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::ir::function::Function;
use crate::ir::node::NodeId;
use crate::ir::relation::{Column, ColumnRole, SpaceEngine, Table};
use crate::ir::tree::Snapshot;
use crate::ir::types::{DerivedType, UnrestrictedType};
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
use super::{get_builtin_functions, Metadata, QueryCache};
use crate::executor::vdbe::{ExecutionInsight, SqlError, SqlStmt};

pub const TEMPLATE: &str = "test";

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
        let columns = vec![
            Column::new(
                "vehicleguid",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "reestrid",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "reestrstatus",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleregno",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclevin",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclevin2",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclechassisnum",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclereleaseyear",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "operationregdoctypename",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "operationregdoc",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "operationregdocissuedate",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "operationregdoccomments",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleptstypename",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleptsnum",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleptsissuedate",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleptsissuer",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleptscomments",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclebodycolor",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclebrand",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclemodel",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclebrandmodel",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclebodynum",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclecost",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclegasequip",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleproducername",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclegrossmass",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclemass",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclesteeringwheeltypeid",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclekpptype",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicletransmissiontype",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicletypename",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclecategory",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicletypeunit",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleecoclass",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehiclespecfuncname",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleenclosedvolume",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleenginemodel",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleenginenum",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleenginepower",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleenginepowerkw",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "vehicleenginetype",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holdrestrictiondate",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "approvalnum",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "approvaldate",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "approvaltype",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "utilizationfeename",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "customsdoc",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "customsdocdate",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "customsdocissue",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "customsdocrestriction",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "customscountryremovalid",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "customscountryremovalname",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerorgname",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerinn",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerogrn",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerkpp",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerpersonlastname",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerpersonfirstname",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerpersonmiddlename",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerpersonbirthdate",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerbirthplace",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerpersonogrnip",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "owneraddressindex",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "owneraddressmundistrict",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "owneraddresssettlement",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "owneraddressstreet",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerpersoninn",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerpersondoccode",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerpersondocnum",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "ownerpersondocdate",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "operationname",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "operationdate",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "operationdepartmentname",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "operationattorney",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "operationlising",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holdertypeid",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersondoccode",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersondocnum",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersondocdate",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersondocissuer",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonlastname",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonfirstname",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonmiddlename",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonbirthdate",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonbirthregionid",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonsex",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonbirthplace",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersoninn",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonsnils",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderpersonogrnip",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressguid",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressregionid",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressregionname",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressdistrict",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressmundistrict",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddresssettlement",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressstreet",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressbuilding",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressstructureid",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressstructurename",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "holderaddressstructure",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "sys_from",
                DerivedType::new(UnrestrictedType::Integer),
                ColumnRole::User,
                false,
            ),
            Column::new(
                "sys_to",
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
        let sharding_key: &[&str] = &["reestrid"];
        let primary_key: &[&str] = &["reestrid"];
        tables.insert(
            "test__gibdd_db__vehicle_reg_and_res100_actual".to_smolstr(),
            Table::new_sharded(
                random(),
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
                random(),
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
    pub fn group(&self, buckets: &Buckets) -> HashMap<String, Vec<u64>> {
        let mut res: HashMap<String, Vec<u64>> = HashMap::new();
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
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        mock_dispatch(self, sub_plan, buckets, port)?;
        Ok(())
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

    fn exec_ir_on_buckets<'p>(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        mock_dispatch(self, sub_plan, buckets, port)?;
        Ok(())
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

    fn exec_ir_on_any_node<'p>(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        mock_dispatch(self, sub_plan, buckets, port)?;
        Ok(())
    }

    fn exec_ir_on_buckets<'p>(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        mock_dispatch(self, sub_plan, buckets, port)?;
        Ok(())
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
    plan: ExecutionPlan,
    buckets: &Buckets,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError> {
    let is_single = !plan.has_segmented_tables() && !plan.has_customization_opcodes();

    match buckets {
        Buckets::All if is_single => {
            let (pattern, params) = to_sql(&plan);
            let mp = rmp_serde::to_vec(&DispatchInfo::All(pattern, params)).unwrap();
            port.add_mp(mp.as_slice());
        }
        Buckets::Any if is_single => {
            let (pattern, params) = to_sql(&plan);
            let mp = rmp_serde::to_vec(&DispatchInfo::Any(pattern, params)).unwrap();
            port.add_mp(mp.as_slice());
        }
        _ => {
            let info = custom_plan_dispatch(runtime, plan, buckets);
            let mp = rmp_serde::to_vec(&DispatchInfo::Filtered(info)).unwrap();
            port.add_mp(mp.as_slice());
        }
    }

    Ok(())
}

fn to_sql(plan: &ExecutionPlan) -> (String, Vec<Value>) {
    let top_id = plan.get_ir_plan().get_top().unwrap();
    let sp = SyntaxPlan::new(plan, top_id, Snapshot::Oldest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let (sql, _) = plan
        .generate_sql(&nodes, TEMPLATE, None, |name: &str, id| {
            table_name(name, id)
        })
        .unwrap();
    let params = plan.get_ir_plan().constants.clone();
    (sql, params)
}

fn custom_plan_dispatch(
    runtime: &RouterRuntimeMock,
    plan: ExecutionPlan,
    buckets: &Buckets,
) -> Vec<(String, Vec<Value>, String, Vec<u64>)> {
    let mut info = Vec::new();
    let mut rs_bucket_vec: Vec<(String, Vec<u64>)> =
        runtime.vshard_mock.group(buckets).drain().collect();
    rs_bucket_vec.sort_by_key(|(rs, _)| rs.clone());
    let rs_ir = prepare_rs_to_ir_map(&rs_bucket_vec, plan).unwrap();
    for (rs, ex_plan) in rs_ir {
        let (pattern, params) = to_sql(&ex_plan);
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
    type ParseTree = AbstractSyntaxTree;
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
        plan.unlink_motion_subtree(*motion_node_id)?;
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
            yield_impl: || {},
            metrics: SchedulerMetrics::noop(),
        }
    }
}

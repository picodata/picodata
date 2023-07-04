//! Router runtime module for the clusterwide SQL.
//! Implements infrastructure to build a distributed
//! query plan and dispatch it to the storage nodes.

use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::helpers::sharding_keys_from_tuple;
use sbroad::executor::engine::helpers::vshard::{
    exec_ir_on_all_buckets, exec_ir_on_some_buckets, get_random_bucket,
};
use sbroad::executor::engine::helpers::{
    dispatch, explain_format, materialize_motion, sharding_keys_from_map,
};
use sbroad::executor::engine::{QueryCache, Router, Vshard};
use sbroad::executor::ir::{ConnectionType, ExecutionPlan, QueryType};
use sbroad::executor::lru::{Cache, LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::protocol::Binary;
use sbroad::frontend::sql::ast::AbstractSyntaxTree;
use sbroad::ir::value::{MsgPackValue, Value};
use sbroad::ir::Plan;

use std::any::Any;
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use super::{meta::router::RouterMetadata, DEFAULT_BUCKET_COUNT};

use ::tarantool::tuple::{KeyDef, Tuple};

thread_local! (
    static PLAN_CACHE: Rc<RefCell<LRUCache<String, Plan>>> = Rc::new(
        RefCell::new(LRUCache::new(DEFAULT_CAPACITY, None).unwrap())
    )
);

#[allow(clippy::module_name_repetitions)]
pub struct RouterRuntime {
    metadata: RefCell<RouterMetadata>,
    bucket_count: u64,
    ir_cache: Rc<RefCell<LRUCache<String, Plan>>>,
}

impl RouterRuntime {
    /// Build a new router runtime.
    ///
    /// # Errors
    /// - If the cache cannot be initialized.
    pub fn new() -> Result<Self, SbroadError> {
        let metadata = RouterMetadata::default();
        let bucket_count = DEFAULT_BUCKET_COUNT;
        let runtime = PLAN_CACHE.with(|cache| RouterRuntime {
            metadata: RefCell::new(metadata),
            bucket_count,
            ir_cache: cache.clone(),
        });
        Ok(runtime)
    }
}

impl QueryCache for RouterRuntime {
    type Cache = LRUCache<String, Plan>;

    fn cache(&self) -> &RefCell<Self::Cache> {
        &self.ir_cache
    }

    fn cache_capacity(&self) -> Result<usize, SbroadError> {
        Ok(self
            .ir_cache
            .try_borrow()
            .map_err(|e| SbroadError::FailedTo(Action::Get, Some(Entity::Cache), format!("{e:?}")))?
            .capacity())
    }

    fn clear_cache(&self) -> Result<(), SbroadError> {
        *self.ir_cache.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Clear, Some(Entity::Cache), format!("{e:?}"))
        })? = Self::Cache::new(self.cache_capacity()?, None)?;
        Ok(())
    }
}

impl Router for RouterRuntime {
    type ParseTree = AbstractSyntaxTree;
    type MetadataProvider = RouterMetadata;

    fn metadata(&self) -> Result<Ref<Self::MetadataProvider>, SbroadError> {
        self.metadata.try_borrow().map_err(|e| {
            SbroadError::FailedTo(Action::Get, Some(Entity::Metadata), format!("{e:?}"))
        })
    }

    fn materialize_motion(
        &self,
        plan: &mut sbroad::executor::ir::ExecutionPlan,
        motion_node_id: usize,
        buckets: &sbroad::executor::bucket::Buckets,
    ) -> Result<sbroad::executor::vtable::VirtualTable, SbroadError> {
        materialize_motion(self, plan, motion_node_id, buckets)
    }

    fn dispatch(
        &self,
        plan: &mut sbroad::executor::ir::ExecutionPlan,
        top_id: usize,
        buckets: &sbroad::executor::bucket::Buckets,
    ) -> Result<Box<dyn std::any::Any>, SbroadError> {
        dispatch(self, plan, top_id, buckets)
    }

    fn explain_format(&self, explain: String) -> Result<Box<dyn std::any::Any>, SbroadError> {
        explain_format(&explain)
    }

    fn extract_sharding_keys_from_map<'rec>(
        &self,
        space: String,
        args: &'rec HashMap<String, Value>,
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        let metadata = self.metadata.try_borrow().map_err(|e| {
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e:?}"))
        })?;
        sharding_keys_from_map(&*metadata, &space, args)
    }

    fn extract_sharding_keys_from_tuple<'rec>(
        &self,
        space: String,
        args: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_keys_from_tuple(&*self.metadata()?, &space, args)
    }
}

pub(crate) fn calculate_bucket_id(tuple: &[&Value], bucket_count: u64) -> Result<u64, SbroadError> {
    let wrapped_tuple = tuple
        .iter()
        .map(|v| MsgPackValue::from(*v))
        .collect::<Vec<_>>();
    let tnt_tuple = Tuple::new(&wrapped_tuple).map_err(|e| {
        SbroadError::FailedTo(Action::Create, Some(Entity::Tuple), format!("{e:?}"))
    })?;
    let mut key_parts = Vec::with_capacity(tuple.len());
    for (pos, value) in tuple.iter().enumerate() {
        let pos = u32::try_from(pos).map_err(|_| {
            SbroadError::FailedTo(
                Action::Create,
                Some(Entity::KeyDef),
                "Tuple is too long".to_string(),
            )
        })?;
        key_parts.push(value.as_key_def_part(pos));
    }
    let key = KeyDef::new(key_parts.as_slice()).map_err(|e| {
        SbroadError::FailedTo(Action::Create, Some(Entity::KeyDef), format!("{e:?}"))
    })?;
    Ok(u64::from(key.hash(&tnt_tuple)) % bucket_count)
}

impl Vshard for RouterRuntime {
    fn exec_ir_on_all(
        &self,
        required: Binary,
        optional: Binary,
        query_type: QueryType,
        conn_type: ConnectionType,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_all_buckets(
            &*self.metadata()?,
            required,
            optional,
            query_type,
            conn_type,
        )
    }

    fn bucket_count(&self) -> u64 {
        self.bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError> {
        calculate_bucket_id(s, self.bucket_count())
    }

    fn exec_ir_on_some(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_some_buckets(self, sub_plan, buckets)
    }
}

impl Vshard for &RouterRuntime {
    fn exec_ir_on_all(
        &self,
        required: Binary,
        optional: Binary,
        query_type: QueryType,
        conn_type: ConnectionType,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_all_buckets(
            &*self.metadata()?,
            required,
            optional,
            query_type,
            conn_type,
        )
    }

    fn bucket_count(&self) -> u64 {
        self.bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError> {
        calculate_bucket_id(s, self.bucket_count())
    }

    fn exec_ir_on_some(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_some_buckets(*self, sub_plan, buckets)
    }
}

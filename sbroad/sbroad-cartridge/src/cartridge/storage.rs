use crate::cartridge::bucket_count;
use crate::cartridge::config::StorageConfiguration;
use sbroad::errors::{Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::helpers::storage::unprepare;
use sbroad::executor::engine::helpers::vshard::{get_random_bucket, CacheInfo};
use sbroad::executor::engine::helpers::EncodedQueryInfo;
use sbroad::executor::engine::helpers::{
    execute_first_cacheable_request, execute_second_cacheable_request, OptionalBytes,
};
use sbroad::executor::engine::DispatchReturnFormat;
use sbroad::executor::engine::{helpers, StorageCache};
use sbroad::executor::engine::{QueryCache, Vshard};
use sbroad::executor::hash::bucket_id_by_tuple;
use sbroad::executor::ir::{ExecutionPlan, QueryType};
use sbroad::executor::lru::{Cache, LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::protocol::{RequiredData, SchemaInfo};
use sbroad::ir::node::NodeId;
use sbroad::ir::value::Value;
use sbroad::utils::MutexLike;
use sbroad::{debug, error, warn};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::any::Any;

use std::fmt::Display;
use tarantool::fiber::Mutex;
use tarantool::sql::Statement;
use tarantool::tlua::LuaFunction;

use super::ConfigurationProvider;

#[allow(clippy::module_name_repetitions)]
pub struct StorageRuntime {
    metadata: Mutex<StorageConfiguration>,
    bucket_count: u64,
    cache: Mutex<CartridgeCache>,
}

pub struct CartridgeCache(LRUCache<SmolStr, (Statement, Vec<NodeId>)>);

impl StorageCache for CartridgeCache {
    fn put(
        &mut self,
        plan_id: SmolStr,
        stmt: Statement,
        _: &SchemaInfo,
        table_ids: Vec<NodeId>,
    ) -> Result<(), SbroadError> {
        self.0.put(plan_id, (stmt, table_ids))
    }

    fn get(&mut self, plan_id: &SmolStr) -> Result<Option<(&Statement, &[NodeId])>, SbroadError> {
        let Some((stmt, table_ids)) = self.0.get(plan_id)? else {
            return Ok(None);
        };
        Ok(Some((stmt, table_ids.as_slice())))
    }

    fn clear(&mut self) -> Result<(), SbroadError> {
        self.0.clear()
    }
}

impl QueryCache for StorageRuntime {
    type Cache = CartridgeCache;
    type Mutex = Mutex<Self::Cache>;

    fn cache(&self) -> &Self::Mutex {
        &self.cache
    }

    fn cache_capacity(&self) -> Result<usize, SbroadError> {
        Ok(self.cache().lock().0.capacity())
    }

    fn clear_cache(&self) -> Result<(), SbroadError> {
        self.cache.lock().clear()
    }

    fn provides_versions(&self) -> bool {
        false
    }

    fn get_table_version(&self, _: &str) -> Result<u64, SbroadError> {
        Err(SbroadError::DoSkip)
    }
}

impl ConfigurationProvider for StorageRuntime {
    type Configuration = StorageConfiguration;

    fn cached_config(&self) -> &impl MutexLike<Self::Configuration> {
        &self.metadata
    }

    fn clear_config(&self) -> Result<(), SbroadError> {
        let mut metadata = self.metadata.lock();
        *metadata = Self::Configuration::new();
        Ok(())
    }

    fn is_config_empty(&self) -> Result<bool, SbroadError> {
        let metadata = self.metadata.lock();
        Ok(metadata.is_empty())
    }

    fn retrieve_config(&self) -> Result<Option<Self::Configuration>, SbroadError> {
        if self.is_config_empty()? {
            let lua = tarantool::lua_state();

            let storage_cache_capacity: LuaFunction<_> =
                lua.eval("return get_storage_cache_capacity;").unwrap();
            let capacity: u64 = match storage_cache_capacity.call() {
                Ok(capacity) => capacity,
                Err(e) => {
                    error!(
                        Option::from("getting storage cache capacity"),
                        &format!("{e:?}"),
                    );
                    return Err(SbroadError::LuaError(format_smolstr!("{e:?}")));
                }
            };
            let storage_capacity = usize::try_from(capacity)
                .map_err(|e| SbroadError::Invalid(Entity::Cache, Some(format_smolstr!("{e:?}"))))?;

            let storage_cache_size_bytes: LuaFunction<_> =
                lua.eval("return get_storage_cache_size_bytes;").unwrap();
            let cache_size_bytes = match storage_cache_size_bytes.call::<u64>() {
                Ok(size_bytes) => size_bytes,
                Err(e) => {
                    error!(
                        Option::from("getting storage cache size bytes"),
                        &format!("{e:?}"),
                    );
                    return Err(SbroadError::LuaError(format_smolstr!("{e:?}")));
                }
            };
            let storage_size_bytes = usize::try_from(cache_size_bytes)
                .map_err(|e| SbroadError::Invalid(Entity::Cache, Some(format_smolstr!("{e}"))))?;

            let mut metadata = StorageConfiguration::new();
            metadata.storage_capacity = storage_capacity;
            metadata.storage_size_bytes = storage_size_bytes;

            return Ok(Some(metadata));
        }
        Ok(None)
    }

    fn update_config(&self, metadata: Self::Configuration) -> Result<(), SbroadError> {
        let mut cached_metadata = self.metadata.lock();
        let storage_size_bytes = metadata.storage_size_bytes;
        *cached_metadata = metadata;
        update_box_param("sql_cache_size", storage_size_bytes);
        Ok(())
    }
}

impl Vshard for StorageRuntime {
    fn exec_ir_on_any_node(
        &self,
        _sub_plan: ExecutionPlan,
        _return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_locally is not supported for the cartridge runtime".to_smolstr()),
        ))
    }

    fn bucket_count(&self) -> u64 {
        self.bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError> {
        Ok(bucket_id_by_tuple(s, self.bucket_count()))
    }

    fn exec_ir_on_buckets(
        &self,
        _sub_plan: ExecutionPlan,
        _buckets: &Buckets,
        _return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_on_some is not supported on the storage".to_smolstr()),
        ))
    }
}

impl StorageRuntime {
    /// Build a new storage runtime.
    ///
    /// # Errors
    /// - Failed to initialize the LRU cache.
    pub fn new() -> Result<Self, SbroadError> {
        let cache: LRUCache<SmolStr, (Statement, Vec<NodeId>)> =
            LRUCache::new(DEFAULT_CAPACITY, Some(Box::new(unprepare)))?;
        let result = StorageRuntime {
            metadata: Mutex::new(StorageConfiguration::new()),
            bucket_count: bucket_count()?,
            cache: Mutex::new(CartridgeCache(cache)),
        };

        Ok(result)
    }

    /// Executes provided plan.
    ///
    /// # Errors
    ///
    /// Will return `Err` if underlying DML/DQL implementation returns `Err`.
    pub fn execute_plan(
        &self,
        required: &mut RequiredData,
        mut raw_optional: OptionalBytes,
        cache_info: CacheInfo,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let res = match required.query_type {
            QueryType::DML => helpers::execute_dml(self, required, raw_optional.get_mut()?),
            QueryType::DQL => {
                let mut info = EncodedQueryInfo::new(raw_optional, required);
                match cache_info {
                    CacheInfo::CacheableFirstRequest => {
                        execute_first_cacheable_request(self, &mut info)
                    }
                    CacheInfo::CacheableSecondRequest => {
                        execute_second_cacheable_request(self, &mut info)
                    }
                }
            }
        };
        res
    }
}

#[allow(unused_variables)]
fn update_box_param<T>(param: &str, val: T)
where
    T: Display,
{
    let lua = tarantool::lua_state();
    match lua.exec(&format!("box.cfg{{{param} = {val}}}")) {
        Ok(()) => debug!(
            Option::from("update_box_param"),
            &format!("box.cfg param {param} was updated to {val}")
        ),
        Err(e) => warn!(
            Option::from("update_box_param"),
            &format!("box.cfg update error: {e}")
        ),
    }
}

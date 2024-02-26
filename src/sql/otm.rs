use std::cell::{OnceCell, RefCell};
use std::time::Duration;

use ahash::AHashMap;
use lazy_static::lazy_static;

use opentelemetry::sdk::export::trace::SpanData;
use opentelemetry::sdk::trace::{
    Config as SdkConfig, Sampler as SdkSampler, Span, SpanProcessor, Tracer as SdkTracer,
    TracerProvider as SdkTracerProvider,
};
use opentelemetry::trace::{SpanId, TracerProvider};
use opentelemetry::Context;
use sbroad::errors::SbroadError;
use sbroad::executor::lru::{Cache, LRUCache};
use serde::{Deserialize, Serialize};
use tarantool::define_str_enum;
use tarantool::index::{Index, IteratorType};
use tarantool::session::su;
use tarantool::space::{space_id_temporary_min, Space, SpaceId, SpaceType};
use tarantool::tuple::{Encode, Tuple};

use crate::schema::ADMIN_ID;
use crate::tlog;

/// The percent of queries that are subject to tracing when
/// using `StatTracer`.
static STAT_TRACER_TRACED_QUERIES_PORTION: f64 = 0.01;

/// The number of unique tracked queries.
pub const STATISTICS_CAPACITY: usize = 100;
thread_local!(pub(super) static TRACKED_QUERIES: RefCell<TrackedQueries> = RefCell::new(TrackedQueries::new()));

lazy_static! {
    // We can't inline provider creation here: under the hood tracer delegates
    // its work to its provider, so provider must outlive each created tracer.
    static ref STATISTICS_PROVIDER: SdkTracerProvider = SdkTracerProvider::builder()
        .with_span_processor(StatCollector::new())
        .with_config(SdkConfig::default().with_sampler(SdkSampler::TraceIdRatioBased(STAT_TRACER_TRACED_QUERIES_PORTION)))
        .build();
    #[derive(Debug)]
    static ref STATISTICS_TRACER: SdkTracer = STATISTICS_PROVIDER.versioned_tracer("stat", None, None);
    /// Like statistic tracer but always create traces. Used only for testing purposes.
    static ref TEST_STATISTICS_PROVIDER: SdkTracerProvider = SdkTracerProvider::builder()
        .with_span_processor(StatCollector::new())
        .build();
    #[derive(Debug)]
    static ref TEST_STATISTICS_TRACER: SdkTracer = TEST_STATISTICS_PROVIDER.versioned_tracer("test_stat", None, None);
}

/// `SpanMap` hash table - stores the mapping between the span id and the span name.
/// The reason is that the span context contains only the span id, so we use
/// this table to save the span name when create the span (and remove it when
/// the span is finished).
type SpanMap = AHashMap<SpanId, String>;
thread_local!(pub static SPAN: RefCell<SpanMap> = RefCell::new(SpanMap::new()));

/// Statistics span processor.
///
/// This span processor is used to collect statistics about running queries.
/// It uses sampling (1% at the moment) to reduce the overhead of collecting
/// statistics. The results are written to `_sql_query` and `_sql_stat`
/// spaces and evicted by LRU strategy (more details in the `table` and
/// `eviction` modules).
///
/// The best way to inspect the statistics on any instance is to use local SQL.
/// For example, to get the top 5 most expensive SELECT SQL queries by the
/// average execution time:
/// ```sql
/// select distinct(q."query_text") from "_sql_stat" as s
/// join "_sql_query" as q
///     on s."query_id" = q."query_id"
/// where lower(q."query_text") like 'select%'
/// order by s."sum"/s."count" desc
/// limit 5
/// ```
///
/// Or to get the flame graph of the most expensive query:
/// ```sql
/// with recursive st as (
///     select * from "_sql_stat" where "query_id" in (select qt."query_id" from qt)
///         and "parent_span" = ''
///     union all
///     select s.* from "_sql_stat" as s, st on s."parent_span" = st."span"
///         and s."query_id" in (select qt."query_id" from qt)
/// ), qt as (
///     select s."query_id" from "_sql_stat" as s
///     join "_sql_query" as q
///         on s."query_id" = q."query_id"
///     order by s."sum"/s."count" desc
///     limit 1
/// )
/// select * from st;
/// ```
#[derive(Debug)]
pub struct StatCollector;

impl StatCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for StatCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl SpanProcessor for StatCollector {
    fn on_start(&self, span: &mut Span, _: &Context) {
        let _su = su(ADMIN_ID).expect("can't fail because session is available");

        let Some(span_data) = span.exported_data() else {
            return;
        };

        let id: String = match span_data.attributes.get(&"id".into()) {
            Some(id) => id.to_string(),
            None => return,
        };

        // We are processing a top level query span. Lets register it.
        if let Some(query_text) = span_data.attributes.get(&"query_sql".into()) {
            let sql_queries = &SqlStatTables::get_or_init().queries;
            let tuple = SqlQueryDef {
                query_id: id,
                query_text: query_text.to_string(),
                ref_counter: 2,
            };
            sql_queries.insert_or_increase_ref_count(tuple);
        }

        // Register current span mapping (span id: span name).
        SPAN.with(|span_table| {
            let key = span_data.span_context.span_id();
            let value = span_data.name.clone();
            span_table.borrow_mut().insert(key, value.to_string());
        });
    }

    fn on_end(&self, span: SpanData) {
        let _su = su(ADMIN_ID).expect("can't fail because session is available");

        let id: String = match span.attributes.get(&"id".into()) {
            Some(id) => id.to_string(),
            None => return,
        };

        let parent_span: String = if span.parent_span_id == SpanId::INVALID {
            String::new()
        } else {
            SPAN.with(|span_table| {
                let span_table = span_table.borrow();
                span_table
                    .get(&span.parent_span_id)
                    .map_or_else(String::new, |span_name| span_name.clone())
            })
        };

        let duration = match span.end_time.duration_since(span.start_time) {
            Ok(duration) => duration,
            // The clock may have gone backwards.
            Err(_) => Duration::from_secs(0),
        }
        .as_secs_f64();
        // Update statistics.
        let sql_stats = &SqlStatTables::get_or_init().stats;
        let stat_def = SqlStatDef {
            query_id: id.to_string(),
            span: String::from(span.name),
            parent_span,
            min: duration,
            max: duration,
            sum: duration,
            count: 1,
        };
        sql_stats.update_span(stat_def);

        // Remove current span id to name mapping.
        SPAN.with(|span_table| {
            span_table.borrow_mut().remove(&span.span_context.span_id());
        });

        // Unreference the query for the top level query span.
        // We don't want to remove the query while some other
        // fiber is still collecting statistics for it.
        if span.attributes.get(&"query_sql".into()).is_some() {
            let sql_queries = &SqlStatTables::get_or_init().queries;
            sql_queries.unref_or_delete(id.as_str());
        }

        // Evict old queries.
        TRACKED_QUERIES.with(|tracked_queries| {
            let mut tracked_queries = tracked_queries.borrow_mut();
            tracked_queries.push(id).unwrap();
        });
    }

    fn force_flush(&self) -> opentelemetry::trace::TraceResult<()> {
        Ok(())
    }

    fn shutdown(&mut self) -> opentelemetry::trace::TraceResult<()> {
        Ok(())
    }
}

define_str_enum! {
    #[derive(Default)]
    pub enum TracerKind {
        /// Always creates traces
        TestTracer = "test",
        /// Randomly creates traces only for 1% of the queries
        #[default]
        StatTracer = "stat",
    }
}

impl TracerKind {
    /// Currently our apis for sql execution take additional
    /// boolean parameter `traceable`.
    /// If set to `true`, we must create trace for the given
    /// query.
    #[inline]
    pub fn from_traceable(traceable: bool) -> Self {
        if traceable {
            TracerKind::TestTracer
        } else {
            TracerKind::StatTracer
        }
    }

    #[inline]
    pub fn get_tracer(&self) -> &'static SdkTracer {
        match self {
            TracerKind::TestTracer => &TEST_STATISTICS_TRACER,
            TracerKind::StatTracer => &STATISTICS_TRACER,
        }
    }
}

/// Currently tracked queries using LRU strategy
/// with threshold of 100 entries in the `_sql_query`
/// space.
pub struct TrackedQueries {
    queries: LRUCache<String, String>,
}

/// Callback when query is pushed out of tracked queries cache.
#[allow(clippy::ptr_arg)]
fn remove_query(query_id: &mut String) -> Result<(), SbroadError> {
    let stats = &SqlStatTables::get_or_init();
    stats.delete_query(query_id.as_str());
    Ok(())
}

impl Default for TrackedQueries {
    fn default() -> Self {
        Self::new()
    }
}

impl TrackedQueries {
    /// Create a new instance of `TrackedQueries`.
    ///
    /// # Panics
    /// - If the `STATISTICS_CAPACITY` is less than 1 (impossible at the moment).
    #[must_use]
    pub fn new() -> Self {
        Self {
            queries: LRUCache::new(STATISTICS_CAPACITY, Some(Box::new(remove_query))).unwrap(),
        }
    }

    /// Add a new query to the tracked queries.
    ///
    /// # Errors
    /// - Internal error in the eviction function.
    pub fn push(&mut self, key: String) -> Result<(), SbroadError> {
        self.queries.put(key, String::new())
    }
}

// Statistics tables.
//
// These are used by the `statistics` span processor. There are two spaces
// used to store the statistics:
// - `_sql_query` space - stores the queries that are currently being executed.
//   Its query id is used as a key for the `_sql_stat` space.
// - `_sql_stat` space - stores the statistics for the query spans. The spans
//   are stored as a flat tree for each query.
pub struct SqlStatTables {
    pub stats: SqlStats,
    pub queries: SqlQueries,
}

impl SqlStatTables {
    fn new() -> tarantool::Result<Self> {
        Ok(Self {
            stats: SqlStats::new()?,
            queries: SqlQueries::new()?,
        })
    }

    pub fn get_or_init() -> &'static Self {
        static mut STORAGE: OnceCell<SqlStatTables> = OnceCell::new();

        unsafe {
            STORAGE.get_or_init(|| {
                Self::new().expect("initialization of stat tables should never fail")
            })
        }
    }

    pub fn delete_query(&self, query_id: &str) {
        if self.queries.unref_or_delete(query_id).is_some() {
            self.stats.delete_query(query_id)
        } else {
            tlog!(
                Error,
                "failed to delete query with id: {query_id}: more than 1 ref"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SqlStats
////////////////////////////////////////////////////////////////////////////////

pub struct SqlStats {
    space: Space,
    index: Index,
}

impl SqlStats {
    const TABLE_NAME: &'static str = "_sql_stat";

    /// Must be called from tx thread
    pub fn id() -> SpaceId {
        static mut TABLE_ID: std::cell::OnceCell<SpaceId> = std::cell::OnceCell::new();

        unsafe {
            *TABLE_ID.get_or_init(|| {
                space_id_temporary_min().expect("getting space id from tx thread shouldn't fail")
            })
        }
    }

    /// Must be called from tx thread
    fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::id())
            .space_type(SpaceType::Temporary)
            .format(SqlStatDef::format())
            .if_not_exists(true)
            .create()?;

        let index = space
            .index_builder("primary")
            .unique(true)
            .part("query_id")
            .part("span")
            .part("parent_span")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, index })
    }

    fn get(&self, query_id: &str, span: &str, parent_span: &str) -> Option<Tuple> {
        match self.index.get(&[query_id, span, parent_span]) {
            Err(e) => {
                tlog!(Error, "failed to get tuple from {}: {e}", Self::TABLE_NAME;);
                None
            }
            Ok(ret) => ret,
        }
    }

    fn update_span(&self, stat: SqlStatDef) {
        fn update_span_impl(stats: &SqlStats, stat: SqlStatDef) -> tarantool::Result<()> {
            match stats.get(&stat.query_id, &stat.span, &stat.parent_span) {
                None => {
                    stats.space.insert(&stat)?;
                }
                Some(tuple) => {
                    let mut decoded = tuple.decode::<SqlStatDef>()?;
                    decoded.min = stat.min.min(decoded.min);
                    decoded.max = stat.max.max(decoded.max);
                    decoded.sum += stat.sum;
                    decoded.count += stat.count;
                    stats.space.replace(&decoded)?;
                }
            }
            Ok(())
        }

        if let Err(e) = update_span_impl(self, stat) {
            tlog!(
                Error,
                "{}: failed to update span stats: {e}",
                Self::TABLE_NAME
            );
        }
    }

    fn delete_query(&self, query_id: &str) {
        fn delete_query_impl(stats: &SqlStats, query_id: &str) -> tarantool::Result<()> {
            let index_scan = stats.space.select(IteratorType::Eq, &[query_id])?;
            let to_delete = index_scan
                .map(|tuple| {
                    tuple
                        .decode::<SqlStatDef>()
                        .map(|tuple| (tuple.query_id, tuple.span, tuple.parent_span))
                })
                .collect::<tarantool::Result<Vec<_>>>()?;

            for key in to_delete {
                stats.space.delete(&key)?;
            }
            Ok(())
        }

        if let Err(e) = delete_query_impl(self, query_id) {
            tlog!(
                Error,
                "{}: failed to delete query with id: {query_id}: {e}",
                Self::TABLE_NAME
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SqlStatDef
////////////////////////////////////////////////////////////////////////////////

/// Tuple definition of sql statistics table,
/// which stores span execution stats.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqlStatDef {
    // Currently it is a hash computed from sql text, if not
    // provided explicitly, see: `query_id` function.
    pub query_id: String,
    // Name of the span, e.g: ast.resolve
    pub span: String,
    // Name of the parent span, e.g: api.router.dispatch
    pub parent_span: String,
    // Minimum execution time of this span
    pub min: f64,
    // Maximum execution time of this span
    pub max: f64,
    // Total execution time of this span
    pub sum: f64,
    // Number of times this span was executed
    pub count: usize,
}

impl Encode for SqlStatDef {}

impl SqlStatDef {
    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::string("query_id"),
            Field::string("span"),
            Field::string("parent_span"),
            Field::double("min"),
            Field::double("max"),
            Field::double("sum"),
            Field::unsigned("count"),
        ]
    }
}

////////////////////////////////////////////////////////////////////////////////
// SqlQueries
////////////////////////////////////////////////////////////////////////////////

pub struct SqlQueries {
    space: Space,
    index: Index,
}

impl SqlQueries {
    pub const TABLE_NAME: &'static str = "_sql_query";

    // Must be called from tx thread
    pub fn id() -> SpaceId {
        static mut TABLE_ID: std::cell::OnceCell<SpaceId> = std::cell::OnceCell::new();

        unsafe {
            *TABLE_ID.get_or_init(|| {
                space_id_temporary_min()
                    .expect("getting space id from tx thread shouldn't fail")
                    .saturating_add(1)
            })
        }
    }

    // Must be called from tx thread
    fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::id())
            .space_type(SpaceType::Temporary)
            .format(SqlQueryDef::format())
            .if_not_exists(true)
            .create()?;

        let index = space
            .index_builder("query_id")
            .unique(true)
            .part("query_id")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, index })
    }

    fn insert_or_increase_ref_count(&self, tuple: SqlQueryDef) {
        if let Err(_e) = self
            .space
            .upsert(&tuple, [("+", SqlQueryDef::FIELD_REF_COUNTER, 1)])
        {
            tlog!(Error, "Space {} upsert error: {}", Self::TABLE_NAME, _e;);
        }
    }

    fn unref_or_delete(&self, query_id: &str) -> Option<SqlQueryDef> {
        fn unref_or_delete_impl(
            queries: &SqlQueries,
            query_id: &str,
        ) -> tarantool::Result<Option<SqlQueryDef>> {
            let Some(tuple) = queries.index.get(&[query_id])? else {
                return Ok(None);
            };

            queries
                .space
                .upsert(&tuple, [("-", SqlQueryDef::FIELD_REF_COUNTER, 1)])?;

            let query_def = tuple.decode::<SqlQueryDef>()?;
            if query_def.ref_counter == 0 {
                queries.space.delete(&[query_id])?;
                return Ok(None);
            }

            Ok(Some(query_def))
        }

        match unref_or_delete_impl(self, query_id) {
            Ok(res) => res,
            Err(e) => {
                tlog!(Error, "{}: unref error: {e}", Self::TABLE_NAME);
                None
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SqlQueryDef
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqlQueryDef {
    pub query_id: String,
    pub query_text: String,
    pub ref_counter: u64,
}

impl Encode for SqlQueryDef {}

impl SqlQueryDef {
    /// Index (0-based) of field "ref_counter" in _sql_query table format.
    pub const FIELD_REF_COUNTER: usize = 2;

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::string("query_id"),
            Field::string("query_text"),
            Field::unsigned("ref_counter"),
        ]
    }
}

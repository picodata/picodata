use crate::sql::port::PicoPortOwned;
use crate::sql::router::{get_current_tier_replicasets_num, replicasets_by_buckets, RouterRuntime};
use crate::vdbe::explain::RawExplainProvider;
use smol_str::{format_smolstr, ToSmolStr};
use sql::errors::{Action, Entity, SbroadError};
use sql::executor::engine::{BlockExecData, BlockQuery, BlockRuntimeHook};
use sql::executor::vdbe::{SqlError, SqlStmt};
use sql::executor::ExecutingQuery;
use sql::executor::Port;
use sql::explain::buckets::BoundedBuckets;
use sql::explain::executor::{
    decode_vdbe_plan, ExplainQueryLocation, MotionInfo, QueryEntry, RawExplainEntry,
};
use sql::explain::ExplainExecutingQuery;
use sql::ir::bucket::{BucketSet, Buckets};
use sql::ir::node::BlockEntries;
use sql::ir::value::Value;
use sql::ir::ExplainOptions;
use std::vec::IntoIter;
use tarantool::msgpack::encode;

/// Create an instance of `ExplainQueryLocation` from `Buckets` and `MotionInfo`.
fn build_explain_query_location(
    buckets: &Buckets,
    motion_info: &MotionInfo,
) -> ExplainQueryLocation {
    let is_dyn_filtered = motion_info.has_segment_motion;

    if let Some(as_empty) = motion_info.has_serialize_as_empty_opcode {
        return match buckets {
            Buckets::Any => ExplainQueryLocation::Router,
            Buckets::Filtered(BucketSet::Exact(set)) if set.is_empty() => {
                ExplainQueryLocation::Router
            }
            Buckets::Filtered(_) | Buckets::All => {
                let replicasets_num = get_current_tier_replicasets_num();
                if !as_empty {
                    if is_dyn_filtered {
                        ExplainQueryLocation::DynFiltered {
                            fraction: Some((1, replicasets_num)),
                        }
                    } else {
                        ExplainQueryLocation::ConstFiltered {
                            fraction: (1, replicasets_num),
                        }
                    }
                } else {
                    ExplainQueryLocation::ConstFiltered {
                        fraction: (replicasets_num - 1, replicasets_num),
                    }
                }
            }
        };
    }

    match buckets {
        Buckets::Any => ExplainQueryLocation::Router,
        Buckets::Filtered(BucketSet::Exact(set)) if set.is_empty() && is_dyn_filtered => {
            ExplainQueryLocation::DynFiltered { fraction: None }
        }
        Buckets::Filtered(BucketSet::Exact(set)) if set.is_empty() => ExplainQueryLocation::Router,
        Buckets::Filtered(_) => {
            let (replicaset_count, all_replicasets) = match replicasets_by_buckets(buckets) {
                Ok(replicasets) => {
                    let all_replicasets = get_current_tier_replicasets_num();
                    (replicasets.len(), all_replicasets)
                }
                Err(_) => {
                    // Defaults to (0, 0) when buckets routing is unavailable in vshard.
                    (0, 0)
                }
            };
            if is_dyn_filtered {
                ExplainQueryLocation::DynFiltered {
                    fraction: Some((replicaset_count, all_replicasets)),
                }
            } else {
                ExplainQueryLocation::ConstFiltered {
                    fraction: (replicaset_count, all_replicasets),
                }
            }
        }
        Buckets::All if is_dyn_filtered => ExplainQueryLocation::DynFiltered { fraction: None },
        Buckets::All => ExplainQueryLocation::Whole,
    }
}

pub fn explain_query(query: ExecutingQuery<'_, RouterRuntime>) -> Result<String, SbroadError> {
    let explain_options = query.get_exec_plan().get_ir_plan().explain_options;

    let mut explain = Vec::new();
    let mut explain_query = ExplainExecutingQuery::from(query);

    if explain_options.contains(ExplainOptions::Logical) {
        let logical = explain_query.explain_logical(build_explain_query_location)?;
        explain.push(logical);
    }

    let buckets_explain = if explain_options.contains(ExplainOptions::Buckets) {
        Some(explain_query.explain_buckets()?)
    } else {
        None
    };

    let forward_explain = if explain_options.contains(ExplainOptions::Forward) {
        Some(explain_query.explain_forward()?)
    } else {
        None
    };

    if explain_options.contains(ExplainOptions::Raw) {
        let mut port = PicoPortOwned::new();
        explain_query.dispatch(&mut port)?;

        let raw_explain = explain_query.explain_raw(&mut port)?;
        if !raw_explain.is_empty() {
            explain.push(raw_explain);
        }
    }

    if let Some(forward) = forward_explain {
        explain.push(forward);
    }

    if let Some(buckets) = buckets_explain {
        explain.push(buckets);
    }

    if explain_options.contains(ExplainOptions::Context) {
        let context = explain_query.explain_context()?;
        explain.push(context);
    }

    // Each entry in `explain` is a plain line without a trailing '\n'.
    // This is intentional: a trailing newline would produce extra blank lines
    // at the end of psql output. Since the entries themselves have no newline,
    // we join them with "\n\n" to separate each entry with a blank line.
    let final_explain = explain.join("\n\n");

    Ok(final_explain)
}

pub(crate) fn block_compile_error(error: SqlError) -> SbroadError {
    match error {
        SqlError::OutdatedStorageSchema => SbroadError::OutdatedStorageSchema,
        error => SbroadError::FailedTo(Action::Build, Some(Entity::Query), error.to_smolstr()),
    }
}

fn explain_block_hook_rows(query: &BlockQuery) -> impl Iterator<Item = &str> {
    query.hooks.iter().filter_map(|hook| match hook {
        BlockRuntimeHook::IdxInsertOnConflictDoUpdate {
            raw_explain_detail, ..
        } => raw_explain_detail.as_deref(),
    })
}

pub fn explain_execute_block<'p>(
    block: BlockExecData,
    buckets: &Buckets,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError> {
    let BlockExecData {
        statements,
        params,
        bucket_count,
        ..
    } = block;
    let params = &mut params.into_iter();

    let bucket_info = BoundedBuckets {
        buckets: buckets.clone(),
        bucket_count,
        is_upper_bound: false,
    };
    let motion_info = MotionInfo::new_for_transaction();

    let explain_one = |explain_query: ExplainQuery,
                       query: &BlockQuery,
                       params: &[Value]|
     -> Result<QueryEntry, SbroadError> {
        let raw_plan_hook_details = explain_block_hook_rows(query);
        explain_query.execute_guarded(params, &bucket_info, motion_info, raw_plan_hook_details)
    };

    let next_params = |params: &mut IntoIter<_>| params.next().expect("not enough params");
    // One entry per query, in execution order -- the same order `params` is
    // indexed by, and the order the router expects when it names the stages.
    for entry in BlockEntries::new(&statements) {
        let query = entry.query;
        let explain_query = ExplainQuery::new(&query.pattern);
        let entry = explain_one(explain_query, query, &next_params(params))?;
        append_query_entry_to_port(RawExplainEntry::Single(entry), port)?;
    }

    Ok(())
}

pub fn append_query_entry_to_port<'p>(
    entry: RawExplainEntry,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError> {
    let query_entry_json = serde_json::to_string(&entry).map_err(|err| {
        SbroadError::Other(format_smolstr!(
            "unable to serialize QueryEntry to JSON: {err}"
        ))
    })?;

    let mp_json = encode(&[query_entry_json]);
    port.add_mp(&mp_json);

    Ok(())
}

/// Contains the SQL query that is executed in VDBE.
pub struct ExplainQuery<'a> {
    sql: &'a str,
}

impl<'a> ExplainQuery<'a> {
    #[must_use]
    pub fn new(sql: &'a str) -> Self {
        Self { sql }
    }

    /// Execute explain query in VDBE and append result to port.
    ///
    /// # Preconditions
    ///
    /// - All temporary tables that are present in query
    ///   must be created before calling that function.
    pub fn execute_guarded(
        self,
        params: &[Value],
        bucket_info: &BoundedBuckets,
        motion_info: MotionInfo,
        raw_plan_hook_details: impl IntoIterator<Item: AsRef<str>>,
    ) -> Result<QueryEntry, SbroadError> {
        let location = build_explain_query_location(&bucket_info.buckets, &motion_info);
        let sql_query = self.sql;

        let raw_explain_hook_err = |e: String| {
            SbroadError::FailedTo(
                Action::Create,
                Some(Entity::Explain),
                format_smolstr!("raw explain hook: {e}"),
            )
        };
        let raw_explain_provider =
            RawExplainProvider::new(raw_plan_hook_details).map_err(raw_explain_hook_err)?;
        let compile_result = match raw_explain_provider {
            Some(mut provider) => {
                let result = provider.compile(sql_query);
                match result {
                    Ok(mut stmt) => {
                        provider.finish().map_err(raw_explain_hook_err)?;
                        stmt.add_owned_payload(provider);
                        Ok(stmt)
                    }
                    Err(err) => Err(err),
                }
            }
            None => SqlStmt::compile(sql_query),
        };

        let tuples = match compile_result {
            Ok(mut stmt) => {
                let mut tmp_port = PicoPortOwned::new();
                // `0` is passed since it should always be possible to execute
                // EXPLAIN(RAW).
                tmp_port.process_stmt(&mut stmt, params, 0)?;

                let tuples = decode_vdbe_plan(&tmp_port)?;
                Ok(tuples)
            }
            Err(err) => Err(err.to_string()),
        };

        let query_entry = QueryEntry::new(
            sql_query.to_string(),
            location,
            bucket_info.clone(),
            params.iter().map(ToString::to_string).collect(),
            tuples,
        );

        Ok(query_entry)
    }
}

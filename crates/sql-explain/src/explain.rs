use crate::explain::buckets::{bounded_buckets_from_query, get_block_statement_buckets};
use crate::explain::executor::BlockStageHeader;
use crate::explain::executor::MotionInfo;
use crate::explain::ir::LogicalExplain;
use crate::explain::utils::format_no_sql;
use crate::explain::utils::format_sql;
use crate::explain::utils::indent;
use crate::{write_explain_header1, write_explain_header2};
use smol_str::ToSmolStr as _;
use sql_executor::executor::engine::helpers::{
    generate_pattern_with_params_for_block, BlockRenderOptions,
};
use sql_executor::executor::engine::{BlockQuery, Router, Vshard as _};
use sql_executor::executor::ExecutingQuery;
use sql_executor::executor::Port;
use sql_ir::errors::SbroadError;
use sql_ir::ir::bucket::Buckets;
use sql_ir::ir::node::block::{Block, BlockOwned};
use sql_ir::ir::node::{AnonymousBlock, BlockEntries};
use sql_ir::ir::options::OptionKind;
use sql_ir::ir::transformation::BlockExecutionRestriction;
use sql_ir::ir::PlanKind;
use sql_ir::ir::{value::Value, ExplainOptions, Plan};
use std::fmt::Write as _;

use crate::explain::executor::{ExplainQueryLocation, RawExplain, RawExplainOptions};
use crate::explain::utils::{format_block_stage_label, format_block_stage_number};

pub mod buckets;
pub mod executor;
pub mod ir;
#[cfg(feature = "mock")]
pub mod mock;
#[cfg(test)]
pub mod tests;
pub mod utils;

/// Logical explain of a bare [`Plan`], without execution context.
///
/// # Errors
/// - Failed to get top node
/// - Failed to build explain
/// - `explain` is not specified in query
pub fn explain_logical(plan: &Plan) -> Result<String, SbroadError> {
    if plan.explain_options != ExplainOptions::Logical {
        return Err(SbroadError::Other(
            "LOGICAL mode for EXPLAIN is not specified in query".to_smolstr(),
        ));
    }

    let top_id = plan.get_top()?;
    let explain = LogicalExplain::new(plan, top_id)?;

    Ok(explain.to_string())
}

/// The local SQL and the parameters of a block statement. If it is not
/// possible to make the local SQL, EXPLAIN shows the reason in its place.
pub type BlockStatementSql = Result<(BlockQuery, Vec<Value>), BlockExecutionRestriction>;

/// The local SQL and routing data of a block statement.
pub struct BlockStatementExplainData {
    pub sql: BlockStatementSql,
    pub buckets: Buckets,
    pub motion_info: MotionInfo,
}

/// The data for the RAW EXPLAIN of a transactional block.
pub struct BlockExplainData {
    /// Statements in `BlockEntries` order.
    pub statements: Vec<BlockStatementExplainData>,
    pub explain_options: ExplainOptions,
    pub bucket_count: u64,
}

pub struct ExplainExecutingQuery<'a, C> {
    inner: ExecutingQuery<'a, C>,
}

impl<'a, C> From<ExecutingQuery<'a, C>> for ExplainExecutingQuery<'a, C> {
    fn from(value: ExecutingQuery<'a, C>) -> Self {
        ExplainExecutingQuery { inner: value }
    }
}

impl<'a, C> ExplainExecutingQuery<'a, C>
where
    C: Router,
{
    pub fn is_block(&self) -> Result<bool, SbroadError> {
        self.inner.top_is(PlanKind::BLOCK)
    }

    /// Writes the RAW EXPLAIN entries of a query to `port`.
    ///
    /// # Errors
    /// - The query is a transactional block. For a block, use
    ///   [`Self::block_explain_data`].
    pub fn dispatch<'p>(&mut self, port: &mut impl Port<'p>) -> Result<(), SbroadError> {
        if self.is_block()? {
            return Err(SbroadError::Other(
                "transactional blocks are explained via block_explain_data".into(),
            ));
        }

        self.inner.dispatch(port)
    }

    /// Makes the RAW EXPLAIN data of a transactional block. The data shows each
    /// statement at the location where the statement runs. EXPLAIN accepts a
    /// block with statements on different buckets, but such a block cannot run.
    pub fn block_explain_data(&mut self) -> Result<BlockExplainData, SbroadError> {
        let plan = self.inner.get_exec_plan().get_ir_plan();
        let top_id = plan.get_top()?;
        let block = plan.get_owned_block_node(top_id)?;
        let BlockOwned::Anonymous(block) = block else {
            unreachable!("plan.is_block() returned true, but top is {block:?}")
        };
        let explain_options = plan.explain_options;
        // Use the tier of the plan. The tier of this node can be different.
        let tier = match plan.tier.as_ref() {
            None => self.inner.get_coordinator().get_current_tier_name()?,
            tier => tier.cloned(),
        };
        let bucket_count = self
            .inner
            .get_coordinator()
            .get_vshard_object_by_tier(tier.as_ref())?
            .bucket_count();

        let statements = self.generate_block_explain_statements(&block)?;

        Ok(BlockExplainData {
            statements,
            explain_options,
            bucket_count,
        })
    }

    /// Render the logical plan. `build_location` names where a query runs
    /// (router / storage / replicasets fraction); the embedding side owns
    /// that knowledge, since it depends on the cluster topology.
    pub fn explain_logical(
        &mut self,
        build_location: impl Fn(&BlockStatementExplainData) -> ExplainQueryLocation,
    ) -> Result<String, SbroadError> {
        let mut buf = String::new();
        let exec_query = &self.inner;
        let explain_options = exec_query.get_exec_plan().get_ir_plan().explain_options;
        if !explain_options.has_single_facet() {
            write_explain_header1!(&mut buf, "# Logical plan").unwrap();
            writeln!(&mut buf).unwrap();
        }

        if exec_query.top_is(PlanKind::BLOCK)? {
            let exec_plan = exec_query.get_exec_plan();
            let top_id = exec_plan.get_ir_plan().get_top()?;
            let block = exec_plan.get_ir_plan().get_owned_block_node(top_id)?;
            let BlockOwned::Anonymous(block) = block else {
                unreachable!("exec_query.is_block() returned true, but top is {block:?}")
            };

            // Write the bucket_id cells of INSERT rows before the logical plan is made.
            let block_explain = self.block_explain_data()?;

            let exec_query = &self.inner;
            let plan = exec_query.get_exec_plan().get_ir_plan();
            let explain_options = plan.explain_options;
            let should_fmt = explain_options.contains(ExplainOptions::Fmt);

            let mut entries = BlockEntries::new(&block.statements)
                .zip(&block_explain.statements)
                .peekable();
            while let Some((entry, statement)) = entries.next() {
                let number = format_block_stage_number(&entry.location);
                let stage = format_block_stage_label(&entry.location);

                let source = build_location(statement);
                write_explain_header2!(&mut buf, "{number} {stage} ({source})").unwrap();
                writeln!(&mut buf).unwrap();

                let sql = match &statement.sql {
                    Ok((query, params)) => format_sql(&query.pattern, params, should_fmt),
                    Err(restriction) => format_no_sql(restriction),
                };
                write!(&mut buf, "{sql}\n\n").unwrap();

                let logical = entry.with(|query_id| LogicalExplain::new(plan, *query_id))?;
                write!(&mut buf, "{logical}").unwrap();

                if entries.peek().is_some() {
                    write!(&mut buf, "\n\n").unwrap();
                }
            }
        } else {
            let plan = exec_query.get_exec_plan().get_ir_plan();
            let top_id = plan.get_top()?;
            let explain = LogicalExplain::new(plan, top_id)?;
            write!(&mut buf, "{explain}").unwrap();
        }

        Ok(buf)
    }

    pub fn explain_forward(&mut self) -> Result<String, SbroadError> {
        let bounded_buckets = bounded_buckets_from_query(&mut self.inner)?;
        let exec_query = &self.inner;
        let coord = exec_query.get_coordinator();
        let forward = coord.get_possible_forward_option(&bounded_buckets.buckets, &mut None)?;

        let mut buf = String::new();
        let explain_options = exec_query.get_exec_plan().get_ir_plan().explain_options;
        if !explain_options.has_single_facet() {
            write_explain_header1!(&mut buf, "# Forward").unwrap();
            writeln!(&mut buf).unwrap();
        }
        writeln!(&mut buf, "forward analysis (on > ro_to_rw > off):").unwrap();
        write!(indent(&mut buf), "forward = {forward}").unwrap();

        Ok(buf)
    }

    pub fn explain_raw<'p>(&mut self, port: &mut impl Port<'p>) -> Result<String, SbroadError> {
        let ir_plan = self.inner.get_exec_plan().get_ir_plan();

        let explain_options = ir_plan.explain_options;
        let mut format_options = RawExplainOptions::empty();
        if explain_options.contains(ExplainOptions::Fmt) {
            format_options.insert(RawExplainOptions::Fmt);
        }

        let top_id = ir_plan.get_top()?;
        let maybe_block_stages = ir_plan
            .get_block_node(top_id)
            .and_then(|block| match block {
                Block::CallProcedure(_) => Ok(vec![]),
                Block::Anonymous(block) => BlockStageHeader::from_anon_block(block),
            })
            .ok();

        if explain_options.contains(ExplainOptions::Buckets) {
            format_options.insert(RawExplainOptions::ShowBuckets);
        }

        // The storage emits one port entry per block statement but knows
        // nothing of the block's shape, so the headers are named here, where
        // the plan still is.
        let stages = maybe_block_stages.unwrap_or_default();
        let raw_explain = RawExplain::from_port(port, format_options, stages)?;
        let mut buf = String::new();
        if !explain_options.has_single_facet() {
            write_explain_header1!(&mut buf, "# Raw plan").unwrap();
            writeln!(&mut buf).unwrap();
        }
        write!(&mut buf, "{raw_explain}").unwrap();

        Ok(buf)
    }

    pub fn explain_buckets(&mut self) -> Result<String, SbroadError> {
        let bounded_buckets = bounded_buckets_from_query(&mut self.inner)?;
        let mut buf = String::new();
        let explain_options = self.inner.get_exec_plan().get_ir_plan().explain_options;
        if !explain_options.has_single_facet() {
            write_explain_header1!(&mut buf, "# Buckets").unwrap();
            writeln!(&mut buf).unwrap();
        }

        write!(&mut buf, "{bounded_buckets}").unwrap();

        Ok(buf)
    }

    pub fn explain_context(&mut self) -> Result<String, SbroadError> {
        let mut buf = String::new();
        let exec_plan = self.inner.get_exec_plan();

        let explain_options = exec_plan.get_ir_plan().explain_options;
        if !explain_options.has_single_facet() {
            write_explain_header1!(&mut buf, "# Context").unwrap();
            writeln!(&mut buf).unwrap();
        }

        let plan = exec_plan.get_ir_plan();
        let opcode_max = plan.effective_options.sql_vdbe_opcode_max;
        let row_max = plan.effective_options.sql_motion_row_max;

        writeln!(&mut buf, "{} = {opcode_max}", OptionKind::VdbeOpcodeMax).unwrap();
        write!(&mut buf, "{} = {row_max}", OptionKind::MotionRowMax).unwrap();

        Ok(buf)
    }

    /// Makes the local SQL and routing data of each block statement. Each INSERT
    /// row uses its own bucket. If it is not possible to make the local SQL of a
    /// statement, the result contains the reason.
    fn generate_block_explain_statements(
        &mut self,
        block: &AnonymousBlock,
    ) -> Result<Vec<BlockStatementExplainData>, SbroadError> {
        let mut statements = Vec::with_capacity(block.statements.len());
        for entry in BlockEntries::new(&block.statements) {
            let buckets =
                entry.with(|query_id| get_block_statement_buckets(&mut self.inner, *query_id))?;
            let id = *entry.query;
            let exec_plan = self.inner.get_exec_plan();
            let motion_info = if exec_plan.get_ir_plan().subtree_has_motions(id)? {
                let has_segment_motion = exec_plan.has_segment_motion(id);
                MotionInfo::new_for_query(has_segment_motion, None)
            } else {
                MotionInfo::new_for_transaction()
            };
            let sql =
                if let Some(restriction) = exec_plan.get_ir_plan().validate_block_statement(id)? {
                    Err(restriction)
                } else {
                    Ok(generate_pattern_with_params_for_block(
                        exec_plan,
                        id,
                        BlockRenderOptions::EXPLAIN,
                    )?)
                };
            statements.push(BlockStatementExplainData {
                sql,
                buckets,
                motion_info,
            });
        }

        Ok(statements)
    }
}

use crate::explain::buckets::bounded_buckets_from_query;
use crate::explain::executor::BlockStageHeader;
use crate::explain::executor::MotionInfo;
use crate::explain::ir::LogicalExplain;
use crate::explain::utils::format_sql;
use crate::explain::utils::indent;
use crate::{write_explain_header1, write_explain_header2};
use smol_str::ToSmolStr as _;
use sql_executor::executor::engine::helpers::generate_pattern_with_params_for_block;
use sql_executor::executor::engine::{BlockQuery, Router};
use sql_executor::executor::ExecutingQuery;
use sql_executor::executor::Port;
use sql_ir::errors::SbroadError;
use sql_ir::ir::bucket::{BucketSet, Buckets};
use sql_ir::ir::node::block::{Block, BlockOwned};
use sql_ir::ir::node::{AnonymousBlock, BlockEntries, BlockStatement};
use sql_ir::ir::options::OptionKind;
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
    pub fn dispatch<'p>(&mut self, port: &mut impl Port<'p>) -> Result<(), SbroadError> {
        self.inner.dispatch(port)
    }

    /// Render the logical plan. `build_location` names where a query runs
    /// (router / storage / replicasets fraction); the embedding side owns
    /// that knowledge, since it depends on the cluster topology.
    pub fn explain_logical(
        &mut self,
        build_location: impl Fn(&Buckets, &MotionInfo) -> ExplainQueryLocation,
    ) -> Result<String, SbroadError> {
        let mut buf = String::new();
        let exec_query = &self.inner;
        let explain_options = exec_query.get_exec_plan().get_ir_plan().explain_options;
        if !explain_options.has_single_facet() {
            write_explain_header1!(&mut buf, "# Logical plan").unwrap();
            writeln!(&mut buf).unwrap();
        }

        if exec_query.is_block()? {
            let exec_plan = exec_query.get_exec_plan();
            let top_id = exec_plan.get_ir_plan().get_top()?;
            let block = exec_plan.get_ir_plan().get_owned_block_node(top_id)?;
            let BlockOwned::Anonymous(block) = block else {
                unreachable!("exec_query.is_block() returned true, but top is {block:?}")
            };

            let logical_explains = self.get_block_logical(&block)?;

            let mut_exec_query = &mut self.inner;
            let buckets = mut_exec_query.calculate_block_buckets(&block)?;
            let block_statements = self.generate_block_patterns(block, &buckets)?;

            let exec_query = &self.inner;
            let explain_options = exec_query.get_exec_plan().get_ir_plan().explain_options;
            let should_fmt = explain_options.contains(ExplainOptions::Fmt);

            // One stage per query, in execution order -- the same order
            // `logical_explains` and `unused_lets` are indexed by.
            let mut entries = BlockEntries::new(&block_statements).enumerate().peekable();
            while let Some((idx, entry)) = entries.next() {
                let number = format_block_stage_number(&entry.location);
                let stage = format_block_stage_label(&entry.location);

                let (query, params) = entry.query;
                let motion_info = MotionInfo::new_for_transaction();
                let source = build_location(&buckets, &motion_info);
                write_explain_header2!(&mut buf, "{number} {stage} ({source})").unwrap();
                writeln!(&mut buf).unwrap();

                let sql = format_sql(&query.pattern, params, should_fmt);
                write!(&mut buf, "{sql}\n\n").unwrap();

                write!(&mut buf, "{}", logical_explains[idx]).unwrap();

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

        let is_block = maybe_block_stages.is_some();
        if explain_options.contains(ExplainOptions::Buckets) && !is_block {
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

    fn get_block_logical(
        &self,
        block: &AnonymousBlock,
    ) -> Result<Vec<LogicalExplain>, SbroadError> {
        let mut explain = Vec::with_capacity(block.statements.len());
        let plan = self.inner.get_exec_plan().get_ir_plan();
        for entry in BlockEntries::new(&block.statements) {
            let explain_entry = entry.with(|query_id| LogicalExplain::new(plan, *query_id))?;
            explain.push(explain_entry);
        }

        Ok(explain)
    }

    #[allow(clippy::type_complexity)]
    fn generate_block_patterns(
        &self,
        block: AnonymousBlock,
        buckets: &Buckets,
    ) -> Result<Vec<BlockStatement<(BlockQuery, Vec<Value>)>>, SbroadError> {
        let block_bucket = match buckets {
            Buckets::Filtered(BucketSet::Exact(set)) => {
                assert!(set.len() == 1);
                set.iter().copied().next()
            }
            _ => None,
        };

        let exec_plan = self.inner.get_exec_plan();
        let mut statements = Vec::with_capacity(block.statements.len());
        for stmt in block.statements {
            statements.push(stmt.try_map(|id| {
                generate_pattern_with_params_for_block(exec_plan, id, block_bucket, false)
            })?);
        }

        Ok(statements)
    }
}

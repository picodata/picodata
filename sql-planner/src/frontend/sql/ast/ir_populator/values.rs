use pest::iterators::Pairs;

use crate::errors::SbroadError;
use crate::executor::engine::Metadata;
use crate::frontend::sql::ast::ParsingPairsMap;
use crate::frontend::sql::type_system::{self, get_parameter_derived_types, TypeAnalyzer};
use crate::ir::node::NodeId;
use crate::ir::types::UnrestrictedType;
use crate::ir::Plan;

use super::{parse_expr_no_type_check, ExpressionWalker};

pub(in crate::frontend::sql) fn parse_values_rows<M>(
    rows: &[usize],
    type_analyzer: &mut TypeAnalyzer,
    desired_types: &[UnrestrictedType],
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<Vec<NodeId>, SbroadError>
where
    M: Metadata,
{
    let param_types = get_parameter_derived_types(type_analyzer);
    let mut values_rows_ids: Vec<NodeId> = Vec::with_capacity(rows.len());
    for ast_row_id in rows {
        let row_pair = pairs_map.remove_pair(*ast_row_id);
        // Don't check types until we have all rows.
        // Consider the following queries:
        //  - `VALUES (1), ('text')`: both rows are fine, but their types cannot be matched
        //  - `VALUES ($1), (1)`: to infer parameter type in the 1st row we need the 2nd row
        let expr_id = parse_expr_no_type_check(
            Pairs::single(row_pair),
            &param_types,
            &[],
            worker,
            plan,
            true,
        )?;
        let values_row_id = plan.add_values_row(expr_id)?;
        plan.fix_subquery_rows(worker, values_row_id)?;
        values_rows_ids.push(values_row_id);
    }

    type_system::analyze_values_rows(
        type_analyzer,
        &values_rows_ids,
        desired_types,
        plan,
        &worker.subquery_replaces,
    )?;

    Ok(values_rows_ids)
}

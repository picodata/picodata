use pest::iterators::{Pair, Pairs};
use smol_str::format_smolstr;

use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ast::{ParsingPairsMap, Rule};
use crate::frontend::sql::type_system::TypeAnalyzer;
use crate::ir::metadata::Metadata;
use crate::ir::node::expression::Expression;
use crate::ir::node::{NodeId, Parameter};
use crate::ir::options::OptionParamValue;
use crate::ir::types::{DerivedType, UnrestrictedType};
use crate::ir::Plan;

use super::{parse_scalar_expr, ExpressionWalker};

// The same limit as in PostgreSQL (http://postgresql.org/docs/16/limits.html)
pub(in crate::frontend::sql) const MAX_PARAMETER_INDEX: usize = 65535;

pub(in crate::frontend::sql) fn parse_parameter_for_option<M: Metadata>(
    type_analyzer: &mut TypeAnalyzer,
    option_node_id: usize,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
    ty: UnrestrictedType,
) -> Result<OptionParamValue, SbroadError> {
    let plan_id = parse_scalar_expr(
        Pairs::single(pairs_map.remove_pair(option_node_id)),
        type_analyzer,
        DerivedType::new(ty),
        &[],
        worker,
        plan,
        true,
    )?;

    let Expression::Parameter(&Parameter { index, .. }) = plan.get_expression_node(plan_id)? else {
        panic!("Expected Parameter expression under Parameter node");
    };

    Ok(OptionParamValue::Parameter {
        index: (index - 1) as usize,
    })
}

fn get_param_index(
    pair: Pair<'_, Rule>,
    tnt_parameters_positions: &[Pair<'_, Rule>],
) -> Result<usize, SbroadError> {
    let param_pair = pair
        .clone()
        .into_inner()
        .next()
        .expect("Concrete param expected under Parameter node");
    match param_pair.as_rule() {
        Rule::TntParameter => {
            let pos = tnt_parameters_positions
                .iter()
                .position(|p| p == &param_pair)
                .expect("Couldn't find tnt parameter pair in a vec of tnt parameters pairs");
            Ok(pos + 1)
        }
        Rule::PgParameter => {
            let inner_unsigned = param_pair
                .into_inner()
                .next()
                .expect("Unsigned not found under PgParameter");
            let value_idx = inner_unsigned.as_str().parse::<usize>().map_err(|_| {
                SbroadError::Invalid(
                    Entity::Query,
                    Some(format_smolstr!(
                        "{inner_unsigned} cannot be parsed as unsigned param number"
                    )),
                )
            })?;
            if value_idx == 0 {
                return Err(SbroadError::Invalid(
                    Entity::Query,
                    Some("$n parameters are indexed from 1!".into()),
                ));
            }
            Ok(value_idx)
        }
        _ => unreachable!("Unexpected Rule met under Parameter"),
    }
}

pub(in crate::frontend::sql) fn parse_param<M: Metadata>(
    pair: Pair<'_, Rule>,
    param_types: &[DerivedType],
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<NodeId, SbroadError> {
    parse_param_with_positions(pair, param_types, &worker.tnt_parameters_positions, plan)
}

pub(in crate::frontend::sql) fn parse_param_with_positions(
    pair: Pair<'_, Rule>,
    param_types: &[DerivedType],
    tnt_parameters_positions: &[Pair<'_, Rule>],
    plan: &mut Plan,
) -> Result<NodeId, SbroadError> {
    let index = get_param_index(pair, tnt_parameters_positions)?;
    if index > MAX_PARAMETER_INDEX {
        return Err(SbroadError::Other(format_smolstr!(
            "parameter index {} is too big (max: {})",
            index,
            MAX_PARAMETER_INDEX
        )));
    }

    let ty = param_types.get(index - 1);
    let ty = ty.cloned().unwrap_or(DerivedType::unknown());
    Ok(plan.add_param(index.try_into().expect("invalid parameter idnex"), ty))
}

use pest::iterators::Pairs;
use pest::pratt_parser::PrattParser;

use crate::errors::SbroadError;
use crate::frontend::sql::ast::{Rule, SelectChildPairTranslation};
use crate::frontend::sql::ir::Translation;
use crate::ir::node::NodeId;
use crate::ir::Plan;

lazy_static::lazy_static! {
    static ref SELECT_PRATT_PARSER: PrattParser<Rule> = {
        use pest::pratt_parser::{Assoc::Left, Op};
        use Rule::{UnionOp, UnionAllOp, ExceptOp};

        PrattParser::new()
            .op(
                Op::infix(UnionOp, Left)
            | Op::infix(UnionAllOp, Left)
            | Op::infix(ExceptOp, Left)
        )
    };
}

#[derive(Clone)]
enum SelectOp {
    Union,
    UnionAll,
    Except,
}

/// Helper struct denoting any combination of
/// * SELECT
/// * UNION (ALL)
/// * EXCEPT
#[derive(Clone)]
enum SelectSet {
    PlanId {
        plan_id: NodeId,
    },
    Infix {
        op: SelectOp,
        left: Box<SelectSet>,
        right: Box<SelectSet>,
    },
}

impl SelectSet {
    fn populate_plan(&self, plan: &mut Plan) -> Result<NodeId, SbroadError> {
        match self {
            SelectSet::PlanId { plan_id } => Ok(*plan_id),
            SelectSet::Infix { op, left, right } => {
                let left_id = left.populate_plan(plan)?;
                let right_id = right.populate_plan(plan)?;
                match op {
                    u @ (SelectOp::Union | SelectOp::UnionAll) => {
                        let remove_duplicates = matches!(u, SelectOp::Union);
                        plan.add_union(left_id, right_id, remove_duplicates)
                    }
                    SelectOp::Except => plan.add_except(left_id, right_id),
                }
            }
        }
    }
}

fn parse_select_set_pratt(
    select_pairs: Pairs<Rule>,
    pos_to_plan_id: &SelectChildPairTranslation,
    ast_to_plan: &Translation,
) -> Result<SelectSet, SbroadError> {
    SELECT_PRATT_PARSER
        .map_primary(|primary| {
            let select_set = match primary.as_rule() {
                Rule::Select => {
                    let mut pairs = primary.into_inner();
                    let mut select_child_pair =
                        pairs.next().expect("select must have at least one child");
                    assert!(matches!(select_child_pair.as_rule(), Rule::Projection));
                    for pair in pairs {
                        if let Rule::OrderBy = pair.as_rule() {
                            select_child_pair = pair;
                            break;
                        }
                    }
                    let ast_id = pos_to_plan_id.get(&select_child_pair.line_col()).unwrap();
                    let id = ast_to_plan.get(*ast_id)?;
                    SelectSet::PlanId { plan_id: id }
                }
                Rule::SelectWithOptionalContinuation => {
                    parse_select_set_pratt(primary.into_inner(), pos_to_plan_id, ast_to_plan)?
                }
                rule => unreachable!("Select::parse expected atomic rule, found {:?}", rule),
            };
            Ok(select_set)
        })
        .map_infix(|lhs, op, rhs| {
            let op = match op.as_rule() {
                Rule::UnionOp => SelectOp::Union,
                Rule::UnionAllOp => SelectOp::UnionAll,
                Rule::ExceptOp => SelectOp::Except,
                rule => unreachable!("Expr::parse expected infix operation, found {:?}", rule),
            };
            Ok(SelectSet::Infix {
                op,
                left: Box::new(lhs?),
                right: Box::new(rhs?),
            })
        })
        .parse(select_pairs)
}

pub(in crate::frontend::sql) fn parse_select(
    select_pairs: Pairs<Rule>,
    pos_to_ast_id: &SelectChildPairTranslation,
    ast_to_plan: &Translation,
    plan: &mut Plan,
) -> Result<NodeId, SbroadError> {
    let select_set = parse_select_set_pratt(select_pairs, pos_to_ast_id, ast_to_plan)?;
    select_set.populate_plan(plan)
}

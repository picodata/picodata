use crate::errors::{Action, Entity, SbroadError};

use crate::ir::Plan;

use smol_str::format_smolstr;

use crate::frontend::sql::ast_new::AnalyzedAst;

// The surface the IR populator will need once it is revived. Kept as a sketch
// of the shape rather than as live imports; everything below comes from
// `sql-ast-new-nodes`.
//
// Column and CTE references in the analyzed tree are positions in enclosing
// statements, so the populator has to keep the stack of statements it is in,
// one `FrameView` each, and read references through `scope::Scope`, as
// rendering does.
//
// // Ast state
// use sql_ast_new_nodes::Analyzed;
//
// // Ast nodes
// use sql_ast_new_nodes::multiset::{MultisetInner, MultisetStmt};
// use sql_ast_new_nodes::select::{SelectList, SelectStmt};
// use sql_ast_new_nodes::table_expression::{From, TableExpression, TableFactor};
// use sql_ast_new_nodes::{DqlStmt, Node, SqlOption};

// mod ir_populator;

pub(super) struct Planner {}

/// Public API
impl Planner {
    pub(super) fn new() -> Self {
        Self {}
    }

    pub(super) fn plan(self, _ast: AnalyzedAst) -> Result<Plan, SbroadError> {
        // The IR populator is not revived yet; returning an error (instead of
        // an empty plan) makes `transform_into_plan` fall back to the old
        // pipeline for every query.
        Err(SbroadError::FailedTo(
            Action::Build,
            Some(Entity::Plan),
            format_smolstr!("planning of the new AST is not implemented yet"),
        ))
    }
}

//! Frontend entry point for the new typed AST.
//!
//! The AST itself lives in crates under `crates/`; this module is only the
//! seam that plugs them into the frontend:
//!
//! ```text
//!   sql-ast-new-grammar        query.pest, PairParser, Rule
//!        ^             ^
//!        |             |
//!   sql-ast-new-nodes  |       the tree, its rendering, the shared AstErr
//!        ^             |
//!        |             |
//!     sql-ast-new-parser       &str -> AbstractSyntaxTree<Raw>
//!
//!   sql-ast-new-corpus         real-world DQL, for tests and benches
//! ```
//!
//! The analysis stage becomes a crate of its own next, hanging off the node
//! declarations rather than off the parser — the two never reference each
//! other. Because an inherent `impl` on a foreign type is illegal across
//! crates, neither stage hangs inherent methods off a node type: each declares
//! a local trait and implements it for the node instead. Node fields are `pub`
//! for the same reason.
//!
//! What is left here is the [`Ast`] implementation that ties parse to analyze.
//! It is legal for the same orphan rule: [`Ast`] is local to this crate, so
//! implementing it for the foreign [`AbstractSyntaxTree`] is allowed.

use smol_str::format_smolstr;

use crate::errors::{Action, Entity, SbroadError};
use crate::frontend::sql::Ast;
use crate::ir::metadata::Metadata;
use crate::ir::types::DerivedType;

pub(in crate::frontend::sql) use sql_ast_new_nodes::AnalyzedAst;
pub use sql_ast_new_nodes::{AbstractSyntaxTree, Analyzed, Raw, RawAst};

impl<'q> Ast<'q> for AbstractSyntaxTree<'q, Raw> {
    type AnalyzedAst = AbstractSyntaxTree<'q, Analyzed>;

    /// Parse `query` into a raw AST.
    fn new(query: &'q str) -> Result<Self, SbroadError> {
        sql_ast_new_parser::parse(query).map_err(SbroadError::from)
    }

    /// Consume the raw tree and produce the analyzed one: names resolved
    /// against `metadata`, expression types inferred. By value — analysis is
    /// a conversion between two tree types, not in-place mutation.
    ///
    /// The analyzer crate lands next; until then this half of the trait is the
    /// same off switch [`transform_into_plan`](super::transform_into_plan)
    /// already uses.
    fn analyze(
        self,
        metadata: &'q impl Metadata,
        param_types: &'q [DerivedType],
    ) -> Result<AnalyzedAst<'q>, SbroadError> {
        analyze_stub(self, metadata, param_types)
    }
}

/// Dispatch-side off switch: [`transform_into_plan`](super::transform_into_plan)
/// calls the stubs, which always fail, so the new pipeline stays dark in
/// production while the real [`new`](Ast::new)/[`analyze`](Ast::analyze)
/// remain exercised by tests.
///
/// Free functions rather than inherent methods: [`AbstractSyntaxTree`] is
/// declared in `sql-ast-new-nodes`, and an inherent `impl` on a foreign type is
/// what the layering rule above forbids.
pub(super) fn new_stub(_query: &str) -> Result<RawAst<'_>, SbroadError> {
    Err(SbroadError::FailedTo(
        Action::Build,
        Some(Entity::AST),
        format_smolstr!("new AST is just a stub for now"),
    ))
}

/// The [`analyze`](Ast::analyze) half of the off switch.
pub(super) fn analyze_stub<'q>(
    _ast: RawAst<'q>,
    _metadata: &'q impl Metadata,
    _param_types: &'q [DerivedType],
) -> Result<AnalyzedAst<'q>, SbroadError> {
    Err(SbroadError::FailedTo(
        Action::Analyze,
        Some(Entity::AST),
        format_smolstr!("new AST analyzer is just a stub for now"),
    ))
}

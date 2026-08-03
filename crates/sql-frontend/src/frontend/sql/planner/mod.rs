use crate::errors::SbroadError;

use crate::ir::Plan;

use crate::frontend::sql::ast_new::AbstractSyntaxTree;

pub(super) struct Planner {}

impl Planner {
    pub(super) fn new() -> Self {
        Self {}
    }

    pub(super) fn plan(self, _ast: AbstractSyntaxTree) -> Result<Plan, SbroadError> {
        Err(SbroadError::DoSkip)
    }
}

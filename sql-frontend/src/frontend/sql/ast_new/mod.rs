use crate::errors::SbroadError;

use crate::ir::types::DerivedType;

use crate::ir::metadata::Metadata;

use crate::frontend::sql::Ast;

#[derive(Default)]
pub(super) struct AbstractSyntaxTree {
    _root: Node,
}

impl<'q> Ast<'q> for AbstractSyntaxTree {
    type AnalyzedAst = AbstractSyntaxTree;

    fn new(_query: &'q str) -> Result<Self, SbroadError> {
        Ok(Self::default())
    }

    fn analyze(
        self,
        _metadata: &'q impl Metadata,
        _param_types: &'q [DerivedType],
    ) -> Result<Self::AnalyzedAst, SbroadError> {
        Ok(Self::AnalyzedAst::default())
    }
}

#[derive(Default)]
pub(super) enum Node {
    #[default]
    Empty,
}

//! Explain dispatcher for mocked environments. Compiled only with the
//! `mock` feature, never in production builds.

use crate::explain::executor::ExplainQueryLocation;
use crate::explain::ExplainExecutingQuery;
use smol_str::ToSmolStr as _;
use sql_executor::executor::engine::Router;
use sql_ir::errors::SbroadError;
use sql_ir::ir::ExplainOptions;

impl<'a, C> ExplainExecutingQuery<'a, C>
where
    C: Router,
{
    /// Explain facets that do not need a running cluster: `LOGICAL` and
    /// `BUCKETS`. The `RAW` facet requires dispatching the query and the
    /// `FORWARD` facet needs the cluster topology, so both error out here.
    /// The production dispatcher lives on the embedding side.
    pub fn explain(&mut self) -> Result<String, SbroadError> {
        let explain_options = self.inner.get_exec_plan().get_ir_plan().explain_options;
        let mut explain = Vec::new();
        if explain_options.contains(ExplainOptions::Logical) {
            let logical = self.explain_logical(|_, _| ExplainQueryLocation::Whole)?;
            explain.push(logical);
        }

        if explain_options.contains(ExplainOptions::Raw) {
            return Err(SbroadError::Other(
                "RAW mode of EXPLAIN is not supported for mocks".to_smolstr(),
            ));
        }

        if explain_options.contains(ExplainOptions::Forward) {
            return Err(SbroadError::Other(
                "FORWARD mode of EXPLAIN is not supported for mocks".to_smolstr(),
            ));
        }

        if explain_options.contains(ExplainOptions::Buckets) {
            let buckets = self.explain_buckets()?;
            explain.push(buckets);
        }

        // Each entry in `explain` is a plain line without a trailing '\n',
        // so we join them with "\n\n" to separate each entry with a blank line.
        let explain = explain.join("\n\n");

        Ok(explain)
    }
}

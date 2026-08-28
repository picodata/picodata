#![cfg_attr(
    not(test),
    deny(
        clippy::todo,
        clippy::unimplemented,
        clippy::panic,
        clippy::unreachable,
        clippy::expect_used,
        clippy::unwrap_used
    )
)]
#![deny(
    rustdoc::broken_intra_doc_links,
    unreachable_pub,
    unused_lifetimes,
    single_use_lifetimes
)]
#![allow(rustdoc::private_intra_doc_links)]

use smol_str::format_smolstr;

use sql_ast_new_nodes::error::AstResult;
use sql_ast_new_nodes::multiset::Ctes;
use sql_ast_new_nodes::select::SelectList;
use sql_ast_new_nodes::table_expression::From;
use sql_ast_new_nodes::Analyzed;

use crate::analyze_invariant_error;

pub(crate) struct Stmt<'q> {
    /// WITH clause of current frame. Empty in case of `VALUES`.
    pub(crate) ctes: Ctes<'q, Analyzed>,
    /// FROM clause of current frame. Empty in case of `VALUES`.
    pub(crate) from: From<'q, Analyzed>,
    /// Bound select list, parked on the frame until the statement takes it back.
    sel_lst: Option<SelectList<'q, Analyzed>>,
}

pub(crate) const MISSING_BOUND_SELECT_LIST_ERR: &str = "missing bound SELECT in current frame";

impl<'q> Stmt<'q> {
    pub(crate) fn new(from: From<'q, Analyzed>) -> Self {
        Self {
            ctes: Ctes::default(),
            from,
            sel_lst: None,
        }
    }

    pub(crate) fn set_bound_sel_lst(&mut self, sel_lst: SelectList<'q, Analyzed>) {
        self.sel_lst = Some(sel_lst);
    }

    pub(crate) fn bound_sel_lst_mut(&mut self) -> AstResult<&mut SelectList<'q, Analyzed>> {
        self.sel_lst.as_mut().ok_or_else(|| {
            analyze_invariant_error(format_smolstr!("{MISSING_BOUND_SELECT_LIST_ERR}"))
        })
    }

    pub(crate) fn take_bound_sel_lst(&mut self) -> AstResult<SelectList<'q, Analyzed>> {
        std::mem::take(&mut self.sel_lst).ok_or_else(|| {
            analyze_invariant_error(format_smolstr!("{MISSING_BOUND_SELECT_LIST_ERR}"))
        })
    }
}

//! Pre-order iteration over a pest parse subtree.
//!
//! # `Tree<'q>`
//! Grammar rules nest more deeply than the AST does. A clause may be wrapped in
//! several intermediate rules that carry no meaning of their own.
//!
//! [`Tree`] flattens that away, letting a parser scan its own subtree as a
//! stream of pairs and match on the rules it cares about.
//!
//!
//! # Traversal
//! The [`gothru`](Tree::gothru) filter prunes descent, not output: a filtered
//! pair is still yielded, only its children are skipped.
//!
//! That is what keeps module boundaries intact. A statement parser walks its own
//! clauses flat, while nested expressions and subqueries arrive as opaque leaves
//! to be handed to the parser that owns them.

use pest::iterators::Pair;

use sql_ast_new_grammar::Rule;

type ParsePairFilter<'q> = fn(&Pair<'q, Rule>) -> bool;

/// Pre-order iterator over a pest parse (sub)tree. The [`gothru`](Tree::gothru) filter
/// prunes descent, not output: a filtered pair is still yielded, only its
/// children are skipped. This lets a parser scan its rule's subtree flat
/// while treating nested rules (expressions, subqueries) as opaque leaves
/// handed to their own parsers.
pub(super) struct Tree<'q> {
    stack: Vec<Pair<'q, Rule>>,
    gothru: ParsePairFilter<'q>,
}

impl<'q> Tree<'q> {
    pub(super) fn from_pair_with_gothru_filter(
        pair: Pair<'q, Rule>,
        gothru: ParsePairFilter<'q>,
    ) -> Self {
        Self {
            stack: vec![pair],
            gothru,
        }
    }
}

impl<'q> From<Pair<'q, Rule>> for Tree<'q> {
    fn from(pair: Pair<'q, Rule>) -> Self {
        Self::from_pair_with_gothru_filter(pair, |_| true)
    }
}

impl<'q> Iterator for Tree<'q> {
    type Item = Pair<'q, Rule>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.stack.pop() {
            Some(pair) => {
                if (self.gothru)(&pair) {
                    for p in pair.clone().into_inner().rev() {
                        self.stack.push(p);
                    }
                }
                Some(pair)
            }
            None => None,
        }
    }
}

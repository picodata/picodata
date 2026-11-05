//! Aliases for the joins and subqueries the query does not name.

use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::collections::HashSet;

/// Generator of aliases for unnamed joins and subqueries, unique among the
/// aliases of the query.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AliasGenerator {
    /// Every alias the generated names must not repeat.
    used: HashSet<SmolStr>,
    join_idx: u64,
    subquery_idx: u64,
}

impl AliasGenerator {
    #[must_use]
    pub fn new(used: HashSet<SmolStr>) -> Self {
        Self {
            used,
            join_idx: 0,
            subquery_idx: 0,
        }
    }

    fn unique(&self, pattern: &str, mut counter: u64) -> (SmolStr, u64) {
        loop {
            let candidate = if counter == 0 {
                pattern.to_smolstr()
            } else {
                format_smolstr!("{}_{}", pattern, counter)
            };
            counter += 1;
            if !self.used.contains(&candidate) {
                break (candidate, counter);
            }
        }
    }

    pub fn subquery_alias(&mut self) -> SmolStr {
        let (name, counter) = self.unique("unnamed_subquery", self.subquery_idx);
        self.subquery_idx = counter;
        name
    }

    pub fn join_alias(&mut self) -> SmolStr {
        let (name, counter) = self.unique("unnamed_join", self.join_idx);
        self.join_idx = counter;
        name
    }
}

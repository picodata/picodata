//! Aliases for the joins and subqueries the query does not name.

use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::collections::HashSet;

use crate::ir::node::relational::Relational;
use crate::ir::node::{ArenaType, Node, NodeId};
use crate::ir::Plan;

/// Generator of aliases for unnamed joins and subqueries, unique among the
/// aliases of the query.
///
/// The frontend names the unnamed subqueries of the parsed query with it, the
/// optimizer the subqueries and the motions it adds. The optimizer's generator
/// (`default`) reads the aliases from the plan, so nothing has to be kept
/// between parsing and planning.
#[derive(Clone, Debug, Default)]
pub struct AliasGenerator {
    /// Every alias the generated names must not repeat. `None` until the
    /// first generated name: reading it from the plan scans all nodes, and
    /// most plans get no generated name at planning.
    used: Option<HashSet<SmolStr>>,
    join_idx: u64,
    subquery_idx: u64,
}

impl AliasGenerator {
    #[must_use]
    pub fn new(used: HashSet<SmolStr>) -> Self {
        Self {
            used: Some(used),
            join_idx: 0,
            subquery_idx: 0,
        }
    }

    /// The aliases in use by `plan`: its table names and the aliases of its
    /// scans and motions, the earlier generated ones included.
    fn plan_aliases(plan: &Plan) -> HashSet<SmolStr> {
        let mut used: HashSet<SmolStr> = plan.relations.tables.keys().cloned().collect();
        for arena_type in [
            ArenaType::Arena32,
            ArenaType::Arena64,
            ArenaType::Arena96,
            ArenaType::Arena136,
            ArenaType::Arena232,
        ] {
            let len = plan.nodes.next_id(arena_type).offset;
            for offset in 0..len {
                let id = NodeId { offset, arena_type };
                let alias = match plan.nodes.get(id) {
                    Some(Node::Relational(rel)) => match rel {
                        Relational::ScanRelation(scan) => scan.alias.as_ref(),
                        Relational::ScanSubQuery(scan) => scan.alias.as_ref(),
                        Relational::ScanFunction(scan) => scan.alias.as_ref(),
                        Relational::ScanCte(scan) => Some(&scan.alias),
                        Relational::Motion(motion) => motion.alias.as_ref(),
                        _ => None,
                    },
                    _ => None,
                };
                if let Some(alias) = alias {
                    used.insert(alias.clone());
                }
            }
        }
        used
    }

    fn unique(&mut self, plan: &Plan, pattern: &str, mut counter: u64) -> (SmolStr, u64) {
        let used = self.used.get_or_insert_with(|| Self::plan_aliases(plan));
        loop {
            let candidate = if counter == 0 {
                pattern.to_smolstr()
            } else {
                format_smolstr!("{}_{}", pattern, counter)
            };
            counter += 1;
            if !used.contains(&candidate) {
                break (candidate, counter);
            }
        }
    }

    pub fn subquery_alias(&mut self, plan: &Plan) -> SmolStr {
        let (name, counter) = self.unique(plan, "unnamed_subquery", self.subquery_idx);
        self.subquery_idx = counter;
        name
    }

    pub fn join_alias(&mut self, plan: &Plan) -> SmolStr {
        let (name, counter) = self.unique(plan, "unnamed_join", self.join_idx);
        self.join_idx = counter;
        name
    }
}

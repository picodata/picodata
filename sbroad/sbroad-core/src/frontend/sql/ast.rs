//! Abstract syntax tree (AST) module.
//!
//! This module contains a definition of the abstract syntax tree
//! constructed from the nodes of the `pest` tree iterator nodes.

extern crate pest;

use std::collections::{HashMap, HashSet};
use std::mem::swap;

use pest::iterators::Pair;
use smol_str::{format_smolstr, SmolStr};

use crate::errors::{Entity, SbroadError};

/// Parse tree
#[derive(Parser)]
#[grammar = "frontend/sql/query.pest"]
pub(super) struct ParseTree;

/// Parse node is a wrapper over the pest pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseNode {
    pub(in crate::frontend::sql) children: Vec<usize>,
    pub(in crate::frontend::sql) rule: Rule,
    pub(in crate::frontend::sql) value: Option<SmolStr>,
}

#[allow(dead_code)]
impl ParseNode {
    pub(super) fn new(rule: Rule, value: Option<SmolStr>) -> Self {
        ParseNode {
            children: vec![],
            rule,
            value,
        }
    }

    /// Return first child from node children.
    ///
    /// # Panics
    ///
    /// Panics a children array is empty.
    pub(super) fn first_child(&self) -> usize {
        *self
            .children
            .first()
            .expect("could not find first child in node")
    }

    /// Return a nth child from node children.
    ///
    /// # Panics
    ///
    /// Panics if there is no n-child in a children array.
    pub(super) fn child_n(&self, n: usize) -> usize {
        *self
            .children
            .get(n)
            .unwrap_or_else(|| panic!("could find {n} child in node"))
    }
}

/// A storage arena of the parse nodes
/// (a node position in the arena vector acts like a reference).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseNodes {
    pub(crate) arena: Vec<ParseNode>,
}

impl Default for ParseNodes {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl ParseNodes {
    /// Get a node from arena
    ///
    /// # Errors
    /// - Failed to get a node from arena.
    pub fn get_node(&self, node: usize) -> Result<&ParseNode, SbroadError> {
        self.arena.get(node).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("from arena with index {node}"),
            )
        })
    }

    /// Get a mutable node from arena
    ///
    /// # Errors
    /// - Failed to get a node from arena.
    pub fn get_mut_node(&mut self, node: usize) -> Result<&mut ParseNode, SbroadError> {
        self.arena.get_mut(node).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("(mutable) from arena with index {node}"),
            )
        })
    }

    /// Push a new node to arena
    pub fn push_node(&mut self, node: ParseNode) -> usize {
        let id = self.next_id();
        self.arena.push(node);
        id
    }

    /// Push `child_id` to the front of `node_id` children
    ///
    /// # Errors
    /// - Failed to get node from arena
    pub fn push_front_child(&mut self, node_id: usize, child_id: usize) -> Result<(), SbroadError> {
        let node = self.get_mut_node(node_id)?;
        node.children.insert(0, child_id);
        Ok(())
    }

    /// Push `child_id` to the back of `node_id` children
    ///
    /// # Errors
    /// - Failed to get node from arena
    pub fn push_back_child(&mut self, node_id: usize, child_id: usize) -> Result<(), SbroadError> {
        let node = self.get_mut_node(node_id)?;
        node.children.push(child_id);
        Ok(())
    }

    /// Sets node children to given children
    ///
    /// # Errors
    /// - failed to get node from arena
    pub fn set_children(
        &mut self,
        node_id: usize,
        new_children: Vec<usize>,
    ) -> Result<(), SbroadError> {
        let node = self.get_mut_node(node_id)?;
        node.children = new_children;
        Ok(())
    }

    /// Get next node id
    #[must_use]
    pub fn next_id(&self) -> usize {
        self.arena.len()
    }

    /// Constructor
    #[must_use]
    pub fn new() -> Self {
        ParseNodes { arena: Vec::new() }
    }

    /// Adds children to already existing node.
    /// New elements are added to the beginning of the current list
    /// as we use inverted node order.
    ///
    /// # Errors
    /// - Failed to retrieve node from arena.
    pub fn add_child(&mut self, node: Option<usize>, child: usize) -> Result<(), SbroadError> {
        if let Some(parent) = node {
            self.get_node(child)?;
            let parent_node = self.arena.get_mut(parent).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Node,
                    format_smolstr!("(mutable) from arena with index {parent}"),
                )
            })?;
            parent_node.children.insert(0, child);
        }
        Ok(())
    }

    /// Update node's value (string from pairs)
    ///
    /// # Errors
    /// - Target node is present in the arena.
    pub fn update_value(&mut self, node: usize, value: Option<SmolStr>) -> Result<(), SbroadError> {
        let node = self.arena.get_mut(node).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("(mutable) from arena with index {node}"),
            )
        })?;
        node.value = value;
        Ok(())
    }
}

/// A wrapper over the pair to keep its parent as well.
pub(super) struct StackParseNode<'n> {
    pub(super) arena_parent_id: Option<usize>,
    pub(super) pair: Pair<'n, Rule>,
}

impl<'n> StackParseNode<'n> {
    /// Constructor
    pub(super) fn new(pair: Pair<'n, Rule>, parent_id: Option<usize>) -> Self {
        StackParseNode {
            arena_parent_id: parent_id,
            pair,
        }
    }
}

/// AST is a tree build on the top of the parse nodes arena.
#[derive(Clone, Debug)]
pub struct AbstractSyntaxTree {
    pub(in crate::frontend::sql) nodes: ParseNodes,
    /// Index of top `ParseNode` in `nodes.arena`.
    pub(in crate::frontend::sql) top: Option<usize>,
    /// Map of { reference node_id -> relation node_ids it refers to }.
    /// See `build_ref_to_relation_map` to understand how it is filled.
    /// Note: corresponding vec has 2 values only in case of Join relational node.
    pub(super) map: HashMap<usize, Vec<usize>>,
}

impl PartialEq for AbstractSyntaxTree {
    fn eq(&self, other: &Self) -> bool {
        self.nodes == other.nodes && self.top == other.top && self.map == other.map
    }
}

#[allow(dead_code)]
impl AbstractSyntaxTree {
    /// Set the top of AST.
    ///
    /// # Errors
    /// - The new top is not in the arena.
    pub fn set_top(&mut self, top: usize) -> Result<(), SbroadError> {
        self.nodes.get_node(top)?;
        self.top = Some(top);
        Ok(())
    }

    /// Get the top of AST.
    ///
    /// # Errors
    /// - AST tree doesn't have a top node.
    pub fn get_top(&self) -> Result<usize, SbroadError> {
        self.top.ok_or_else(|| {
            SbroadError::Invalid(Entity::AST, Some("no top node found in AST".into()))
        })
    }

    /// Bring join AST to expected kind
    ///
    /// Inner join can be specified as `inner join` or `join` in user query,
    /// add `inner` to join if the second form was used
    pub(super) fn normalize_join_ast(&mut self, join_id: usize) -> Result<(), SbroadError> {
        let node = self.nodes.get_node(join_id)?;
        if let Rule::Join = node.rule {
            if node.children.len() < 3 {
                let inner_node = ParseNode {
                    children: vec![],
                    rule: Rule::InnerJoinKind,
                    value: Some("inner".into()),
                };
                let inner_id = self.nodes.push_node(inner_node);
                let mut_node = self.nodes.get_mut_node(join_id)?;
                mut_node.children.insert(0, inner_id);
            }
        } else {
            return Err(SbroadError::Invalid(
                Entity::ParseNode,
                Some(format_smolstr!("expected join parse node, got: {node:?}")),
            ));
        }
        Ok(())
    }

    /// Rewrite `Update` AST to IR friendly one.
    ///
    /// `update t .. from s where expr` is transformed into
    /// ```text
    /// update t ..
    ///     join
    ///         scan t
    ///         s
    ///         condition expr
    /// ```
    ///
    /// `update t .. where expr` ->
    /// ```text
    /// update t ..
    ///     where expr
    ///         scan t
    /// ```
    ///
    /// `update t .. from s` ->
    /// ```text
    /// update t ..
    ///     join
    ///         scan t
    ///         s
    ///         condition true
    /// ```
    ///
    /// # Errors
    /// - invalid number of children for Update
    /// - unexpected rule of some child
    #[allow(clippy::too_many_lines)]
    pub(super) fn transform_update(&mut self) -> Result<(), SbroadError> {
        let update_id: usize = {
            let mut update_id: Option<usize> = None;
            for id in 0..self.nodes.arena.len() {
                let node = self.nodes.get_node(id)?;
                if node.rule == Rule::Update {
                    update_id = Some(id);
                    break;
                }
            }
            if let Some(id) = update_id {
                id
            } else {
                return Ok(());
            }
        };
        let node = self.nodes.get_node(update_id)?;
        let table_id = *node
            .children
            .first()
            .expect("expected Update AST node to have at least two children");
        let update_list_id = *node
            .children
            .get(1)
            .expect("expected Update AST node to have at least two children");
        let upd_table_scan = ParseNode {
            children: vec![table_id],
            rule: Rule::Scan,
            value: None,
        };
        let upd_table_scan_id = self.nodes.push_node(upd_table_scan);
        let node = self.nodes.get_node(update_id)?;
        match node.children.len() {
            // update t set ..
            2 => {
                self.nodes
                    .set_children(update_id, vec![upd_table_scan_id, table_id, update_list_id])?;
            }
            // update t set .. from .. OR update t set .. where ..
            3 => {
                // update t set a = 1 where id = 1
                let child_id = *node.children.get(2).unwrap();
                let is_selection = matches!(self.nodes.get_node(child_id)?.rule, Rule::Selection);
                let update_child_id = if is_selection {
                    self.nodes.push_front_child(child_id, upd_table_scan_id)?;
                    child_id
                } else {
                    // update t set a = t.a + t1.b from t1
                    let true_literal_node = ParseNode {
                        children: vec![],
                        rule: Rule::True,
                        value: Some("true".into()),
                    };
                    let true_literal_id = self.nodes.push_node(true_literal_node);
                    let expr_node = ParseNode {
                        children: vec![true_literal_id],
                        rule: Rule::Expr,
                        value: None,
                    };
                    let expr_id = self.nodes.push_node(expr_node);
                    let inner_kind_id = self.nodes.push_node(ParseNode {
                        children: vec![],
                        rule: Rule::InnerJoinKind,
                        value: None,
                    });
                    let join_node = ParseNode {
                        children: vec![upd_table_scan_id, inner_kind_id, child_id, expr_id],
                        rule: Rule::Join,
                        value: None,
                    };
                    self.nodes.push_node(join_node)
                };
                self.nodes
                    .set_children(update_id, vec![update_child_id, table_id, update_list_id])?;
            }
            4 => {
                // update t set a = t.a + t1.b from t1 where expr
                let expr_id = *node.children.get(3).unwrap();
                let right_scan_id = *node.children.get(2).unwrap();
                let inner_kind_id = self.nodes.push_node(ParseNode {
                    children: vec![],
                    rule: Rule::InnerJoinKind,
                    value: None,
                });
                let join_node = ParseNode {
                    children: vec![upd_table_scan_id, inner_kind_id, right_scan_id, expr_id],
                    rule: Rule::Join,
                    value: None,
                };
                let update_child_id = self.nodes.push_node(join_node);
                self.nodes
                    .set_children(update_id, vec![update_child_id, table_id, update_list_id])?;
            }
            _ => {
                return Err(SbroadError::UnexpectedNumberOfValues(
                    "expected Update ast node to have at most 4 children!".into(),
                ))
            }
        }
        Ok(())
    }

    /// Put delete table under delete filter to support reference resolution.
    pub(super) fn transform_delete(&mut self) -> Result<(), SbroadError> {
        let arena_len = self.nodes.arena.len();
        for id in 0..arena_len {
            let node = self.nodes.get_node(id)?;
            if node.rule != Rule::Delete {
                continue;
            }
            let (table_id, filter_id) = if let (Some(table_id), Some(filter_id)) =
                (node.children.first(), node.children.get(1))
            {
                (*table_id, *filter_id)
            } else {
                continue;
            };
            let filter_node = self.nodes.get_node(filter_id)?;
            if filter_node.rule != Rule::DeleteFilter {
                return Err(SbroadError::Invalid(
                    Entity::ParseNode,
                    Some(format_smolstr!(
                        "expected delete filter as a second child, got: {filter_node:?}"
                    )),
                ));
            }
            let mut new_filter_children = Vec::with_capacity(filter_node.children.len() + 1);
            new_filter_children.push(table_id);
            new_filter_children.extend(filter_node.children.iter().copied());
            self.nodes.set_children(filter_id, new_filter_children)?;
            self.nodes.set_children(id, vec![filter_id])?;
        }
        Ok(())
    }

    /// Transform select AST to IR friendly one. At the end of transformation
    /// all `Select` nodes are replaced with their first children (always `Projection`).
    /// - When some node contains `Select` as a child, that child is replaced with
    ///   `Projection` (`Select`'s first child).
    /// - When `Select` is a top node, its `Projection` (first child) becomes a new top.
    pub(super) fn transform_select(&mut self) -> Result<(), SbroadError> {
        let mut selects: HashSet<usize> = HashSet::new();
        for id in 0..self.nodes.arena.len() {
            let node = self.nodes.get_node(id)?;
            if node.rule == Rule::Select {
                selects.insert(id);
            }
            if node.rule == Rule::Join {
                self.normalize_join_ast(id)?;
            }
        }
        for node in &selects {
            let select = self.nodes.get_node(*node)?;
            let children: Vec<usize> = select.children.clone();
            self.reorder_select_children(*node, &children)?;
        }

        // Collect select nodes' parents.
        let mut parents: Vec<(usize, usize)> = Vec::new();
        for (id, node) in self.nodes.arena.iter().enumerate() {
            for (children_pos, child_id) in node.children.iter().enumerate() {
                if selects.contains(child_id) {
                    parents.push((id, children_pos));
                }
            }
        }
        // Remove select nodes that have parents.
        for (parent_id, children_pos) in parents {
            let parent = self.nodes.get_node(parent_id)?;
            let child_id = *parent.children.get(children_pos).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Node,
                    format_smolstr!("at expected position {children_pos}"),
                )
            })?;
            let child = self.nodes.get_node(child_id)?;
            let mut node_id = *child.children.first().ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues("Selection node has no children.".into())
            })?;
            let parent = self.nodes.get_mut_node(parent_id)?;
            swap(&mut parent.children[children_pos], &mut node_id);
        }
        // Remove select if it is a top node.
        let top_id = self.get_top()?;
        if selects.contains(&top_id) {
            let top = self.nodes.get_node(top_id)?;
            let child_id = *top.children.first().ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(
                    "Selection node doesn't contain any children.".into(),
                )
            })?;
            self.set_top(child_id)?;
        }
        Ok(())
    }

    fn reorder_select_children(
        &mut self,
        select_id: usize,
        children: &[usize],
    ) -> Result<(), SbroadError> {
        // SQL grammar produces a defined order of children in select node,
        // for a projection with scan:
        // 1. Projection: required
        // 2. Scan: required (bind with Projection)
        // 3. Join: optional (can be repeated multiple times)
        // 4. Selection: optional
        // 5. GroupBy: optional
        // 6. Having: optional
        // 7. OrderBy: optional
        //
        // But for projection without scan (like `select 1`)
        // Scan is optional, and all other nodes are not required.
        //
        // We need to reorder this sequence to the following:
        // 1. OrderBy: optional
        // 2. Projection: required
        // 3. Having: optional
        // 4. GroupBy: optional
        // 5. Selection: optional
        // 6. Join: optional (can be repeated multiple times)
        // 7. Scan: required
        let mut proj_id: Option<usize> = None;
        let mut scan_id: Option<usize> = None;
        let mut join_ids = if children.len() > 2 {
            Vec::with_capacity(children.len() - 2)
        } else {
            Vec::new()
        };
        let mut filter_id: Option<usize> = None;
        let mut group_id: Option<usize> = None;
        let mut having_id: Option<usize> = None;
        let mut order_by_id: Option<usize> = None;

        for child_id in children {
            let child = self.nodes.get_node(*child_id)?;
            match child.rule {
                Rule::Projection => proj_id = Some(*child_id),
                Rule::Scan => scan_id = Some(*child_id),
                Rule::Join => join_ids.push(*child_id),
                Rule::Selection => filter_id = Some(*child_id),
                Rule::GroupBy => group_id = Some(*child_id),
                Rule::Having => having_id = Some(*child_id),
                Rule::OrderBy => order_by_id = Some(*child_id),
                _ => panic!("{} {:?}", "Unexpected rule in select children:", child.rule),
            }
        }

        // Projection and Scan are required. If they are not present, there is an error
        // in the SQL grammar.
        let proj_id = proj_id.expect("Projection node is required in select node");

        // The order of the nodes in the chain is partially reversed.
        // Original nodes from grammar:
        // Projection -> Scan -> Join1 -> ... -> JoinK -> Selection -> GroupBy -> Having -> OrderBy.
        // We need to change the order of the chain to:
        // OrderBy -> Projection -> Having -> GroupBy -> Selection -> JoinK -> ... -> Join1
        let mut chain = Vec::with_capacity(children.len() - 1);

        // Note that OrderBy must go above Projection because
        // we need to operate with its (projection) output.
        if let Some(order_by_id) = order_by_id {
            chain.push(order_by_id);
        }
        chain.push(proj_id);
        if let Some(having_id) = having_id {
            chain.push(having_id);
        }
        if let Some(group_id) = group_id {
            chain.push(group_id);
        }
        if let Some(filter_id) = filter_id {
            chain.push(filter_id);
        }
        while let Some(join_id) = join_ids.pop() {
            chain.push(join_id);
        }

        let Some(mut child_id) = scan_id else {
            self.nodes.set_children(select_id, vec![proj_id])?;
            return Ok(());
        };

        while let Some(id) = chain.pop() {
            self.nodes.push_front_child(id, child_id)?;
            child_id = id;
        }
        self.nodes.set_children(select_id, vec![child_id])?;

        Ok(())
    }
}

//! Rendering a tree back as SQL: the [`Render`] trait every node implements,
//! and the helpers shared by every stage's snapshot tests.
//!
//! # Why not `Display`
//! Rendering a [`Raw`](crate::Raw) tree needs nothing but the tree. Rendering an
//! [`Analyzed`](crate::Analyzed) one does not: a resolved column reference names
//! its relation by position in an enclosing statement, so printing it as
//! `table.column` takes the stack of statements the walk is inside of. That
//! stack is the state-specific [`AstState::RenderScope`]: `()` for `Raw`, the
//! frames pushed by [`AstState::enter_stmt`] for `Analyzed`. It is threaded
//! through the walk exactly like the scope of
//! [`StructuralEq`](crate::StructuralEq), and the impls are generic over the
//! state the same way.
//!
//! `Display` survives where nothing below needs a scope: the leaves
//! (identifiers, operators, literals' pieces), the tree root
//! ([`AbstractSyntaxTree`](crate::AbstractSyntaxTree), which starts from an empty
//! scope), and a few `Raw` nodes the parser's tests render on their own.
//!
//! The test helpers live here rather than in each stage because `#[cfg(test)]`
//! does not cross a crate boundary and the stages are separate crates.

use std::fmt::{Display, Formatter, Result};
use std::rc::Rc;

use crate::AstState;

/// Write the node as SQL into `f`, resolving whatever the state needs resolved
/// through `scope`. Inside the walk every child goes through this method, never
/// through `Display`, so the scope reaches the column references.
pub trait Render<'q, State: AstState<'q>> {
    fn render<'a>(&'a self, f: &mut Formatter<'_>, scope: &mut State::RenderScope<'a>) -> Result
    where
        'q: 'a;
}

impl<'q, S: AstState<'q>, T: Render<'q, S>> Render<'q, S> for Box<T> {
    fn render<'a>(&'a self, f: &mut Formatter<'_>, scope: &mut S::RenderScope<'a>) -> Result
    where
        'q: 'a,
    {
        T::render(self, f, scope)
    }
}

impl<'q, S: AstState<'q>, T: Render<'q, S>> Render<'q, S> for Rc<T> {
    fn render<'a>(&'a self, f: &mut Formatter<'_>, scope: &mut S::RenderScope<'a>) -> Result
    where
        'q: 'a,
    {
        T::render(self, f, scope)
    }
}

/// Render a state-independent leaf through its [`Display`]: the scope is not
/// consulted.
macro_rules! render_via_display {
    ($($ty:ty),* $(,)?) => {
        $(
            impl<'q, S: $crate::AstState<'q>> $crate::rendering::Render<'q, S> for $ty {
                fn render<'a>(
                    &'a self,
                    f: &mut ::std::fmt::Formatter<'_>,
                    _scope: &mut S::RenderScope<'a>,
                ) -> ::std::fmt::Result
                where
                    'q: 'a,
                {
                    ::std::fmt::Display::fmt(self, f)
                }
            }
        )*
    };
}
pub(crate) use render_via_display;

/// [`Display`] for a `Raw` node below the root, which needs no scope. Only the
/// nodes the parser's tests render on their own get one.
macro_rules! raw_display {
    ($($ty:ident),* $(,)?) => {
        $(
            impl ::std::fmt::Display for $ty<'_, $crate::Raw> {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    $crate::rendering::Render::render(self, f, &mut ())
                }
            }
        )*
    };
}
pub(crate) use raw_display;

/// A node paired with the scope it is rendered in, so that a node below the
/// root can be formatted (an error message quoting an analyzed subtree).
pub struct Rendered<'a, 'q: 'a, S: AstState<'q>, T: Render<'q, S>> {
    node: &'a T,
    scope: S::RenderScope<'a>,
}

impl<'a, 'q: 'a, S: AstState<'q>, T: Render<'q, S>> Rendered<'a, 'q, S, T> {
    pub fn new(node: &'a T, scope: S::RenderScope<'a>) -> Self {
        Self { node, scope }
    }
}

impl<'a, 'q: 'a, S: AstState<'q>, T: Render<'q, S>> Display for Rendered<'a, 'q, S, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let mut scope = self.scope.clone();
        self.node.render(f, &mut scope)
    }
}

/// Collapses whitespace runs to a single space and trims the ends, so a snapshot
/// can be written on one line no matter how [`Display`] breaks
/// the rendering.
///
/// Whitespace *inside* a single-quoted literal is content, not layout, and is
/// left alone: `SELECT 'a  b'` must not normalize to `SELECT 'a b'`.
///
/// The `''` escape needs no special handling.
pub fn normalize_whitespace(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_string = false;
    let mut pending_space = false;

    let mut last_quote: Option<char> = None;
    for c in s.chars() {
        if in_string {
            out.push(c);
            in_string = (last_quote.is_some_and(|ch| ch == '\'') && c != '\'')
                || (last_quote.is_some_and(|ch| ch == '"' && c != '"'));
            continue;
        }
        if c.is_ascii_whitespace() {
            // Emitted only once something follows it, which trims both ends.
            pending_space = !out.is_empty();
            continue;
        }
        if pending_space {
            out.push(' ');
            pending_space = false;
        }
        out.push(c);
        in_string = c == '\'' || c == '"';
        if in_string {
            last_quote = Some(c);
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::normalize_whitespace;

    /// Whitespace inside a string literal is data.
    #[test]
    fn normalize_whitespace_preserves_string_literals() {
        insta::assert_snapshot!(
            normalize_whitespace(r#"  SELECT\n   'a  b',\t'c\nd', "a  b"  FROM t  "#),
            @r#"SELECT\n 'a  b',\t'c\nd', "a  b" FROM t"#
        );
        insta::assert_snapshot!(
            normalize_whitespace("SELECT 'it''s  fine'  FROM  t"),
            @"SELECT 'it''s  fine' FROM t"
        );
        insta::assert_snapshot!(
            normalize_whitespace("SELECT ''  FROM  t"),
            @"SELECT '' FROM t"
        );
        insta::assert_snapshot!(
            normalize_whitespace(r#"SELECT 'a"  b'  FROM  t"#),
            @r#"SELECT 'a"  b' FROM t"#
        );
    }
}

//! The rendering round trip every `render_*` test helper runs.
//!
//! # The invariant
//! `sql-ast-new-corpus` states it for whole statements (`corpus_dql_round_trip`)
//! and this is the same contract, applied to the fragments the unit tests pin:
//! rendering back the exact input text is not a goal, so render-idempotence is
//! the fixed point that *is* checkable — and because rendering re-derives
//! grouping from the same precedence ladder the parser used, a tree grouped the
//! wrong way fails the re-render comparison.
//!
//! The corpus covers the shapes production sends; the unit tests cover the odd
//! corners, which is exactly where a dropped parenthesis hides.
//!
//!
//! # Why it lives in the helpers
//! Every rendering assertion in the crate goes through a `render_*` helper, so
//! putting the round trip there buys the invariant for all of them without a
//! line of per-test boilerplate.
//!
//! Only the first rendering is snapshotted. The second is compared against it
//! with [`assert_eq`], which says the same thing — the inline snapshot already
//! pins the first — and sidesteps insta's constraint that two inline snapshots
//! expanded from one helper share a `file!()`/`line!()` and so cannot be
//! rewritten independently by `cargo insta accept`.
//!
//! It also means a broken round trip panics *before* `assert_snapshot!` is
//! handed a value: it is not a snapshot diff, and `cargo insta accept` can
//! never paper over it.

use std::fmt::Display;

use sql_ast_new_nodes::rendering::normalize_whitespace;

/// Render `query`, re-parse that rendering and render it again; the two
/// renderings must be byte-identical.
///
/// Returns the first rendering, whitespace-normalized so the caller's snapshot
/// can be written on one line. The comparison itself is on the *raw* renderings,
/// which is what the corpus does and which additionally catches a rendering
/// whose layout is unstable.
///
/// `render_once` hands back the rendering rather than the tree so that the
/// borrow the AST holds on the rendered [`String`] stays inside the closure:
/// a bound over a lifetime-generic node type (`for<'a> Fn(&'a str) -> T<'a>`)
/// is not expressible.
#[track_caller]
pub(crate) fn round_trip<E: Display>(
    query: &str,
    render_once: impl Fn(&str) -> Result<String, E>,
) -> String {
    let rendered = match render_once(query) {
        Ok(rendered) => rendered,
        // Not `.expect(...)`: parse diagnostics are multi-line, and `expect`
        // renders them through `Debug` with the newlines escaped.
        Err(err) => panic!("`{query}` must parse: {err}"),
    };
    let re_rendered = match render_once(&rendered) {
        Ok(re_rendered) => re_rendered,
        Err(err) => panic!("rendering of `{query}` does not re-parse: {err}\nrendered: {rendered}"),
    };
    assert_eq!(
        re_rendered, rendered,
        "rendering of `{query}` is not stable"
    );

    normalize_whitespace(&rendered)
}

#[cfg(test)]
mod tests {
    use super::round_trip;

    /// A rendering that is stable passes, and what comes back is normalized for
    /// the snapshot rather than the raw rendering that was compared.
    #[test]
    fn stable_rendering_is_normalized_for_the_snapshot() {
        let rendered = round_trip("a   b", |q| Ok::<_, String>(q.to_owned()));

        assert_eq!(rendered, "a b");
    }

    /// The harness must bite on a rendering that only settles after more passes:
    /// one that keeps adding a layer is well-formed on its own, and only the
    /// second rendering exposes it.
    #[test]
    #[should_panic(expected = "is not stable")]
    fn unstable_rendering_is_caught() {
        round_trip("a", |q| Ok::<_, String>(format!("({q})")));
    }

    /// And on a rendering the parser refuses — the other way a dropped
    /// parenthesis shows up.
    #[test]
    #[should_panic(expected = "does not re-parse")]
    fn unparseable_rendering_is_caught() {
        round_trip("a", |q| {
            if q == "a" {
                Ok("b".to_owned())
            } else {
                Err("no")
            }
        });
    }
}

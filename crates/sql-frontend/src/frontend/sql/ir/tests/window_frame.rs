use crate::frontend::sql::ast::{ParseTree, Rule};
use sql_ast_new_grammar::{parse_limited, PairParser, Rule as NewRule};
use sql_executor::test_helpers::sql_to_optimized_ir;
use sql_explain::explain::explain_logical;

/// The frame of `sum(x) OVER (ROWS <bounds>)` as the explain shows it, with
/// the window inline or named in the WINDOW clause.
fn frame(bounds: &str, named: bool) -> String {
    let (over, window) = if named {
        ("w".to_owned(), format!(" WINDOW w AS (ROWS {bounds})"))
    } else {
        (format!("(ROWS {bounds})"), String::new())
    };
    let sql = format!(
        "explain (logical) SELECT sum(x) OVER {over} FROM \
        (SELECT 1 AS x, 1 AS preceding, 1 AS following, 1 AS unbounded, 1 AS current) AS t{window}"
    );
    let explain = explain_logical(&sql_to_optimized_ir(&sql, vec![])).unwrap();
    let (_, frame) = explain.split_once("over (rows ").unwrap();
    frame.split_once(") ->").unwrap().0.to_owned()
}

/// `unbounded`, `current`, `preceding` and `following` are not keywords:
/// the special bounds are not offsets with such columns, and the columns
/// still work as offsets. Inline and named windows are built by different
/// code.
#[test]
fn frame_bounds() {
    for named in [false, true] {
        let frame = |bounds| frame(bounds, named);
        assert_eq!(
            frame("BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"),
            "between unbounded preceding and unbounded following"
        );
        assert_eq!(
            frame("BETWEEN CURRENT ROW AND 1 FOLLOWING"),
            "between current row and 1::int following"
        );
        assert_eq!(
            frame("BETWEEN preceding PRECEDING AND following FOLLOWING"),
            "between t.preceding::int preceding and t.following::int following"
        );
        assert_eq!(
            frame("BETWEEN unbounded PRECEDING AND current FOLLOWING"),
            "between unbounded preceding and t.current::int following"
        );
    }
}

/// An error before a bound lists the special bounds as before: pest lists the
/// rules in the order they are declared.
#[test]
fn frame_bound_error() {
    let query = "select sum(a) over (rows between 1 preceding and ;) from t";
    let err = parse_limited::<ParseTree, Rule>(Rule::Command, query)
        .map(drop)
        .unwrap_err();
    let expected = "expected Expr, PrecedingUnbounded, CurrentRow, or FollowingUnbounded";
    assert!(err.contains(expected), "{err}");
}

/// A missing AND after the first bound is reported at its place in both
/// parsers, not at the start of the bound, where the special bounds failed.
#[test]
fn frame_missing_and_error() {
    for first in ["current row", "unbounded following", "1 preceding"] {
        let query = format!("select sum(a) over (rows between {first} 1 following) from t");
        let at = format!("1:{}", query.find(" 1 following").unwrap() + 2);
        for err in [
            parse_limited::<ParseTree, Rule>(Rule::Command, &query).map(drop),
            parse_limited::<PairParser, NewRule>(NewRule::Command, &query).map(drop),
        ] {
            let err = err.unwrap_err();
            assert!(err.contains(&at) && err.contains("expected And"), "{err}");
        }
    }
}

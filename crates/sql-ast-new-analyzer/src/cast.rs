//! Cast rules, applied while types are derived.
//!
//! Two things live here, both reached from the tail of
//! [`ExprTypeDeriver::derive_types`](crate::ExprTypeDeriver::derive_types):
//!
//! * [`can_cast`] — whether a user-written `CAST(x AS t)` has a conversion path at all.
//! * [`coerce_text_literal`] — constant fold. A text literal the type system decided
//!   to coerce is parsed *now*, so `'x'::int` is an analysis error rather than a runtime one,
//!   and `'1'` in an `int` context becomes the literal `1` instead of a `CAST('1' AS int)` wrapper.

use smol_str::format_smolstr;

use sql_ast_new_nodes::error::AstResult;
use sql_ast_new_nodes::expr::{Expr, ExprInner, LiteralKind, QuotesType};
use sql_ast_new_nodes::Analyzed;
use sql_ir::ir::types::{DerivedType, UnrestrictedType};
use sql_ir::ir::value::parse_str_as;
use sql_type_system::expr::Type;

use crate::analyze_error;

/// Whether `CAST(<from> AS <to>)` has a conversion path.
///
/// Modelled on PostgreSQL's `find_coercion_pathway` in explicit-cast context,
/// restricted to the scalar types Picodata has:
///
/// * anything converts to and from a string, through the type's text input and
///   output functions
/// * the numeric types convert among themselves
/// * `bool` and `int` convert both ways.
///
/// Everything else is rejected: `bool` against `decimal`/`double`/`uuid`/`datetime`,
/// the numeric types against `uuid`/`datetime`, and `uuid` against `datetime`.
///
/// Arrays, `map` and `any` are deliberately left outside these rules.
pub(crate) fn can_cast(from: UnrestrictedType, to: UnrestrictedType) -> bool {
    use UnrestrictedType::{Boolean, Integer, String};

    if from == to {
        return true;
    }

    // Only the scalar types are ruled on
    if !from.is_scalar() || !to.is_scalar() {
        return true;
    }

    // Text input/output conversion reaches every type in both directions.
    if from == String || to == String {
        return true;
    }

    if is_numeric(from) && is_numeric(to) {
        return true;
    }

    matches!((from, to), (Boolean, Integer) | (Integer, Boolean))
}

fn is_numeric(ty: UnrestrictedType) -> bool {
    matches!(
        ty,
        UnrestrictedType::Integer | UnrestrictedType::Decimal | UnrestrictedType::Double
    )
}

/// Fold a text literal the type system asked to coerce into `to`, in place.
///
/// Resolve an untyped literal during parse analysis and run the target type's
/// input function right there, so a literal that cannot be read as the target
/// is a planning error, not a runtime one.
///
/// Returns:
/// - `Ok(true)` when the node was rewritten and the caller must *not* wrap it in a `CAST`
/// - `Ok(false)` when the literal was checked but left alone, so the coercion still has
///   to be materialized the usual way.
pub(crate) fn coerce_text_literal(expr: &mut Expr<'_, Analyzed>, to: Type) -> AstResult<bool> {
    let (inner, _) = expr.parts_mut();
    let ExprInner::Literal(literal) = inner else {
        return Ok(false);
    };
    if literal.kind != LiteralKind::Text {
        return Ok(false);
    }
    let Some(target) = DerivedType::from(to).get().filter(|ty| ty.is_scalar()) else {
        return Ok(false);
    };

    // The parser strips only the outer quotes, so an embedded quote is still doubled in the slice.
    let unescaped = unescape(literal.value);
    let Some(value) = parse_str_as(&unescaped, target) else {
        return Err(analyze_error(format_smolstr!(
            "invalid input syntax for type {target}: \"{unescaped}\""
        )));
    };

    // Coerce to `Integer`/`Decimal`/`Double`/`Boolean`.
    // String is unreachable there (it means coerce string to string).
    let Some(kind) = literal_kind_of(target) else {
        return Ok(false);
    };

    if unescaped != literal.value || !relexes_as_literal(literal.value) {
        return Ok(false);
    }

    // The slice has to already *be* what the parsed value renders back as:
    // `'01'` reads as 1 but prints as `01`, `'1.0'` as a double prints as `1`.
    if value.to_string() != literal.value {
        return Ok(false);
    }

    literal.quotes = QuotesType::None;
    literal.kind = kind;
    Ok(true)
}

fn unescape(value: &str) -> std::borrow::Cow<'_, str> {
    if value.contains("''") {
        std::borrow::Cow::Owned(value.replace("''", "'"))
    } else {
        std::borrow::Cow::Borrowed(value)
    }
}

/// The literal kind that records `ty`, or `None` for a type with
/// no literal syntax in this grammar. `uuid` and `datetime` have
/// no literal syntax.
fn literal_kind_of(ty: UnrestrictedType) -> Option<LiteralKind> {
    match ty {
        UnrestrictedType::Integer => Some(LiteralKind::Integer),
        UnrestrictedType::Decimal => Some(LiteralKind::Numeric),
        UnrestrictedType::Double => Some(LiteralKind::Double),
        UnrestrictedType::Boolean => Some(LiteralKind::Boolean),
        UnrestrictedType::String => None,
        UnrestrictedType::Uuid
        | UnrestrictedType::Datetime
        | UnrestrictedType::Map
        | UnrestrictedType::Array(_)
        | UnrestrictedType::Any => None,
    }
}

/// Whether `text`, printed bare, reads back as a literal token.
///
/// The type is pinned by the `::type` suffix the analyzed rendering adds, so the
/// text only has to lex as *some* literal — not as one of the target's kind.
fn relexes_as_literal(text: &str) -> bool {
    if text == "true" || text == "false" {
        return true;
    }
    let mut digits = 0_usize;
    let mut dots = 0_usize;
    for byte in text.bytes() {
        match byte {
            b'0'..=b'9' => digits += 1,
            b'.' => dots += 1,
            _ => return false,
        }
    }
    digits > 0 && dots <= 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use UnrestrictedType::{Boolean, Datetime, Decimal, Double, Integer, String, Uuid};

    /// The full scalar matrix, written out so a change to `can_cast` has to be
    /// a deliberate edit here too. `.` accepts, `x` rejects.
    ///
    /// Rows are the source type, columns the target, both in `SCALARS` order.
    /// Verified against PostgreSQL 19beta1 over `new_ast_mds/pg19_parity`, with
    /// the `bool`/`int` pair as the documented int-width divergence.
    const SCALARS: [UnrestrictedType; 7] =
        [Boolean, Integer, Decimal, Double, String, Uuid, Datetime];

    const MATRIX: [&str; 7] = [
        // bool int dec dbl str uid dtm
        /* bool */ ". . x x . x x",
        /* int  */ ". . . . . x x",
        /* dec  */ "x . . . . x x",
        /* dbl  */ "x . . . . x x",
        /* str  */ ". . . . . . .",
        /* uid  */ "x x x x . . x",
        /* dtm  */ "x x x x . x .",
    ];

    #[test]
    fn cast_matrix() {
        for (row, from) in SCALARS.iter().enumerate() {
            let expected: Vec<char> = MATRIX[row].chars().filter(|c| !c.is_whitespace()).collect();
            assert_eq!(expected.len(), SCALARS.len(), "row {from} is malformed");
            for (col, to) in SCALARS.iter().enumerate() {
                let want = expected[col] == '.';
                assert_eq!(
                    can_cast(*from, *to),
                    want,
                    "can_cast({from}, {to}) should be {want}"
                );
            }
        }
    }

    #[test]
    fn non_scalars_are_not_ruled_on() {
        use sql_ir::ir::types::NestedType;
        let array = UnrestrictedType::Array(NestedType::Integer);
        for ty in SCALARS {
            assert!(can_cast(ty, array), "{ty} to an array");
            assert!(can_cast(array, ty), "an array to {ty}");
            assert!(can_cast(ty, UnrestrictedType::Map), "{ty} to map");
            assert!(can_cast(ty, UnrestrictedType::Any), "{ty} to any");
            assert!(can_cast(UnrestrictedType::Any, ty), "any to {ty}");
        }
    }

    #[test]
    fn relexing_rejects_what_is_not_a_bare_literal() {
        assert!(relexes_as_literal("1"));
        assert!(relexes_as_literal("1.5"));
        assert!(relexes_as_literal("true"));
        assert!(relexes_as_literal("false"));

        assert!(!relexes_as_literal(""));
        assert!(!relexes_as_literal(" 1 "));
        assert!(!relexes_as_literal("-1"));
        assert!(!relexes_as_literal("+1"));
        assert!(!relexes_as_literal("1.2.3"));
        assert!(!relexes_as_literal("."));
        assert!(!relexes_as_literal("1e5"));
        assert!(!relexes_as_literal("t"));
        assert!(!relexes_as_literal("TRUE"));
    }
}

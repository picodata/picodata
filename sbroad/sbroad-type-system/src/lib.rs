/// # Sbroad type system
///
/// The type system answers two questions:
///  * **Type checking**: For a given expression, ensure that all operators are defined for the operand types.
///  * **Type inference**: For a given expression, infer its resulting type.
///
/// Both tasks require a set of supported rules and allowed type coercions.
///
/// ## Rules and coercions
///
/// A rule defines an operator (or a function), its arguments' types, and the type of the return value.
/// Coercions are defined as mappings from a type to a set of types it can be coerced to.
///
/// `(int, int) -> int` is an example of a rule for "+" that takes two integer values and produces an integer.
///
/// In the following examples, numbers are assumed to have the integer type:
///  * `1 + 1`
///    Both operands are integers, so they match the rule `+(int, int) -> int`.
///  * `(1 + 2) + 3`
///    First, `(1 + 2)` matches the rule and returns an integer.
///    Then the result is added to an integer, so the expression matches the rule for integers.
///
/// To support numeric values, we need to add a rule: `+(numeric, numeric) -> numeric`,
/// making expressions like `1.5::numeric + 1.5::numeric` valid.
/// However, this won’t work for mixed expressions like `1 + 1.5`. A possible solution is to
/// add more rules with all possible combinations of types, but this is not scalable.
/// A better approach is to add a coercion from `int` to `numeric`, so `1 + 1.5` matches the
/// `+(numeric, numeric)` rule with implicit coercion of the integer value `1` to numeric `1.0`.
/// Adding this coercion can create ambiguity. For example, `1 + 1` matches both
/// the rule `+(int, int)` and `+(numeric, numeric)` with coercions.
/// The system resolves ambiguity by introducing costs. The cost is equal to the number of
/// coercions needed for an exact match; the rule with the lowest cost is chosen.
/// 1) `1 + 1` matches `+(int, int)` (cost: 0);
/// 2) `1 + 1` matches `+(numeric, numeric)` (cost: 2);
///
/// The first match is chosen as the cheapest.
///
/// ## Placeholders
///
/// The type system supports placeholders, which do not have a fixed type but can be defined by their context.
/// For instance, in the expression `1 + $1`, the placeholder should have the integer type because
/// it’s added to an integer.
/// The information about desired types is passed down from parent expressions.
/// For operators, desired types are defined according to the arguments' types.
///
/// Consider `1 + $1` with two overloads for the operator `+`:
/// `+(int, int) -> int` and `+(numeric, numeric) -> numeric`.
///
/// 1) `+(int, int) -> int`
///
///    Infer arguments' types:
///    * `1::int`, desired type = `int` ⇒ `1::int`, cost = 0
///    * `$1::unknown`, desired type = `int` ⇒ `$1::int`, cost = 0
///
///    Result: Matches the rule with a cost of 0.
/// 2) `+(numeric, numeric) -> numeric`
///
///    Infer arguments' types:
///    * `1::int`, desired type = `numeric` ⇒ `1::numeric`, cost = 1 (coercion)
///    * `$1::unknown`, desired type = `numeric` ⇒ `$1::numeric`, cost = 0
///
///    Result: Matches the rule with a cost of 1.
///
/// Both overloads match the expression, and the system selects one based on the desired type and costs:
/// 1) If there is a desired type, select the rule returning the desired type with the lowest cost (if any).
/// 2) Otherwise, select the rule with the lowest cost (if any).
/// 3) If multiple rules have the lowest cost, select the one whose return type is coercible to the
///    return types of all other rules (if any).
/// 4) If no rule is chosen, return an error.
///
/// This type system is inspired by CockroachDB’s Summer type system
/// ([RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160203_typing.md)).
/// However, the proposed algorithm is simpler and operates in a single pass, resulting in more
/// localized analysis. For example, in `SELECT $1 WHERE $1 = 1`, if we try analyze projection first,
/// the type of `$1` in the projection won't be inferred, even though it could theoretically
/// be determined from the selection.
pub mod error;
pub mod expr;
pub mod type_system;

pub use expr::{Expr, ExprKind};
pub use type_system::{Function, TypeReport, TypeSystem};

#[cfg(test)]
mod tests {
    use crate::{
        error::Error,
        expr::{Expr as GenericExpr, ExprKind as GenericExprKind, Type},
        type_system::{Function, FunctionKind, TypeAnalyzer as GenericTypeAnalyzer, TypeSystem},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use Type::*;

    type Expr = GenericExpr<usize>;
    type ExprKind = GenericExprKind<usize>;
    type TypeAnalyzer<'a> = GenericTypeAnalyzer<'a, usize>;

    fn expr(kind: ExprKind) -> Expr {
        static EXPR_COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = EXPR_COUNTER.fetch_add(1, Ordering::Relaxed);
        Expr::new(id, kind)
    }

    fn lit(ty: Type) -> Expr {
        expr(ExprKind::Literal(ty))
    }

    fn param(name: &str) -> Expr {
        let idx = name.strip_prefix("$").expect("params should start with $");
        let idx = idx.parse::<u16>().unwrap() - 1;
        expr(ExprKind::Parameter(idx))
    }

    fn binary(op: impl Into<String>, left: Expr, right: Expr) -> Expr {
        expr(ExprKind::Operator(op.into(), vec![left, right]))
    }

    fn coalesce(exprs: Vec<Expr>) -> Expr {
        expr(ExprKind::Coalesce(exprs))
    }

    fn cast(e: Expr, to: Type) -> Expr {
        expr(ExprKind::Cast(e.into(), to))
    }

    fn null() -> Expr {
        expr(ExprKind::Null)
    }

    fn case(when_exprs: Vec<Expr>, result_exprs: Vec<Expr>) -> Expr {
        expr(ExprKind::Case {
            when_exprs,
            result_exprs,
        })
    }

    fn default_type_system() -> TypeSystem {
        let functions = vec![
            Function::new_operator("+", [Type::Unsigned, Type::Unsigned], Type::Unsigned),
            Function::new_operator("+", [Type::Integer, Type::Integer], Type::Integer),
            Function::new_operator("+", [Type::Double, Type::Double], Type::Double),
            Function::new_operator("+", [Type::Numeric, Type::Numeric], Type::Numeric),
            Function::new_operator("||", [Type::Text, Type::Text], Type::Text),
            Function::new_operator("OR", [Type::Boolean, Type::Boolean], Type::Boolean),
            Function::new_operator("=", [Type::Integer, Type::Integer], Type::Boolean),
            Function::new_operator("=", [Type::Double, Type::Double], Type::Boolean),
            Function::new_operator("=", [Type::Numeric, Type::Numeric], Type::Boolean),
            Function::new_operator("=", [Type::Text, Type::Text], Type::Boolean),
            Function::new_operator("=", [Type::Boolean, Type::Boolean], Type::Boolean),
        ];

        TypeSystem::new(functions)
    }

    fn type_system_with_reordered_rules() -> TypeSystem {
        let functions = vec![
            Function::new_operator("=", [Type::Boolean, Type::Boolean], Type::Boolean),
            Function::new_operator("=", [Type::Double, Type::Double], Type::Boolean),
            Function::new_operator("+", [Type::Double, Type::Double], Type::Double),
            Function::new_operator("OR", [Type::Boolean, Type::Boolean], Type::Boolean),
            Function::new_operator("+", [Type::Numeric, Type::Numeric], Type::Numeric),
            Function::new_operator("||", [Type::Text, Type::Text], Type::Text),
            Function::new_operator("+", [Type::Integer, Type::Integer], Type::Integer),
            Function::new_operator("=", [Type::Text, Type::Text], Type::Boolean),
            Function::new_operator("=", [Type::Integer, Type::Integer], Type::Boolean),
            Function::new_operator("+", [Type::Unsigned, Type::Unsigned], Type::Unsigned),
            Function::new_operator("=", [Type::Numeric, Type::Numeric], Type::Boolean),
        ];

        TypeSystem::new(functions)
    }

    #[test]
    fn simple_tests() {
        let type_system = default_type_system();
        let type_system_reordered = type_system_with_reordered_rules();

        let exprs = [
            (binary("+", lit(Integer), lit(Integer)), None, Type::Integer),
            (binary("+", lit(Integer), lit(Double)), None, Double),
            (binary("+", lit(Integer), lit(Numeric)), None, Numeric),
            (binary("+", lit(Double), lit(Numeric)), None, Double),
            (binary("+", lit(Double), lit(Numeric)), Some(Double), Double),
            (binary("+", param("$1"), param("$2")), None, Unsigned),
            (binary("+", param("$1"), param("$2")), Some(Double), Double),
            (binary("+", lit(Unsigned), lit(Unsigned)), None, Unsigned),
            (binary("+", lit(Unsigned), lit(Integer)), None, Integer),
            (binary("+", lit(Unsigned), lit(Double)), None, Double),
            (
                binary("+", lit(Unsigned), lit(Unsigned)),
                Some(Numeric),
                Numeric,
            ),
            (
                binary("+", lit(Double), lit(Numeric)),
                Some(Numeric),
                Numeric,
            ),
            (
                binary("+", cast(lit(Text), Double), lit(Numeric)),
                None,
                Double,
            ),
            (
                binary("+", cast(lit(Text), Double), lit(Numeric)),
                Some(Double),
                Double,
            ),
            (
                binary("+", lit(Double), lit(Numeric)),
                Some(Numeric),
                Numeric,
            ),
            (
                binary("+", lit(Integer), lit(Integer)),
                Some(Numeric),
                Numeric,
            ),
            (
                binary("+", lit(Type::Integer), param("$1")),
                Some(Numeric),
                Numeric,
            ),
            (
                coalesce(vec![lit(Integer), lit(Integer), param("$1")]),
                None,
                Integer,
            ),
            (
                coalesce(vec![lit(Integer), lit(Integer), param("$1")]),
                Some(Double),
                Double,
            ),
            (
                coalesce(vec![
                    coalesce(vec![lit(Integer), lit(Integer), param("$1")]),
                    coalesce(vec![lit(Double), param("$2")]),
                    param("$3"),
                ]),
                None,
                Double,
            ),
            (
                coalesce(vec![null(), lit(Integer), param("$1")]),
                Some(Integer),
                Integer,
            ),
            (coalesce(vec![lit(Double), lit(Numeric)]), None, Double),
            (coalesce(vec![lit(Double), lit(Numeric)]), None, Double),
            (
                coalesce(vec![lit(Double), lit(Numeric)]),
                Some(Double),
                Double,
            ),
            (
                coalesce(vec![lit(Double), lit(Numeric)]),
                Some(Numeric),
                Numeric,
            ),
            (
                coalesce(vec![lit(Double), lit(Numeric), lit(Numeric), lit(Numeric)]),
                None,
                Double,
            ),
            (
                coalesce(vec![lit(Unsigned), lit(Integer), lit(Double), lit(Numeric)]),
                None,
                Double,
            ),
            (
                coalesce(vec![lit(Unsigned), lit(Integer), param("$1"), lit(Numeric)]),
                None,
                Numeric,
            ),
            (binary("=", lit(Text), param("$1")), None, Boolean),
            (binary("=", lit(Integer), param("$1")), None, Boolean),
            (binary("=", lit(Double), param("$1")), None, Boolean),
            (binary("=", lit(Numeric), param("$1")), None, Boolean),
            (binary("=", lit(Boolean), param("$1")), None, Boolean),
            (binary("=", null(), null()), None, Boolean),
            (binary("=", lit(Boolean), param("$1")), None, Boolean),
            (
                case(
                    vec![
                        binary("=", lit(Integer), lit(Integer)),
                        binary("=", lit(Integer), lit(Double)),
                    ],
                    vec![lit(Integer), lit(Double)],
                ),
                None,
                Double,
            ),
            (
                case(
                    vec![lit(Integer), lit(Numeric)],
                    vec![lit(Double), param("$1")],
                ),
                None,
                Double,
            ),
        ];

        for (expr, desired_type, result_type) in exprs {
            let mut analyzer = TypeAnalyzer::new(&type_system);
            let report = analyzer.analyze(&expr, desired_type).unwrap();
            assert_eq!(report.get_type(&expr.id), result_type);

            // For the 2nd run result will be returned from cache and should not change.
            let report = analyzer.analyze(&expr, desired_type).unwrap();
            assert_eq!(report.get_type(&expr.id), result_type);

            // Ensure that analysis result doesn't depends on functions order.
            let mut analyzer = TypeAnalyzer::new(&type_system_reordered);
            let report = analyzer.analyze(&expr, desired_type).unwrap();
            assert_eq!(report.get_type(&expr.id), result_type);
        }
    }

    #[test]
    fn simple_errors() {
        let type_system = default_type_system();
        let type_system_reordered = type_system_with_reordered_rules();

        let exprs = [
            (
                binary("+", lit(Type::Double), lit(Type::Text)),
                None,
                "could not resolve operator overload for +(double, text)",
            ),
            (
                binary("+", lit(Type::Text), lit(Type::Text)),
                None,
                "could not resolve operator overload for +(text, text)",
            ),
            (
                binary("-~>", lit(Type::Numeric), lit(Type::Boolean)),
                None,
                "operator -~> does not exist",
            ),
            (
                binary("-~>", param("$1"), lit(Type::Double)),
                None,
                "operator -~> does not exist",
            ),
            (
                coalesce(vec![lit(Text), lit(Integer), param("$1")]),
                None,
                "COALESCE types text, int and unknown cannot be matched",
            ),
            (
                coalesce(vec![lit(Text), lit(Integer), param("$1")]),
                Some(Text),
                "COALESCE types text, int and unknown cannot be matched",
            ),
            (
                coalesce(vec![lit(Boolean), lit(Integer), param("$1")]),
                Some(Text),
                "COALESCE types bool, int and unknown cannot be matched",
            ),
            (
                coalesce(vec![lit(Boolean), lit(Numeric), param("$1")]),
                Some(Text),
                "COALESCE types bool, numeric and unknown cannot be matched",
            ),
            (
                coalesce(vec![lit(Unsigned), lit(Integer), lit(Double), lit(Boolean)]),
                None,
                "COALESCE types unsigned, int, double and bool cannot be matched",
            ),
        ];

        for (expr, desired_type, err_msg) in exprs {
            let mut analyzer = TypeAnalyzer::new(&type_system);
            let error = analyzer.analyze(&expr, desired_type).unwrap_err();
            assert_eq!(error.to_string(), err_msg);

            let mut analyzer = TypeAnalyzer::new(&type_system_reordered);
            let error = analyzer.analyze(&expr, desired_type).unwrap_err();
            assert_eq!(error.to_string(), err_msg);
        }
    }

    #[test]
    fn complex_tests() {
        let type_system = default_type_system();
        let type_system_reordered = type_system_with_reordered_rules();

        let tests = [
            (
                binary("+", binary("+", param("$1"), param("$2")), lit(Integer)),
                None,
                Integer,
            ),
            (
                binary(
                    "+",
                    binary("+", param("$1"), coalesce(vec![param("$1"), lit(Double)])),
                    lit(Integer),
                ),
                None,
                Double,
            ),
            (
                binary(
                    "+",
                    binary("+", param("$1"), coalesce(vec![param("$1"), lit(Double)])),
                    lit(Integer),
                ),
                Some(Numeric),
                Numeric,
            ),
            (
                binary(
                    "+",
                    binary("+", param("$1"), binary("+", param("$2"), param("$3"))),
                    binary("+", param("$4"), binary("+", param("$5"), param("$6"))),
                ),
                Some(Numeric),
                Numeric,
            ),
            (
                binary(
                    "+",
                    binary("+", param("$1"), coalesce(vec![param("$2"), param("$3")])),
                    param("$4"),
                ),
                Some(Numeric),
                Numeric,
            ),
            (
                binary(
                    "=",
                    binary("||", param("$1"), coalesce(vec![param("$2"), lit(Text)])),
                    param("$4"),
                ),
                None,
                Boolean,
            ),
            (
                binary(
                    "=",
                    binary("||", param("$1"), coalesce(vec![param("$2"), param("$3")])),
                    param("$4"),
                ),
                None,
                Boolean,
            ),
            (
                binary(
                    "=",
                    binary("||", param("$1"), coalesce(vec![param("$2"), param("$3")])),
                    param("$4"),
                ),
                Some(Text),
                Boolean,
            ),
            (
                binary(
                    "=",
                    binary("||", param("$1"), coalesce(vec![param("$2"), param("$3")])),
                    null(),
                ),
                None,
                Boolean,
            ),
            (
                coalesce(vec![
                    binary("+", param("$1"), param("$2")),
                    binary("+", param("$3"), lit(Integer)),
                    coalesce(vec![null(), lit(Double), lit(Integer), param("$4")]),
                    coalesce(vec![null(), param("$5")]),
                    coalesce(vec![lit(Double), lit(Numeric)]),
                ]),
                None,
                Double,
            ),
            (
                coalesce(vec![
                    binary("+", param("$1"), param("$2")),
                    binary("+", param("$3"), null()),
                    coalesce(vec![null(), param("$4")]),
                    coalesce(vec![null(), param("$5")]),
                    coalesce(vec![
                        null(),
                        binary("+", param("$6"), coalesce(vec![null(), lit(Integer)])),
                    ]),
                ]),
                None,
                Integer,
            ),
            (
                coalesce(vec![
                    binary("+", param("$1"), param("$2")),
                    binary("+", param("$3"), null()),
                    coalesce(vec![null(), param("$4")]),
                    coalesce(vec![null(), param("$5")]),
                    coalesce(vec![
                        null(),
                        binary("+", param("$6"), coalesce(vec![null(), lit(Integer)])),
                    ]),
                ]),
                Some(Numeric),
                Numeric,
            ),
            (
                binary(
                    "||",
                    coalesce(vec![null(), param("$1"), param("$2")]),
                    coalesce(vec![null(), param("$3"), param("$4")]),
                ),
                None,
                Text,
            ),
            (
                cast(
                    binary(
                        "||",
                        coalesce(vec![null(), param("$1"), param("$2")]),
                        coalesce(vec![null(), param("$3"), param("$4")]),
                    ),
                    Text,
                ),
                None,
                Text,
            ),
            (
                cast(coalesce(vec![null(), param("$1"), param("$2")]), Text),
                None,
                Text,
            ),
            (
                cast(
                    coalesce(vec![
                        binary("+", param("$1"), param("$2")),
                        binary("+", param("$3"), lit(Integer)),
                        coalesce(vec![null(), lit(Double), lit(Integer), param("$4")]),
                        coalesce(vec![null(), param("$5")]),
                        coalesce(vec![lit(Double), lit(Numeric)]),
                    ]),
                    Double,
                ),
                None,
                Double,
            ),
            (
                cast(
                    coalesce(vec![
                        binary("+", param("$1"), param("$2")),
                        binary("+", param("$3"), lit(Integer)),
                        coalesce(vec![null(), lit(Double), lit(Integer), param("$4")]),
                        coalesce(vec![null(), param("$5")]),
                        coalesce(vec![lit(Double), lit(Numeric)]),
                    ]),
                    Numeric,
                ),
                None,
                Numeric,
            ),
            (
                cast(
                    coalesce(vec![
                        binary("+", param("$1"), param("$2")),
                        binary("+", param("$3"), lit(Integer)),
                        coalesce(vec![null(), lit(Double), lit(Integer), param("$4")]),
                        coalesce(vec![null(), param("$5")]),
                        coalesce(vec![lit(Double), lit(Numeric)]),
                    ]),
                    Integer,
                ),
                None,
                Integer,
            ),
            (
                binary("+", cast(lit(Double), Double), cast(lit(Numeric), Numeric)),
                None,
                Numeric,
            ),
            (
                binary("+", cast(lit(Double), Double), lit(Numeric)),
                None,
                Double,
            ),
        ];

        for (expr, desired_type, result_type) in tests {
            let mut analyzer = TypeAnalyzer::new(&type_system);
            let report = analyzer.analyze(&expr, desired_type).unwrap();
            assert_eq!(report.get_type(&expr.id), result_type);

            // For the 2nd run result will be returned from cache and should not change.
            let report = analyzer.analyze(&expr, desired_type).unwrap();
            assert_eq!(report.get_type(&expr.id), result_type);

            // Ensure that analysis result doesn't depends on functions order.
            let mut analyzer = TypeAnalyzer::new(&type_system_reordered);
            let report = analyzer.analyze(&expr, desired_type).unwrap();
            assert_eq!(report.get_type(&expr.id), result_type);
        }
    }

    #[test]
    fn parameters_inference() {
        let type_system = default_type_system();
        let mut analyzer = TypeAnalyzer::new(&type_system);

        // infer parameter type
        let expr = binary("+", lit(Integer), param("$1"));
        let _report = analyzer.analyze(&expr, None).unwrap();
        assert_eq!(analyzer.get_parameters_types(), &[Integer]);

        // analyze the same expression, type shouldn't change
        let _report = analyzer.analyze(&expr, None).unwrap();
        assert_eq!(analyzer.get_parameters_types(), &[Integer]);

        // infer the same type from another expression
        let expr = binary("+", param("$1"), lit(Integer));
        let _report = analyzer.analyze(&expr, None).unwrap();
        assert_eq!(analyzer.get_parameters_types(), &[Integer]);

        // ensure that we can use integer parameter in expressions with compatible types
        let expr = binary("+", param("$1"), lit(Numeric));
        let _report = analyzer.analyze(&expr, None).unwrap();
        assert_eq!(analyzer.get_parameters_types(), &[Integer]);

        // one more type with cache
        let expr = binary("+", param("$1"), lit(Numeric));
        let _report = analyzer.analyze(&expr, None).unwrap();
        assert_eq!(analyzer.get_parameters_types(), &[Integer]);

        // ensure new desired type do not change parameter type
        let expr = binary("+", param("$1"), lit(Numeric));
        let _report = analyzer.analyze(&expr, Some(Numeric)).unwrap();
        assert_eq!(analyzer.get_parameters_types(), &[Integer]);

        // ensure that int as an inferred parameter type cannot be matched with text
        let expr = binary("=", param("$1"), lit(Text));
        let err = analyzer.analyze(&expr, None).unwrap_err();
        assert_eq!(
            err,
            Error::CouldNotResolveOverload {
                kind: FunctionKind::Operator,
                name: "=".into(),
                argtypes: vec![Integer, Text],
            }
        );

        // ensure that expression is resolved to parameter type
        let expr = binary("+", param("$1"), param("$1"));
        let report = analyzer.analyze(&expr, None).unwrap();
        assert_eq!(report.get_type(&expr.id), Integer);
        assert_eq!(analyzer.get_parameters_types(), &[Integer]);

        // infer parameter type for $2 from $1
        let expr = binary("+", param("$1"), param("$2"));
        let report = analyzer.analyze(&expr, None).unwrap();
        assert_eq!(report.get_type(&expr.id), Integer);
        assert_eq!(analyzer.get_parameters_types(), &[Integer, Integer]);

        // ensure that parameter can be coerced to desired type without changing its type
        let expr = param("$1");
        let report = analyzer.analyze(&expr, Some(Double)).unwrap();
        assert_eq!(report.get_type(&expr.id), Double);
        assert_eq!(analyzer.get_parameters_types(), &[Integer, Integer]);

        // ensure that parameter type coercion works in homogeneous expressions
        let expr = coalesce(vec![param("$1"), lit(Double), param("$2")]);
        let report = analyzer.analyze(&expr, None).unwrap();
        assert_eq!(report.get_type(&expr.id), Double);
        assert_eq!(analyzer.get_parameters_types(), &[Integer, Integer]);
        if let ExprKind::Coalesce(children) = expr.kind {
            for child in &children {
                assert_eq!(report.get_type(&child.id), Double)
            }
        }

        // ensure that parameter type coercion works in more complex homogeneous expressions
        let expr = coalesce(vec![
            binary("+", param("$1"), param("$1")),
            binary("+", lit(Double), lit(Integer)),
            binary("+", param("$2"), param("$1")),
            binary("+", lit(Double), param("$1")),
        ]);
        let report = analyzer.analyze(&expr, None).unwrap();
        assert_eq!(report.get_type(&expr.id), Double);
        assert_eq!(analyzer.get_parameters_types(), &[Integer, Integer]);
        if let ExprKind::Coalesce(children) = expr.kind {
            for child in &children {
                assert_eq!(report.get_type(&child.id), Double)
            }
        }
    }

    #[test]
    fn ensure_no_caching_issues() {
        let type_system = default_type_system();

        let lparam = param("$1");
        let rparam = param("$2");
        let (lid, rid) = (lparam.id, rparam.id);
        let expr = binary("+", lparam, rparam);
        let mut analyzer = TypeAnalyzer::new(&type_system);

        let report = analyzer.analyze(&expr, None).unwrap();
        assert_eq!(report.get_type(&lid), Unsigned);
        assert_eq!(report.get_type(&rid), Unsigned);
        assert_eq!(analyzer.get_parameters_types(), &[Unsigned, Unsigned]);

        // Cache {
        //   (lid, Unsigned): Report { lid: Unsigned, $1::unsigned }
        //   (lid, Double):   Report { lid: Double, $2:double }
        //
        //   (rid, Unsigned): Report { rid: Unsigned, $2::unsigned }
        //   (rid, Double):   Report { rid: Double, $2::double }
        // }

        let report = analyzer.analyze(&expr, Some(Double)).unwrap();
        assert_eq!(report.get_type(&lid), Double);
        assert_eq!(report.get_type(&rid), Double);
        assert_eq!(analyzer.get_parameters_types(), &[Unsigned, Unsigned]);

        // analyze($1 + $2):
        // 1) +(Unsigned, Unsigned):
        //   r[0] = analyze_many($1, $2, [Unsigned, Unsigned]) // cached
        //
        // 2) +(Double, Double):
        //   // cached, but params are Double and Double, so this is actually a miss,
        //   // thanks to the parameters types check in `TypeAnalyzerCore::try_get_cached`.
        //   r[1] = analyze_many($1, $2, [Double, Double])
        //     $1::unsigned, desired = Double => $1::double (expr is coerced to double, but param type is unsigned)
        //     $2::unsigned, desired = Double => $2::double (expr is coerced to double, but param type is unsigned)
        //
        // return r[1] // r[1].returns = desired_type = Double
    }
}

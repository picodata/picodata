use std::collections::HashMap;

use crate::collection;
use crate::ir::node::ReferenceTarget::Leaf;
use crate::ir::relation::{DerivedType, Type};
use crate::ir::transformation::helpers::check_transformation;
use crate::ir::value::Value;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

use super::{EqClass, EqClassChain, EqClassConst, EqClassExpr, EqClassRef};

fn derive_equalities(plan: &mut Plan) {
    plan.derive_equalities().unwrap();
}

#[test]
fn equality_propagation1() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = 2 AND "c" = 1 OR "d" = 1"#;
    let actual_pattern_params = check_transformation(input, vec![], &derive_equalities);

    assert_eq!(
        actual_pattern_params.params,
        vec![
            Value::from(1_u64),
            Value::from(1_u64),
            Value::from(2_u64),
            Value::from(1_u64),
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE (((("t"."c" = CAST($1 AS unsigned)) and ("t"."a" = CAST($2 AS unsigned))) and ("t"."b" = CAST($3 AS unsigned))) and ("t"."c" = "t"."a")) or ("t"."d" = CAST($4 AS unsigned))"#
    );
}

#[test]
fn equality_propagation2() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = NULL AND "b" = NULL"#;
    let actual_pattern_params = check_transformation(input, vec![], &derive_equalities);

    assert_eq!(actual_pattern_params.params, vec![Value::Null, Value::Null]);
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ("t"."a" = $1) and ("t"."b" = $2)"#
    );
}

#[test]
fn equality_propagation3() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = null AND "a" = null"#;
    let actual_pattern_params = check_transformation(input, vec![], &derive_equalities);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::Null, Value::from(1_u64), Value::Null]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE (("t"."a" = $1) and ("t"."a" = CAST($2 AS unsigned))) and ("t"."b" = $3)"#
    );
}

#[test]
fn equality_propagation4() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = null AND "a" = null AND "b" = 1"#;
    let actual_pattern_params = check_transformation(input, vec![], &derive_equalities);

    assert_eq!(
        actual_pattern_params.params,
        vec![
            Value::from(1_u64),
            Value::Null,
            Value::from(1_u64),
            Value::Null,
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE (((("t"."b" = CAST($1 AS unsigned)) and ("t"."a" = $2)) and ("t"."a" = CAST($3 AS unsigned))) and ("t"."b" = $4)) and ("t"."b" = "t"."a")"#
    );
}

#[test]
fn equality_propagation5() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = 1 AND "c" = 1 AND "d" = 1"#;
    let actual_pattern_params = check_transformation(input, vec![], &derive_equalities);

    assert_eq!(
        actual_pattern_params.params,
        vec![
            Value::from(1_u64),
            Value::from(1_u64),
            Value::from(1_u64),
            Value::from(1_u64),
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE (((((("t"."d" = CAST($1 AS unsigned)) and ("t"."c" = CAST($2 AS unsigned))) and ("t"."a" = CAST($3 AS unsigned))) and ("t"."b" = CAST($4 AS unsigned))) and ("t"."d" = "t"."c")) and ("t"."c" = "t"."b")) and ("t"."b" = "t"."a")"#
    );
}

#[derive(Default)]
struct ColumnBuilder {
    next_pos: usize,
    name_to_pos: HashMap<&'static str, usize>,
}

impl ColumnBuilder {
    fn make_test_column(&mut self, name: &'static str) -> super::EqClassExpr {
        // assuming all columns refer to the same relational node,
        // different name means different position
        let position = *self.name_to_pos.entry(name).or_insert_with(|| {
            let p = self.next_pos;
            self.next_pos += 1;
            p
        });

        EqClassExpr::EqClassRef(EqClassRef {
            target: Leaf,
            position,
            col_type: DerivedType::new(Type::Integer),
            asterisk_source: None,
        })
    }
}

fn make_const(value: usize) -> EqClassExpr {
    EqClassExpr::EqClassConst(EqClassConst {
        value: Value::Unsigned(value as u64),
    })
}

#[test]
fn equality_classes() {
    let mut builder = ColumnBuilder::default();
    let mut eqcs = EqClassChain::new();
    let cola = builder.make_test_column("a");
    let val1 = make_const(1);
    let colb = builder.make_test_column("b");
    let colc = builder.make_test_column("c");
    let cold = builder.make_test_column("d");

    // { a, b, 1}
    eqcs.insert(&cola, &val1);
    eqcs.insert(&colb, &val1);

    assert_eq!(
        eqcs.list,
        vec![EqClass {
            set: collection!(cola.clone(), colb.clone(), val1.clone())
        }]
    );

    // { a, b, 1}, {c, d}
    eqcs.insert(&colc, &cold);

    assert_eq!(
        eqcs.list,
        vec![
            EqClass {
                set: collection!(cola.clone(), colb.clone(), val1.clone())
            },
            EqClass {
                set: collection!(colc.clone(), cold.clone())
            }
        ]
    );

    // { a, b, 1, c, d}
    eqcs.insert(&colc, &val1);

    let expected = vec![EqClass {
        set: collection!(
            cola.clone(),
            colb.clone(),
            val1.clone(),
            colc.clone(),
            cold.clone()
        ),
    }];
    assert_eq!(eqcs.list, expected);

    // test we don't create equality classes with nulls
    // as it's useless
    let null = EqClassExpr::EqClassConst(EqClassConst { value: Value::Null });
    eqcs.insert(&cola, &null);
    assert_eq!(eqcs.list, expected);

    // we used only c = d, so substruct pairs
    // should return {a, b}
    // note: we don't need {a, b, 1}, because
    // a = 1 and b = 1
    // is already present in expression
    let substructed = eqcs.subtract_pairs();
    assert_eq!(
        substructed.list,
        vec![EqClass {
            set: collection!(cola.clone(), colb.clone())
        },]
    );
}

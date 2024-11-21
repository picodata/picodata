use std::collections::HashMap;

use crate::collection;
use crate::ir::relation::Type;
use crate::ir::transformation::helpers::check_transformation;
use crate::ir::value::Value;
use crate::ir::Plan;
use crate::{backend::sql::ir::PatternWithParams, ir::node::NodeId};
use pretty_assertions::assert_eq;

use super::{EqClass, EqClassChain, EqClassConst, EqClassExpr, EqClassRef};

fn derive_equalities(plan: &mut Plan) {
    plan.derive_equalities().unwrap();
}

#[test]
fn equality_propagation1() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = 2 AND "c" = 1 OR "d" = 1"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ("t"."c") = (?) and ("t"."a") = (?) and ("t"."b") = (?)"#,
            r#"and ("t"."c") = ("t"."a") or ("t"."d") = (?)"#,
        ),
        vec![
            Value::from(1_u64),
            Value::from(1_u64),
            Value::from(2_u64),
            Value::from(1_u64),
        ],
    );

    assert_eq!(
        check_transformation(input, vec![], &derive_equalities),
        expected
    );
}

#[test]
fn equality_propagation2() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = NULL AND "b" = NULL"#;

    let expected = PatternWithParams::new(
        r#"SELECT "t"."a" FROM "t" WHERE ("t"."a") = (?) and ("t"."b") = (?)"#.to_string(),
        vec![Value::Null, Value::Null],
    );

    assert_eq!(
        check_transformation(input, vec![], &derive_equalities),
        expected
    );
}

#[test]
fn equality_propagation3() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = null AND "a" = null"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ("t"."a") = (?) and ("t"."a") = (?) and ("t"."b") = (?)"#,
        ),
        vec![Value::Null, Value::from(1_u64), Value::Null],
    );

    assert_eq!(
        check_transformation(input, vec![], &derive_equalities),
        expected
    );
}

#[test]
fn equality_propagation4() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = null AND "a" = null AND "b" = 1"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ("t"."b") = (?) and ("t"."a") = (?) and ("t"."a") = (?)"#,
            r#"and ("t"."b") = (?) and ("t"."a") = ("t"."b")"#,
        ),
        vec![
            Value::from(1_u64),
            Value::Null,
            Value::from(1_u64),
            Value::Null,
        ],
    );

    assert_eq!(
        check_transformation(input, vec![], &derive_equalities),
        expected
    );
}

#[test]
fn equality_propagation5() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = 1 AND "c" = 1 AND "d" = 1"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ("t"."d") = (?) and ("t"."c") = (?)"#,
            r#"and ("t"."a") = (?) and ("t"."b") = (?)"#,
            r#"and ("t"."a") = ("t"."d") and ("t"."d") = ("t"."c")"#,
            r#"and ("t"."c") = ("t"."b")"#,
        ),
        vec![
            Value::from(1_u64),
            Value::from(1_u64),
            Value::from(1_u64),
            Value::from(1_u64),
        ],
    );

    assert_eq!(
        check_transformation(input, vec![], &derive_equalities),
        expected
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
            targets: Some(vec![0]),
            position,
            parent: Some(NodeId {
                offset: 0,
                arena_type: crate::ir::node::ArenaType::Arena64,
            }),
            col_type: Type::Integer,
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

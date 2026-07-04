use sql::helpers::sql_to_optimized_ir;

#[test]
fn except_transform_with_dag_plan() {
    // In this plan we have Const node referred twice:
    // both `data` and `output` of `ValuesRow` fields
    // are refferring to it. Let's check except transformation
    // with global table works in this case.

    let input =
        r#"explain (logical) select 1 from (values (1)) except select e from t2 where e = 1"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r"
    except
      projection (1::int -> col_1)
        scan unnamed_subquery
          motion [policy: full, program: ReshardIfNeeded]
            values
              value ROW(1::int)
      motion [policy: full, program: ReshardIfNeeded]
        intersect
          projection (t2.e::int -> e)
            selection (t2.e::int = 1::int)
              scan t2
          projection (1::int -> col_1)
            scan unnamed_subquery
              motion [policy: full, program: ReshardIfNeeded]
                values
                  value ROW(1::int)
    ");
}

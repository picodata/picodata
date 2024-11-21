use crate::{
    frontend::Ast,
    ir::node::{ddl::Ddl, CreateTable},
};
use pretty_assertions::assert_eq;
use smol_str::{SmolStr, ToSmolStr};

use crate::{
    executor::engine::mock::RouterConfigurationMock,
    frontend::sql::ast::AbstractSyntaxTree,
    ir::{ddl::ColumnDef, relation::Type},
};

#[test]
fn infer_not_null_on_pk1() {
    let input = r#"create table t (a int primary key) distributed globally"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata).unwrap();
    let top_id = plan.get_top().unwrap();
    let top_node = plan.get_ddl_node(top_id).unwrap();

    let Ddl::CreateTable(CreateTable {
        format,
        primary_key,
        ..
    }) = top_node
    else {
        panic!("expected create table")
    };

    let def = ColumnDef {
        name: "a".into(),
        data_type: Type::Integer,
        is_nullable: false,
    };

    assert_eq!(format, &vec![def]);

    let expected_pk: Vec<SmolStr> = vec!["a".into()];
    assert_eq!(primary_key, &expected_pk);
}

#[test]
fn infer_not_null_on_pk2() {
    let input =
        r#"create table t (a int, b int not null, c int, primary key (a, b)) distributed globally"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata).unwrap();
    let top_id = plan.get_top().unwrap();
    let top_node = plan.get_ddl_node(top_id).unwrap();

    let Ddl::CreateTable(CreateTable {
        format,
        primary_key,
        ..
    }) = top_node
    else {
        panic!("expected create table")
    };

    let def_a = ColumnDef {
        name: "a".into(),
        data_type: Type::Integer,
        is_nullable: false,
    };

    let def_b = ColumnDef {
        name: "b".into(),
        data_type: Type::Integer,
        is_nullable: false,
    };

    let def_c = ColumnDef {
        name: "c".into(),
        data_type: Type::Integer,
        is_nullable: true,
    };

    assert_eq!(format, &vec![def_a, def_b, def_c]);

    let expected_pk: Vec<SmolStr> = vec!["a".into(), "b".into()];
    assert_eq!(primary_key, &expected_pk);
}

#[test]
fn infer_not_null_on_pk3() {
    let input = r#"create table t (a int null, b int not null, c int, primary key (a, b)) distributed globally"#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata).unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("Primary key mustn't contain nullable columns.")
    );
}

#[test]
fn infer_sk_from_pk() {
    let input = r#"create table t ("a" int, "b" int, c int, primary key ("a", "b"))"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata).unwrap();
    let top_id = plan.get_top().unwrap();
    let top_node = plan.get_ddl_node(top_id).unwrap();

    let Ddl::CreateTable(CreateTable { sharding_key, .. }) = top_node else {
        panic!("expected create table")
    };

    assert_eq!(
        sharding_key.as_ref().unwrap(),
        &vec!["a".to_smolstr(), "b".to_smolstr()]
    );
}

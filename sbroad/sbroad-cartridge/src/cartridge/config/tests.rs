use super::*;
use pretty_assertions::assert_eq;

#[test]
fn test_yaml_schema_parser() {
    let test_schema = "spaces:
  EMPLOYEES:
    engine: memtx
    is_local: false
    temporary: false
    format:
      - name: ID
        is_nullable: false
        type: number
      - name: sysFrom
        is_nullable: false
        type: number
      - name: FIRST_NAME
        is_nullable: false
        type: string
      - name: sysOp
        is_nullable: false
        type: number
      - name: bucket_id
        is_nullable: true
        type: unsigned
    indexes:
      - type: TREE
        name: ID
        unique: true
        parts:
          - path: ID
            type: number
            is_nullable: false
          - path: sysFrom
            type: number
            is_nullable: false
      - type: TREE
        name: bucket_id
        unique: false
        parts:
          - path: bucket_id
            type: unsigned
            is_nullable: true
    sharding_key:
      - ID
  hash_testing:
    is_local: false
    temporary: false
    engine: memtx
    format:
      - name: identification_number
        type: integer
        is_nullable: false
      - name: product_code
        type: string
        is_nullable: false
      - name: product_units
        type: integer
        is_nullable: false
      - name: sys_op
        type: number
        is_nullable: false
      - name: bucket_id
        type: unsigned
        is_nullable: true
    indexes:
      - name: id
        unique: true
        type: TREE
        parts:
          - path: identification_number
            is_nullable: false
            type: integer
      - name: bucket_id
        unique: false
        parts:
          - path: bucket_id
            is_nullable: true
            type: unsigned
        type: TREE
    sharding_key:
      - identification_number
      - product_code";

    let mut s = RouterConfiguration::new();
    s.load_schema(test_schema).unwrap();

    let expected_keys = vec!["identification_number", "product_code"];
    let actual_keys = s.sharding_key_by_space("hash_testing").unwrap();
    assert_eq!(actual_keys, expected_keys);
}

#[test]
fn test_getting_table_segment() {
    let test_schema = "spaces:
  hash_testing:
    is_local: false
    temporary: false
    engine: memtx
    format:
      - name: identification_number
        type: integer
        is_nullable: false
      - name: product_code
        type: string
        is_nullable: false
      - name: product_units
        type: boolean
        is_nullable: false
      - name: sys_op
        type: number
        is_nullable: false
      - name: detail
        type: array
        is_nullable: false
      - name: bucket_id
        type: unsigned
        is_nullable: true
    indexes:
      - name: id
        unique: true
        type: TREE
        parts:
          - path: identification_number
            is_nullable: false
            type: integer
      - name: bucket_id
        unique: false
        parts:
          - path: bucket_id
            is_nullable: true
            type: unsigned
        type: TREE
    sharding_key:
      - identification_number
      - product_code";

    let mut s = RouterConfiguration::new();
    s.set_sharding_column("bucket_id".into());
    s.load_schema(test_schema).unwrap();

    let expected = Table::new_sharded(
        "hash_testing",
        vec![
            Column::new(
                "identification_number",
                Type::Integer,
                ColumnRole::User,
                false,
            ),
            Column::new("product_code", Type::String, ColumnRole::User, false),
            Column::new("product_units", Type::Boolean, ColumnRole::User, false),
            Column::new("sys_op", Type::Number, ColumnRole::User, false),
            Column::new("detail", Type::Array, ColumnRole::User, false),
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding, true),
        ],
        &["identification_number", "product_code"],
        &["identification_number"],
        SpaceEngine::Memtx,
    )
    .unwrap();

    assert_eq!(
        s.table("invalid_table").unwrap_err(),
        SbroadError::NotFound(Entity::Space, r#"invalid_table"#.into())
    );
    assert_eq!(s.table("hash_testing").unwrap(), expected);
}

#[test]
fn test_waiting_timeout() {
    let mut s = RouterConfiguration::new();
    s.set_waiting_timeout(200);

    assert_ne!(s.waiting_timeout(), 360);

    assert_eq!(s.waiting_timeout(), 200);
}

#[test]
fn test_invalid_schema() {
    let test_schema = r#"spaces:
      TEST_SPACE:
        engine: memtx
        is_local: false
        temporary: false
        format:
          - name: bucket_id
            type: unsigned
            is_nullable: false
          - name: FID
            type: invalid_type
            is_nullable: false
        indexes:
          - type: TREE
            name: primary
            unique: true
            parts:
              - path: FID
                type: integer
                is_nullable: false
          - type: TREE
            name: bucket_id
            unique: false
            parts:
              - path: bucket_id
                type: unsigned
                is_nullable: true
        sharding_key:
          - FID
"#;

    let mut s = RouterConfiguration::new();
    s.set_sharding_column("\"bucket_id\"".into());

    assert_eq!(
        s.load_schema(test_schema).unwrap_err(),
        SbroadError::Invalid(
            Entity::Type,
            Some("Unable to transform invalid_type to Type.".into())
        )
    );
}

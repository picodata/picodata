use super::*;
use insta::assert_yaml_snapshot;
use pretty_assertions::assert_eq;
use tarantool::decimal;

#[test]
fn boolean() {
    assert_eq!(Value::Boolean(true), Value::from(true));
    assert_eq!(Value::Boolean(false), Value::from(false));
    assert_ne!(Value::from(true), Value::from(false));
}

#[test]
fn uuid() {
    let uid = uuid::Uuid::new_v4();
    let t_uid_1 = Uuid::parse_str(&uid.to_string()).unwrap();
    let t_uid_2 = Uuid::parse_str(&uuid::Uuid::new_v4().to_string()).unwrap();
    let v_uid = Value::Uuid(t_uid_1);

    assert_eq!(Value::from(t_uid_1), v_uid);
    assert_eq!(format!("{}", v_uid), uid.to_string());
    assert_eq!(v_uid.get_type(), DerivedType::new(UnrestrictedType::Uuid));
    assert_eq!(v_uid.eq(&Value::Uuid(t_uid_1)), Trivalent::True);
    assert_eq!(v_uid.eq(&Value::Uuid(t_uid_2)), Trivalent::False);
    assert_eq!(
        v_uid.eq(&Value::String(t_uid_1.to_string())),
        Trivalent::False
    );
    assert_eq!(
        v_uid.partial_cmp(&Value::Uuid(t_uid_1)),
        Some(TrivalentOrdering::Equal)
    );
    assert_ne!(
        v_uid.partial_cmp(&Value::Uuid(t_uid_2)),
        Some(TrivalentOrdering::Equal)
    );
    assert_eq!(
        Value::String(uid.to_string())
            .cast_and_encode(&DerivedType::new(UnrestrictedType::Uuid))
            .is_ok(),
        true
    );
    assert_eq!(v_uid.partial_cmp(&Value::String(t_uid_2.to_string())), None);
}

#[test]
fn uuid_negative() {
    assert_eq!(
        Value::String("hello".to_string())
            .cast_and_encode(&DerivedType::new(UnrestrictedType::Uuid))
            .unwrap_err(),
        SbroadError::Invalid(
            Entity::Value,
            Some(SmolStr::from("Failed to cast 'hello' to uuid."))
        )
    );
}

#[test]
fn decimal() {
    assert_eq!(Value::from(decimal!(0)), Value::from(Decimal::from(0)));
    assert_eq!(
        Value::from(Decimal::from(0)),
        Value::from(Decimal::from_str("0.0").unwrap())
    );
    assert_eq!(
        Value::from(Decimal::from_str("1.000000000000000").unwrap()),
        Value::from(Decimal::from_str("1").unwrap())
    );
    assert_eq!(
        Value::from(Decimal::from_str("9223372036854775807").unwrap()),
        Value::from(Decimal::from(9_223_372_036_854_775_807_i64))
    );
    assert_ne!(
        Value::from(decimal!(1)),
        Value::from("0.9999999999999999".parse::<Decimal>().unwrap())
    );
}

#[test]
#[allow(clippy::excessive_precision)]
fn double() {
    assert_eq!(
        Value::Double(0.0_f64.into()),
        Value::from(Double::from(0.0000_f64))
    );
    assert_eq!(
        Value::Double(0.999_999_999_999_999_7e-308_f64.into()),
        Value::from(Double::from(1e-308_f64))
    );
    assert_eq!(Value::from(f64::NAN), Value::Null);
    assert_ne!(
        Value::Double(Double::from(0.999_999_999_999_999_6e-308_f64)),
        Value::from(Double::from(0.999_999_999_999_999_7e-308_f64))
    );
}

#[test]
fn integer() {
    assert_eq!(Value::Integer(0), Value::from(0_i64));
    assert_eq!(
        Value::Integer(9_223_372_036_854_775_807_i64),
        Value::from(9_223_372_036_854_775_807_i64)
    );
    assert_eq!(
        Value::Integer(-9_223_372_036_854_775_807_i64),
        Value::from(-9_223_372_036_854_775_807_i64)
    );
    assert_ne!(
        Value::Integer(9_223_372_036_854_775_807_i64),
        Value::from(-9_223_372_036_854_775_807_i64)
    );
}

#[test]
fn string() {
    assert_eq!(Value::String(String::new()), Value::from(""));
    assert_eq!(Value::String("hello".to_string()), Value::from("hello"));
    assert_eq!(
        Value::String("hello".to_string()),
        Value::from("hello".to_string())
    );
    assert_ne!(
        Value::String("hello".to_string()),
        Value::from("world".to_string())
    );
}

#[test]
fn tuple() {
    let t = Tuple::from(vec![Value::Integer(0), Value::String("hello".to_string())]);
    assert_eq!(
        Value::Tuple(t),
        Value::from(vec![Value::from(0), Value::from("hello")])
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn equivalence() {
    // Boolean
    assert_eq!(
        Trivalent::False,
        Value::Boolean(true).eq(&Value::Boolean(false))
    );
    assert_eq!(
        Trivalent::False,
        Value::Boolean(true).eq(&Value::from(Double::from(1e0_f64)))
    );
    assert_eq!(
        Trivalent::False,
        Value::Boolean(true).eq(&Value::from(decimal!(1e0)))
    );
    assert_eq!(Trivalent::False, Value::Boolean(false).eq(&Value::from(0)));
    assert_eq!(
        Trivalent::False,
        Value::Boolean(true).eq(&Value::from(1_i64))
    );
    assert_eq!(Trivalent::False, Value::Boolean(false).eq(&Value::from("")));
    assert_eq!(
        Trivalent::False,
        Value::Boolean(true).eq(&Value::from("hello"))
    );
    assert_eq!(
        Trivalent::True,
        Value::Boolean(true).eq(&Value::Boolean(true))
    );
    assert_eq!(Trivalent::Unknown, Value::Boolean(true).eq(&Value::Null));

    // Decimal
    assert_eq!(
        Trivalent::True,
        Value::from(decimal!(0.000)).eq(&Value::from(0))
    );
    assert_eq!(
        Trivalent::True,
        Value::from(decimal!(0.000)).eq(&Value::from(decimal!(0)))
    );
    assert_eq!(
        Trivalent::True,
        Value::from(decimal!(0.000)).eq(&Value::from(0))
    );
    assert_eq!(
        Trivalent::True,
        Value::from(decimal!(0.000)).eq(&Value::from(0_i64))
    );
    assert_eq!(
        Trivalent::False,
        Value::from(decimal!(0.000)).eq(&Value::from(false))
    );
    assert_eq!(
        Trivalent::False,
        Value::from(decimal!(0.000)).eq(&Value::from(""))
    );
    assert_eq!(
        Trivalent::Unknown,
        Value::from(decimal!(0.000)).eq(&Value::Null)
    );

    // Double
    assert_eq!(
        Trivalent::True,
        Value::Double(Double::from(0.000_f64)).eq(&Value::from(0))
    );
    assert_eq!(
        Trivalent::True,
        Value::Double(Double::from(0.000_f64)).eq(&Value::from(0_i64))
    );
    assert_eq!(
        Trivalent::False,
        Value::Double(Double::from(0.000_f64)).eq(&Value::from(false))
    );
    assert_eq!(
        Trivalent::False,
        Value::Double(Double::from(0.000_f64)).eq(&Value::from(""))
    );
    assert_eq!(
        Trivalent::Unknown,
        Value::Double(Double::from(0.000_f64)).eq(&Value::Null)
    );
    assert_eq!(
        Trivalent::True,
        Value::Double(Double::from(f64::INFINITY)).eq(&Value::from(f64::INFINITY))
    );
    assert_eq!(
        Trivalent::True,
        Value::Double(Double::from(f64::NEG_INFINITY)).eq(&Value::from(f64::NEG_INFINITY))
    );
    assert_eq!(
        Trivalent::Unknown,
        Value::Double(Double::from(f64::NAN)).eq(&Value::from(f64::NAN))
    );

    // Null
    assert_eq!(Trivalent::Unknown, Value::Null.eq(&Value::Null));
    assert_eq!(Trivalent::Unknown, Value::Null.eq(&Value::Boolean(false)));
    assert_eq!(
        Trivalent::Unknown,
        Value::Null.eq(&Value::Double(f64::NAN.into()))
    );
    assert_eq!(Trivalent::Unknown, Value::Null.eq(&Value::from("")));

    // String
    assert_eq!(
        Trivalent::False,
        Value::from("hello").eq(&Value::from(" hello "))
    );
    assert_eq!(
        Trivalent::True,
        Value::from("hello").eq(&Value::from("hello".to_string()))
    );
    assert_eq!(Trivalent::Unknown, Value::from("hello").eq(&Value::Null));
}

#[test]
#[allow(clippy::too_many_lines)]
fn arithmetic() {
    // Add
    assert_eq!(
        Value::from(decimal!(1.000)),
        Value::from(decimal!(0.000))
            .add(&Value::from(1.000))
            .unwrap()
    );
    assert_eq!(
        Value::from(decimal!(1.000)),
        Value::from(decimal!(0.000))
            .add(&Value::Double(Double { value: 1.0 }))
            .unwrap()
    );
    assert_eq!(
        Err(SbroadError::Invalid(
            Entity::Value,
            Some("Can't cast Double(Double { value: NaN }) to decimal".to_smolstr()),
        )),
        Value::Double(Double::from(f64::NAN)).add(&Value::Integer(1))
    );

    // Sub
    assert_eq!(
        Value::from(decimal!(1.000)),
        Value::from(decimal!(2.000))
            .sub(&Value::Double(Double { value: 1.0 }))
            .unwrap()
    );
    assert_eq!(
        Value::from(decimal!(5.500)),
        Value::from(decimal!(8.000))
            .sub(&Value::from(2.500))
            .unwrap()
    );

    // Mult
    assert_eq!(
        Value::from(decimal!(8.000)),
        Value::from(decimal!(2.000)).mult(&Value::from(4)).unwrap()
    );
    assert_eq!(
        Value::from(decimal!(3.999)),
        Value::from(3).mult(&Value::from(1.333)).unwrap()
    );
    assert_eq!(
        Value::from(decimal!(555)),
        Value::from(5.0).mult(&Value::Integer(111)).unwrap()
    );

    // Div
    assert_eq!(
        Value::from(decimal!(3)),
        Value::from(9.0).div(&Value::Integer(3)).unwrap()
    );
    assert_eq!(
        Value::from(decimal!(1)),
        Value::Integer(2).div(&Value::Integer(2)).unwrap()
    );
    assert_eq!(
        Err(SbroadError::Invalid(
            Entity::Value,
            Some(
                "Only numerical values can be casted to Decimal. String(\"\") was met".to_smolstr()
            )
        )),
        Value::from("").div(&Value::Integer(2))
    );
    assert_eq!(
        Err(SbroadError::Invalid(
            Entity::Value,
            Some("Can not divide Integer(1) by zero Integer(0)".to_smolstr())
        )),
        Value::Integer(1).div(&Value::Integer(0))
    );

    // Negate
    assert_eq!(
        Value::from(decimal!(-3)),
        Value::from(3.0).negate().unwrap()
    );
    assert_eq!(
        Value::from(decimal!(-1)),
        Value::Integer(1).negate().unwrap()
    );
    assert_eq!(
        Value::from(decimal!(0)),
        Value::Double(Double::from(0.0)).negate().unwrap()
    );
}

#[test]
fn concatenation() {
    assert_eq!(
        Value::from("hello"),
        Value::from("").concat(&Value::from("hello")).unwrap()
    );
    assert_eq!(
        Value::from("ab"),
        Value::from("a").concat(&Value::from("b")).unwrap()
    );
    assert_eq!(
        Err(SbroadError::Invalid(
            Entity::Value,
            Some("Integer(1) and String(\"b\") must be strings to be concatenated".to_smolstr())
        )),
        Value::Integer(1).concat(&Value::from("b"))
    )
}

#[test]
fn and_or() {
    // And
    assert_eq!(
        Value::from(true),
        Value::from(true).and(&Value::from(true)).unwrap()
    );
    assert_eq!(
        Value::from(false),
        Value::from(false).and(&Value::from(true)).unwrap()
    );
    assert_eq!(
        Value::from(false),
        Value::from(true).and(&Value::from(false)).unwrap()
    );
    assert_eq!(
        Value::from(false),
        Value::from(false).and(&Value::from(false)).unwrap()
    );

    // Or
    assert_eq!(
        Value::from(true),
        Value::from(true).or(&Value::from(false)).unwrap()
    );
    assert_eq!(
        Value::from(false),
        Value::from(false).or(&Value::from(false)).unwrap()
    );
    assert_eq!(
        Err(SbroadError::Invalid(
            Entity::Value,
            Some(
                "Integer(1) and Boolean(false) must be booleans to be applied to OR operation"
                    .to_smolstr()
            )
        )),
        Value::Integer(1).or(&Value::from(false))
    );
}

#[test]
fn trivalent() {
    assert_eq!(
        Trivalent::False,
        Value::from(Trivalent::False).eq(&Value::Boolean(true))
    );
    assert_eq!(
        Trivalent::True,
        Value::from(Trivalent::True).eq(&Value::Boolean(true))
    );
    assert_eq!(
        Trivalent::Unknown,
        Value::from(Trivalent::False).eq(&Value::Null)
    );
    assert_eq!(Trivalent::Unknown, Value::Null.eq(&Value::Null));
}

#[test]
fn array_tuple_cast_coerces_elements() {
    let t = Tuple::from(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);
    let v = Value::Tuple(t.clone());

    // `Any` element type carries avoids cloning.
    let encoded = v
        .cast_and_encode(&DerivedType::new(UnrestrictedType::Array(NestedType::Any)))
        .unwrap();
    if let EncodedValue::Owned(_) = encoded {
        panic!("expected pass-through by reference");
    }

    // A typed array whose elements already match the element type needs no coercion.
    let encoded = v
        .cast_and_encode(&DerivedType::new(UnrestrictedType::Array(
            NestedType::Integer,
        )))
        .unwrap();
    if let EncodedValue::Owned(_) = encoded {
        panic!("expected pass-through by reference");
    }

    // Integers stay integers when cast to an INT array.
    let casted = v
        .clone()
        .cast(UnrestrictedType::Array(NestedType::Integer))
        .unwrap();
    assert_yaml_snapshot!(casted, @r"
    Tuple:
      - Integer: 1
      - Integer: 2
      - Integer: 3
    ");

    // An element that needs real conversion runs per-element coercion, which clones into an owned value.
    let encoded = v
        .cast_and_encode(&DerivedType::new(UnrestrictedType::Array(
            NestedType::Double,
        )))
        .unwrap();
    assert!(matches!(encoded, EncodedValue::Owned(_)));

    // Decimal elements coerce to Double when cast to a DOUBLE array column type.
    let dec = Value::Tuple(Tuple::from(vec![
        Value::from(Decimal::from_str("1").unwrap()),
        Value::from(Decimal::from_str("2.5").unwrap()),
        Value::Null,
    ]));
    let casted = dec
        .cast(UnrestrictedType::Array(NestedType::Double))
        .unwrap();
    assert_yaml_snapshot!(casted, @r#"
    Tuple:
      - Double: 1
      - Double: 2.5
      - "Null"
    "#);

    let err = Value::Integer(7)
        .cast(UnrestrictedType::Array(NestedType::Any))
        .unwrap_err();
    assert!(matches!(err, SbroadError::Invalid(Entity::Value, _)));
}

#[test]
fn cast_array_truncates_decimal_and_double_to_int() {
    let decimals = Value::Tuple(Tuple::from(vec![
        Value::from(Decimal::from_str("1.4").unwrap()),
        Value::from(Decimal::from_str("2.5").unwrap()),
        Value::from(Decimal::from_str("-1.5").unwrap()),
    ]));
    assert_yaml_snapshot!(decimals.cast_array(NestedType::Integer).unwrap(), @r"
    Tuple:
      - Integer: 1
      - Integer: 2
      - Integer: -1
    ");

    let doubles = Value::Tuple(Tuple::from(vec![
        Value::Double(1.4_f64.into()),
        Value::Double(2.5_f64.into()),
        Value::Double((-1.5_f64).into()),
    ]));
    assert_yaml_snapshot!(doubles.cast_array(NestedType::Integer).unwrap(), @r"
    Tuple:
      - Integer: 1
      - Integer: 2
      - Integer: -1
    ");
}

#[test]
fn cast_scalar_decimal_and_double_truncate_to_int() {
    fn decimal_cast(x: &str) -> Value {
        let d = Decimal::from_str(x).unwrap();
        Value::from(d).cast(UnrestrictedType::Integer).unwrap()
    }

    assert_yaml_snapshot!(decimal_cast("1.4"), @"Integer: 1");
    assert_yaml_snapshot!(decimal_cast("2.5"), @"Integer: 2");
    assert_yaml_snapshot!(decimal_cast("-1.5"), @"Integer: -1");
    assert_yaml_snapshot!(decimal_cast("7"), @"Integer: 7");

    fn double_cast(x: f64) -> Value {
        let d = Value::Double(x.into());
        d.cast(UnrestrictedType::Integer).unwrap()
    }

    assert_yaml_snapshot!(double_cast(1.4), @"Integer: 1");
    assert_yaml_snapshot!(double_cast(2.5), @"Integer: 2");
    assert_yaml_snapshot!(double_cast(-1.5), @"Integer: -1");
    assert_yaml_snapshot!(double_cast(7.), @"Integer: 7");

    assert_yaml_snapshot!(
        Value::Integer(42).cast(UnrestrictedType::Integer).unwrap(),
        @"Integer: 42"
    );
    assert_yaml_snapshot!(Value::Null.cast(UnrestrictedType::Integer).unwrap(), @r#""Null""#);
}

#[test]
fn cast_array_nulls_empty_and_delegates() {
    assert_yaml_snapshot!(
        Value::Null.cast_array(NestedType::Integer).unwrap(),
        @r#""Null""#
    );

    let with_null = Value::Tuple(Tuple::from(vec![Value::Integer(1), Value::Null]));
    assert_yaml_snapshot!(
        with_null.cast_array(NestedType::Integer).unwrap(),
        @r#"
    Tuple:
      - Integer: 1
      - "Null"
    "#
    );

    assert_yaml_snapshot!(
        Value::Tuple(Tuple::from(vec![]))
            .cast_array(NestedType::Integer)
            .unwrap(),
        @"Tuple: []"
    );

    let to_double = Value::Tuple(Tuple::from(vec![Value::Integer(1), Value::Integer(2)]));
    assert_yaml_snapshot!(
        to_double.cast_array(NestedType::Double).unwrap(),
        @r"
    Tuple:
      - Double: 1
      - Double: 2
    "
    );

    let err = Value::Integer(7)
        .cast_array(NestedType::Integer)
        .unwrap_err();
    assert!(matches!(err, SbroadError::Invalid(Entity::Value, _)));
}

#[test]
fn msgpack_value_tuple_serializes_as_bare_msgpack_array() {
    let t = Tuple::from(vec![
        Value::Integer(1),
        Value::Null,
        Value::String("hi".into()),
    ]);
    let v = Value::Tuple(t);
    let encoded: EncodedValue<'_> = (&v).into();

    let bytes = rmp_serde::to_vec(&encoded).expect("serialize");
    let decoded: rmpv::Value =
        rmpv::decode::read_value(&mut bytes.as_slice()).expect("rmpv decode");
    let elements = match decoded {
        rmpv::Value::Array(items) => items,
        other => panic!("expected bare msgpack array, got {other:?}"),
    };
    assert_eq!(elements.len(), 3);
    assert!(matches!(elements[0], rmpv::Value::Integer(_)));
    assert!(matches!(elements[1], rmpv::Value::Nil));
    assert!(matches!(elements[2], rmpv::Value::String(_)));
}

#[test]
fn encode_trait_value_tuple_is_bare_array_and_round_trips() {
    let v = Value::Tuple(Tuple::from(vec![
        Value::Integer(1),
        Value::Null,
        Value::Integer(3),
    ]));

    // Encode through the same trait the motion materialization uses.
    let mut bytes = Vec::new();
    v.encode(&mut bytes, &Context::DEFAULT).expect("encode");

    // It must be a bare 3-element array, never `[[1,null,3]]`.
    let decoded_rmpv = rmpv::decode::read_value(&mut bytes.as_slice()).expect("rmpv decode");
    match decoded_rmpv {
        rmpv::Value::Array(items) => {
            assert_eq!(items.len(), 3, "expected bare array, got nested: {items:?}");
            assert!(matches!(items[0], rmpv::Value::Integer(_)));
            assert!(matches!(items[1], rmpv::Value::Nil));
            assert!(matches!(items[2], rmpv::Value::Integer(_)));
        }
        other => panic!("expected bare msgpack array, got {other:?}"),
    }

    // And encode/decode must be symmetric.
    let round_tripped = Value::decode(&mut bytes.as_slice(), &Context::DEFAULT).expect("decode");
    assert_eq!(round_tripped, v);
}

#[test]
fn trivalent_ordering() {
    assert_eq!(
        TrivalentOrdering::Less,
        TrivalentOrdering::from(false.cmp(&true))
    );
    assert_eq!(TrivalentOrdering::Equal, TrivalentOrdering::from(1.cmp(&1)));
    assert_eq!(
        TrivalentOrdering::Greater,
        TrivalentOrdering::from("b".cmp(""))
    );
}

//! Equal values of some types are stored in several forms, each hashed to its
//! own bucket: `1`, `1.0` and `1.00` decimals, an integer and a decimal, one
//! moment in two time zones. A class holds values of one type only, and the
//! facts pin a sharding key only for INT, TEXT, UUID and BOOL keys: a pin of
//! another type would miss the rows stored in another form. Elsewhere the
//! node keeps its motion and is routed by its condition. Classes of the other
//! types still co-locate join sides, as on master.
//!
//! `t5` is sharded by the integer `a` (`b` is an integer), `dec_t` by the
//! decimal `a`, `t` by the integers `(a, b)`.

use sql::helpers::sql_to_ir_without_bind;
use sql::ir::bucket::Buckets;
use sql::ir::options::Options;
use sql::ir::types::{DerivedType, UnrestrictedType};
use sql::ir::value::Value;
use sql::ir::Plan;
use sql_executor::executor::engine::mock::RouterRuntimeMock;
use sql_executor::executor::ExecutingQuery;

fn optimized(query: &str, params: &[DerivedType]) -> Plan {
    let mut plan = sql_to_ir_without_bind(query, params);
    plan.optimize().unwrap();
    plan
}

fn explain(query: &str) -> String {
    sql::explain::explain_logical(&optimized(&format!("EXPLAIN (LOGICAL) {query}"), &[])).unwrap()
}

/// Whether the facts pinned a key or `bucket_id` of `query`.
fn pinned(query: &str) -> bool {
    !optimized(query, &[]).facts_proof.routes.is_empty()
}

/// The buckets the mock executor routes the input of the top `count(*)` of
/// `plan`, bound to `params`, to: the aggregate itself runs on any node when
/// its input reads no bucket.
fn routed(plan: &Plan, params: Vec<Value>) -> Result<Buckets, String> {
    let mut bound = plan.clone();
    bound.bind_statement(params, Options::default()).unwrap();
    let top = bound.get_top().unwrap();
    let top = bound.get_first_rel_child(top).unwrap();
    let runtime = RouterRuntimeMock::new();
    let mut executing = ExecutingQuery::from_plan(&runtime, bound).unwrap();
    executing.bucket_discovery(top).map_err(|e| e.to_string())
}

fn decimal(s: &str) -> Value {
    Value::Decimal(Box::new(s.parse().unwrap()))
}

/// A constant joins the class of the columns of its type only, and pins an
/// INT key through `a = b`; one of another type does not.
#[test]
fn int_key_is_pinned_through_a_column_by_a_value_of_its_type() {
    let query = "SELECT count(*) FROM t5 WHERE a = b AND b = 1";
    assert!(pinned(query), "{query}");
    assert!(
        optimized(query, &[]).slices().slices().is_empty(),
        "{query}"
    );
    for query in [
        "SELECT count(*) FROM t5 WHERE a = b AND b = 1.0",
        "SELECT count(*) FROM t5 WHERE a = 1.0",
        "SELECT count(*) FROM t5 WHERE a = 1e0",
    ] {
        assert!(!pinned(query), "{query}");
    }
}

/// DECIMAL, DOUBLE and DATETIME keys are never pinned by the facts.
#[test]
fn keys_of_types_stored_in_several_forms_are_not_pinned() {
    for query in [
        "SELECT count(*) FROM dec_t WHERE a = 1.0",
        "SELECT count(*) FROM dec_t WHERE a = 1.0 AND a = 1.00",
        "SELECT count(*) FROM dec_t AS l JOIN dec_t AS r ON l.a = r.a WHERE r.a = 1.0",
    ] {
        assert!(!pinned(query), "{query}");
        assert!(
            !optimized(query, &[]).slices().slices().is_empty(),
            "{query}"
        );
    }
}

#[test]
fn only_types_stored_in_one_form_hash_by_value() {
    use UnrestrictedType::*;
    for ty in [Integer, String, Uuid, Boolean] {
        assert!(DerivedType::new(ty).hashes_by_value(), "{ty:?}");
    }
    for ty in [Decimal, Double, Datetime, Map, Any] {
        assert!(!DerivedType::new(ty).hashes_by_value(), "{ty:?}");
    }
    assert!(!DerivedType::unknown().hashes_by_value());
}

/// Decimal keys co-locate join sides as on master, by `ON` or by constants.
/// Equal keys stored in two forms may live in different buckets, and such
/// pairs are lost, as on master and as by `IN` over the same keys: only a
/// broadcast of one side would find them, which is not worth its price.
#[test]
fn join_on_decimal_keys_is_co_located_as_on_master() {
    for query in [
        "SELECT l.b FROM t5 AS l JOIN t5 AS r ON l.a = r.a",
        "SELECT l.b FROM dec_t AS l JOIN dec_t AS r ON l.a = r.a",
        "SELECT l.b FROM dec_t AS l LEFT JOIN dec_t AS r ON l.a = r.a",
        "SELECT l.b FROM dec_t AS l JOIN dec_t AS r ON true WHERE l.a = 1.00 AND r.a = 1.0",
        "SELECT l.b FROM dec_t AS l WHERE l.a IN (SELECT r.a FROM dec_t AS r)",
    ] {
        let explain = explain(query);
        assert!(!explain.contains("motion"), "{query}\n{explain}");
    }
    // The other side is resharded by the key, not broadcast.
    let query =
        "SELECT l.b FROM dec_t AS l JOIN (SELECT b::decimal AS x FROM t5) AS r ON l.a = r.x";
    let explain = explain(query);
    assert!(explain.contains("policy: segment"), "{query}\n{explain}");
}

/// Two constants of one class contradict each other: no bucket.
#[test]
fn unequal_constants_of_a_class_leave_no_bucket() {
    let plan = optimized(
        "SELECT count(*) FROM t5 WHERE a = b AND b = 1 AND a = 2",
        &[],
    );
    assert_eq!(Ok(Buckets::new_empty()), routed(&plan, vec![]));
}

/// A composite key is pinned only when every part is.
#[test]
fn composite_key_needs_every_part_pinned() {
    assert!(pinned("SELECT count(*) FROM t WHERE a = 1 AND b = 2"));
    for query in [
        "SELECT count(*) FROM t WHERE a = 1 AND b = 2.0",
        "SELECT count(*) FROM t WHERE a = 1",
    ] {
        assert!(!pinned(query), "{query}");
    }
}

/// `bucket_id` is pinned by its number, of any numeric type (see
/// `bucket_id_number_rounds_as_on_master`), and a parameter routes by its
/// bound value: NULL leaves no bucket, a value of another type than the
/// parameter is refused, not hashed apart.
#[test]
fn special_values() {
    assert!(pinned("SELECT count(*) FROM t5 WHERE bucket_id = 42"));
    assert!(pinned("SELECT count(*) FROM t5 WHERE bucket_id = 42.0"));

    let int = DerivedType::new(UnrestrictedType::Integer);
    let plan = optimized("SELECT count(*) FROM t5 WHERE a = b AND b = $1", &[int]);
    assert!(plan.slices().slices().is_empty());
    let bucket = {
        use sql_executor::executor::engine::Vshard;
        RouterRuntimeMock::new()
            .determine_bucket_id(&[&Value::from(1)])
            .unwrap()
    };
    assert_eq!(
        Ok(Buckets::new_filtered([bucket].into_iter().collect())),
        routed(&plan, vec![Value::from(1)])
    );
    assert_eq!(Ok(Buckets::new_empty()), routed(&plan, vec![Value::Null]));
    let err = routed(&plan, vec![decimal("1.0")]).unwrap_err();
    assert!(err.contains("facts route expects int"), "{err}");
}

/// `bucket_id` is the bucket number, not a hash: a number of another type
/// pins it to the bucket it rounds to, as the condition walk does on master
/// (`value_to_bucket_id`), at planning for a literal and at bind for a
/// parameter. A number of no bucket leaves the node no bucket.
#[test]
fn bucket_id_number_rounds_as_on_master() {
    let one = |id| Ok(Buckets::new_filtered([id].into_iter().collect()));
    let query = |value: &str| format!("SELECT count(*) FROM test_space WHERE bucket_id = {value}");
    for value in ["42", "42.0", "42.5", "42e0"] {
        let plan = optimized(&query(value), &[]);
        assert!(plan.slices().slices().is_empty(), "{value}");
        assert_eq!(one(42), routed(&plan, vec![]), "{value}");
    }
    for value in ["0.5", "-3.0"] {
        let plan = optimized(&query(value), &[]);
        assert!(plan.slices().slices().is_empty(), "{value}");
        assert_eq!(Ok(Buckets::new_empty()), routed(&plan, vec![]), "{value}");
    }

    let dec = DerivedType::new(UnrestrictedType::Decimal);
    let plan = optimized(&query("$1"), &[dec]);
    assert!(plan.slices().slices().is_empty());
    assert_eq!(one(42), routed(&plan, vec![decimal("42.5")]));
    assert_eq!(
        Ok(Buckets::new_empty()),
        routed(&plan, vec![decimal("0.5")])
    );
    assert_eq!(Ok(Buckets::new_empty()), routed(&plan, vec![Value::Null]));
    let err = routed(&plan, vec![Value::from(42.5)]).unwrap_err();
    assert!(err.contains("facts route expects decimal"), "{err}");

    let double = DerivedType::new(UnrestrictedType::Double);
    let plan = optimized(&query("$1"), &[double]);
    assert!(plan.slices().slices().is_empty());
    assert_eq!(one(42), routed(&plan, vec![Value::from(42.5)]));
}

/// A number of another type compared with `bucket_id` together with other
/// pins: the route holds every bucket a row may be in, and the condition
/// walk drops the buckets the pins disagree on.
#[test]
fn bucket_id_number_with_other_pins_routes_as_on_master() {
    let one = |id| Ok(Buckets::new_filtered([id].into_iter().collect()));
    let none = Ok(Buckets::new_empty());
    let dec = DerivedType::new(UnrestrictedType::Decimal);

    let plan = optimized(
        "SELECT count(*) FROM test_space WHERE bucket_id = 42 AND bucket_id = $1",
        &[dec],
    );
    assert_eq!(one(42), routed(&plan, vec![decimal("42.0")]));
    assert_eq!(none, routed(&plan, vec![decimal("43.0")]));

    let plan = optimized(
        "SELECT count(*) FROM test_space WHERE bucket_id = $1 AND bucket_id = $2",
        &[dec, dec],
    );
    assert_eq!(
        one(42),
        routed(&plan, vec![decimal("42.5"), decimal("42.7")])
    );
    assert_eq!(none, routed(&plan, vec![decimal("42.5"), decimal("43.5")]));
}

/// A number of no bucket in the `ON` of a LEFT JOIN empties its right input
/// only: the left rows stay.
#[test]
fn bucket_id_number_of_no_bucket_keeps_left_join_rows() {
    let plan = optimized(
        "SELECT t5.a FROM t5 LEFT JOIN test_space AS ts ON ts.bucket_id = 0.5",
        &[],
    );
    assert_eq!(Ok(Buckets::All), routed(&plan, vec![]));
    let plan = optimized(
        "SELECT t5.a FROM t5 LEFT JOIN test_space AS ts ON ts.bucket_id = $1",
        &[DerivedType::new(UnrestrictedType::Decimal)],
    );
    assert_eq!(Ok(Buckets::All), routed(&plan, vec![decimal("0.5")]));
}

/// Enrichment keeps `bucket_id = 42.5`: it is no `bucket_id = 42` for the
/// storage, only for routing.
#[cfg(feature = "enrich_restrictions")]
#[test]
fn enrichment_keeps_a_bucket_id_number_of_another_type() {
    let explain = explain("SELECT id FROM test_space WHERE bucket_id = 42.5");
    assert!(explain.contains("42.5::decimal"), "{explain}");
}

/// Enrichment derives `key = pin` from no decimal class: the condition walk
/// would pin the key by the form of the pin.
#[cfg(feature = "enrich_restrictions")]
#[test]
fn enrichment_derives_no_pin_of_a_decimal_key() {
    let int = explain("SELECT l.b FROM t5 AS l JOIN t5 AS r ON l.a = r.a WHERE r.a = 1");
    assert!(int.contains("l.a::int = 1::int"), "{int}");
    let dec = explain("SELECT l.b FROM dec_t AS l JOIN dec_t AS r ON l.a = r.a WHERE r.a = 1.0");
    assert!(!dec.contains("l.a::decimal = 1.0"), "{dec}");
}

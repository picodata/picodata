//! Equal values of some types are stored in several forms, each hashed to its
//! own bucket: `1`, `1.0` and `1.00` decimals, one moment in two time zones.
//! A class holds values of one type only, and a pin of a type stored in
//! several forms would route a key by its form and miss the rows stored in
//! another: enrichment derives no `key = pin` from it. Classes of these
//! types still co-locate join sides, as on master.
//!
//! `t5` is sharded by the integer `a`, `dec_t` by the decimal `a`.

use sql::helpers::sql_to_ir_without_bind;
use sql::ir::types::{DerivedType, UnrestrictedType};
use sql::ir::Plan;

fn optimized(query: &str, params: &[DerivedType]) -> Plan {
    let mut plan = sql_to_ir_without_bind(query, params);
    plan.optimize().unwrap();
    plan
}

fn explain(query: &str) -> String {
    sql::explain::explain_logical(&optimized(&format!("EXPLAIN (LOGICAL) {query}"), &[])).unwrap()
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

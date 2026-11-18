use super::constant_pair_rejects_eq;
use crate::ir::node::NodeId;
use crate::ir::types::DerivedType;
use crate::ir::value::Value;
use crate::ir::Plan;

#[test]
fn same_kind_constants() {
    assert!(!constant_pair_rejects_eq(&Value::from(1), &Value::from(1)));
    assert!(constant_pair_rejects_eq(&Value::from(1), &Value::from(2)));
    assert!(!constant_pair_rejects_eq(
        &Value::from("a"),
        &Value::from("a")
    ));
    assert!(constant_pair_rejects_eq(
        &Value::from("a"),
        &Value::from("b")
    ));
}

#[test]
fn null_constants() {
    assert!(constant_pair_rejects_eq(&Value::Null, &Value::from(1)));
    assert!(constant_pair_rejects_eq(&Value::from(1), &Value::Null));
    assert!(constant_pair_rejects_eq(&Value::Null, &Value::Null));
}

/// `Value::eq` disagrees with itself here: integer-to-double drops the low bit,
/// while double-to-integer compares exactly. Neither orientation may reject.
#[test]
fn mixed_numeric_constants_are_undecided() {
    let int = Value::from(9_007_199_254_740_993_i64);
    let double = Value::from(9_007_199_254_740_992.0);
    assert!(!constant_pair_rejects_eq(&int, &double));
    assert!(!constant_pair_rejects_eq(&double, &int));
    assert!(!constant_pair_rejects_eq(
        &Value::from(1),
        &Value::from(2.5)
    ));
}

/// `Value::eq` never reports tuples equal, so it proves nothing about them.
#[test]
fn tuple_constants_are_undecided() {
    let a = Value::Tuple(vec![Value::from(1)]);
    let b = Value::Tuple(vec![Value::from(2)]);
    assert!(!constant_pair_rejects_eq(&a, &a.clone()));
    assert!(!constant_pair_rejects_eq(&a, &b));
}

/// Whether constants make `left = right` provably not `TRUE`.
fn rejects(plan: &Plan, left: NodeId, right: NodeId) -> bool {
    let left = plan.get_expression_node(left).unwrap();
    let right = plan.get_expression_node(right).unwrap();
    plan.constants_reject_eq(&left, &right).unwrap()
}

/// Build `ROW(left) = ROW(right)` operands; `None` stands for a parameter.
fn rows(plan: &mut Plan, pairs: &[(Option<i64>, Option<i64>)]) -> (NodeId, NodeId) {
    let mut operand = |value: Option<i64>| match value {
        Some(v) => plan.nodes.add_const(Value::from(v)),
        None => plan.add_param(1, DerivedType::unknown()),
    };
    let (left, right): (Vec<_>, Vec<_>) =
        pairs.iter().map(|&(l, r)| (operand(l), operand(r))).unzip();
    (plan.nodes.add_row(left), plan.nodes.add_row(right))
}

#[test]
fn row_contradiction_after_equal_pair() {
    let mut plan = Plan::default();
    let (left, right) = rows(
        &mut plan,
        &[(Some(1), Some(1)), (Some(2), Some(1)), (None, None)],
    );
    assert!(rejects(&plan, left, right));
}

#[test]
fn row_contradiction_at_last_position() {
    let mut plan = Plan::default();
    let (left, right) = rows(
        &mut plan,
        &[(None, Some(1)), (Some(1), Some(1)), (Some(3), Some(4))],
    );
    assert!(rejects(&plan, left, right));
}

#[test]
fn row_without_contradiction() {
    let mut plan = Plan::default();
    let (left, right) = rows(
        &mut plan,
        &[(None, Some(1)), (Some(1), Some(1)), (Some(2), None)],
    );
    assert!(!rejects(&plan, left, right));
}

#[test]
fn scalar_constants() {
    let mut plan = Plan::default();
    let one = plan.nodes.add_const(Value::from(1));
    let two = plan.nodes.add_const(Value::from(2));
    assert!(rejects(&plan, one, two));
    assert!(!rejects(&plan, one, one));
}

/// `NULL` rejects equality against anything, including a non-constant operand.
#[test]
fn null_against_non_constant() {
    let mut plan = Plan::default();
    let null = plan.nodes.add_const(Value::Null);
    let param = plan.add_param(1, DerivedType::unknown());
    assert!(rejects(&plan, null, param));
    assert!(rejects(&plan, param, null));

    let one = plan.nodes.add_const(Value::from(1));
    let param2 = plan.add_param(2, DerivedType::unknown());
    let null2 = plan.nodes.add_const(Value::Null);
    let left = plan.nodes.add_row(vec![param2, one]);
    let one2 = plan.nodes.add_const(Value::from(1));
    let right = plan.nodes.add_row(vec![null2, one2]);
    assert!(rejects(&plan, left, right));
}

/// The keys hashed through one cache: equal parts of a type that hashes by
/// value share the hash, others are hashed every time.
#[test]
fn key_buckets_share_equal_values_of_one_type() {
    use super::{conjunct_key_buckets, Buckets, BucketsResolver, HashedKeys, MAX_HASHED_KEYS};
    use smol_str::SmolStr;
    use std::cell::{Cell, RefCell};

    struct Counting(Cell<usize>);
    impl BucketsResolver for Counting {
        type Buckets = Buckets;
        fn buckets_from_motion(
            &self,
            _: NodeId,
        ) -> Result<Option<Buckets>, crate::errors::SbroadError> {
            Ok(None)
        }
        fn buckets_from_values(
            &self,
            _: &[&Value],
            _: Option<&SmolStr>,
        ) -> Result<Buckets, crate::errors::SbroadError> {
            self.0.set(self.0.get() + 1);
            Ok(Buckets::new_filtered(
                [self.0.get() as u64].into_iter().collect(),
            ))
        }
        fn buckets_from_bucket_id(
            &self,
            _: &Value,
            _: Option<&SmolStr>,
        ) -> Result<Buckets, crate::errors::SbroadError> {
            unreachable!()
        }
    }

    /// The buckets of one key alone.
    fn key_buckets<'p>(
        resolver: &Counting,
        key: &[&'p Value],
        hashed: Option<&RefCell<HashedKeys<'p, Buckets>>>,
    ) -> Buckets {
        let mut acc = None;
        conjunct_key_buckets(resolver, key, None, hashed, &mut acc).unwrap();
        acc.unwrap()
    }

    let decimal = |text: &str| Value::Decimal(Box::new(text.parse().unwrap()));
    let (one, text_one, uuid, yes) = (
        Value::from(1),
        Value::from("1"),
        Value::Uuid(tarantool::uuid::Uuid::nil()),
        Value::Boolean(true),
    );
    let (dec_one, dec_one_0) = (decimal("1"), decimal("1.0"));
    let (one_again, text_again) = (Value::from(1), Value::from("1"));
    let (dbl_one, plus_zero, minus_zero) = (Value::from(1.0), Value::from(0.0), Value::from(-0.0));
    let negative = |v: &Value| matches!(v, Value::Double(d) if d.value.is_sign_negative());
    assert!(!negative(&plus_zero) && negative(&minus_zero));
    // Each key and whether it is hashed when its turn comes.
    let keys: Vec<(Vec<&Value>, bool)> = vec![
        (vec![&one], true),
        (vec![&one_again], false),
        (vec![&text_one], true),
        (vec![&text_again], false),
        (vec![&uuid], true),
        (vec![&uuid], false),
        (vec![&yes], true),
        (vec![&yes], false),
        (vec![&one, &text_one], true),
        (vec![&text_one, &one], true),
        (vec![&one_again, &text_again], false),
        (vec![&dec_one], true),
        (vec![&dec_one], true),
        (vec![&dec_one_0], true),
        (vec![&one, &dec_one], true),
        (vec![&one, &dec_one], true),
        (vec![&dbl_one], true),
        (vec![&dbl_one], true),
        (vec![&plus_zero], true),
        (vec![&minus_zero], true),
        (vec![&plus_zero], true),
        (vec![&minus_zero], true),
        (vec![&one, &dbl_one], true),
        (vec![&one, &dbl_one], true),
    ];
    // Distinct keys of an `IN` list, more than the cache keeps.
    let many: Vec<Value> = (100..100 + 4 * MAX_HASHED_KEYS as i64)
        .map(Value::from)
        .collect();
    let resolver = Counting(Cell::new(0));
    let hashed = RefCell::new(HashedKeys::new());
    let mut first: Vec<(Vec<&Value>, Buckets)> = Vec::new();
    for (key, hashes) in &keys {
        let before = resolver.0.get();
        let buckets = key_buckets(&resolver, key, Some(&hashed));
        assert_eq!(*hashes, resolver.0.get() > before, "{key:?}");
        if !hashes {
            let earlier = first.iter().find(|(k, _)| k == key).map(|(_, b)| b);
            assert_eq!(earlier, Some(&buckets), "{key:?}");
        }
        first.push((key.clone(), buckets));
    }
    // Only the INT, TEXT, UUID, BOOL and composite keys above are kept.
    assert_eq!(hashed.borrow().len(), 6);
    // A key narrows the buckets it is conjuncted into, kept or not: `1` has
    // the first bucket, a decimal gets a new one.
    let before = resolver.0.get();
    let wider = || Some(Buckets::new_filtered([1, 1000].into_iter().collect()));
    let (mut kept, mut fresh) = (wider(), wider());
    conjunct_key_buckets(&resolver, &[&one], None, Some(&hashed), &mut kept).unwrap();
    assert_eq!(before, resolver.0.get());
    assert_eq!(kept, Some(Buckets::new_filtered([1].into_iter().collect())));
    conjunct_key_buckets(&resolver, &[&dec_one], None, Some(&hashed), &mut fresh).unwrap();
    assert_eq!(fresh, Some(Buckets::new_empty()));
    // The cache stops growing at its limit, so that a key is not compared
    // with all the others. The first keys fill it up, the second round finds
    // only them, the rest are hashed every time.
    let before = resolver.0.get();
    for value in many.iter().chain(&many) {
        key_buckets(&resolver, &[value], Some(&hashed));
    }
    let room = MAX_HASHED_KEYS - 6;
    assert_eq!(resolver.0.get() - before, 2 * many.len() - room);
    assert_eq!(hashed.borrow().len(), MAX_HASHED_KEYS);
    // Without a cache every key is hashed.
    let resolver = Counting(Cell::new(0));
    for (key, _) in &keys {
        key_buckets(&resolver, key, None);
    }
    assert_eq!(keys.len(), resolver.0.get());
}

/// The conjunction that reuses its operands gives what the one by reference
/// does, on every pair of kinds of buckets.
#[test]
fn conjunct_owned_is_conjunct() {
    use super::{BucketSet, Buckets};
    let exact = |ids: &[u64]| Buckets::new_filtered(ids.iter().copied().collect());
    let all = [
        Buckets::All,
        Buckets::Any,
        exact(&[]),
        exact(&[1]),
        exact(&[1, 2]),
        exact(&[2, 3, 4]),
        Buckets::Filtered(BucketSet::EstimatedCount { lower: 0, upper: 1 }),
        Buckets::Filtered(BucketSet::EstimatedCount { lower: 2, upper: 5 }),
    ];
    for a in &all {
        for b in &all {
            assert_eq!(
                a.clone().conjunct_owned(b.clone()),
                a.conjunct(b),
                "{a} and {b}"
            );
        }
    }
}

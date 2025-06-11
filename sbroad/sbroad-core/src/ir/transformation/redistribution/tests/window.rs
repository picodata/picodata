use super::*;
use crate::ir::node::Window;
use crate::ir::relation::{DerivedType, Type};
use crate::ir::Plan;
use pretty_assertions::assert_eq;

/// Check that a window with a partition by a single column
/// results a segment distribution.
#[test]
fn window_simple_partition_by() {
    let mut plan = Plan::default();
    let a_ref = plan.nodes.add_ref(
        None,
        Some(vec![0]),
        0,
        DerivedType::new(Type::Boolean),
        None,
    );
    let window = Window {
        name: None,
        partition: Some(vec![a_ref]),
        ordering: None,
        frame: None,
    };
    let window_id = plan.nodes.push(window.into());
    let key = Key::new(vec![0]);
    let mut keys = KeySet::empty();
    keys.insert(key);
    assert_eq!(
        plan.window_dist(window_id),
        Ok(Distribution::Segment { keys })
    );
}

// Check that a window with a partition by complicated expressions
// (not just a single column) results a single distribution.
#[test]
fn window_complex_partition_by() {
    let mut plan = Plan::default();
    let a_ref = plan.nodes.add_ref(
        None,
        Some(vec![0]),
        0,
        DerivedType::new(Type::Boolean),
        None,
    );
    let b_ref = plan.nodes.add_ref(
        None,
        Some(vec![0]),
        1,
        DerivedType::new(Type::Boolean),
        None,
    );
    let sum = plan.nodes.add_bool(a_ref, Bool::And, b_ref).unwrap();
    let window = Window {
        name: None,
        partition: Some(vec![sum]),
        ordering: None,
        frame: None,
    };
    let window_id = plan.nodes.push(window.into());
    assert_eq!(plan.window_dist(window_id), Ok(Distribution::Single));
}

/// Check that a window with an empty partition by
/// results a single distribution.
#[test]
fn window_empty_partition_by() {
    let mut plan = Plan::default();

    // Check empty vector as partition.
    let window = Window {
        name: None,
        partition: Some(vec![]),
        ordering: None,
        frame: None,
    };
    let window_id = plan.nodes.push(window.into());
    assert_eq!(plan.window_dist(window_id), Ok(Distribution::Single));

    // Check None as partition.
    let window = Window {
        name: None,
        partition: None,
        ordering: None,
        frame: None,
    };
    let window_id = plan.nodes.push(window.into());
    assert_eq!(plan.window_dist(window_id), Ok(Distribution::Single));
}

/// Check non-empty key intersection in segmented window distributions.
#[test]
fn window_segmented_key_intersection() {
    // Left window distribution.
    let key_0 = Key::new(vec![0]);
    let key_1 = Key::new(vec![1]);
    let mut this_keys = KeySet::empty();
    this_keys.insert(key_0);
    this_keys.insert(key_1.clone());
    let this_dist = Distribution::Segment { keys: this_keys };

    // Right window distribution.
    let key_2 = Key::new(vec![2]);
    let mut other_keys = KeySet::empty();
    other_keys.insert(key_1.clone());
    other_keys.insert(key_2);
    let other_dist = Distribution::Segment { keys: other_keys };

    // Expected intersection.
    let mut expected_keys = KeySet::empty();
    expected_keys.insert(key_1);
    let expected_dist = Distribution::Segment {
        keys: expected_keys,
    };

    assert_eq!(compare_window_dist(&this_dist, &other_dist), expected_dist);
    assert_eq!(compare_window_dist(&other_dist, &this_dist), expected_dist);
}

/// Check empty key intersection in segmented window distributions.
#[test]
fn window_segmented_key_intersection_empty() {
    // Left window distribution.
    let key_0 = Key::new(vec![0]);
    let mut this_keys = KeySet::empty();
    this_keys.insert(key_0);
    let this_dist = Distribution::Segment { keys: this_keys };

    // Right window distribution.
    let key_1 = Key::new(vec![1]);
    let mut other_keys = KeySet::empty();
    other_keys.insert(key_1);
    let other_dist = Distribution::Segment { keys: other_keys };

    assert_eq!(
        compare_window_dist(&this_dist, &other_dist),
        Distribution::Single,
    );
    assert_eq!(
        compare_window_dist(&other_dist, &this_dist),
        Distribution::Single,
    );
}

/// Check single distribution in window distributions
#[test]
fn window_single_distribution() {
    // Segment distribution.
    let key_0 = Key::new(vec![0]);
    let mut keys = KeySet::empty();
    keys.insert(key_0);
    let dist = Distribution::Segment { keys };

    assert_eq!(
        compare_window_dist(&dist, &Distribution::Single),
        Distribution::Single,
    );
    assert_eq!(
        compare_window_dist(&Distribution::Single, &dist),
        Distribution::Single,
    );
}

#[test]
#[should_panic(expected = "Unexpected window distribution")]
fn window_unreachable_any_left() {
    compare_window_dist(&Distribution::Any, &Distribution::Single);
}

#[test]
#[should_panic(expected = "Unexpected window distribution")]
fn window_unreachable_any_right() {
    compare_window_dist(&Distribution::Single, &Distribution::Any);
}

#[test]
#[should_panic(expected = "Unexpected window distribution")]
fn window_unreachable_global_left() {
    compare_window_dist(&Distribution::Global, &Distribution::Single);
}

#[test]
#[should_panic(expected = "Unexpected window distribution")]
fn window_unreachable_global_right() {
    compare_window_dist(&Distribution::Single, &Distribution::Global);
}

//! Coordinator-side build phase for dynamic filter pushdown.
//!
//! Drives §5.4 of the plan: right after `materialize_motion` fills the
//! build-side virtual table, this module consults `plan.dynamic_filters`
//! for any spec whose `build_motion_id` matches, canonically hashes each
//! materialized row's JOIN-key columns, and stores the finalized
//! `DynamicFilter` in `ExecutionPlan.filter_state` under the spec's
//! `filter_id`. The probe side reads the filter back through the DQL
//! packet (§5.3) by matching the same sidecar `filter_id`.
//!
//! Canonical hashing must be bit-for-bit identical between the
//! coordinator (this module) and the storage (`canonical_bytes_into`
//! re-used from the local executor). Per-type byte forms are
//! type-tagged so values across types cannot collide:
//! * `Integer`:  tag 0x01 + 8 bytes (i64 LE).
//! * `Double`:   tag 0x02 + 8 bytes (f64 LE) with NaN → canonical
//!   quiet-NaN bit pattern and `-0.0` → `+0.0`.
//! * `Decimal`:  tag 0x03 + trimmed string bytes — mirrors the existing
//!   `ToHashString` rule so `1.0` ≡ `1.000`.
//! * `Boolean`:  tag 0x04 + 1 byte.
//! * `String`:   tag 0x05 + UTF-8 bytes.
//! * `Uuid`:     tag 0x06 + 16 bytes.
//! * `Datetime`: tag 0x07 + Display bytes (mirrors `ToHashString`).
//! * `Tuple`:    tag 0x08 + recursive canonical bytes with separators.
//! * `Null`:     handled before the hasher when `NullPolicy::Skip`;
//!   otherwise `TupleHasher::add_null()`.
//!
//! Between columns the caller writes `TupleHasher::add_separator()` so
//! `("ab", "")` and `("a", "b")` cannot collide.

use sql_dynfilter::{DynamicFilter, TupleHasher};
use sql_protocol::dql_encoder::ApplySpec;

use crate::errors::{Entity, SbroadError};
use crate::executor::ir::{ExecutionPlan, FilterState};
use crate::executor::vtable::VirtualTable;
use crate::ir::node::{NodeId, NullPolicy};
use crate::ir::value::Value;

/// Trait the picodata layer implements to feed filter probe outcomes
/// into Prometheus counters. Registered once at startup via
/// `set_filter_metrics_sink`; unit tests leave it `None`.
pub trait FilterMetricsSink: std::fmt::Debug + Send + Sync {
    /// Called once per probed row. `hit == true` ⇒ the row passed the
    /// filter (kept); `hit == false` ⇒ the row was filtered out
    /// (dropped). Selectivity = filtered / (passed + filtered).
    fn record_probe(&self, hit: bool);
}

static FILTER_METRICS_SINK: std::sync::OnceLock<std::sync::Arc<dyn FilterMetricsSink>> =
    std::sync::OnceLock::new();

/// Install the global sink that `apply_filters_to_vtable` reports probe
/// outcomes to. Idempotent: a second call is a no-op (returns `Err`).
pub fn set_filter_metrics_sink(
    sink: std::sync::Arc<dyn FilterMetricsSink>,
) -> Result<(), std::sync::Arc<dyn FilterMetricsSink>> {
    FILTER_METRICS_SINK.set(sink)
}

#[inline]
fn report_probe(hit: bool) {
    if let Some(sink) = FILTER_METRICS_SINK.get() {
        sink.record_probe(hit);
    }
}

const TAG_INTEGER: u8 = 0x01;
const TAG_DOUBLE: u8 = 0x02;
const TAG_DECIMAL: u8 = 0x03;
const TAG_BOOLEAN: u8 = 0x04;
const TAG_STRING: u8 = 0x05;
const TAG_UUID: u8 = 0x06;
const TAG_DATETIME: u8 = 0x07;
const TAG_TUPLE: u8 = 0x08;

/// Write canonical bytes for a single `Value` into `hasher`. `Null` is
/// handled by the caller (per `NullPolicy`) — passing `Null` here mixes
/// a distinct null tag.
pub fn canonical_bytes_into(value: &Value, hasher: &mut TupleHasher) {
    match value {
        Value::Integer(v) => {
            hasher.add_bytes(&[TAG_INTEGER]);
            hasher.add_bytes(&v.to_le_bytes());
        }
        Value::Double(v) => {
            hasher.add_bytes(&[TAG_DOUBLE]);
            let mut bits = v.value.to_bits();
            if v.value.is_nan() {
                bits = f64::NAN.to_bits();
            } else if v.value == 0.0 {
                bits = 0.0_f64.to_bits();
            }
            hasher.add_bytes(&bits.to_le_bytes());
        }
        Value::Decimal(v) => {
            hasher.add_bytes(&[TAG_DECIMAL]);
            let trimmed = v.trim().to_string();
            hasher.add_bytes(trimmed.as_bytes());
        }
        Value::Boolean(v) => {
            hasher.add_bytes(&[TAG_BOOLEAN]);
            hasher.add_bytes(&[u8::from(*v)]);
        }
        Value::String(v) => {
            hasher.add_bytes(&[TAG_STRING]);
            hasher.add_bytes(v.as_bytes());
        }
        Value::Uuid(v) => {
            hasher.add_bytes(&[TAG_UUID]);
            hasher.add_bytes(v.as_bytes());
        }
        Value::Datetime(v) => {
            hasher.add_bytes(&[TAG_DATETIME]);
            hasher.add_bytes(v.to_string().as_bytes());
        }
        Value::Tuple(t) => {
            hasher.add_bytes(&[TAG_TUPLE]);
            for elem in &t.0 {
                canonical_bytes_into(elem, hasher);
                hasher.add_separator();
            }
        }
        Value::Null => {
            hasher.add_null();
        }
    }
}

/// Outcome of hashing a single row's JOIN-key columns.
#[derive(Debug, Clone, Copy)]
pub enum KeyHash {
    /// The row should be dropped before filter insertion / lookup —
    /// happens under `NullPolicy::Skip` when any key column is `Null`.
    Drop,
    /// Canonical u64 hash of the row's key columns.
    Value(u64),
}

/// Hash one row's key columns according to `null_policy`. The order of
/// `positions` must match the matching `ApplyFilter`'s `keys` order; a
/// reorder on either side produces silently wrong filters.
pub fn hash_row(row: &[Value], positions: &[usize], null_policy: NullPolicy) -> KeyHash {
    if matches!(null_policy, NullPolicy::Skip) {
        for &pos in positions {
            if matches!(row.get(pos), Some(Value::Null) | None) {
                return KeyHash::Drop;
            }
        }
    }
    let mut hasher = TupleHasher::new();
    for &pos in positions {
        let value = row.get(pos).unwrap_or(&Value::Null);
        canonical_bytes_into(value, &mut hasher);
        hasher.add_separator();
    }
    KeyHash::Value(hasher.finish())
}

/// Build any `DynamicFilter`s whose `BuildFilter` parent sits directly
/// over the just-materialized `motion_id`. Stores them in
/// `exec_plan.filter_state` keyed by `filter_id`. No-op when nothing
/// references this motion as a build source.
pub fn build_filters_for_motion(
    exec_plan: &mut ExecutionPlan,
    motion_id: NodeId,
) -> Result<(), SbroadError> {
    let build_filters = collect_build_filters_above_motion(exec_plan, motion_id)?;
    if build_filters.is_empty() {
        return Ok(());
    }

    // The vtable lives behind an `Rc`; cloning the handle is cheap and
    // keeps it alive across the mutable borrow of `filter_state` below
    // without forcing a deep copy of every build-side row.
    let vtable = exec_plan.get_motion_vtable(motion_id)?;

    for (filter_id, key_positions, null_policy) in build_filters {
        let mut filter = DynamicFilter::new(vtable.get_tuples().len());
        for row in vtable.get_tuples() {
            match hash_row(row, &key_positions, null_policy) {
                KeyHash::Drop => {}
                KeyHash::Value(h) => filter.insert(h),
            }
        }
        filter.finalize().map_err(|e| {
            SbroadError::FailedTo(
                crate::errors::Action::Build,
                Some(Entity::Plan),
                smol_str::format_smolstr!("dynamic filter finalize failed: {e:?}"),
            )
        })?;

        let apply_spec =
            collect_apply_spec_by_filter_id(exec_plan, filter_id)?.ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Plan,
                    Some(smol_str::format_smolstr!(
                        "DynamicFilterSpec with filter_id {filter_id} has no matching probe spec"
                    )),
                )
            })?;

        exec_plan.filter_state.insert(
            filter_id,
            FilterState {
                filter,
                key_arity: key_positions.len(),
                apply_spec,
            },
        );
    }
    Ok(())
}

/// Build the `ApplySpec` from the sidecar spec keyed by `filter_id`.
/// Returns `None` only if no spec carries this id — which the caller
/// treats as an IR-shape error.
fn collect_apply_spec_by_filter_id(
    exec_plan: &ExecutionPlan,
    target_filter_id: u32,
) -> Result<Option<ApplySpec>, SbroadError> {
    let plan = exec_plan.get_ir_plan();
    let Some(spec) = plan
        .dynamic_filters
        .iter()
        .find(|s| s.filter_id == target_filter_id)
    else {
        return Ok(None);
    };
    let mut key_positions = Vec::with_capacity(spec.probe_columns.len());
    for &pos in &spec.probe_columns {
        key_positions.push(u32::try_from(pos).map_err(|_| {
            SbroadError::Invalid(
                Entity::Plan,
                Some(smol_str::format_smolstr!(
                    "Probe key position {pos} exceeds u32::MAX"
                )),
            )
        })?);
    }
    Ok(Some(ApplySpec::new(key_positions, spec.null_policy.into())))
}

/// Discover every dynamic-filter spec whose `build_motion_id` is
/// `motion_id`, returning `(filter_id, build_columns, null_policy)`
/// for each. Reads from the sidecar, never walks the IR.
fn collect_build_filters_above_motion(
    exec_plan: &ExecutionPlan,
    motion_id: NodeId,
) -> Result<Vec<(u32, Vec<usize>, NullPolicy)>, SbroadError> {
    let plan = exec_plan.get_ir_plan();
    let out = plan
        .dynamic_filters
        .iter()
        .filter(|s| s.build_motion_id == motion_id)
        .map(|s| (s.filter_id, s.build_columns.clone(), s.null_policy))
        .collect();
    Ok(out)
}

/// Apply phase counterpart to `build_filters_for_motion`. Runs against
/// the freshly-materialized probe-side vtable **before** it is wrapped
/// in `Rc` by `set_motion_vtable`, dropping rows whose JOIN-key hash is
/// not present in the matching `DynamicFilter`.
///
/// Lookup order:
///   1. Walk the motion's IR child. If it is an `ApplyFilter`, the
///      probe is filterable; otherwise no-op.
///   2. Look up `exec_plan.filter_state[filter_id]` — populated by the
///      build slice's `build_filters_for_motion` call (slice ordering
///      from §5.5 guarantees the build slice ran first).
///   3. Hash each row's key columns under the recorded `null_policy`
///      and `vtable.get_mut_tuples().retain(...)` rows that hit.
///
/// `ApplyFilter.keys[i]` must be a `Reference` whose `position` indexes
/// the child's output — by construction this is also the column layout
/// of the materialized vtable rows.
///
/// Note: this is **coordinator-side** application. The DQL packet still
/// ships the filter bytes (§5.3) so a future patch can move the retain
/// to the storage; for now the bytes travel unread on the storage side
/// and the savings come from skipping JOIN-probe rows after they reach
/// the coordinator. Correctness is the same; only wire bytes differ.
pub fn apply_filters_to_vtable(
    exec_plan: &ExecutionPlan,
    motion_id: NodeId,
    vtable: &mut VirtualTable,
) -> Result<(), SbroadError> {
    let Some((filter_id, key_positions, null_policy)) =
        collect_apply_filter_under_motion(exec_plan, motion_id)?
    else {
        return Ok(());
    };

    let state = exec_plan.filter_state.get(&filter_id).ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Plan,
            Some(smol_str::format_smolstr!(
                "Probe spec references filter_id {filter_id} not built by any prior slice"
            )),
        )
    })?;

    if state.key_arity != key_positions.len() {
        return Err(SbroadError::Invalid(
            Entity::Plan,
            Some(smol_str::format_smolstr!(
                "Probe key arity {} != build key arity {}",
                key_positions.len(),
                state.key_arity
            )),
        ));
    }

    let FilterState { filter, .. } = state;
    vtable
        .get_mut_tuples()
        .retain(|row| match hash_row(row, &key_positions, null_policy) {
            KeyHash::Drop => {
                report_probe(false);
                false
            }
            KeyHash::Value(h) => {
                let hit = filter.contains(h);
                report_probe(hit);
                hit
            }
        });

    Ok(())
}

/// Returns `(filter_id, probe_columns, null_policy)` if `motion_id`
/// is the probe motion for any sidecar spec, or `None` otherwise.
fn collect_apply_filter_under_motion(
    exec_plan: &ExecutionPlan,
    motion_id: NodeId,
) -> Result<Option<(u32, Vec<usize>, NullPolicy)>, SbroadError> {
    let plan = exec_plan.get_ir_plan();
    let Some(spec) = plan
        .dynamic_filters
        .iter()
        .find(|s| s.probe_motion_id == motion_id)
    else {
        return Ok(None);
    };
    Ok(Some((
        spec.filter_id,
        spec.probe_columns.clone(),
        spec.null_policy,
    )))
}

#[cfg(feature = "mock")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ir::ExecutionPlan;
    use crate::executor::vtable::VirtualTable;
    use crate::ir::expression::ColumnWithScan;
    use crate::ir::operator::{Bool, JoinKind};
    use crate::ir::relation::{SpaceEngine, Table};
    use crate::ir::tests::{column_user_non_null, sharding_column};
    use crate::ir::transformation::redistribution::{MotionPolicy, Program};
    use crate::ir::types::UnrestrictedType;
    use crate::ir::value::double::Double;
    use crate::ir::Plan;
    use rand::random;
    use smol_str::SmolStr;
    use sql_dynfilter::DynamicFilter;
    use std::str::FromStr;
    use tarantool::decimal::Decimal as TDecimal;

    fn hash_one(v: &Value) -> u64 {
        let mut h = TupleHasher::new();
        canonical_bytes_into(v, &mut h);
        h.add_separator();
        h.finish()
    }

    #[test]
    fn integer_canonical_is_stable() {
        assert_eq!(hash_one(&Value::Integer(42)), hash_one(&Value::Integer(42)));
        assert_ne!(hash_one(&Value::Integer(1)), hash_one(&Value::Integer(2)));
    }

    #[test]
    fn integer_string_dont_collide() {
        assert_ne!(
            hash_one(&Value::Integer(1)),
            hash_one(&Value::String("1".into()))
        );
    }

    #[test]
    fn double_negative_zero_normalized() {
        let pos = Value::Double(Double::from(0.0));
        let neg = Value::Double(Double::from(-0.0));
        assert_eq!(hash_one(&pos), hash_one(&neg));
    }

    #[test]
    fn double_nan_canonical() {
        let nan_a = Value::Double(Double::from(f64::NAN));
        let nan_b = Value::Double(Double::from(f64::from_bits(f64::NAN.to_bits() ^ 1)));
        assert_eq!(hash_one(&nan_a), hash_one(&nan_b));
    }

    #[test]
    fn decimal_trims_trailing_zeros() {
        let a = Value::Decimal(Box::new(TDecimal::from_str("1").unwrap()));
        let b = Value::Decimal(Box::new(TDecimal::from_str("1.000").unwrap()));
        assert_eq!(hash_one(&a), hash_one(&b));
    }

    #[test]
    fn hash_row_skip_drops_null() {
        let row = vec![Value::Integer(1), Value::Null, Value::Integer(3)];
        assert!(matches!(
            hash_row(&row, &[0, 1], NullPolicy::Skip),
            KeyHash::Drop
        ));
    }

    #[test]
    fn hash_row_insert_keeps_null() {
        let row = vec![Value::Integer(1), Value::Null];
        assert!(matches!(
            hash_row(&row, &[0, 1], NullPolicy::Insert),
            KeyHash::Value(_)
        ));
    }

    #[test]
    fn hash_row_position_order_matters() {
        let row = vec![Value::Integer(1), Value::Integer(2)];
        let ab = hash_row(&row, &[0, 1], NullPolicy::Skip);
        let ba = hash_row(&row, &[1, 0], NullPolicy::Skip);
        match (ab, ba) {
            (KeyHash::Value(a), KeyHash::Value(b)) => assert_ne!(a, b),
            _ => panic!("expected two hashes"),
        }
    }

    /// Mirror of the resolver's `make_two_motion_inner_join` fixture, kept
    /// in-module to drive end-to-end tests of the §5.4 build/apply phases.
    /// Returns `(plan, join_id, probe_motion_id, build_motion_id)`.
    fn make_two_motion_inner_join() -> (Plan, NodeId, NodeId, NodeId) {
        let mut plan = Plan::default();
        let t1 = Table::new_sharded(
            random(),
            "t1",
            vec![
                column_user_non_null(SmolStr::from("a"), UnrestrictedType::Integer),
                column_user_non_null(SmolStr::from("b"), UnrestrictedType::Integer),
                sharding_column(),
            ],
            &["a"],
            &["a"],
            SpaceEngine::Memtx,
        )
        .unwrap();
        plan.add_rel(t1);
        let scan_t1 = plan.add_scan("t1", None).unwrap();

        let t2 = Table::new_sharded(
            random(),
            "t2",
            vec![
                column_user_non_null(SmolStr::from("c"), UnrestrictedType::Integer),
                column_user_non_null(SmolStr::from("d"), UnrestrictedType::Integer),
                sharding_column(),
            ],
            &["d"],
            &["d"],
            SpaceEngine::Memtx,
        )
        .unwrap();
        plan.add_rel(t2);
        let scan_t2 = plan.add_scan("t2", None).unwrap();

        let motion_t1 = plan
            .add_motion(scan_t1, &MotionPolicy::Full, Program::default())
            .unwrap();
        let motion_t2 = plan
            .add_motion(scan_t2, &MotionPolicy::Full, Program::default())
            .unwrap();

        let a_ref = plan
            .add_ref_from_left_branch(motion_t1, motion_t2, ColumnWithScan::new("a", None))
            .unwrap();
        let d_ref = plan
            .add_ref_from_right_branch(motion_t1, motion_t2, ColumnWithScan::new("d", None))
            .unwrap();
        let condition = plan.nodes.add_bool(a_ref, Bool::Eq, d_ref).unwrap();
        let join = plan
            .add_join(motion_t1, motion_t2, condition, JoinKind::Inner)
            .unwrap();
        plan.set_top(join).unwrap();
        (plan, join, motion_t1, motion_t2)
    }

    /// Run the §5.9 resolver on a two-motion inner join and return the
    /// resolved plan together with the recorded sidecar `filter_id`.
    /// The IR shape is unchanged from `make_two_motion_inner_join`; all
    /// filter metadata lives in `plan.dynamic_filters`.
    fn prepare_resolved_plan() -> (Plan, NodeId, NodeId, NodeId, u32) {
        let (mut plan, join, probe_motion, build_motion) = make_two_motion_inner_join();
        plan.insert_dynamic_filters_for_inner_joins(join).unwrap();
        let filter_id = plan
            .dynamic_filters
            .first()
            .expect("resolver should record one spec")
            .filter_id;
        (plan, join, probe_motion, build_motion, filter_id)
    }

    /// Look up the build-side column position for the given filter from
    /// the sidecar. Tests call this where they previously walked the
    /// `BuildFilter` IR node's `Reference` keys.
    fn build_pos_for(plan: &Plan, filter_id: u32) -> usize {
        plan.dynamic_filters
            .iter()
            .find(|s| s.filter_id == filter_id)
            .expect("spec for filter_id")
            .build_columns[0]
    }

    /// Look up the probe-side column position for the given filter.
    fn apply_pos_for(plan: &Plan, filter_id: u32) -> usize {
        plan.dynamic_filters
            .iter()
            .find(|s| s.filter_id == filter_id)
            .expect("spec for filter_id")
            .probe_columns[0]
    }

    /// Build a finalized `DynamicFilter` containing exactly the hashes
    /// produced by `keys`, where each key is hashed as a single-column
    /// row under `NullPolicy::Skip`.
    fn build_filter_from_keys(keys: &[Value]) -> DynamicFilter {
        let mut filter = DynamicFilter::new(keys.len());
        for k in keys {
            let row = vec![k.clone()];
            if let KeyHash::Value(h) = hash_row(&row, &[0], NullPolicy::Skip) {
                filter.insert(h);
            }
        }
        filter.finalize().unwrap();
        filter
    }

    #[test]
    fn apply_no_op_when_motion_has_no_apply_filter() {
        // Pre-resolver plan: probe motion's child is a Projection, not
        // an ApplyFilter. The apply pass must leave the vtable alone.
        let (plan, _join, probe_motion, _build_motion) = make_two_motion_inner_join();
        let exec_plan = ExecutionPlan::new(plan);
        let mut vtable = VirtualTable::new();
        vtable.add_tuple(vec![Value::Integer(1)]);
        vtable.add_tuple(vec![Value::Integer(2)]);
        apply_filters_to_vtable(&exec_plan, probe_motion, &mut vtable).unwrap();
        assert_eq!(vtable.get_tuples().len(), 2);
    }

    #[test]
    fn apply_retains_hits_and_drops_misses() {
        let (plan, _join, probe_motion, _build_motion, filter_id) = prepare_resolved_plan();
        let mut exec_plan = ExecutionPlan::new(plan);

        // Build filter contains only key=1; key=2 must be filtered out.
        let filter = build_filter_from_keys(&[Value::Integer(1)]);
        exec_plan.filter_state.insert(
            filter_id,
            FilterState {
                filter,
                key_arity: 1,
                apply_spec: ApplySpec::new(vec![0u32], sql_dynfilter::NullPolicy::Skip),
            },
        );

        let mut vtable = VirtualTable::new();
        vtable.add_tuple(vec![Value::Integer(1), Value::Integer(10)]);
        vtable.add_tuple(vec![Value::Integer(2), Value::Integer(20)]);
        vtable.add_tuple(vec![Value::Integer(1), Value::Integer(30)]);

        apply_filters_to_vtable(&exec_plan, probe_motion, &mut vtable).unwrap();
        let kept: Vec<i64> = vtable
            .get_tuples()
            .iter()
            .map(|row| match &row[0] {
                Value::Integer(v) => *v,
                _ => panic!("expected integer in column 0"),
            })
            .collect();
        assert_eq!(kept, vec![1, 1]);
    }

    #[test]
    fn apply_null_policy_skip_drops_null_rows() {
        let (plan, _join, probe_motion, _build_motion, filter_id) = prepare_resolved_plan();
        let mut exec_plan = ExecutionPlan::new(plan);

        // Filter contains key=1 only. The null row must be dropped by
        // NullPolicy::Skip *before* the contains check is consulted.
        let filter = build_filter_from_keys(&[Value::Integer(1)]);
        exec_plan.filter_state.insert(
            filter_id,
            FilterState {
                filter,
                key_arity: 1,
                apply_spec: ApplySpec::new(vec![0u32], sql_dynfilter::NullPolicy::Skip),
            },
        );

        let mut vtable = VirtualTable::new();
        vtable.add_tuple(vec![Value::Integer(1)]);
        vtable.add_tuple(vec![Value::Null]);
        vtable.add_tuple(vec![Value::Integer(2)]);

        apply_filters_to_vtable(&exec_plan, probe_motion, &mut vtable).unwrap();
        assert_eq!(vtable.get_tuples().len(), 1);
        assert!(matches!(vtable.get_tuples()[0][0], Value::Integer(1)));
    }

    #[test]
    fn apply_errors_on_missing_filter_state() {
        let (plan, _join, probe_motion, _build_motion, _filter_id) = prepare_resolved_plan();
        let exec_plan = ExecutionPlan::new(plan);
        // filter_state intentionally empty — the prior slice didn't run.
        let mut vtable = VirtualTable::new();
        vtable.add_tuple(vec![Value::Integer(1)]);
        let err = apply_filters_to_vtable(&exec_plan, probe_motion, &mut vtable).unwrap_err();
        assert!(
            format!("{err}").contains("not built by any prior slice"),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn apply_errors_on_arity_mismatch() {
        let (plan, _join, probe_motion, _build_motion, filter_id) = prepare_resolved_plan();
        let mut exec_plan = ExecutionPlan::new(plan);

        // Resolver-chosen arity is 1 (single equi-column). Lie that the
        // build side hashed 2-column rows — the guard must catch it.
        let filter = build_filter_from_keys(&[Value::Integer(1)]);
        exec_plan.filter_state.insert(
            filter_id,
            FilterState {
                filter,
                key_arity: 2,
                apply_spec: ApplySpec::new(vec![0u32, 1u32], sql_dynfilter::NullPolicy::Skip),
            },
        );

        let mut vtable = VirtualTable::new();
        vtable.add_tuple(vec![Value::Integer(1)]);
        let err = apply_filters_to_vtable(&exec_plan, probe_motion, &mut vtable).unwrap_err();
        assert!(format!("{err}").contains("arity"), "unexpected err: {err}");
    }

    #[test]
    fn canonical_bytes_cover_every_value_variant() {
        // Bit-for-bit determinism across all 9 type-tagged variants.
        // If any branch diverges, build/apply hashes silently disagree
        // and the filter drops correct rows. We compare each variant
        // against itself and against its neighbours to guard both
        // stability and tag-discrimination.
        use crate::frontend::sql::try_parse_datetime;

        let now = try_parse_datetime("2024-05-04T12:30:00Z").unwrap();
        let later = try_parse_datetime("2024-05-04T12:30:01Z").unwrap();
        let uuid_a = tarantool::uuid::Uuid::from_bytes([
            0xCA, 0xFE, 0xBA, 0xBE, 0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB,
            0xCD, 0xEF,
        ]);
        let uuid_b = tarantool::uuid::Uuid::from_bytes([
            0xCA, 0xFE, 0xBA, 0xBE, 0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB,
            0xCD, 0xEE,
        ]);

        // (representative, neighbour-that-must-not-collide)
        let cases: Vec<(Value, Value)> = vec![
            (Value::Integer(42), Value::Integer(43)),
            (
                Value::Double(Double::from(1.5)),
                Value::Double(Double::from(1.5000001)),
            ),
            (
                Value::Decimal(Box::new(TDecimal::from_str("1.5").unwrap())),
                Value::Decimal(Box::new(TDecimal::from_str("1.6").unwrap())),
            ),
            (Value::Boolean(true), Value::Boolean(false)),
            (Value::String("k".into()), Value::String("kk".into())),
            (Value::Uuid(uuid_a), Value::Uuid(uuid_b)),
            (Value::Datetime(now), Value::Datetime(later)),
            (
                Value::Tuple(crate::ir::value::Tuple::from(vec![Value::Integer(1)])),
                Value::Tuple(crate::ir::value::Tuple::from(vec![Value::Integer(2)])),
            ),
        ];
        for (rep, neighbour) in &cases {
            assert_eq!(
                hash_one(rep),
                hash_one(rep),
                "non-deterministic hash for {rep:?}"
            );
            assert_ne!(
                hash_one(rep),
                hash_one(neighbour),
                "neighbour collision for {rep:?} vs {neighbour:?}"
            );
        }
        // Null must distinguishable from every concrete value.
        for (rep, _) in &cases {
            assert_ne!(
                hash_one(&Value::Null),
                hash_one(rep),
                "Null collides with {rep:?}"
            );
        }
    }

    #[test]
    fn canonical_bytes_discriminate_across_type_tags() {
        // Different *types* with byte forms that overlap if the tag were
        // dropped (e.g. the i64 LE of 1 vs the UTF-8 of "1") must hash to
        // different values. This is what stops a 1-row build of an
        // INT column from accidentally also matching a `'1'` STRING
        // probe.
        assert_ne!(
            hash_one(&Value::Integer(1)),
            hash_one(&Value::String("1".into()))
        );
        assert_ne!(
            hash_one(&Value::Integer(0)),
            hash_one(&Value::Boolean(false))
        );
        assert_ne!(
            hash_one(&Value::Double(Double::from(1.0))),
            hash_one(&Value::Decimal(Box::new(TDecimal::from_str("1").unwrap())))
        );
    }

    #[test]
    fn nested_tuple_canonical_is_order_sensitive() {
        // The Tuple branch recurses element-wise with separators; swapping
        // sibling order must change the hash. Without this, a tuple of
        // (1, 2) would collide with (2, 1) and the apply phase would
        // mis-retain rows whose key columns happened to share a multiset
        // but not an order.
        let ab = Value::Tuple(crate::ir::value::Tuple::from(vec![
            Value::Integer(1),
            Value::Integer(2),
        ]));
        let ba = Value::Tuple(crate::ir::value::Tuple::from(vec![
            Value::Integer(2),
            Value::Integer(1),
        ]));
        assert_ne!(hash_one(&ab), hash_one(&ba));
    }

    #[test]
    fn hash_row_arity_two_position_swap_changes_hash() {
        // Multi-column key paths feed the hasher in `positions` order.
        // Swapping the column ordering must produce a different u64,
        // mirroring the build/apply pair's invariant that the resolver
        // hand the same ordering to both sides.
        let row = vec![Value::String("a".into()), Value::String("b".into())];
        let ab = hash_row(&row, &[0, 1], NullPolicy::Skip);
        let ba = hash_row(&row, &[1, 0], NullPolicy::Skip);
        match (ab, ba) {
            (KeyHash::Value(a), KeyHash::Value(b)) => assert_ne!(a, b),
            _ => panic!("expected two values"),
        }
    }

    #[test]
    fn hash_row_insert_null_distinct_from_default_value() {
        // NullPolicy::Insert hashes a null tag rather than skipping the
        // row. The resulting hash must differ from a row where the same
        // column carries any concrete value, otherwise the build side
        // would treat (NULL, x) and (sentinel, x) as the same key.
        let row_null = vec![Value::Integer(1), Value::Null];
        let row_concrete = vec![Value::Integer(1), Value::Integer(0)];
        let h_null = match hash_row(&row_null, &[0, 1], NullPolicy::Insert) {
            KeyHash::Value(v) => v,
            KeyHash::Drop => panic!("Insert must not drop"),
        };
        let h_concrete = match hash_row(&row_concrete, &[0, 1], NullPolicy::Insert) {
            KeyHash::Value(v) => v,
            KeyHash::Drop => panic!("Insert must not drop"),
        };
        assert_ne!(h_null, h_concrete);
    }

    #[test]
    fn build_filters_for_motion_inserts_into_filter_state() {
        // Drive `build_filters_for_motion` end-to-end without the apply
        // pass: prove that the build side actually populates
        // `filter_state` with a finalized filter whose `contains` agrees
        // with a manually computed hash for each input row.
        let (plan, _join, _probe_motion, build_motion, filter_id) = prepare_resolved_plan();
        let build_pos = build_pos_for(&plan, filter_id);
        let mut exec_plan = ExecutionPlan::new(plan);

        let keys: Vec<i64> = vec![10, 20, 30];
        let row_width = build_pos + 1;
        let mut vt = VirtualTable::new();
        for &k in &keys {
            let mut row = vec![Value::Null; row_width];
            row[build_pos] = Value::Integer(k);
            vt.add_tuple(row);
        }
        exec_plan
            .get_mut_vtables()
            .insert(build_motion, std::rc::Rc::new(vt));
        build_filters_for_motion(&mut exec_plan, build_motion).unwrap();

        let state = exec_plan.filter_state.get(&filter_id).expect("populated");
        assert_eq!(state.key_arity, 1);
        for &k in &keys {
            let row = vec![Value::Integer(k)];
            match hash_row(&row, &[0], NullPolicy::Skip) {
                KeyHash::Value(h) => assert!(state.filter.contains(h)),
                KeyHash::Drop => unreachable!(),
            }
        }
    }

    #[test]
    fn build_filters_for_motion_no_op_when_motion_has_no_build_parent() {
        // The build pass must do nothing — including not allocating an
        // empty filter — when no BuildFilter sits above the given motion.
        // Otherwise stray empty filters would leak into the wire packet
        // and confuse storages that gate on a non-empty filter_state.
        let (plan, _join, _probe_motion, build_motion) = make_two_motion_inner_join();
        let mut exec_plan = ExecutionPlan::new(plan);
        let mut vt = VirtualTable::new();
        vt.add_tuple(vec![Value::Integer(1)]);
        exec_plan
            .get_mut_vtables()
            .insert(build_motion, std::rc::Rc::new(vt));
        build_filters_for_motion(&mut exec_plan, build_motion).unwrap();
        assert!(exec_plan.filter_state.is_empty());
    }

    #[test]
    fn large_scale_fuse_path_build_apply_e2e() {
        // Stress the full §5.4 build/apply pipeline at a key count that
        // pushes `DynamicFilter::new` past `FUSE_THRESHOLD`, then verify
        // (a) every built key survives apply, (b) every disjoint key is
        // dropped (modulo the bounded Fuse FPR). Catches drift between
        // the build-side `canonical_bytes_into` and any future
        // storage-side reimplementation.
        let (plan, _join, probe_motion, build_motion, filter_id) = prepare_resolved_plan();
        let build_pos = build_pos_for(&plan, filter_id);
        let apply_pos = apply_pos_for(&plan, filter_id);
        let row_width = build_pos.max(apply_pos) + 1;

        let mut exec_plan = ExecutionPlan::new(plan);

        let n: i64 = (sql_dynfilter::FUSE_THRESHOLD as i64) * 4; // ~1024
        let mut build_vt = VirtualTable::new();
        for k in 0..n {
            let mut row = vec![Value::Null; row_width];
            row[build_pos] = Value::Integer(k);
            build_vt.add_tuple(row);
        }
        exec_plan
            .get_mut_vtables()
            .insert(build_motion, std::rc::Rc::new(build_vt));
        build_filters_for_motion(&mut exec_plan, build_motion).unwrap();

        // Probe: half hits (0..n), half misses (10*n .. 10*n + n). With
        // 8-bit fingerprints the worst-case FPR is ~1/256; we allow 5%.
        let mut probe_vt = VirtualTable::new();
        let mut expected_hits: usize = 0;
        for k in 0..n {
            let mut row = vec![Value::Null; row_width];
            row[apply_pos] = Value::Integer(k);
            probe_vt.add_tuple(row);
            expected_hits += 1;
        }
        for k in (10 * n)..(11 * n) {
            let mut row = vec![Value::Null; row_width];
            row[apply_pos] = Value::Integer(k);
            probe_vt.add_tuple(row);
        }
        apply_filters_to_vtable(&exec_plan, probe_motion, &mut probe_vt).unwrap();

        // All `expected_hits` rows must survive. Allow at most 5% extra
        // (false-positive misses), comfortably above XOR8's theoretical
        // ~0.4% FPR.
        let survived = probe_vt.get_tuples().len();
        assert!(
            survived >= expected_hits,
            "lost true hits: {survived} < {expected_hits}"
        );
        let fpr_allowance = expected_hits + (n as usize / 20).max(1);
        assert!(
            survived <= fpr_allowance,
            "too many survivors: {survived} > allowance {fpr_allowance}"
        );
    }

    #[test]
    fn apply_filter_string_keys_distinguish_correctly() {
        // Exercise the String tag on the apply side: tags differ from
        // integers, so a filter built on `'1'` must NOT match the
        // integer 1 even though their canonical body bytes overlap.
        let (plan, _join, probe_motion, _build_motion, filter_id) = prepare_resolved_plan();
        let apply_pos = apply_pos_for(&plan, filter_id);
        let mut exec_plan = ExecutionPlan::new(plan);

        // Build a filter that contains only the canonical-hash of '1'
        // (String). The integer 1 must be dropped.
        let mut filter = DynamicFilter::new(1);
        let row = vec![Value::String("1".into())];
        if let KeyHash::Value(h) = hash_row(&row, &[0], NullPolicy::Skip) {
            filter.insert(h);
        }
        filter.finalize().unwrap();
        exec_plan.filter_state.insert(
            filter_id,
            FilterState {
                filter,
                key_arity: 1,
                apply_spec: ApplySpec::new(vec![0u32], sql_dynfilter::NullPolicy::Skip),
            },
        );

        let row_width = apply_pos + 1;
        let mut vt = VirtualTable::new();
        let mut r1 = vec![Value::Null; row_width];
        r1[apply_pos] = Value::Integer(1);
        vt.add_tuple(r1);
        let mut r2 = vec![Value::Null; row_width];
        r2[apply_pos] = Value::String("1".into());
        vt.add_tuple(r2);
        apply_filters_to_vtable(&exec_plan, probe_motion, &mut vt).unwrap();
        // Only the String row survives.
        assert_eq!(vt.get_tuples().len(), 1);
        assert!(matches!(&vt.get_tuples()[0][apply_pos], Value::String(s) if s.as_str() == "1"));
    }

    #[test]
    fn build_then_apply_roundtrip_lets_matching_rows_through() {
        // End-to-end: build the filter from the build-side vtable, then
        // apply it to a probe-side vtable whose JOIN-key column overlaps
        // the build keys at the resolver-chosen positions. Rows whose
        // key matches must survive; rows whose key does not must be
        // dropped. Proves canonical hashing is bit-identical across the
        // two phases.
        let (plan, _join, probe_motion, build_motion, filter_id) = prepare_resolved_plan();
        let build_pos = build_pos_for(&plan, filter_id);
        let apply_pos = apply_pos_for(&plan, filter_id);
        let mut exec_plan = ExecutionPlan::new(plan);

        // Build vtable: only the build-key column matters. Fill it with
        // values {1, 2, 3} at `build_pos`; pad the rest with sentinels
        // to make the row wide enough.
        let row_width = build_pos.max(apply_pos) + 1;
        let build_keys: Vec<i64> = vec![1, 2, 3];
        let mut build_vtable = VirtualTable::new();
        for &k in &build_keys {
            let mut row = vec![Value::Null; row_width];
            row[build_pos] = Value::Integer(k);
            build_vtable.add_tuple(row);
        }
        exec_plan
            .get_mut_vtables()
            .insert(build_motion, std::rc::Rc::new(build_vtable));
        build_filters_for_motion(&mut exec_plan, build_motion).unwrap();

        // Probe vtable: same layout shape, but key column at `apply_pos`.
        let probe_keys: Vec<i64> = vec![2, 99, 3, 4];
        let mut probe_vtable = VirtualTable::new();
        for &k in &probe_keys {
            let mut row = vec![Value::Null; row_width];
            row[apply_pos] = Value::Integer(k);
            probe_vtable.add_tuple(row);
        }
        apply_filters_to_vtable(&exec_plan, probe_motion, &mut probe_vtable).unwrap();
        let kept: Vec<i64> = probe_vtable
            .get_tuples()
            .iter()
            .map(|row| match &row[apply_pos] {
                Value::Integer(v) => *v,
                _ => panic!("expected integer at apply_pos"),
            })
            .collect();
        assert_eq!(kept, vec![2, 3]);
    }
}

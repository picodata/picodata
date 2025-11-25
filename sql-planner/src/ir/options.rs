use crate::errors::{Entity, SbroadError};
use crate::ir::node::relational::Relational;
use crate::ir::value::Value;
use crate::ir::Plan;
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;
use std::fmt::{Display, Formatter};

pub const DEFAULT_SQL_MOTION_ROW_MAX: u64 = 5000;
pub const DEFAULT_SQL_VDBE_OPCODE_MAX: u64 = 45000;

/// SQL options specified by user in `option(..)` clause.
///
/// Note: ddl options are handled separately.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Options {
    /// Maximum size of the virtual table that this query can produce or use during
    /// query execution. This limit is checked on storage before sending a result table,
    /// and on router before appending the result from one storage to results from other
    /// storages. Value of `0` indicates that this limit is disabled.
    ///
    /// Note: this limit allows the out of memory error for query execution in the following
    /// scenario: if already received vtable has `X` rows and `X + a` causes the OOM, then
    /// if one of the storages returns `a` or more rows, the OOM will occur.
    pub sql_motion_row_max: i64,
    /// Options passed to `box.execute` function on storages. Currently there is only one option
    /// `sql_vdbe_opcode_max`.
    pub sql_vdbe_opcode_max: i64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            sql_motion_row_max: DEFAULT_SQL_MOTION_ROW_MAX as i64,
            sql_vdbe_opcode_max: DEFAULT_SQL_VDBE_OPCODE_MAX as i64,
        }
    }
}

/// Like [`Options`], but with some values unspecified.
#[derive(Default, Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct PartialOptions {
    pub sql_motion_row_max: Option<i64>,
    pub sql_vdbe_opcode_max: Option<i64>,
}

impl PartialOptions {
    /// Creates a full [`Options`] value.
    ///
    /// If a value is specified in `self`, then it will be used.
    /// Otherwise, the corresponding value from `defaults` will be used.
    pub fn unwrap_or(&self, defaults: Options) -> Options {
        Options {
            sql_motion_row_max: self
                .sql_motion_row_max
                .unwrap_or(defaults.sql_motion_row_max),
            sql_vdbe_opcode_max: self
                .sql_vdbe_opcode_max
                .unwrap_or(defaults.sql_vdbe_opcode_max),
        }
    }
}

// ==== ast types ====

#[derive(PartialEq, Eq, Debug, Clone, Deserialize, Serialize)]
pub enum OptionParamValue {
    /// This option contains a literal value
    Value { val: Value },
    /// This option value is parametrized
    Parameter {
        /// Index of the referred parameter, starting with 0
        index: usize,
    },
}

/// A pair of [`OptionKind`] and associated value for it specified in an OPTIONS clause
///
/// `T` could be either [`OptionParamValue`] for unresolved options (can can contain references to parameters),
///   or [`Value`] for already resolved options.
#[derive(PartialEq, Eq, Debug, Clone, Deserialize, Serialize)]
pub struct OptionSpec<T> {
    pub kind: OptionKind,
    pub val: T,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, Deserialize, Serialize)]
pub enum OptionKind {
    /// `sql_vdbe_opcode_max`
    VdbeOpcodeMax,
    /// `sql_motion_row_max`
    MotionRowMax,
}

impl Display for OptionKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            OptionKind::VdbeOpcodeMax => "sql_vdbe_opcode_max",
            OptionKind::MotionRowMax => "sql_motion_row_max",
        };
        write!(f, "{s}")
    }
}

// ==== validation types ====

/// Like [`Option`], but tracks whether the option wasn't specified
/// and should be kept as default, or if was specified, but the value isn't known yet.
///
/// This is needed to validate the usage of option referring to a query parameter
/// before the parameter values are supplied.
#[derive(Default, Copy, Clone, Debug, PartialEq, Eq)]
enum LoweredOptionValue<T> {
    /// Not specified, so kept at the default value
    #[default]
    Default,
    /// Specified, but concrete value not known
    Unknown,
    /// Specified and the value known
    Known(T),
}

impl<T> LoweredOptionValue<T> {
    /// Specify the value of the option.
    /// If `None` value is supplied, the option will be marked as specified, but with unknown value.
    ///
    /// If an option was already specified, this function will overwrite the previous value.
    pub fn specify_opt(&mut self, value: Option<T>) {
        *self = match value {
            None => LoweredOptionValue::Unknown,
            Some(value) => LoweredOptionValue::Known(value),
        };
    }

    /// Gets the value of the option, if known
    pub fn try_get_value(self) -> Option<T> {
        match self {
            LoweredOptionValue::Default | LoweredOptionValue::Unknown => None,
            LoweredOptionValue::Known(value) => Some(value),
        }
    }

    /// Unwraps an option to the default value. Will fall back to `default` if it wasn't specified or panic if it was specified with unknown a value.
    pub fn unwrap(self, default: T) -> T {
        match self {
            LoweredOptionValue::Default => default,
            LoweredOptionValue::Unknown => {
                panic!("Called `PartialOptions::unwrap()` on `PartialOptionValue::Unknown` value")
            }
            LoweredOptionValue::Known(value) => value,
        }
    }
}

/// Like [`PartialOptions`], but tracks whether the option wasn't specified
/// and should be kept as default, or if was specified, but the value isn't known yet.
///
/// This is needed to validate the usage of option referring to a query parameter
/// before the parameter values are supplied.
#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub(super) struct LoweredOptions {
    sql_motion_row_max: LoweredOptionValue<i64>,
    sql_vdbe_opcode_max: LoweredOptionValue<i64>,
}

impl LoweredOptions {
    pub fn unwrap(self, default: Options) -> Options {
        Options {
            sql_motion_row_max: self.sql_motion_row_max.unwrap(default.sql_motion_row_max),
            sql_vdbe_opcode_max: self.sql_vdbe_opcode_max.unwrap(default.sql_vdbe_opcode_max),
        }
    }
}

/// Lower option specification into a [`LoweredOptions`].
///
/// # Errors
/// - Invalid parameter value for given option
/// - The same option used more than once
pub(super) fn lower_options(
    // TODO: if we pass a type in absence of a value, we will be able to fail more invalid queries earlier
    resolved_options: &[OptionSpec<Option<Value>>],
) -> Result<LoweredOptions, SbroadError> {
    fn lower_unsigned(kind: OptionKind, val: &Value) -> Result<i64, SbroadError> {
        match *val {
            // Supporting conversion from integer is important for parametrized options via pgproto:
            // pgproto requires parameter types to be inferred. Since there is no longer an
            // `Unsigned` type in the typesystem exposed to the user, we get the next best thing and use an integer.
            Value::Integer(num) if num >= 0 => Ok(num),
            ref val => Err(SbroadError::Invalid(
                Entity::OptionSpec,
                Some(format_smolstr!(
                    "expected option {} to be a non-negative integer, got: {val:?}",
                    kind
                )),
            )),
        }
    }

    let mut result = LoweredOptions::default();

    for &OptionSpec { kind, ref val } in resolved_options {
        // for better UX we _could_ collect all the possible errors before short-circuiting to an error condition
        // but there are no primitives in sbroad to support this :(

        // all the options use `Unsigned` type for now, so we can lower the type immediately
        let value = val
            .as_ref()
            .map(|val| lower_unsigned(kind, val))
            .transpose()?;

        match kind {
            OptionKind::VdbeOpcodeMax => result.sql_vdbe_opcode_max.specify_opt(value),
            OptionKind::MotionRowMax => result.sql_motion_row_max.specify_opt(value),
        }
    }

    Ok(result)
}

impl Plan {
    fn get_inserted_values_count(&self) -> Result<Option<usize>, SbroadError> {
        let id = self.get_top()?;
        if let Ok(Relational::Insert(_)) = self.get_relation_node(id) {
            // if it's an insert - try to determine number of values we are trying to insert.
            // for unoptimized queries we can look at the first child to find the Values node,
            // optimized queries have motions, however, and we have to support this too
            let child_id = self.get_first_rel_child(id)?;
            match self.get_relation_node(child_id)? {
                Relational::Motion(_) => {
                    let child2_id = self.get_first_rel_child(child_id)?;
                    if let Relational::Values(values) = self.get_relation_node(child2_id)? {
                        return Ok(Some(values.children.len()));
                    }
                }
                Relational::Values(values) => return Ok(Some(values.children.len())),
                _ => {}
            }
        }

        Ok(None)
    }

    /// Validate options usage.
    ///
    /// # Errors
    /// - This is an insert query, and it has more than `sql_motion_row_max` values.
    pub(super) fn validate_options_usage(
        &self,
        lowered: &LoweredOptions,
    ) -> Result<(), SbroadError> {
        // We need to check if the plan has a top node and if it is an Insert with Values.
        // If it is, we can determine the number of values in the Values node and use it
        // to make an early decision about the maximum number of rows we can handle.
        let values_count = self.get_inserted_values_count()?;

        // NB: this will not perform validation if a default value for `sql_motion_row_max` is used
        // FIXME: use let_chains once we are on new enough rust version
        if let (Some(values_count), Some(limit)) =
            (values_count, lowered.sql_motion_row_max.try_get_value())
        {
            if limit > 0 && limit < values_count as i64 {
                return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                    "Exceeded maximum number of rows ({}) in virtual table: {}",
                    limit,
                    values_count,
                )));
            }
        }

        Ok(())
    }
}

#[cfg(all(test, feature = "mock"))]
mod test {
    use crate::ir::transformation::helpers::{sql_to_ir, sql_to_optimized_ir};

    #[test]
    fn test_inserted_values_count() {
        let q = "insert into t values (-1, 1, 42, 42), (-2, 2, 42, 42), (-3, 3, 42, 42)";

        assert_eq!(
            sql_to_ir(q, Vec::new())
                .get_inserted_values_count()
                .unwrap(),
            Some(3)
        );
        assert_eq!(
            sql_to_optimized_ir(q, Vec::new())
                .get_inserted_values_count()
                .unwrap(),
            Some(3)
        );
        let q = "select 1";

        assert_eq!(
            sql_to_ir(q, Vec::new())
                .get_inserted_values_count()
                .unwrap(),
            None
        );
        assert_eq!(
            sql_to_optimized_ir(q, Vec::new())
                .get_inserted_values_count()
                .unwrap(),
            None
        );
    }
}

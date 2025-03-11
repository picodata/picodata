use crate::error::Error;
use crate::expr::{ComparisonOperator, Expr, ExprKind, Type, UnaryOperator};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::hash::{BuildHasherDefault, DefaultHasher, Hash};
use std::iter::zip;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FunctionKind {
    Scalar,
    Aggregate,
    Window,
    Operator,
}

impl Display for FunctionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            FunctionKind::Operator => "operator",
            FunctionKind::Scalar | FunctionKind::Aggregate => "function",
            FunctionKind::Window => "window function",
        };
        write!(f, "{}", kind)
    }
}

/// Function or operator signature.
/// The resolution rules are the same for all function kinds, but different expressions consider
/// functions with different kinds. For example, `ExprKind::Window` will consider only
/// functions with kind `FunctionKind::Window`.
///
/// In addition, aggregate and window functions can have additional limitations.
/// For instance, they cannot be used in a WHERE clause.
#[derive(Debug, Clone)]
pub struct Function {
    pub name: String,
    pub args_types: Vec<Type>,
    pub return_type: Type,
    pub kind: FunctionKind,
}

impl Function {
    pub fn new_operator(
        name: impl Into<String>,
        args_types: impl Into<Vec<Type>>,
        return_type: Type,
    ) -> Self {
        Self {
            name: name.into(),
            args_types: args_types.into(),
            return_type,
            kind: FunctionKind::Operator,
        }
    }

    pub fn new_scalar(
        name: impl Into<String>,
        args_types: impl Into<Vec<Type>>,
        return_type: Type,
    ) -> Self {
        Self {
            name: name.into(),
            args_types: args_types.into(),
            return_type,
            kind: FunctionKind::Scalar,
        }
    }

    pub fn new_aggregate(
        name: impl Into<String>,
        args_types: impl Into<Vec<Type>>,
        return_type: Type,
    ) -> Self {
        Self {
            name: name.into(),
            args_types: args_types.into(),
            return_type,
            kind: FunctionKind::Aggregate,
        }
    }

    pub fn new_window(
        name: impl Into<String>,
        args_types: impl Into<Vec<Type>>,
        return_type: Type,
    ) -> Self {
        Self {
            name: name.into(),
            args_types: args_types.into(),
            return_type,
            kind: FunctionKind::Window,
        }
    }
}

// Faster hasher that is vulnerable for DoS attacks,
// which isn't a problem for us, because we do not process any user input.
type StableHasher = BuildHasherDefault<DefaultHasher>;
type StableHashMap<K, V> = HashMap<K, V, StableHasher>;

/// Result of type analysis.
/// It contains information about expression types and required type coercions.
/// The user must perform coercions in accordance with the report to make original expression match
/// the inferred types.
#[derive(Debug, Clone)]
pub struct TypeReport<Id: Hash + Eq + Clone> {
    // TODO: consider using smallvec for small reports.
    // TODO: consider using different hash table or different state.
    types: StableHashMap<Id, Type>,
    casts: StableHashMap<Id, Type>,
}

impl<Id: Hash + Eq + Clone> TypeReport<Id> {
    fn new() -> Self {
        Self {
            types: StableHashMap::default(),
            casts: StableHashMap::default(),
        }
    }

    /// Get expression type.
    ///
    /// # Panics
    /// Panics if there is no such id in the report.
    #[track_caller]
    pub fn get_type(&self, expr: &Expr<Id>) -> Type {
        self.types[&expr.id]
    }

    /// Merge 2 reports.
    pub fn extend(&mut self, other: Self) {
        self.types.extend(other.types);
        self.casts.extend(other.casts);
    }

    /// Report expression type.
    fn report(&mut self, id: &Id, ty: Type) {
        self.types.insert(id.clone(), ty);
    }

    /// Report required coercion for an expression.
    fn cast(&mut self, id: &Id, ty: Type) {
        self.casts.insert(id.clone(), ty);
    }

    /// Calculate report cost, which equals to the number of required coercions.
    /// Reports with lower costs are preferred by the type inference algorithm.
    fn cost(&self) -> usize {
        self.casts.len()
    }
}

/// Type system defines a set of supported functions and type coercions.
pub struct TypeSystem {
    /// Mapping of function names to their available overloads.
    functions: StableHashMap<String, Vec<Function>>,
}

impl TypeSystem {
    pub fn new(functions_list: Vec<Function>) -> Self {
        let mut functions = StableHashMap::default();
        for function in functions_list {
            functions
                .entry(function.name.clone())
                .or_insert_with(Vec::new)
                .push(function);
        }

        Self { functions }
    }

    pub fn can_coerce(&self, from: Type, to: Type) -> bool {
        if from == to {
            return true;
        }

        use Type::*;
        match from {
            Unsigned => matches!(to, Integer | Double | Numeric),
            Integer => matches!(to, Double | Numeric),
            Double => matches!(to, Numeric),
            _else => false,
        }
    }
}

type TypeAnalyzerCache<Id> = StableHashMap<(Id, Option<Type>), TypeReport<Id>>;

/// Analyzes and infers expression types using a provided `TypeSystem` which defines
/// supported operations and type coercions.
///
/// WARNING: Analyzer caches intermediate results for efficiency. This caching is safe when the
/// same expression is analyzed multiple times, or when analyzing a compound expression
/// containing previously analyzed subexpressions (e.g analyzing 1 + b after analyzing 1 and b).
/// The analyzer becomes invalid when previously analyzed expressions are modified. In than case
/// a new analyzer instance must be created.
pub struct TypeAnalyzer<'a, Id: Hash + Eq + Clone> {
    /// Type system providing functions and coercions.
    type_system: &'a TypeSystem,
    /// Cache for intermediate results to avoid redundant recalculations.
    cache: TypeAnalyzerCache<Id>,
}

impl<'a, Id: Hash + Eq + Clone> TypeAnalyzer<'a, Id> {
    pub fn new(type_system: &'a TypeSystem) -> Self {
        Self {
            type_system,
            cache: Default::default(),
        }
    }

    /// Perform type analysis. All expression types and coercions are reported in `TypeReport`.
    /// `desired_type` gives a hint on what type is expected, but the inferred type can be
    /// different. This is the caller responsibility to ensure that the expression has a
    /// suitable type.
    pub fn analyze(
        &mut self,
        expr: &Expr<Id>,
        desired_type: Option<Type>,
    ) -> Result<TypeReport<Id>, Error> {
        if let Some(report) = self.cache.borrow().get(&(expr.id.clone(), desired_type)) {
            return Ok(report.clone());
        }

        let report = self.analyze_expr(expr, desired_type)?;

        self.cache
            .insert((expr.id.clone(), desired_type), report.clone());
        Ok(report)
    }

    fn analyze_expr(
        &mut self,
        expr: &Expr<Id>,
        desired_type: Option<Type>,
    ) -> Result<TypeReport<Id>, Error> {
        match &expr.kind {
            ExprKind::Null => {
                // TODO: should we explicitly cast null to desired?
                let ty = desired_type.unwrap_or(Type::Unknown);
                let mut report = TypeReport::new();
                report.report(&expr.id, ty);
                Ok(report)
            }
            ExprKind::Reference(ty) => {
                let mut report = TypeReport::new();
                report.report(&expr.id, *ty);
                Ok(report)
            }
            ExprKind::Literal(ty) => {
                // Literals are similar to references, except literals can be coerced to desired.
                let mut report = TypeReport::new();

                if *ty == Type::Numeric && desired_type == Some(Type::Double) {
                    // Floating pointer literals have `numeric` type by default. However, in a
                    // context with `double` values it should be coerced to `double`. Note that
                    // this can only be done for literals, as expressions like `1.5`
                    // do not have a fixed type.
                    // Example: `insert into t (double_col) values (1.5)`
                    report.cast(&expr.id, Type::Double);
                    report.report(&expr.id, Type::Double);
                    return Ok(report);
                }

                // Coerce literal type to desired if possible.
                if let Some(desired_type) = desired_type {
                    if *ty != desired_type && self.can_coerce(*ty, desired_type) {
                        report.cast(&expr.id, desired_type);
                        report.report(&expr.id, desired_type);
                        return Ok(report);
                    }
                }

                report.report(&expr.id, *ty);
                Ok(report)
            }
            ExprKind::Parameter(name) => {
                if let Some(desired_type) = desired_type {
                    // TODO: ensure type is not a pseudo-type
                    // https://www.postgresql.org/docs/current/datatype-pseudo.html
                    // TEST: `select $1 = null;`
                    let mut report = TypeReport::new();
                    report.report(&expr.id, desired_type);
                    return Ok(report);
                }
                Err(Error::CouldNotDetermineParameterType(name.to_string()))
            }
            ExprKind::Cast(inner, to) => {
                let mut report = self.analyze(inner, Some(*to))?;
                // TODO: check if we can cast expression
                report.report(&expr.id, *to);
                Ok(report)
            }
            ExprKind::Operator(ref name, ref args) => {
                let (ty, mut report) = self.analyze_operator_args(name, args, desired_type)?;
                report.report(&expr.id, ty);
                Ok(report)
            }
            ExprKind::Function(ref name, ref args) => {
                let (ty, mut report) = self.analyze_function_args(name, args, desired_type)?;
                report.report(&expr.id, ty);
                Ok(report)
            }
            ExprKind::WindowFunction {
                ref name,
                ref args,
                filter,
                over,
            } => {
                let (ty, mut report) = self.analyze_window_args(name, args, desired_type)?;
                let over_report = self.analyze(over, None)?;
                report.extend(over_report);

                if let Some(filter) = filter {
                    // TODO: ensure filter expression has boolean type
                    let filter_report = self.analyze(filter, Some(Type::Boolean))?;
                    report.extend(filter_report);
                }

                report.report(&expr.id, ty);
                Ok(report)
            }
            ExprKind::Window {
                order_by,
                partition_by,
                frame,
            } => {
                let desired_types = vec![Type::Unknown; order_by.len()];
                let order_by_report = self.analyze_many(order_by, &desired_types)?;

                let desired_types = vec![Type::Unknown; partition_by.len()];
                let mut partition_by_report = self.analyze_many(partition_by, &desired_types)?;

                if let Some(frame) = frame {
                    if frame.bound_offsets.len() > 2 {
                        return Err(Error::Other(format!(
                            "invalid boud offsets number: {}",
                            frame.bound_offsets.len()
                        )));
                    }
                    assert!(frame.bound_offsets.len() <= 2);
                    for offset in &frame.bound_offsets {
                        // TODO: ensure no variables, otherwise return
                        // "argument of ROWS must not contain variables" error
                        let report = self.analyze(offset, Some(Type::Integer))?;
                        if report.get_type(offset) != Type::Integer {
                            return Err(Error::IncorrectFrameArgumentType(
                                frame.kind,
                                report.get_type(offset),
                            ));
                        }
                    }
                }

                partition_by_report.extend(order_by_report);
                Ok(partition_by_report)
            }
            ExprKind::Coalesce(ref args) => {
                let (ty, mut report) =
                    self.analyze_homogeneous_exprs("COALESCE", args, desired_type)?;
                report.report(&expr.id, ty);
                Ok(report)
            }
            ExprKind::Comparison(op, left, right) => {
                let mut report = self.analyze_comparison_operation(*op, left, right)?;
                report.report(&expr.id, Type::Boolean);
                Ok(report)
            }
            ExprKind::Case {
                when_exprs,
                result_exprs,
            } => {
                let (_, when_report) =
                    self.analyze_homogeneous_exprs("CASE/WHEN", when_exprs, Some(Type::Boolean))?;
                let (result_ty, mut result_report) =
                    self.analyze_homogeneous_exprs("CASE/THEN", result_exprs, desired_type)?;
                result_report.extend(when_report);
                result_report.report(&expr.id, result_ty);
                Ok(result_report)
            }
            ExprKind::Unary(op, child) => {
                let mut report = match op {
                    UnaryOperator::Not => {
                        let report = self.analyze(child, Some(Type::Boolean))?;
                        if report.get_type(child) != Type::Boolean {
                            return Err(Error::UnexpectedNotArgumentType(report.get_type(child)));
                        }
                        report
                    }
                    UnaryOperator::IsNull => self.analyze(child, None)?,
                    UnaryOperator::Exists => {
                        if let ExprKind::Subquery(_) = child.kind {
                            // TODO: analyze subquery
                            TypeReport::new()
                        } else {
                            return Err(Error::Other(
                                "EXISTS can only be applied to subquery".into(),
                            ));
                        }
                    }
                };
                report.report(&expr.id, Type::Boolean);
                Ok(report)
            }
            ExprKind::Subquery(types) => {
                if types.len() == 1 {
                    let mut report = TypeReport::new();
                    report.report(&expr.id, types[0]);
                    return Ok(report);
                }

                Err(Error::SubqueryMustReturnOnlyOneColumn)
            }
            ExprKind::Row(_) => {
                // Rows cannot be analyzed as the system does not support tuples, so we cannot
                // return a reasonable type. However, in some contexts we explicitly check children
                // and perform special measures.
                // See comparison operations for an example of how rows values are handled.
                Err(Error::RowValueMisused)
            }
        }
    }

    // TODO: support desired types
    /// Analyze rows and ensure they have homogeneous types and equal lengths.
    pub fn analyze_homogeneous_rows(
        &mut self,
        ctx: &'static str,
        rows: &[Vec<Expr<Id>>],
    ) -> Result<TypeReport<Id>, Error> {
        let ncolumns = rows.first().map(|r| r.len()).unwrap_or(0);
        for row in rows {
            if row.len() != ncolumns {
                return Err(Error::ListsMustAllBeTheSameLentgh(ctx));
            }
        }

        // TODO: Avoid temporary allocation by creating a columns iterator that yields ith
        // values from every row. `std::iter::from_fn` seems to be handy for that.
        let mut columns = vec![Vec::with_capacity(rows.len()); ncolumns];
        for row in rows {
            for (idx, value) in row.iter().enumerate() {
                columns[idx].push(value);
            }
        }

        let mut report = TypeReport::new();
        for column in columns {
            let (_, r) = self.analyze_homogeneous_exprs(ctx, &column, None)?;
            report.extend(r);
        }

        Ok(report)
    }

    /// Resolve operator overload.
    fn analyze_operator_args(
        &mut self,
        name: &str,
        args: &[Expr<Id>],
        desired_type: Option<Type>,
    ) -> Result<(Type, TypeReport<Id>), Error> {
        let kind = FunctionKind::Operator;
        self.analyze_function_args_impl(name, args, kind, desired_type)?
            .ok_or_else(|| self.could_not_resolve_function_overload_error(kind, name, args))
    }

    /// Resolve scalar or aggregate function overload.
    fn analyze_function_args(
        &mut self,
        name: &str,
        args: &[Expr<Id>],
        desired_type: Option<Type>,
    ) -> Result<(Type, TypeReport<Id>), Error> {
        if let Some(result) =
            self.analyze_function_args_impl(name, args, FunctionKind::Scalar, desired_type)?
        {
            return Ok(result);
        }

        if let Some(result) =
            self.analyze_function_args_impl(name, args, FunctionKind::Aggregate, desired_type)?
        {
            // TODO: introduce context and ensure that aggregate is not misused:
            //  - we are not in where clause
            //  - aggregate is not nested into another aggregate
            //  - something else?
            return Ok(result);
        }

        // Note: Errors for Scalar and Aggregate are the same.
        Err(self.could_not_resolve_function_overload_error(FunctionKind::Scalar, name, args))
    }

    /// Resolve window function overload.
    fn analyze_window_args(
        &mut self,
        name: &str,
        args: &[Expr<Id>],
        desired_type: Option<Type>,
    ) -> Result<(Type, TypeReport<Id>), Error> {
        // TODO: introduce context and ensure that window is not misused
        let kind = FunctionKind::Window;
        self.analyze_function_args_impl(name, args, kind, desired_type)?
            .ok_or_else(|| self.could_not_resolve_function_overload_error(kind, name, args))
    }

    /// Resolve function overload by it's name, arguments and kind.
    ///
    /// # Returns
    /// - `Ok(Some(_))` if overload is resolved
    /// - `Err(_)` if argument analysis encounters an error
    /// - `Ok(None)` if arguments are valid but overload resolution failed
    fn analyze_function_args_impl(
        &mut self,
        name: &str,
        args: &[Expr<Id>],
        kind: FunctionKind,
        desired_type: Option<Type>,
    ) -> Result<Option<(Type, TypeReport<Id>)>, Error> {
        // First, select overloads with the given name.
        let Some(overloads) = self.type_system.functions.get(name) else {
            return Err(Error::FunctionDoesNotExist {
                name: name.to_string(),
                kind,
            });
        };

        // Filter overloads with same number of arguments as it was passed and required kind.
        let overloads: Vec<_> = overloads
            .iter()
            .filter(|f| f.args_types.len() == args.len() && f.kind == kind)
            .collect();

        // Then, for every overload analyze arguments types passing overload types as desired.
        let mut resolved_overloads = Vec::new();
        for overload in overloads {
            let mut report = self.analyze_many(args, &overload.args_types)?;
            let args_types = args.iter().map(|e| report.get_type(e));

            if zip(args_types, &overload.args_types).all(|(t1, t2)| self.can_coerce(t1, *t2)) {
                // Make arguments match overload types.
                for (arg, ty) in zip(args, &overload.args_types) {
                    if report.get_type(arg) != *ty {
                        report.cast(&arg.id, *ty);
                        report.report(&arg.id, *ty);
                    }
                }
                resolved_overloads.push((overload, report));
            }
        }

        // Finally, pick the most suitable candidate,
        // taking into account the costs and the desired type.

        // Start with overloads returning desired type.
        if desired_type.is_some() {
            let mut best_matches = select_best_overloads(&resolved_overloads, desired_type);
            if best_matches.len() == 1 {
                let (func, report) = best_matches.pop().unwrap();
                return Ok(Some((func.return_type, report)));
            }
        }

        // If the previous step didn't succeed, ignore desired type.
        let mut best_matches = select_best_overloads(&resolved_overloads, None);
        if best_matches.len() == 1 {
            let (func, report) = best_matches.pop().unwrap();
            return Ok(Some((func.return_type, report)));
        }

        // In case of ambiguity, select overload that returns a type that can be coerced
        // to other return types. By choosing the "weakest" type we allow the caller to coerce it
        // to a more suitable.
        for (func, report) in &best_matches {
            let return_type = func.return_type;
            let mut other_types = best_matches.iter().map(|(f, _)| f.return_type);
            if other_types.all(|other| self.can_coerce(return_type, other)) {
                return Ok(Some((return_type, report.clone())));
            }
        }

        Ok(None)
    }

    fn could_not_resolve_function_overload_error(
        &mut self,
        kind: FunctionKind,
        name: &str,
        exprs: &[impl Borrow<Expr<Id>>],
    ) -> Error {
        let argtypes = self.analyze_exprs_types_no_error(exprs);
        Error::could_not_resolve_overload(kind, name, argtypes)
    }

    fn analyze_exprs_types_no_error(&mut self, exprs: &[impl Borrow<Expr<Id>>]) -> Vec<Type> {
        exprs
            .iter()
            .map(|e| e.borrow())
            .map(|e| {
                if let Ok(report) = self.analyze(e, None) {
                    report.get_type(e)
                } else {
                    Type::Unknown
                }
            })
            .collect()
    }

    fn analyze_comparison_operation(
        &mut self,
        op: ComparisonOperator,
        left: &Expr<Id>,
        right: &Expr<Id>,
    ) -> Result<TypeReport<Id>, Error> {
        // Handle `a IN (1,2)` first.
        if let (ComparisonOperator::In, ExprKind::Row(right_row)) = (op, &right.kind) {
            if let ExprKind::Row(_) = left.kind {
                return Err(Error::Other("IN operator for rows is not supported".into()));
            }
            let mut exprs = vec![left];
            exprs.append(&mut right_row.iter().collect::<Vec<_>>());
            let (_, report) = self.analyze_homogeneous_exprs("IN", &exprs, None)?;
            return Ok(report);
        }

        let op_fmt = match op {
            // In error messages for in operator we should report '=' operator.
            ComparisonOperator::In => ComparisonOperator::Eq.as_str(),
            _ => op.as_str(),
        };
        let report = match (&left.kind, &right.kind) {
            (ExprKind::Row(left_row), ExprKind::Row(right_row)) => {
                // Example: `(a,b) = (1,2)`
                if left_row.len() != right_row.len() {
                    return Err(Error::UnequalNumberOfEntriesInRowExpression(
                        left_row.len(),
                        right_row.len(),
                    ));
                }

                let mut report = TypeReport::new();
                for (l, r) in zip(left_row, right_row) {
                    let (_, r) = self.analyze_homogeneous_operator_args(op_fmt, &[l, r], None)?;
                    report.extend(r);
                }

                report
            }
            (ExprKind::Row(row), ExprKind::Subquery(sq_types))
            | (ExprKind::Subquery(sq_types), ExprKind::Row(row)) => {
                // Example: `(1, 2) = (select 1, 2)`
                if row.len() != sq_types.len() {
                    return Err(Error::SubqueryReturnsUnexpectedNumberOfColumns(
                        sq_types.len(),
                        row.len(),
                    ));
                }

                // TODO: It'd be better to infer row types first and then use these types
                // as desired for a subquery.
                let report = self.analyze_many(row, sq_types)?;
                let row_types = row.iter().map(|e| report.get_type(e));

                // TODO: Avoid type reordering if subquery is on the left.
                // Ensure types match.
                for (row_type, sq_type) in zip(row_types, sq_types) {
                    // TODO: actually coerce types
                    if !self.can_coerce(row_type, *sq_type) && !self.can_coerce(*sq_type, row_type)
                    {
                        return Err(Error::could_not_resolve_overload(
                            FunctionKind::Operator,
                            op_fmt,
                            [row_type, *sq_type],
                        ));
                    }
                }

                report
            }
            (ExprKind::Subquery(left_types), ExprKind::Subquery(right_types)) => {
                // Example: `(select 1, 2) = (values (1, 2))`
                if left_types.len() != right_types.len() {
                    return Err(Error::SubqueryReturnsUnexpectedNumberOfColumns(
                        left_types.len(),
                        right_types.len(),
                    ));
                }

                for (l, r) in zip(left_types, right_types) {
                    // TODO: actually coerce types
                    if !self.can_coerce(*l, *r) && !self.can_coerce(*r, *l) {
                        return Err(Error::could_not_resolve_overload(
                            FunctionKind::Operator,
                            op_fmt,
                            [*l, *r],
                        ));
                    }
                }

                TypeReport::new()
            }
            _ => {
                // Examples: `1 + 2 = 2 * 2`, `1 + 2 = (select 3)`
                let (_, report) =
                    self.analyze_homogeneous_operator_args(op_fmt, &[left, right], None)?;
                report
            }
        };
        Ok(report)
    }

    /// Analyze expressions and coerce them to a common type.
    /// This should be used for expressions like COALESCE or CASE where the number of arguments and
    /// types are not fixed, but types are expected to be the same (accounting coercions).
    ///
    /// Note: This function differs from `analyze_homogenous_func_args` only in the error message.
    pub fn analyze_homogeneous_exprs(
        &mut self,
        context: &'static str,
        args: &[impl Borrow<Expr<Id>>],
        desired_type: Option<Type>,
    ) -> Result<(Type, TypeReport<Id>), Error> {
        self.analyze_homogeneous_exprs_impl(args, desired_type)?
            .ok_or_else(|| self.types_cannot_be_matched_error(context, args))
    }

    /// Analyze expressions and coerce them to a common type.
    /// This should be used for operators which argument types are not fixed,
    /// but types are expected to be the same (accounting coercions).
    /// Examples are `=` and `in` operators.
    ///
    /// Note: This function differs from `analyze_homogenous_exprs` only in the error message.
    pub fn analyze_homogeneous_operator_args(
        &mut self,
        context: &str,
        args: &[impl Borrow<Expr<Id>>],
        desired_type: Option<Type>,
    ) -> Result<(Type, TypeReport<Id>), Error> {
        let kind = FunctionKind::Operator;
        self.analyze_homogeneous_exprs_impl(args, desired_type)?
            .ok_or_else(|| self.could_not_resolve_function_overload_error(kind, context, args))
    }

    /// Analyze expressions and coerce them to a common type if possible.
    ///
    /// # Returns
    /// - `Ok(Some(_))` on successful coercion to a shared type
    /// - `Err(_)` if argument analysis encounters an error
    /// - `Ok(None)` if arguments are valid but there is no a common type, enabling
    ///   the caller to generate context-specific error messaging
    fn analyze_homogeneous_exprs_impl(
        &mut self,
        args: &[impl Borrow<Expr<Id>>],
        desired_type: Option<Type>,
    ) -> Result<Option<(Type, TypeReport<Id>)>, Error> {
        // First, find candidates for a common type.
        let mut types = HashSet::new();
        if let Some(desired_type) = desired_type {
            types.insert(desired_type);
        }

        let mut error = None;
        for arg in args {
            if desired_type.is_some() {
                let report = self.analyze(arg.borrow(), desired_type)?;
                types.insert(report.get_type(arg.borrow()));
            }

            // Desired type is None, so analysis may fail.
            // Consider parameter expressions.
            match self.analyze(arg.borrow(), None) {
                Ok(report) => {
                    types.insert(report.get_type(arg.borrow()));
                }
                Err(err) => error = Some(err),
            }
        }

        // Re-throw last error if there are no candidates.
        // TODO: consider adding default desired type (text), so the error will be thrown from the
        // first analysis in the loop above.
        if types.is_empty() {
            return Err(error.unwrap());
        }

        // Then, analyze expressions with candidate types as desired.
        // Filter reports that can be resolved to a common type.
        let mut candidates = Vec::new();
        for ty in types {
            let desired_types = vec![ty; args.len()];
            let mut report = self.analyze_many(args, &desired_types)?;
            let mut args_types = args.iter().map(|e| report.get_type(e.borrow()));
            let can_coerce_all = args_types.all(|arg_type| self.can_coerce(arg_type, ty));
            if can_coerce_all {
                // Make arguments match overload types.
                for arg in args {
                    if report.get_type(arg.borrow()) != ty {
                        report.cast(&arg.borrow().id, ty);
                    }
                }
                candidates.push((ty, report));
            }
        }

        // Finally, pick the most suitable candidate, taking into account candidates
        // and desired type.
        //
        // Note that in contrast to function analysis we ignore costs here.
        // Consider costs in the following examples:
        //  - `COALESCE(1.5, 1.5::double)`
        //    `numeric` and `double` literals met, prefer `double`, similar to PostgreSQL.
        //    For both types costs are the same (1), and we choose `double`, as it can be coerced
        //    to `numeric`. Seems fine.
        //  - `COALESCE(1.5, 1.5, 1.5::double)`
        //    Again, `numeric` and `double` literals met, and we'd expect `double`.
        //    However, the result will be `numeric`, as this cost is lower (1 vs 2).
        if candidates.len() == 1 {
            return Ok(Some(candidates.pop().unwrap()));
        }

        if let Some(desired_type) = desired_type {
            for candidate in &candidates {
                if candidate.0 == desired_type {
                    return Ok(Some(candidate.clone()));
                }
            }
        }

        // In case of ambiguity, try to find a type that can be coerced to other types.
        // By choosing the "weakest" type we allow the caller to coerce it to a more suitable.
        //
        // This also helps to resolve expressions like `COALESCE(1.5::double, 2.5)` where
        // `double` expressions and `numeric` literals are mixed to `double` type, similar to
        // PostgreSQL.
        for (ty, report) in &candidates {
            let mut other_types = candidates.iter().map(|(t, _)| t);
            if other_types.all(|other| self.can_coerce(*ty, *other)) {
                return Ok(Some((*ty, report.clone())));
            }
        }

        Ok(None)
    }

    fn types_cannot_be_matched_error(
        &mut self,
        context: &'static str,
        exprs: &[impl Borrow<Expr<Id>>],
    ) -> Error {
        let types = self.analyze_exprs_types_no_error(exprs);
        Error::TypesCannotBeMatched {
            context: context.to_string(),
            types,
        }
    }

    fn analyze_many(
        &mut self,
        exprs: &[impl Borrow<Expr<Id>>],
        desired_types: &[Type],
    ) -> Result<TypeReport<Id>, Error> {
        let mut united_report = TypeReport::new();
        for (expr, desired_type) in zip(exprs, desired_types) {
            let report = self.analyze(expr.borrow(), Some(*desired_type))?;
            united_report.extend(report);
        }
        Ok(united_report)
    }

    #[inline(always)]
    pub fn can_coerce(&self, from: Type, to: Type) -> bool {
        self.type_system.can_coerce(from, to)
    }
}

/// Select overloads that have the lowest cost, i.e. require less coercions.
fn select_best_overloads<'a, Id: Hash + Eq + Clone>(
    candidates: &[(&'a Function, TypeReport<Id>)],
    required_type: Option<Type>,
) -> Vec<(&'a Function, TypeReport<Id>)> {
    let mut min_cost = usize::MAX;
    let mut best_matches = Vec::new();
    for (func, report) in candidates {
        if let Some(reqired_type) = required_type {
            if func.return_type != reqired_type {
                continue;
            }
        }

        use std::cmp::Ordering;
        match report.cost().cmp(&min_cost) {
            Ordering::Less => {
                best_matches = vec![(*func, report.clone())];
                min_cost = report.cost();
            }
            Ordering::Equal => {
                best_matches.push((*func, report.clone()));
            }
            Ordering::Greater => (),
        }
    }

    best_matches
}

//! Rendering: every node's `Display`, and the precedence ladder that decides
//! where parentheses have to be re-inserted.
//!
//! Rendering is part of correctness here, not a debugging aid. The parser drops
//! the parentheses a query was written with, so the only way a rendered tree can
//! re-parse into the same tree is for this module to put them back — which is
//! what makes a rendering snapshot a test of tree *shape*.
//!
//! The tiers below mirror `EXPR_PRATT_PARSER` in `parsing/expr/mod.rs`.
//! The two must stay in sync, and the precedence tests are what enforces it.

use std::fmt::{Display, Error, Formatter};

use sql_ir::ir::types::{CastType, NestedType};

use super::*;

impl<'q, State: AstState<'q>> Expr<'q, State> {
    /// Binding tier of this expression's rendered form: [`Precedence::Atom`]
    /// for self-delimited primaries, the operator's tier otherwise.
    pub(crate) fn precedence(&self) -> Precedence {
        match &self.inner {
            ExprInner::BinaryOperation(operation) => operation.op.precedence(),
            // The only unary operator the grammar produces is NOT: numeric
            // signs are folded into the literals at parse time.
            ExprInner::UnaryOperation(_) | ExprInner::Exists(Exists { is_not: true, .. }) => {
                Precedence::Not
            }
            ExprInner::Like(_)
            | ExprInner::Similar(_)
            | ExprInner::Between(_)
            | ExprInner::In(_) => Precedence::Matching,
            ExprInner::Is(_) => Precedence::Is,
            ExprInner::Cast(cast) => match cast.syntax {
                CastSyntax::Postfix => Precedence::Postfix,
                CastSyntax::Call => Precedence::Atom,
            },
            ExprInner::Index(_) => Precedence::Postfix,
            ExprInner::Nil
            | ExprInner::ColumnRef(_)
            | ExprInner::Literal(_)
            | ExprInner::SubQuery(_)
            | ExprInner::Row(_)
            | ExprInner::Array(_)
            | ExprInner::WindowFunction(_)
            | ExprInner::FunctionCall(_)
            | ExprInner::Parameter(_)
            | ExprInner::Trim(_)
            | ExprInner::Substring(_)
            | ExprInner::Case(_)
            | ExprInner::Exists(_)
            | ExprInner::TimeFunction(_) => Precedence::Atom,
        }
    }
}

impl<'q, State: AstState<'q>> Display for Expr<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        State::fmt_expr(self, f)
    }
}

impl<'q, State: AstState<'q>> Display for ExprInner<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            ExprInner::Nil => write!(f, "Nil"),
            ExprInner::BinaryOperation(operation) => write!(f, "{operation}"),
            ExprInner::UnaryOperation(UnaryOperation { operand, operator }) => {
                write!(f, "{operator} ")?;
                write_operand(f, operand, Precedence::Not, OperandPos::Left)
            }
            ExprInner::ColumnRef(col_ref) => write!(f, "{col_ref}"),
            ExprInner::Literal(Literal { value, quotes, .. }) => {
                let quote = quotes.quote();
                write!(f, "{quote}{value}{quote}")
            }
            ExprInner::SubQuery(query) => write!(f, "({query})"),
            ExprInner::Row(row) => write!(f, "{row}"),
            ExprInner::Array(array) => write!(f, "{array}"),
            ExprInner::WindowFunction(window_function) => write!(f, "{window_function}"),
            ExprInner::FunctionCall(function_call) => write!(f, "{function_call}"),
            ExprInner::Parameter(parameter) => write!(f, "{parameter}"),
            ExprInner::Cast(cast) => write!(f, "{cast}"),
            ExprInner::Like(like) => write!(f, "{like}"),
            ExprInner::Similar(similar) => write!(f, "{similar}"),
            ExprInner::Between(between) => write!(f, "{between}"),
            ExprInner::In(in_expr) => write!(f, "{in_expr}"),
            ExprInner::Is(is_expr) => write!(f, "{is_expr}"),
            ExprInner::Index(index) => write!(f, "{index}"),
            ExprInner::Trim(trim) => write!(f, "{trim}"),
            ExprInner::Substring(substring) => write!(f, "{substring}"),
            ExprInner::Case(case) => write!(f, "{case}"),
            ExprInner::Exists(exists) => write!(f, "{exists}"),
            ExprInner::TimeFunction(time_function) => write!(f, "{time_function}"),
        }
    }
}

/// Binding tiers of a rendered expression, loosest to tightest.
/// Sections follow the operator-precedence table above.
///
/// The derived [`Ord`] follows declaration order.
/// The tiers mirror `EXPR_PRATT_PARSER` in `parsing/expr/mod.rs`.
#[allow(private_interfaces)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Precedence {
    /// `OR`.
    Or,
    /// `AND`.
    And,
    /// Prefix `NOT`.
    Not,
    /// Postfix `IS [NOT] TRUE/FALSE/NULL/UNKNOWN`.
    Is,
    /// `= <> < > <= >=`.
    Comparison,
    /// The single non-associative matching tier:
    /// `[NOT] BETWEEN _ AND`, `[NOT] LIKE`/`ILIKE`, `[NOT] SIMILAR` and
    /// `[NOT] IN`, including nodes that carry an `ESCAPE` clause.
    Matching,
    /// `||` and any other operator
    Infix,
    /// `+ -`.
    Additive,
    /// `* / %`.
    Multiplicative,
    /// Postfix `::type` and `[index]`: one tier, since the grammar accepts
    /// them interleaved (`ExprAtomValue = ... ~ (IndexPostfix | CastPostfix)*`)
    /// and grouping among postfixes is their left-to-right stream order, so
    /// `x::int[1]` and `x[1]::int` both render and re-parse bare.
    Postfix,
    /// Self-delimited primaries: literals, column references, rows,
    /// parenthesized forms, calls, `CASE ... END`, etc.
    Atom,
}

/// Position of an operand relative to its operator. Left-associative tiers
/// keep an equal-tier operand's grouping when it is re-parsed on the left,
/// but not on the right: `a - (b - c)` rendered bare is `a - b - c`, which
/// regroups as `(a - b) - c`. [`Left`](OperandPos::Left) also covers prefix operands
/// and postfix children, where repetition (`NOT NOT a`, `a[1][2]`, `x::int::text`)
/// likewise re-parses into the same grouping. The comparison and matching
/// tiers are non-associative: an equal-tier operand is a parse error on either
/// side, so [`NonAssoc`](OperandPos::NonAssoc) parenthesizes it in both positions.
#[derive(Clone, Copy)]
enum OperandPos {
    Left,
    Right,
    NonAssoc,
}

/// Render `operand` in a position that admits unparenthesized expressions of
/// tier `op` ([`Left`](OperandPos::Left) position) or strictly tighter
/// ([`Right`](OperandPos::Right) and [`NonAssoc`](OperandPos::NonAssoc)
/// positions), wrapping it in parentheses when re-parsing the bare form
/// would regroup the expression or fail to parse.
fn write_operand<'q, State: AstState<'q>>(
    f: &mut Formatter<'_>,
    operand: &Expr<'q, State>,
    op: Precedence,
    pos: OperandPos,
) -> Result<(), Error> {
    let precedence = State::display_precedence(operand);
    let needs_parentheses = match pos {
        OperandPos::Left => precedence < op,
        OperandPos::Right | OperandPos::NonAssoc => precedence <= op,
    };
    if needs_parentheses {
        write!(f, "({operand})")
    } else {
        write!(f, "{operand}")
    }
}

impl BinaryOp {
    fn precedence(&self) -> Precedence {
        match self {
            BinaryOp::Boolean(BooleanOp::Or) => Precedence::Or,
            BinaryOp::Boolean(BooleanOp::And) => Precedence::And,
            BinaryOp::Comparison(_) => Precedence::Comparison,
            BinaryOp::Arithmetic(ArithmeticOp::Add | ArithmeticOp::Subtract) => {
                Precedence::Additive
            }
            BinaryOp::Arithmetic(
                ArithmeticOp::Multiply | ArithmeticOp::Divide | ArithmeticOp::Modulo,
            ) => Precedence::Multiplicative,
            BinaryOp::Concat => Precedence::Infix,
        }
    }
}

impl<'q, State: AstState<'q>> Display for BinaryOperation<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let precedence = self.op.precedence();
        // Comparisons are non-associative; every other binary tier is
        // left-associative.
        let left_pos = if matches!(self.op, BinaryOp::Comparison(_)) {
            OperandPos::NonAssoc
        } else {
            OperandPos::Left
        };
        write_operand(f, &self.left, precedence, left_pos)?;
        write!(f, " {} ", self.op)?;
        write_operand(f, &self.right, precedence, OperandPos::Right)
    }
}

/// Renders `table.column` (or bare `column`). Unlike the rest of the node
/// [`Display`] cluster, this leaf impl is used in production: the analyzer's
/// column-resolution errors print the offending reference through it. It
/// reaches no other node's [`Display`].
impl Display for RawColumnRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        if let Some(table_name) = self.table_name.as_ref() {
            write!(f, "{table_name}.{}", self.column_name)
        } else {
            write!(f, "{}", self.column_name)
        }
    }
}

impl<'q, State: AstState<'q>> Display for ValuesRow<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let Some((first_value, other_values)) = self.values.split_first() else {
            return write!(f, "()");
        };
        write!(f, "({first_value}")?;
        for value in other_values {
            write!(f, ", {value}")?;
        }
        write!(f, ")")
    }
}

impl<'q, State: AstState<'q>> Display for ArrayLiteral<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "ARRAY[")?;
        if let Some((first_elem, other_elems)) = self.elems.split_first() {
            write!(f, "{first_elem}")?;
            for elem in other_elems {
                write!(f, ", {elem}")?;
            }
        }
        write!(f, "]")
    }
}

impl<'q, State: AstState<'q>> Display for FunctionCall<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}({})", self.name, self.args)
    }
}

impl<'q, State: AstState<'q>> Display for FunctionCallArgs<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            FunctionCallArgs::CountAsterisk => write!(f, "*"),
            FunctionCallArgs::Exprs { distinct, exprs } => {
                if *distinct {
                    write!(f, "DISTINCT ")?;
                }
                let Some((first, rest)) = exprs.split_first() else {
                    return Ok(());
                };
                write!(f, "{first}")?;
                for expr in rest {
                    write!(f, ", {expr}")?;
                }
                Ok(())
            }
        }
    }
}

impl Display for Parameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "${}", self.0)
    }
}

/// SQL spelling of a cast target as accepted by the grammar's
/// [`CastTarget`](sql_ast_new_grammar::Rule::CastTarget) rule:
/// [`CastType`]'s own [`Display`] prints `map[]` for JSON arrays, which does not
/// re-parse (`map` is not [`Type`](sql_ast_new_grammar::Rule::Type)).
struct CastTypeSql(CastType);

impl Display for CastTypeSql {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self.0 {
            CastType::Array(NestedType::Map) => write!(f, "json[]"),
            CastType::Array(NestedType::Any) => write!(f, "string[]"),
            CastType::Json => write!(f, "json"),
            ty => write!(f, "{ty}"),
        }
    }
}

impl<'q, State: AstState<'q>> Display for Cast<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self.syntax {
            CastSyntax::Call => write!(f, "CAST({} AS {})", self.child, CastTypeSql(self.ty)),
            CastSyntax::Postfix => {
                write_operand(f, &self.child, Precedence::Postfix, OperandPos::Left)?;
                write!(f, "::{}", CastTypeSql(self.ty))
            }
        }
    }
}

impl<'q, State: AstState<'q>> Display for Like<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let op = if self.is_ilike { "ILIKE" } else { "LIKE" };
        write_operand(f, &self.left, Precedence::Matching, OperandPos::NonAssoc)?;
        write!(f, " {}{op} ", if self.is_not { "NOT " } else { "" })?;
        write_operand(f, &self.right, Precedence::Matching, OperandPos::NonAssoc)?;
        if let Some(escape) = &self.escape {
            write!(f, " ESCAPE ")?;
            write_operand(f, escape, Precedence::Matching, OperandPos::NonAssoc)?;
        }
        Ok(())
    }
}

impl<'q, State: AstState<'q>> Display for Similar<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write_operand(f, &self.left, Precedence::Matching, OperandPos::NonAssoc)?;
        write!(f, " {}SIMILAR ", if self.is_not { "NOT " } else { "" })?;
        write_operand(f, &self.right, Precedence::Matching, OperandPos::NonAssoc)?;
        if let Some(escape) = &self.escape {
            write!(f, " ESCAPE ")?;
            write_operand(f, escape, Precedence::Matching, OperandPos::NonAssoc)?;
        }
        Ok(())
    }
}

impl<'q, State: AstState<'q>> Display for Between<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write_operand(f, &self.left, Precedence::Matching, OperandPos::NonAssoc)?;
        write!(f, " {}BETWEEN ", if self.is_not { "NOT " } else { "" })?;
        // The middle operand is grammatically `BExpr`.
        // Only the symbol operators — comparisons, `||` and arithmetic — over postfixed atoms.
        // No keyword operators (AND/OR/NOT/IS/IN/LIKE/SIMILAR/BETWEEN/ESCAPE).
        let center_precedence = State::display_precedence(&self.center);
        let center_needs_parentheses =
            center_precedence < Precedence::Comparison || center_precedence == Precedence::Matching;
        if center_needs_parentheses {
            write!(f, "({})", self.center)?;
        } else {
            write!(f, "{}", self.center)?;
        }
        write!(f, " AND ")?;
        write_operand(f, &self.right, Precedence::Matching, OperandPos::NonAssoc)
    }
}

impl<'q, State: AstState<'q>> Display for InExpr<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write_operand(f, &self.left, Precedence::Matching, OperandPos::NonAssoc)?;
        write!(
            f,
            " {}IN {}",
            if self.is_not { "NOT " } else { "" },
            self.rhs.inner_ref()
        )
    }
}

impl<'q, State: AstState<'q>> Display for IsExpr<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write_operand(f, &self.child, Precedence::Is, OperandPos::Left)?;
        write!(
            f,
            " IS {}{}",
            if self.is_not { "NOT " } else { "" },
            match self.value {
                Some(true) => "TRUE",
                Some(false) => "FALSE",
                None => "NULL",
            }
        )
    }
}

impl<'q, State: AstState<'q>> Display for IndexExpr<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write_operand(f, &self.child, Precedence::Postfix, OperandPos::Left)?;
        write!(f, "[{}]", self.which)
    }
}

impl Display for TrimKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            TrimKind::Leading => write!(f, "LEADING"),
            TrimKind::Trailing => write!(f, "TRAILING"),
            TrimKind::Both => write!(f, "BOTH"),
        }
    }
}

impl<'q, State: AstState<'q>> Display for Trim<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "TRIM(")?;
        if self.kind.is_none() && self.pattern.is_none() {
            return write!(f, "{})", self.target);
        }
        if let Some(kind) = &self.kind {
            write!(f, "{kind} ")?;
        }
        if let Some(pattern) = &self.pattern {
            write!(f, "{pattern} ")?;
        }
        write!(f, "FROM {})", self.target)
    }
}

impl<'q, State: AstState<'q>> Display for Substring<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Substring::FromFor(target, start, length) => {
                write!(f, "SUBSTRING({target} FROM {start} FOR {length})")
            }
            Substring::Regular(target, start, length) => {
                write!(f, "SUBSTRING({target}, {start}, {length})")
            }
            Substring::For(target, length) => write!(f, "SUBSTRING({target} FOR {length})"),
            Substring::From(target, start) => write!(f, "SUBSTRING({target} FROM {start})"),
            Substring::Similar(expr) => write!(f, "SUBSTRING({expr})"),
        }
    }
}

impl<'q, State: AstState<'q>> Display for Case<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "CASE")?;
        if let Some(search) = &self.search {
            write!(f, " {search}")?;
        }
        for (condition, result) in &self.when_blocks {
            write!(f, " WHEN {condition} THEN {result}")?;
        }
        if let Some(else_expr) = &self.else_expr {
            write!(f, " ELSE {else_expr}")?;
        }
        write!(f, " END")
    }
}

impl<'q, State: AstState<'q>> Display for Exists<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "{}EXISTS ({})",
            if self.is_not { "NOT " } else { "" },
            self.subquery
        )
    }
}

impl Display for TimeFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let (name, precision) = match self {
            TimeFunction::CurrentDate => ("CURRENT_DATE", &None),
            TimeFunction::CurrentTime(precision) => ("CURRENT_TIME", precision),
            TimeFunction::CurrentTimestamp(precision) => ("CURRENT_TIMESTAMP", precision),
            TimeFunction::LocalTime(precision) => ("LOCALTIME", precision),
            TimeFunction::LocalTimestamp(precision) => ("LOCALTIMESTAMP", precision),
        };
        write!(f, "{name}")?;
        if let Some(precision) = precision {
            write!(f, "({precision})")?;
        }
        Ok(())
    }
}

impl Display for BinaryOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            BinaryOp::Arithmetic(op) => write!(f, "{op}"),
            BinaryOp::Comparison(op) => write!(f, "{op}"),
            BinaryOp::Boolean(op) => write!(f, "{op}"),
            BinaryOp::Concat => write!(f, "||"),
        }
    }
}

impl Display for ArithmeticOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let op = match self {
            ArithmeticOp::Add => "+",
            ArithmeticOp::Subtract => "-",
            ArithmeticOp::Multiply => "*",
            ArithmeticOp::Divide => "/",
            ArithmeticOp::Modulo => "%",
        };
        write!(f, "{op}")
    }
}

impl Display for CmpOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let op = match self {
            CmpOp::Eq => "=",
            CmpOp::Neq => "<>",
            CmpOp::Lt => "<",
            CmpOp::Gt => ">",
            CmpOp::Lte => "<=",
            CmpOp::Gte => ">=",
        };
        write!(f, "{op}")
    }
}

impl Display for BooleanOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let op = match self {
            BooleanOp::And => "AND",
            BooleanOp::Or => "OR",
        };
        write!(f, "{op}")
    }
}

impl Display for UnaryOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let op = match self {
            UnaryOp::Plus => "+",
            UnaryOp::Minus => "-",
            UnaryOp::Not => "NOT",
        };
        write!(f, "{op}")
    }
}

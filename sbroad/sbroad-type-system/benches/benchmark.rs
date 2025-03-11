use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sbroad_type_system::expr::{Expr as GenericExpr, ExprKind as GenericExprKind, Type};
use sbroad_type_system::type_system::{Function, TypeAnalyzer as GenericTypeAnalyzer, TypeSystem};
use std::sync::atomic::{AtomicUsize, Ordering};

type Expr = GenericExpr<usize>;
type ExprKind = GenericExprKind<usize>;
type TypeAnalyer<'a> = GenericTypeAnalyzer<'a, usize>;

static EXPR_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn count_exprs(new_expr: fn() -> Expr) -> usize {
    let id1 = EXPR_COUNTER.fetch_add(0, Ordering::Relaxed);
    let _ = new_expr();
    let id2 = EXPR_COUNTER.fetch_add(0, Ordering::Relaxed);
    id2 - id1
}

fn expr(kind: ExprKind) -> Expr {
    let id = EXPR_COUNTER.fetch_add(1, Ordering::Relaxed);
    Expr::new(id, kind)
}

fn lit(ty: Type) -> Expr {
    expr(ExprKind::Literal(ty))
}

fn param(name: impl Into<String>) -> Expr {
    expr(ExprKind::Parameter(name.into()))
}

fn binary(op: impl Into<String>, left: Expr, right: Expr) -> Expr {
    expr(ExprKind::Function(op.into(), vec![left, right]))
}

fn coalesce(exprs: Vec<Expr>) -> Expr {
    expr(ExprKind::Coalesce(exprs))
}

fn type_system() -> TypeSystem {
    let functions = vec![
        Function::new_scalar("+", [Type::Integer, Type::Integer], Type::Integer),
        Function::new_scalar("+", [Type::Double, Type::Double], Type::Double),
        Function::new_scalar("+", [Type::Numeric, Type::Numeric], Type::Numeric),
    ];

    TypeSystem::new(functions)
}

fn small_expr() -> Expr {
    let a = lit(Type::Integer);
    let b = lit(Type::Double);
    let c = coalesce(vec![lit(Type::Numeric), param("$1")]);
    binary("+", binary("+", a, b), c)
}

fn small_expr_benc(c: &mut Criterion) {
    let type_system = type_system();
    let expr_count = count_exprs(small_expr);
    let title = format!("small expr ({expr_count} exprs)");
    c.bench_function(&title, |b| {
        b.iter(|| {
            // NOTE: we include expression allocation in benchmark to simulate the overhead
            // caused by translation of user expressions into our expressions
            let expr = small_expr();

            let mut analyzer = TypeAnalyer::new(&type_system);
            analyzer.analyze(&expr, None).unwrap();
        })
    });
}

fn medium_expr() -> Expr {
    let a = small_expr();
    let b = small_expr();
    let c = coalesce(vec![small_expr(), small_expr()]);
    binary("+", binary("+", a, b), c)
}

fn medium_expr_bench(c: &mut Criterion) {
    let type_system = type_system();
    let expr_count = count_exprs(medium_expr);
    let title = format!("medium expr ({expr_count} exprs)");
    c.bench_function(&title, |b| {
        b.iter(|| {
            // NOTE: we include expression allocation in benchmark to simulate the overhead
            // caused by translation of user expressions into our expressions
            let expr = medium_expr();

            let mut analyzer = TypeAnalyer::new(&type_system);
            analyzer.analyze(&expr, None).unwrap();
        })
    });
}

fn big_expr() -> Expr {
    let a = medium_expr();
    let b = medium_expr();
    let c = coalesce(vec![medium_expr(), medium_expr()]);
    binary("+", binary("+", a, b), c)
}

fn big_expr_bench(c: &mut Criterion) {
    let type_system = type_system();
    let expr_count = count_exprs(big_expr);
    let title = format!("big expr ({expr_count} exprs)");
    c.bench_function(&title, |b| {
        b.iter(|| {
            // NOTE: we include expression allocation in benchmark to simulate the overhead
            // caused by translation of user expressions into our expressions
            let expr = big_expr();

            let mut analyzer = TypeAnalyer::new(&type_system);
            analyzer.analyze(&expr, None).unwrap();
        })
    });
}

fn huge_expr() -> Expr {
    let a = big_expr();
    let b = big_expr();
    let c = coalesce(vec![big_expr(), big_expr()]);
    binary("+", binary("+", a, b), c)
}

fn huge_expr_bench(c: &mut Criterion) {
    let type_system = type_system();
    let expr_count = count_exprs(huge_expr);
    let title = format!("huge expr ({expr_count} exprs)");
    c.bench_function(&title, |b| {
        b.iter(|| {
            // NOTE: we include expression allocation in benchmark to simulate the overhead
            // caused by translation of user expressions into our expressions
            let expr = huge_expr();

            let mut analyzer = TypeAnalyer::new(&type_system);
            analyzer.analyze(&expr, None).unwrap();
        })
    });
}

fn huge_expr_analysis_only_bench(c: &mut Criterion) {
    let type_system = type_system();
    let expr_count = count_exprs(huge_expr);
    let title = format!("huge expr analysis_only ({expr_count} exprs)");
    let expr = black_box(huge_expr());
    c.bench_function(&title, |b| {
        b.iter(|| {
            let mut analyzer = TypeAnalyer::new(&type_system);
            analyzer.analyze(&expr, None).unwrap();
        })
    });
}

criterion_group!(
    benches,
    small_expr_benc,
    medium_expr_bench,
    big_expr_bench,
    huge_expr_analysis_only_bench,
    huge_expr_bench,
);
criterion_main!(benches);

use super::{AbstractSyntaxTree, Ast};
use sql_ast_new_corpus::{corpus_catalog, corpus_queries};
use sql_frontend::frontend::sql::transform_into_plan_old;

#[test]
fn corpus_queries_analyze_in_both_pipelines() {
    let catalog = corpus_catalog();
    for query in corpus_queries() {
        let (name, sql) = query.into_parts();
        AbstractSyntaxTree::new(&sql)
            .and_then(|ast| ast.analyze(&catalog, &[]))
            .unwrap_or_else(|err| panic!("new pipeline rejects {name}: {err}"));
        transform_into_plan_old(&sql, &[], &catalog)
            .unwrap_or_else(|err| panic!("old pipeline rejects {name}: {err}"));
    }
}

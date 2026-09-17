use tarantool::index::{Part, SortOrder};

use crate::cli::export::catalog::model::{RawIndex, RawIndexOption};
use crate::cli::export::render::plain::ident::quote_ident;

pub(super) fn render(index: &RawIndex, table_name: &str) -> String {
    let unique = if index.options.contains(&RawIndexOption::Unique(true)) {
        "UNIQUE "
    } else {
        ""
    };

    // `unique` is a keyword, not an option.
    let with = render_options(
        index
            .options
            .iter()
            .filter(|option| !matches!(option, RawIndexOption::Unique(_))),
    )
    .map(|options| format!(" WITH ({options})"))
    .unwrap_or_default();

    format!(
        "CREATE {unique}INDEX {name} ON {table} USING {index_type} ({parts}){with};\n",
        name = quote_ident(&index.name),
        table = quote_ident(table_name),
        index_type = index.ty,
        parts = render_parts(&index.parts),
    )
}

/// `name = value, name = value` for a `WITH (...)` clause, sorted by name;
/// `None` when there are no options.
pub(super) fn render_options<'option>(
    options: impl IntoIterator<Item = &'option RawIndexOption>,
) -> Option<String> {
    let mut pairs: Vec<_> = options.into_iter().map(RawIndexOption::as_pair).collect();
    if pairs.is_empty() {
        return None;
    }
    pairs.sort_by_key(|&(name, _)| name);

    Some(
        pairs
            .iter()
            .map(|(name, value)| format!("{name} = {value}"))
            .collect::<Vec<_>>()
            .join(", "),
    )
}

pub(super) fn render_parts<'part>(parts: impl IntoIterator<Item = &'part Part<String>>) -> String {
    parts
        .into_iter()
        .map(|part| {
            let order = match part.sort_order {
                Some(SortOrder::Desc) => " DESC",
                Some(SortOrder::Asc) | None => "",
            };
            format!("{}{order}", quote_ident(&part.field))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::export::render::tests::make_tree_index;
    use insta::assert_snapshot;

    fn render_one(index: &RawIndex) -> String {
        render(index, "t")
    }

    #[test]
    fn a_plain_secondary_index() {
        let index = make_tree_index("idx_payload", 2, &["payload"]);

        assert_snapshot!(
            render_one(&index),
            @"CREATE INDEX idx_payload ON t USING tree (payload);"
        );
    }

    #[test]
    fn unique_becomes_a_keyword_rather_than_an_option() {
        let mut index = make_tree_index("idx_u", 2, &["payload"]);
        index.options = vec![RawIndexOption::Unique(true)];

        assert_snapshot!(
            render_one(&index),
            @"CREATE UNIQUE INDEX idx_u ON t USING tree (payload);"
        );
    }

    #[test]
    fn a_non_unique_flag_is_not_printed_at_all() {
        let mut index = make_tree_index("idx_u", 2, &["payload"]);
        index.options = vec![RawIndexOption::Unique(false)];

        assert_snapshot!(
            render_one(&index),
            @"CREATE INDEX idx_u ON t USING tree (payload);"
        );
    }

    #[test]
    fn the_sort_order_of_every_part_is_kept() {
        let mut index = make_tree_index("idx_multi", 2, &["a", "b", "c"]);
        index.parts[1].sort_order = Some(SortOrder::Desc);
        index.parts[2].sort_order = Some(SortOrder::Asc);

        assert_snapshot!(
            render_one(&index),
            @"CREATE INDEX idx_multi ON t USING tree (a, b DESC, c);"
        );
    }

    #[test]
    fn options_are_printed_in_a_stable_order() {
        let mut index = make_tree_index("idx_o", 2, &["a"]);
        index.options = vec![
            RawIndexOption::PageSize(1024),
            RawIndexOption::Unique(true),
            RawIndexOption::BloomFalsePositiveRate("0.125".into()),
            RawIndexOption::Hint(true),
        ];

        assert_snapshot!(
            render_one(&index),
            @"CREATE UNIQUE INDEX idx_o ON t USING tree (a) WITH (bloom_fpr = 0.125, hint = true, page_size = 1024);"
        );
    }

    #[test]
    fn names_that_need_quoting_get_it() {
        let mut index = make_tree_index("Idx Name", 2, &["it's"]);
        index.options = vec![];

        assert_snapshot!(
            render_one(&index),
            @r#"CREATE INDEX "Idx Name" ON t USING tree ("it's");"#
        );
    }
}

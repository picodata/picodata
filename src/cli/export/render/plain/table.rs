use crate::catalog::pico_bucket::DEFAULT_BUCKET_ID_COLUMN_NAME;
use crate::cli::export::catalog::model::{RawDistribution, RawIndex, RawTable, RawTableOption};
use crate::cli::export::render::plain::ident::{quote_ident, render_comment};
use crate::cli::export::render::plain::index::{render_options, render_parts};
use crate::cli::export::render::plain::types::render_sql_type;
use crate::cli::export::ExportError;
use crate::schema::ShardingFn;

pub(super) fn render(table: &RawTable, primary_key: &RawIndex) -> Result<String, ExportError> {
    let distribution = render_distribution(table)?;
    let is_sharded = !matches!(table.distribution, RawDistribution::Global);

    // The server adds `bucket_id` to a sharded table itself; a global table may
    // have a user column with that name. It may sit anywhere in the format.
    let is_bucket_id_implicit = |name: &str| is_sharded && name == DEFAULT_BUCKET_ID_COLUMN_NAME;

    let mut definitions = table
        .format
        .iter()
        .filter(|column| !is_bucket_id_implicit(&column.name))
        .map(|column| {
            let field_type = render_sql_type(column.field_type).map_err(|unsupported| {
                ExportError::unsupported(format!(
                    "table `{}`, column `{}`: {unsupported}",
                    table.name, column.name
                ))
            })?;
            let null = if column.is_nullable { "" } else { " NOT NULL" };
            Ok(format!(
                "    {} {field_type}{null}",
                quote_ident(&column.name)
            ))
        })
        .collect::<Result<Vec<_>, ExportError>>()?;

    definitions.push(render_primary_key(table, primary_key, is_sharded)?);

    let unlogged = if table.options.contains(&RawTableOption::Unlogged(true)) {
        "UNLOGGED "
    } else {
        ""
    };
    // `CREATE TABLE ... WITH (bloom_fpr = ...)` sets vinyl options of the
    // table, but `_pico_table.opts` never sees them: they are stored as the
    // options of the table's primary index, so the information for the `WITH (...)` clause
    // must be retrieved from there
    let vinyl_options = primary_key
        .options
        .iter()
        .filter(|option| option.is_vinyl());
    let with = render_options(vinyl_options)
        .map(|options| format!("WITH ({options})\n"))
        .unwrap_or_default();

    // The grammar requires `USING`, `WITH`, `DISTRIBUTED` in this order.
    Ok(format!(
        "{description}CREATE {unlogged}TABLE {name} (\n{definitions}\n)\n\
         USING {engine}\n{with}{distribution};\n",
        // Specifying the description via DDL is not supported. But the field already exists,
        // so the description is kept as an SQL comment above the statement.
        // See: https://git.picodata.io/core/picodata/-/work_items/3228
        description = render_comment(&table.description),
        name = quote_ident(&table.name),
        definitions = definitions.join(",\n"),
        engine = table.engine,
    ))
}

fn render_distribution(table: &RawTable) -> Result<String, ExportError> {
    match &table.distribution {
        RawDistribution::Global => Ok("DISTRIBUTED GLOBALLY".to_owned()),
        RawDistribution::ShardedImplicitly(sharding_key, sharding_function, tier) => {
            if *sharding_function != ShardingFn::Murmur3 {
                return Err(ExportError::unsupported(format!(
                    "table `{}` is sharded with `{}`, and DDL can only use murmur3",
                    table.name,
                    sharding_function.as_str(),
                )));
            }

            // Always explicit: otherwise the receiving cluster picks its own default tier.
            Ok(format!(
                "DISTRIBUTED BY ({key}) IN TIER {tier}",
                key = sharding_key
                    .iter()
                    .map(|column| quote_ident(column))
                    .collect::<Vec<_>>()
                    .join(", "),
                tier = quote_ident(tier),
            ))
        }
        RawDistribution::ShardedByField(field, _) => Err(ExportError::unsupported(format!(
            "table `{}` is sharded explicitly by field `{field}`, which has no DDL syntax",
            table.name
        ))),
    }
}

fn render_primary_key(
    table: &RawTable,
    primary_key: &RawIndex,
    is_sharded: bool,
) -> Result<String, ExportError> {
    // `bucket_id` is present in the key only if the condition `pk_contains_bucket_id` is true.
    let keeps_bucket_id = table
        .options
        .contains(&RawTableOption::PkContainsBucketId(true));
    let parts: Vec<_> = primary_key
        .parts
        .iter()
        // `bucket_id` is stored in the key in two cases:
        // either it stores as a part of primary key, or the table is global,
        // in which case `bucket_id` is a column name like any other.
        // In all other cases, it is a specific column that DDL neither declares nor mentions in the key.
        .filter(|part| {
            keeps_bucket_id || !(is_sharded && part.field == DEFAULT_BUCKET_ID_COLUMN_NAME)
        })
        .collect();

    if parts.is_empty() {
        return Err(ExportError::unsupported(format!(
            "table `{}` has an empty primary key",
            table.name
        )));
    }

    Ok(format!("    PRIMARY KEY ({})", render_parts(parts)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::export::catalog::model::RawIndexOption;
    use crate::cli::export::render::tests::{make_field, make_primary_key, make_sharded_table};
    use insta::assert_snapshot;
    use tarantool::index::SortOrder;
    use tarantool::space::FieldType;

    fn render_statement(table: &RawTable, primary_key: &RawIndex) -> String {
        render(table, primary_key).expect("the table is exported")
    }

    fn render_failure(table: &RawTable, primary_key: &RawIndex) -> String {
        render(table, primary_key)
            .expect_err("the table cannot be exported")
            .kind
            .to_string()
    }

    #[test]
    fn a_sharded_table_drops_its_bucket_id_and_names_its_tier() {
        let table = make_sharded_table();
        let primary_key = make_primary_key(&["id"]);

        assert_snapshot!(render_statement(&table, &primary_key), @r"
        CREATE TABLE t (
            id INT NOT NULL,
            payload TEXT,
            PRIMARY KEY (id)
        )
        USING memtx
        DISTRIBUTED BY (id) IN TIER default;
        ");
    }

    #[test]
    fn the_bucket_id_of_a_sharded_table_is_found_wherever_it_sits() {
        insta::allow_duplicates! {
            for position in 0..3 {
                let mut table = make_sharded_table();
                let bucket_id = table.format.remove(1);
                table.format.insert(position, bucket_id);
                let primary_key = make_primary_key(&["id"]);

                assert_snapshot!(render_statement(&table, &primary_key), @r"
                CREATE TABLE t (
                    id INT NOT NULL,
                    payload TEXT,
                    PRIMARY KEY (id)
                )
                USING memtx
                DISTRIBUTED BY (id) IN TIER default;
                ");
            }
        }
    }

    #[test]
    fn a_global_table_keeps_a_column_of_its_own_called_bucket_id() {
        let mut table = make_sharded_table();
        table.distribution = RawDistribution::Global;
        let primary_key = make_primary_key(&["id"]);

        assert_snapshot!(render_statement(&table, &primary_key), @r"
        CREATE TABLE t (
            id INT NOT NULL,
            bucket_id UNSIGNED NOT NULL,
            payload TEXT,
            PRIMARY KEY (id)
        )
        USING memtx
        DISTRIBUTED GLOBALLY;
        ");
    }

    #[test]
    fn pk_contains_bucket_id_puts_it_back_into_the_key_only() {
        let mut table = make_sharded_table();
        table.options = vec![RawTableOption::PkContainsBucketId(true)];
        table.distribution = RawDistribution::ShardedImplicitly(
            vec!["id".into()],
            ShardingFn::Murmur3,
            "default".into(),
        );
        let primary_key = make_primary_key(&["bucket_id", "id"]);

        // `bucket_id` is in the key, but still absent from the column list.
        assert_snapshot!(render_statement(&table, &primary_key), @r"
        CREATE TABLE t (
            id INT NOT NULL,
            payload TEXT,
            PRIMARY KEY (bucket_id, id)
        )
        USING memtx
        DISTRIBUTED BY (id) IN TIER default;
        ");
    }

    #[test]
    fn without_pk_contains_bucket_id_bucket_id_never_reaches_the_key() {
        let table = make_sharded_table();
        let primary_key = make_primary_key(&["bucket_id", "id"]);

        assert_snapshot!(render_statement(&table, &primary_key), @r"
        CREATE TABLE t (
            id INT NOT NULL,
            payload TEXT,
            PRIMARY KEY (id)
        )
        USING memtx
        DISTRIBUTED BY (id) IN TIER default;
        ");
    }

    #[test]
    fn the_sort_order_of_the_key_survives() {
        let table = make_sharded_table();
        let mut primary_key = make_primary_key(&["id", "payload"]);
        primary_key.parts[0] = primary_key.parts[0].clone().sort_order(SortOrder::Desc);

        assert_snapshot!(render_statement(&table, &primary_key), @r"
        CREATE TABLE t (
            id INT NOT NULL,
            payload TEXT,
            PRIMARY KEY (id DESC, payload)
        )
        USING memtx
        DISTRIBUTED BY (id) IN TIER default;
        ");
    }

    #[test]
    fn unlogged_is_a_keyword_before_table_and_never_an_option() {
        let mut table = make_sharded_table();
        table.options = vec![RawTableOption::Unlogged(true)];
        let primary_key = make_primary_key(&["id"]);

        assert_snapshot!(render_statement(&table, &primary_key), @r"
        CREATE UNLOGGED TABLE t (
            id INT NOT NULL,
            payload TEXT,
            PRIMARY KEY (id)
        )
        USING memtx
        DISTRIBUTED BY (id) IN TIER default;
        ");
    }

    #[test]
    fn synchronous_is_not_printed_at_all() {
        let mut table = make_sharded_table();
        table.options = vec![RawTableOption::Synchronous(true)];
        let primary_key = make_primary_key(&["id"]);

        assert_snapshot!(render_statement(&table, &primary_key), @r"
        CREATE TABLE t (
            id INT NOT NULL,
            payload TEXT,
            PRIMARY KEY (id)
        )
        USING memtx
        DISTRIBUTED BY (id) IN TIER default;
        ");
    }

    #[test]
    fn vinyl_options_come_from_the_primary_index_without_unique() {
        let mut table = make_sharded_table();
        table.engine = "vinyl".to_owned();
        let mut primary_key = make_primary_key(&["id"]);
        primary_key.options = vec![
            RawIndexOption::PageSize(1024),
            RawIndexOption::Unique(true),
            RawIndexOption::RunSizeRatio("3.5".into()),
            RawIndexOption::BloomFalsePositiveRate("0.125".into()),
        ];

        // `unique` belongs to the index, not to the table options.
        assert_snapshot!(render_statement(&table, &primary_key), @r"
        CREATE TABLE t (
            id INT NOT NULL,
            payload TEXT,
            PRIMARY KEY (id)
        )
        USING vinyl
        WITH (bloom_fpr = 0.125, page_size = 1024, run_size_ratio = 3.5)
        DISTRIBUTED BY (id) IN TIER default;
        ");
    }

    #[test]
    fn the_clauses_come_in_the_order_the_grammar_demands() {
        let mut table = make_sharded_table();
        table.engine = "vinyl".to_owned();
        let mut primary_key = make_primary_key(&["id"]);
        primary_key.options = vec![RawIndexOption::PageSize(1024)];

        // `USING`, then `WITH`, then `DISTRIBUTED`.
        assert_snapshot!(render_statement(&table, &primary_key), @r"
        CREATE TABLE t (
            id INT NOT NULL,
            payload TEXT,
            PRIMARY KEY (id)
        )
        USING vinyl
        WITH (page_size = 1024)
        DISTRIBUTED BY (id) IN TIER default;
        ");
    }

    #[test]
    fn a_multi_column_sharding_key_keeps_its_order() {
        let mut table = make_sharded_table();
        table.distribution = RawDistribution::ShardedImplicitly(
            vec!["payload".into(), "id".into()],
            ShardingFn::Murmur3,
            "storage".into(),
        );
        let primary_key = make_primary_key(&["id"]);

        assert_snapshot!(render_statement(&table, &primary_key), @r"
        CREATE TABLE t (
            id INT NOT NULL,
            payload TEXT,
            PRIMARY KEY (id)
        )
        USING memtx
        DISTRIBUTED BY (payload, id) IN TIER storage;
        ");
    }

    #[test]
    fn a_sharding_function_other_than_murmur3_fails_the_dump() {
        let mut table = make_sharded_table();
        table.distribution = RawDistribution::ShardedImplicitly(
            vec!["id".into()],
            ShardingFn::Crc32,
            "default".into(),
        );
        let primary_key = make_primary_key(&["id"]);

        assert_snapshot!(
            render_failure(&table, &primary_key),
            @"table `t` is sharded with `crc32`, and DDL can only use murmur3"
        );
    }

    #[test]
    fn a_table_sharded_by_field_fails_the_dump() {
        let mut table = make_sharded_table();
        table.distribution = RawDistribution::ShardedByField("bucket_id".into(), "default".into());
        let primary_key = make_primary_key(&["id"]);

        assert_snapshot!(
            render_failure(&table, &primary_key),
            @"table `t` is sharded explicitly by field `bucket_id`, which has no DDL syntax"
        );
    }

    #[test]
    fn a_column_of_a_type_ddl_cannot_spell_fails_the_dump() {
        let mut table = make_sharded_table();
        table
            .format
            .push(make_field("mystery", FieldType::Scalar, true));
        let primary_key = make_primary_key(&["id"]);

        assert_snapshot!(
            render_failure(&table, &primary_key),
            @"table `t`, column `mystery`: the column type `scalar` has no equivalent in picodata's DDL"
        );
    }

    #[test]
    fn every_column_type_makes_it_into_the_ddl() {
        use tarantool::space::TypedArray;

        let mut table = make_sharded_table();
        table.distribution = RawDistribution::Global;
        table.format = vec![
            make_field("c_int", FieldType::Integer, false),
            make_field("c_unsigned", FieldType::Unsigned, true),
            make_field("c_double", FieldType::Double, true),
            make_field("c_decimal", FieldType::Decimal, true),
            make_field("c_bool", FieldType::Boolean, true),
            make_field("c_text", FieldType::String, true),
            make_field("c_uuid", FieldType::Uuid, true),
            make_field("c_datetime", FieldType::Datetime, true),
            make_field("c_json", FieldType::Map, true),
            make_field("c_text_arr", FieldType::Array(TypedArray::String), true),
            make_field("c_int_arr", FieldType::Array(TypedArray::Integer), true),
            make_field("c_json_arr", FieldType::Array(TypedArray::Map), true),
        ];
        let primary_key = make_primary_key(&["c_int"]);

        assert_snapshot!(render_statement(&table, &primary_key), @r"
        CREATE TABLE t (
            c_int INT NOT NULL,
            c_unsigned UNSIGNED,
            c_double DOUBLE,
            c_decimal DECIMAL,
            c_bool BOOL,
            c_text TEXT,
            c_uuid UUID,
            c_datetime DATETIME,
            c_json JSON,
            c_text_arr TEXT[],
            c_int_arr INT[],
            c_json_arr JSON[],
            PRIMARY KEY (c_int)
        )
        USING memtx
        DISTRIBUTED GLOBALLY;
        ");
    }

    #[test]
    fn names_and_descriptions_are_escaped_or_commented() {
        let mut table = make_sharded_table();
        table.distribution = RawDistribution::Global;
        table.name = "Quoted Name".to_owned();
        table.description = "a table\nwith notes".to_owned();
        table.format = vec![
            make_field("select", FieldType::Integer, false),
            make_field("it's", FieldType::String, true),
        ];
        let primary_key = make_primary_key(&["select"]);

        let rendered = render(&table, &primary_key).expect("renders");

        assert_snapshot!(rendered, @r#"
        -- a table
        -- with notes
        CREATE TABLE "Quoted Name" (
            "select" INT NOT NULL,
            "it's" TEXT,
            PRIMARY KEY ("select")
        )
        USING memtx
        DISTRIBUTED GLOBALLY;
        "#);
    }
}

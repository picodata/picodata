pub(super) mod plain;

use std::collections::HashMap;

use crate::cli::export::catalog::model::{RawIndex, RawTable};
use crate::cli::export::catalog::Catalog;
use crate::cli::export::ExportError;
use crate::info::PICODATA_VERSION;

pub(super) struct DumpHeader {
    exporter_version: String,
    /// RFC 3339.
    taken_at: String,
}

impl DumpHeader {
    pub(super) fn now() -> Self {
        let taken_at = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| "unknown".to_owned());

        Self {
            exporter_version: PICODATA_VERSION.to_owned(),
            taken_at,
        }
    }
}

pub(super) trait ExportFormat {
    fn render_header(&mut self, catalog: &Catalog, header: &DumpHeader) -> Result<(), ExportError>;

    fn render_table_with_pk(
        &mut self,
        table: &RawTable,
        primary_key: &RawIndex,
    ) -> Result<(), ExportError>;

    fn render_secondary_indexes(
        &mut self,
        index: &RawIndex,
        table_name: &str,
    ) -> Result<(), ExportError>;
}

pub(super) fn render_dump(
    format: &mut impl ExportFormat,
    catalog: &Catalog,
    header: &DumpHeader,
) -> Result<(), ExportError> {
    format.render_header(catalog, header)?;

    let indexes_by_table = catalog.indexes.iter().fold(
        HashMap::<i64, Vec<&RawIndex>>::new(),
        |mut grouped, index| {
            grouped.entry(index.table_id).or_default().push(index);
            grouped
        },
    );
    let indexes_of = |table_id: i64| {
        indexes_by_table
            .get(&table_id)
            .map_or(&[][..], Vec::as_slice)
    };

    catalog.tables.iter().try_for_each(|table| {
        let primary_key = indexes_of(table.id)
            .iter()
            .find(|index| index.id == PRIMARY_KEY_ID)
            .ok_or_else(|| {
                ExportError::unsupported(format!(
                    "table `{}` has no primary key in `_pico_index`",
                    table.name
                ))
            })?;

        format.render_table_with_pk(table, primary_key)
    })?;

    catalog
        .tables
        .iter()
        .flat_map(|table| {
            indexes_of(table.id)
                .iter()
                .filter(|index| index.id != PRIMARY_KEY_ID)
                .map(move |&index| (index, table))
        })
        .try_for_each(|(index, table)| format.render_secondary_indexes(index, &table.name))
}

const PRIMARY_KEY_ID: i64 = 0;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::export::catalog::model::{RawDistribution, RawIndexOption, RawTable, RawTier};
    use crate::cli::export::render::plain::PlainTextFormat;
    use crate::config::ReplicationMode;
    use crate::schema::ShardingFn;
    use insta::assert_snapshot;
    use tarantool::index::Part;
    use tarantool::space::{Field, FieldType};

    /// Every fixture below describes this one table and its indexes.
    const TABLE_ID: i64 = 1025;

    pub(super) fn make_field(name: &str, field_type: FieldType, is_nullable: bool) -> Field {
        Field {
            name: name.to_owned(),
            field_type,
            is_nullable,
        }
    }

    pub(super) fn make_tree_index(name: &str, id: i64, fields: &[&str]) -> RawIndex {
        RawIndex {
            table_id: TABLE_ID,
            id,
            name: name.to_owned(),
            ty: "tree".to_owned(),
            options: vec![RawIndexOption::Unique(id == PRIMARY_KEY_ID)],
            parts: fields
                .iter()
                .map(|&field| Part::field(field.to_owned()))
                .collect(),
        }
    }

    /// `t (id INT NOT NULL, payload TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id)`.
    pub(super) fn make_sharded_table() -> RawTable {
        RawTable {
            id: TABLE_ID,
            name: "t".to_owned(),
            distribution: RawDistribution::ShardedImplicitly(
                vec!["id".into()],
                ShardingFn::Murmur3,
                "default".into(),
            ),
            format: vec![
                make_field("id", FieldType::Integer, false),
                make_field("bucket_id", FieldType::Unsigned, false),
                make_field("payload", FieldType::String, true),
            ],
            engine: "memtx".to_owned(),
            description: String::new(),
            options: Vec::new(),
        }
    }

    pub(super) fn make_primary_key(fields: &[&str]) -> RawIndex {
        make_tree_index("pk", PRIMARY_KEY_ID, fields)
    }

    fn make_header() -> DumpHeader {
        DumpHeader {
            exporter_version: "26.3.0".to_owned(),
            taken_at: "2026-09-08T00:00:00Z".to_owned(),
        }
    }

    fn make_catalog() -> Catalog {
        let table = make_sharded_table();
        let primary_key = make_primary_key(&["id"]);
        let secondary = make_tree_index("idx_payload", 2, &["payload"]);

        Catalog {
            instance_versions: vec!["26.3.0".to_owned()],
            catalog_version: Some("26.3.1".to_owned()),
            tiers: vec![RawTier {
                name: "default".to_owned(),
                replication_factor: 1,
                bucket_count: 3000,
                replication_mode: ReplicationMode::Async,
            }],
            tables: vec![table],
            indexes: vec![primary_key, secondary],
        }
    }

    fn render(catalog: &Catalog) -> String {
        let mut dump = Vec::new();
        render_dump(
            &mut PlainTextFormat::new(&mut dump),
            catalog,
            &make_header(),
        )
        .expect("renders");
        String::from_utf8(dump).expect("a dump is UTF-8")
    }

    fn header_of(dump: &str) -> String {
        dump.lines()
            .take_while(|line| line.starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn a_cluster_without_a_catalog_version_says_so_instead_of_leaving_a_blank() {
        let mut catalog = make_catalog();
        catalog.catalog_version = None;

        let dump = render(&catalog);

        assert_snapshot!(header_of(&dump), @r"
        --
        -- picodata export
        --
        -- Dumped by:       picodata 26.3.0
        -- Dumped from:     picodata 26.3.0
        -- Catalog version: not reported by this cluster
        -- Dumped at:       2026-09-08T00:00:00Z
        --
        -- Tables and indexes only. Users, roles, privileges, procedures,
        -- audit policies, plugins, ALTER SYSTEM settings and the table data
        -- itself are not part of this dump.
        --
        -- The tiers below must already exist on the target cluster. SQL cannot
        -- create a tier: configure them before applying this dump.
        --
        --   default: replication_factor = 1, bucket_count = 3000, replication_mode = async
        --
        ");
    }

    #[test]
    fn a_whole_small_dump() {
        let dump = render(&make_catalog());

        // The dump ends with a newline: the snapshot below cannot show that.
        assert!(dump.ends_with(";\n"), "{dump}");
        assert_snapshot!(dump, @r"
        --
        -- picodata export
        --
        -- Dumped by:       picodata 26.3.0
        -- Dumped from:     picodata 26.3.0
        -- Catalog version: 26.3.1
        -- Dumped at:       2026-09-08T00:00:00Z
        --
        -- Tables and indexes only. Users, roles, privileges, procedures,
        -- audit policies, plugins, ALTER SYSTEM settings and the table data
        -- itself are not part of this dump.
        --
        -- The tiers below must already exist on the target cluster. SQL cannot
        -- create a tier: configure them before applying this dump.
        --
        --   default: replication_factor = 1, bucket_count = 3000, replication_mode = async
        --

        CREATE TABLE t (
            id INT NOT NULL,
            payload TEXT,
            PRIMARY KEY (id)
        )
        USING memtx
        DISTRIBUTED BY (id) IN TIER default;

        CREATE INDEX idx_payload ON t USING tree (payload);
        ");
    }

    #[test]
    fn tables_come_before_indexes() {
        let dump = render(&make_catalog());

        let table = dump.find("CREATE TABLE").expect("has a table");
        let index = dump.find("CREATE INDEX").expect("has an index");
        assert!(table < index, "{dump}");
    }

    #[test]
    fn a_table_without_a_primary_key_fails_the_dump() {
        let mut catalog = make_catalog();
        catalog.indexes.retain(|index| index.id != PRIMARY_KEY_ID);

        let mut dump = Vec::new();
        let error = render_dump(
            &mut PlainTextFormat::new(&mut dump),
            &catalog,
            &make_header(),
        )
        .expect_err("the dump cannot be written");

        assert_snapshot!(error.kind, @"table `t` has no primary key in `_pico_index`");
    }

    #[test]
    fn a_table_that_ddl_cannot_spell_fails_the_whole_dump() {
        let mut catalog = make_catalog();
        catalog.tables[0].distribution =
            RawDistribution::ShardedByField("bucket_id".into(), "default".into());

        let mut dump = Vec::new();
        let error = render_dump(
            &mut PlainTextFormat::new(&mut dump),
            &catalog,
            &make_header(),
        )
        .expect_err("the dump cannot be written");

        assert_snapshot!(
            error.kind,
            @"table `t` is sharded explicitly by field `bucket_id`, which has no DDL syntax"
        );
    }

    #[test]
    fn instances_on_different_versions_are_shown() {
        let mut catalog = make_catalog();
        catalog.instance_versions = vec!["26.2.0".to_owned(), "26.3.0".to_owned()];

        let dump = render(&catalog);

        assert_snapshot!(header_of(&dump), @r"
        --
        -- picodata export
        --
        -- Dumped by:       picodata 26.3.0
        -- Dumped from:     picodata 26.2.0, 26.3.0
        --                  Instances are on different versions: an upgrade is in progress.
        -- Catalog version: 26.3.1
        -- Dumped at:       2026-09-08T00:00:00Z
        --
        -- Tables and indexes only. Users, roles, privileges, procedures,
        -- audit policies, plugins, ALTER SYSTEM settings and the table data
        -- itself are not part of this dump.
        --
        -- The tiers below must already exist on the target cluster. SQL cannot
        -- create a tier: configure them before applying this dump.
        --
        --   default: replication_factor = 1, bucket_count = 3000, replication_mode = async
        --
        ");
    }

    #[test]
    fn every_tier_is_listed_as_a_precondition() {
        let mut catalog = make_catalog();
        catalog.tiers.push(RawTier {
            name: "storage".to_owned(),
            replication_factor: 3,
            bucket_count: 16384,
            replication_mode: ReplicationMode::Sync,
        });

        let dump = render(&catalog);

        assert_snapshot!(header_of(&dump), @r"
        --
        -- picodata export
        --
        -- Dumped by:       picodata 26.3.0
        -- Dumped from:     picodata 26.3.0
        -- Catalog version: 26.3.1
        -- Dumped at:       2026-09-08T00:00:00Z
        --
        -- Tables and indexes only. Users, roles, privileges, procedures,
        -- audit policies, plugins, ALTER SYSTEM settings and the table data
        -- itself are not part of this dump.
        --
        -- The tiers below must already exist on the target cluster. SQL cannot
        -- create a tier: configure them before applying this dump.
        --
        --   default: replication_factor = 1, bucket_count = 3000, replication_mode = async
        --   storage: replication_factor = 3, bucket_count = 16384, replication_mode = sync
        --
        ");
    }
}

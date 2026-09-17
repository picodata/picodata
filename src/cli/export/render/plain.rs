mod ident;
mod index;
mod table;
mod types;

use std::io::{self, Write};

use crate::cli::export::catalog::model::{RawIndex, RawTable};
use crate::cli::export::catalog::Catalog;
use crate::cli::export::render::{DumpHeader, ExportFormat};
use crate::cli::export::ExportError;

pub struct PlainTextFormat<W> {
    output: W,
    /// For formatting purposes, so that the secondary indexes form one block.
    has_written_an_index: bool,
}

impl<W: Write> PlainTextFormat<W> {
    pub fn new(output: W) -> Self {
        Self {
            output,
            has_written_an_index: false,
        }
    }
}

impl<W: Write> ExportFormat for PlainTextFormat<W> {
    fn render_header(&mut self, catalog: &Catalog, header: &DumpHeader) -> Result<(), ExportError> {
        let instance_versions: Vec<&str> = catalog
            .instance_versions
            .iter()
            .map(String::as_str)
            .collect();

        let upgrade_note = if instance_versions.len() > 1 {
            "--                  Instances are on different versions: an upgrade is in progress.\n"
        } else {
            ""
        };

        const CATALOG_NOT_REPORTED: &str = "not reported by this cluster";
        write!(
            self.output,
            "--\n\
             -- picodata dump\n\
             --\n\
             -- Dumped by:       picodata {exporter}\n\
             -- Dumped from:     picodata {cluster}\n\
             {upgrade_note}\
             -- Catalog version: {catalog}\n\
             -- Dumped at:       {taken_at}\n\
             --\n\
             -- Tables and indexes only. Users, roles, privileges, procedures,\n\
             -- audit policies, plugins, ALTER SYSTEM settings and the table data\n\
             -- itself are not part of this dump.\n\
             --\n",
            exporter = header.exporter_version,
            cluster = instance_versions.join(", "),
            // The tables are shaped by the catalog.
            catalog = catalog
                .catalog_version
                .as_deref()
                .unwrap_or(CATALOG_NOT_REPORTED),
            taken_at = header.taken_at,
        )?;

        if catalog.tiers.is_empty() {
            return Ok(());
        }

        self.output.write_all(
            b"-- The tiers below must already exist on the target cluster. SQL cannot\n\
              -- create a tier: configure them before applying this dump.\n\
              --\n",
        )?;
        let mut tiers: Vec<_> = catalog.tiers.iter().collect();
        tiers.sort_by(|left, right| left.name.cmp(&right.name));
        tiers.iter().try_for_each(|tier| -> io::Result<()> {
            writeln!(
                self.output,
                "--   {name}: replication_factor = {factor}, bucket_count = {buckets}, \
                 replication_mode = {mode}",
                name = tier.name,
                factor = tier.replication_factor,
                buckets = tier.bucket_count,
                mode = tier.replication_mode,
            )
        })?;
        self.output.write_all(b"--\n")?;
        Ok(())
    }

    fn render_table_with_pk(
        &mut self,
        table: &RawTable,
        primary_key: &RawIndex,
    ) -> Result<(), ExportError> {
        let statement = table::render(table, primary_key)?;

        write!(self.output, "\n{statement}")?;
        Ok(())
    }

    fn render_secondary_indexes(
        &mut self,
        index: &RawIndex,
        table_name: &str,
    ) -> Result<(), ExportError> {
        let statement = index::render(index, table_name);

        let separator = if std::mem::replace(&mut self.has_written_an_index, true) {
            ""
        } else {
            "\n"
        };
        write!(self.output, "{separator}{statement}")?;
        Ok(())
    }
}

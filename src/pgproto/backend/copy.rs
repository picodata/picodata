use crate::audit;
use crate::cas;
use crate::config::{DEFAULT_SQL_LOG, DYNAMIC_CONFIG};
use crate::pgproto::error::{PedanticError, PgError, PgErrorCode, PgResult};
use crate::pgproto::value::{FieldFormat, PgValue};
use crate::sql::copy::{
    prepare_copy_target, CopyFlushReasonKind, CopyFlushThresholds, CopyFlushThresholdsByScope,
    CopyTargetError, PendingCopyBatch, PreparedCopyTarget, ShardedCopyDestination,
};
use crate::sql::storage::StorageRuntime;
use bytes::{Bytes, BytesMut};
use postgres_types::Oid;
use prometheus::{Histogram, HistogramOpts, IntCounter, IntCounterVec, Opts};
use smol_str::{format_smolstr, SmolStr};
use sql::executor::vtable::VTableTuple;
use sql::ir::value::Value as SbroadValue;
use sql::{CopyFormat, CopyStatement as ParsedCopyStatement};
use sql_protocol::dml::insert::ConflictPolicy;
use std::{collections::HashMap, sync::LazyLock, time::Instant};
use tarantool::error::{IntoBoxError, TarantoolErrorCode};

// Fallback flush targets bound COPY session memory. Query options may override them.
const COPY_FALLBACK_FLUSH_ROWS: usize = 1024;
const COPY_FALLBACK_FLUSH_BYTES: usize = 1 << 20;

// Fallback raw text COPY row guard for a row that spans CopyData frames without a line terminator.
const COPY_FALLBACK_ROW_BYTES: usize = 1 << 20;

pub(crate) static PGPROTO_COPY_SESSIONS_STARTED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_pgproto_copy_sessions_started_total",
        "Total number of pgproto COPY FROM STDIN sessions started",
    ))
    .expect("Failed to create pico_pgproto_copy_sessions_started_total counter")
});

pub(crate) static PGPROTO_COPY_BYTES_RECEIVED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_pgproto_copy_bytes_received_total",
        "Total number of pgproto COPY FROM STDIN payload bytes received",
    ))
    .expect("Failed to create pico_pgproto_copy_bytes_received_total counter")
});

pub(crate) static PGPROTO_COPY_ROWS_INSERTED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_pgproto_copy_rows_inserted_total",
        "Total number of rows inserted by pgproto COPY FROM STDIN",
    ))
    .expect("Failed to create pico_pgproto_copy_rows_inserted_total counter")
});

pub(crate) static PGPROTO_COPY_BATCHES_FLUSHED_TOTAL: LazyLock<IntCounterVec> =
    LazyLock::new(|| {
        IntCounterVec::new(
            Opts::new(
                "pico_pgproto_copy_batches_flushed_total",
                "Total number of pgproto COPY FROM STDIN batches flushed",
            ),
            &["reason"],
        )
        .expect("Failed to create pico_pgproto_copy_batches_flushed_total counter")
    });

pub(crate) static PGPROTO_COPY_BATCH_FLUSH_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "pico_pgproto_copy_batch_flush_duration",
        "Histogram of pgproto COPY FROM STDIN batch flush durations (in seconds)",
    ))
    .expect("Failed to create pico_pgproto_copy_batch_flush_duration histogram")
});

pub(crate) static PGPROTO_COPY_RECORD_LIMIT_ERRORS_TOTAL: LazyLock<IntCounter> =
    LazyLock::new(|| {
        IntCounter::with_opts(Opts::new(
            "pico_pgproto_copy_record_limit_errors_total",
            "Total number of pgproto COPY FROM STDIN record-limit violations",
        ))
        .expect("Failed to create pico_pgproto_copy_record_limit_errors_total counter")
    });

fn bad_copy_format(reason: impl Into<Box<crate::pgproto::error::DynError>>) -> PgError {
    PedanticError::new(PgErrorCode::BadCopyFileFormat, reason).into()
}

fn record_limit_error(record_byte_limit: usize) -> PgError {
    PGPROTO_COPY_RECORD_LIMIT_ERRORS_TOTAL.inc();
    bad_copy_format(format!(
        "COPY row exceeds maximum size of {} bytes",
        record_byte_limit
    ))
}

#[derive(Debug, Clone)]
pub struct CopySpec {
    schema_name: Option<SmolStr>,
    table_name: SmolStr,
    columns: Vec<SmolStr>,
    delimiter: u8,
    null_marker: Vec<u8>,
    header: bool,
    conflict_policy: ConflictPolicy,
    session_flush_rows: Option<usize>,
    destination_flush_rows: Option<usize>,
    session_flush_bytes: Option<usize>,
    destination_flush_bytes: Option<usize>,
    row_bytes: Option<usize>,
}

impl CopySpec {
    pub fn try_from_statement(statement: ParsedCopyStatement) -> PgResult<Self> {
        fn unsupported_copy(reason: impl std::fmt::Display) -> PgError {
            PgError::FeatureNotSupported(format_smolstr!("{reason}"))
        }

        let sql::CopyStatement::From(copy_from) = statement;

        // TODO: extend COPY decoding beyond text once the MVP protocol and execution
        // contract is settled. CSV and binary should plug into the same resolved spec
        // and session flow instead of growing ad hoc branches here.
        if copy_from.options.format != CopyFormat::Text {
            return Err(unsupported_copy(format_smolstr!(
                "COPY format {} is not supported",
                copy_format_name(copy_from.options.format)
            )));
        }

        let delimiter = match copy_from.options.delimiter.as_deref() {
            Some(delimiter) => parse_copy_delimiter(delimiter)?,
            None => b'\t',
        };
        let null_marker = copy_from
            .options
            .null_string
            .unwrap_or_else(|| String::from("\\N"));

        Ok(Self {
            schema_name: copy_from.table.schema_name,
            table_name: copy_from.table.table_name,
            columns: copy_from.table.columns,
            delimiter,
            null_marker: null_marker.into_bytes(),
            header: copy_from.options.header,
            conflict_policy: (&copy_from.options.conflict_strategy).into(),
            session_flush_rows: copy_from.options.session_flush_rows,
            destination_flush_rows: copy_from.options.destination_flush_rows,
            session_flush_bytes: copy_from.options.session_flush_bytes,
            destination_flush_bytes: copy_from.options.destination_flush_bytes,
            row_bytes: copy_from.options.row_bytes,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PreparedCopy {
    spec: CopySpec,
    query_text: SmolStr,
    audit_enabled: bool,
    sql_log_enabled: bool,
}

impl PreparedCopy {
    pub fn try_from_statement(statement: ParsedCopyStatement, query_text: &str) -> PgResult<Self> {
        let spec = CopySpec::try_from_statement(statement)?;
        let audit_enabled = audit::policy::is_dml_audit_enabled_for_current_user()?;
        let sql_log_enabled = DYNAMIC_CONFIG
            .sql_log
            .try_current_value()
            .unwrap_or(DEFAULT_SQL_LOG);

        Ok(Self {
            spec,
            query_text: query_text.into(),
            audit_enabled,
            sql_log_enabled,
        })
    }

    pub fn query_for_audit(&self) -> Option<&str> {
        self.audit_enabled.then_some(self.query_text.as_str())
    }

    pub fn query_for_logging(&self) -> Option<&str> {
        self.sql_log_enabled.then_some(self.query_text.as_str())
    }

    pub fn spec(&self) -> &CopySpec {
        &self.spec
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CopyStart {
    pub column_count: usize,
}

#[derive(Clone, Copy, Debug)]
enum CopyFlushReason {
    CopyDone,
    Destination(CopyFlushReasonKind),
    Session(CopyFlushReasonKind),
}

impl CopyFlushReason {
    fn label(self) -> &'static str {
        match self {
            CopyFlushReason::CopyDone => "copy_done",
            CopyFlushReason::Destination(CopyFlushReasonKind::Rows) => "destination_flush_rows",
            CopyFlushReason::Destination(CopyFlushReasonKind::Bytes) => "destination_flush_bytes",
            CopyFlushReason::Session(CopyFlushReasonKind::Rows) => "session_flush_rows",
            CopyFlushReason::Session(CopyFlushReasonKind::Bytes) => "session_flush_bytes",
        }
    }
}

#[derive(Debug)]
enum CopyWriteSession {
    Global(PendingCopyBatch),
    Sharded {
        batches: HashMap<ShardedCopyDestination, PendingCopyBatch>,
        rows: usize,
        bytes: usize,
    },
}

impl CopyWriteSession {
    fn new(global: bool) -> Self {
        if global {
            Self::Global(PendingCopyBatch::default())
        } else {
            Self::Sharded {
                batches: HashMap::new(),
                rows: 0,
                bytes: 0,
            }
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Global(batch) => batch.is_empty(),
            Self::Sharded { rows, .. } => *rows == 0,
        }
    }

    fn destination_would_exceed(
        &self,
        destination: Option<&ShardedCopyDestination>,
        next_row_bytes: usize,
        limits: CopyFlushThresholds,
    ) -> Option<CopyFlushReasonKind> {
        match (self, destination) {
            (Self::Sharded { batches, .. }, Some(destination)) => batches
                .get(destination)?
                .would_exceed(next_row_bytes, limits),
            _ => None,
        }
    }

    fn session_would_exceed(
        &self,
        next_row_bytes: usize,
        limits: CopyFlushThresholds,
    ) -> Option<CopyFlushReasonKind> {
        match self {
            Self::Global(batch) => batch.would_exceed(next_row_bytes, limits),
            Self::Sharded { rows, bytes, .. } => limits.would_exceed(*rows, *bytes, next_row_bytes),
        }
    }

    fn push(&mut self, encoded_row: Vec<u8>, destination: Option<&ShardedCopyDestination>) {
        match (self, destination) {
            (Self::Global(batch), None) => batch.push(encoded_row),
            (
                Self::Sharded {
                    batches,
                    rows,
                    bytes,
                },
                Some(destination),
            ) => {
                *rows = rows.saturating_add(1);
                *bytes = bytes.saturating_add(encoded_row.len());
                if let Some(batch) = batches.get_mut(destination) {
                    batch.push(encoded_row);
                } else {
                    let mut batch = PendingCopyBatch::default();
                    batch.push(encoded_row);
                    batches.insert(destination.clone(), batch);
                }
            }
            (Self::Global(_), Some(_)) => {
                unreachable!("global COPY row must not have a sharded destination")
            }
            (Self::Sharded { .. }, None) => {
                unreachable!("sharded COPY row must have a destination")
            }
        }
    }

    fn destination_reached(
        &self,
        destination: &ShardedCopyDestination,
        limits: CopyFlushThresholds,
    ) -> Option<CopyFlushReasonKind> {
        match self {
            Self::Global(_) => None,
            Self::Sharded { batches, .. } => batches.get(destination)?.reached(limits),
        }
    }

    fn session_reached(&self, limits: CopyFlushThresholds) -> Option<CopyFlushReasonKind> {
        match self {
            Self::Global(batch) => batch.reached(limits),
            Self::Sharded { rows, bytes, .. } => limits.reached(*rows, *bytes),
        }
    }

    fn sharded_destination(
        &self,
        destination: &ShardedCopyDestination,
    ) -> Option<&PendingCopyBatch> {
        match self {
            Self::Global(_) => None,
            Self::Sharded { batches, .. } => batches.get(destination),
        }
    }

    fn clear_sharded_destination(&mut self, destination: &ShardedCopyDestination) {
        if let Self::Sharded {
            batches,
            rows,
            bytes,
        } = self
        {
            if let Some(batch) = batches.get_mut(destination) {
                *rows = rows.saturating_sub(batch.rows());
                *bytes = bytes.saturating_sub(batch.bytes());
                batch.clear();
            }
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Global(batch) => batch.clear(),
            Self::Sharded {
                batches,
                rows,
                bytes,
            } => {
                *rows = 0;
                *bytes = 0;
                for batch in batches.values_mut() {
                    batch.clear();
                }
            }
        }
    }
}

pub(crate) struct CopySession {
    target: PreparedCopyTarget,
    field_oids: Vec<Oid>,
    runtime: StorageRuntime,
    header: bool,
    flush_thresholds: CopyFlushThresholdsByScope,
    row_byte_limit: usize,
    skipped_header: bool,
    record_reader: TextRecordReader,
    row_parser: TextRowParser,
    decoded_values: VTableTuple,
    write: CopyWriteSession,
    inserted_rows: usize,
}

impl CopySession {
    fn new(
        target: PreparedCopyTarget,
        field_oids: Vec<Oid>,
        runtime: StorageRuntime,
        delimiter: u8,
        null_marker: Vec<u8>,
        header: bool,
        flush_thresholds: CopyFlushThresholdsByScope,
        row_byte_limit: usize,
    ) -> Self {
        let field_count = field_oids.len();
        let row_parser = TextRowParser::new(delimiter, null_marker, field_oids.len());
        let write = CopyWriteSession::new(target.is_global());
        Self {
            target,
            field_oids,
            runtime,
            header,
            flush_thresholds,
            row_byte_limit,
            skipped_header: false,
            record_reader: TextRecordReader::with_capacity(row_byte_limit.min(8 << 10)),
            row_parser,
            decoded_values: Vec::with_capacity(field_count),
            write,
            inserted_rows: 0,
        }
    }

    pub(crate) fn on_copy_data(&mut self, data: Bytes) -> PgResult<()> {
        PGPROTO_COPY_BYTES_RECEIVED_TOTAL.inc_by(data.len() as u64);
        self.ensure_incoming_record_limit(data.as_ref())?;
        self.record_reader.push(data.as_ref());

        while let Some(record) = self.record_reader.next_record()? {
            self.process_record(&record)?;
        }

        self.ensure_pending_record_limit()?;

        Ok(())
    }

    pub(crate) fn on_copy_done(mut self) -> PgResult<usize> {
        self.ensure_pending_record_limit()?;
        if let Some(record) = self.record_reader.finish_record()? {
            self.process_record(&record)?;
        }
        self.flush_pending_rows(CopyFlushReason::CopyDone)?;
        Ok(self.inserted_rows)
    }

    fn process_record(&mut self, record: &[u8]) -> PgResult<()> {
        self.ensure_record_size(record.len())?;

        if self.header && !self.skipped_header {
            self.skipped_header = true;
            return Ok(());
        }

        self.decode_record(record)?;
        let (encoded_row, destination) = self
            .target
            .prepare_pending_row(&self.runtime, &self.decoded_values)
            .map_err(|error| map_copy_target_flush_error(self.target.table_name(), error))?;
        let row_bytes = encoded_row.len();

        // Flush in two phases: `destination_would_exceed` /
        // `session_would_exceed` trigger a pre-`write.push` flush when this
        // row will not fit, then `destination_reached` / `session_reached`
        // trigger a post-`write.push` flush when this row made the pending
        // batch exactly full. The asymmetry is intentional so
        // `flush_pending_destination` / `flush_pending_rows` report the right
        // `CopyFlushReason`.
        if let Some(reason) = self.write.destination_would_exceed(
            destination.as_ref(),
            row_bytes,
            self.flush_thresholds.destination,
        ) {
            if let Some(destination) = destination.as_ref() {
                self.flush_pending_destination(destination, CopyFlushReason::Destination(reason))?;
            }
        }
        if let Some(reason) = self
            .write
            .session_would_exceed(row_bytes, self.flush_thresholds.session)
        {
            self.flush_pending_rows(CopyFlushReason::Session(reason))?;
        }

        self.write.push(encoded_row, destination.as_ref());
        if let Some(destination) = destination.as_ref() {
            if let Some(reason) = self
                .write
                .destination_reached(destination, self.flush_thresholds.destination)
            {
                self.flush_pending_destination(destination, CopyFlushReason::Destination(reason))?;
            }
        }
        if let Some(reason) = self.write.session_reached(self.flush_thresholds.session) {
            self.flush_pending_rows(CopyFlushReason::Session(reason))?;
        }

        Ok(())
    }

    fn decode_record(&mut self, record: &[u8]) -> PgResult<()> {
        self.row_parser
            .decode_record_into(record, &self.field_oids, &mut self.decoded_values)
    }

    fn flush_pending_destination(
        &mut self,
        destination: &ShardedCopyDestination,
        reason: CopyFlushReason,
    ) -> PgResult<()> {
        let Some(batch) = self.write.sharded_destination(destination) else {
            return Ok(());
        };
        let started_at = Instant::now();
        let flush_result = self
            .target
            .flush_sharded_batches(&self.runtime, std::iter::once((destination, batch)))
            .map_err(|error| map_copy_target_flush_error(self.target.table_name(), error));
        PGPROTO_COPY_BATCH_FLUSH_DURATION.observe(started_at.elapsed().as_secs_f64());
        let inserted = flush_result?;
        self.write.clear_sharded_destination(destination);
        self.finish_flush(inserted, reason);
        Ok(())
    }

    fn flush_pending_rows(&mut self, reason: CopyFlushReason) -> PgResult<()> {
        if self.write.is_empty() {
            return Ok(());
        }

        let started_at = Instant::now();
        let flush_result = match &self.write {
            CopyWriteSession::Global(batch) => self.target.flush_global_batch(batch),
            CopyWriteSession::Sharded { batches, .. } => self
                .target
                .flush_sharded_batches(&self.runtime, batches.iter()),
        }
        .map_err(|error| map_copy_target_flush_error(self.target.table_name(), error));
        PGPROTO_COPY_BATCH_FLUSH_DURATION.observe(started_at.elapsed().as_secs_f64());
        let inserted = flush_result?;
        self.write.clear();
        self.finish_flush(inserted, reason);
        Ok(())
    }

    fn finish_flush(&mut self, inserted: usize, reason: CopyFlushReason) {
        PGPROTO_COPY_BATCHES_FLUSHED_TOTAL
            .with_label_values(&[reason.label()])
            .inc();
        PGPROTO_COPY_ROWS_INSERTED_TOTAL.inc_by(inserted as u64);
        self.inserted_rows += inserted;
    }

    fn ensure_pending_record_limit(&self) -> PgResult<()> {
        let pending = pending_record_len(self.record_reader.pending.as_ref());
        self.ensure_record_size(pending)
    }

    fn ensure_incoming_record_limit(&self, incoming: &[u8]) -> PgResult<()> {
        if !incoming_exceeds_record_limit(
            self.record_reader.pending.as_ref(),
            self.record_reader.scan_escaped,
            incoming,
            self.row_byte_limit,
        ) {
            return Ok(());
        }

        Err(record_limit_error(self.row_byte_limit))
    }

    fn ensure_record_size(&self, record_len: usize) -> PgResult<()> {
        if record_len <= self.row_byte_limit {
            return Ok(());
        }

        Err(record_limit_error(self.row_byte_limit))
    }
}

#[derive(Default)]
struct TextRecordReader {
    pending: BytesMut,
    eol_style: EolStyle,
    scan_offset: usize,
    scan_escaped: bool,
}

impl TextRecordReader {
    fn with_capacity(initial_capacity: usize) -> Self {
        Self {
            pending: BytesMut::with_capacity(initial_capacity),
            ..Self::default()
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        self.pending.extend_from_slice(bytes);
    }

    fn next_record(&mut self) -> PgResult<Option<Bytes>> {
        let Some((line_end, line_ending_len, eol_style)) = self.find_record_end() else {
            return Ok(None);
        };
        self.register_eol(eol_style)?;
        let mut record = self.pending.split_to(line_end + line_ending_len);
        record.truncate(line_end);
        self.reset_scan();
        Ok(Some(record.freeze()))
    }

    fn finish_record(&mut self) -> PgResult<Option<Bytes>> {
        if self.pending.is_empty() {
            return Ok(None);
        }

        let mut record = self.pending.split();
        if ends_with_unescaped_cr(record.as_ref()) {
            self.register_eol(EolStyle::Cr)?;
            record.truncate(record.len() - 1);
        }
        self.reset_scan();
        Ok(Some(record.freeze()))
    }

    fn register_eol(&mut self, eol_style: EolStyle) -> PgResult<()> {
        if matches!(self.eol_style, EolStyle::Unknown) {
            self.eol_style = eol_style;
            return Ok(());
        }

        if self.eol_style == eol_style {
            return Ok(());
        }

        Err(bad_copy_format(format!(
            "COPY data has mixed line endings: expected {} but found {}",
            self.eol_style.name(),
            eol_style.name()
        )))
    }

    fn find_record_end(&mut self) -> Option<(usize, usize, EolStyle)> {
        let mut idx = self.scan_offset;
        let mut escaped = self.scan_escaped;

        while idx < self.pending.len() {
            if escaped {
                escaped = false;
                idx += 1;
                continue;
            }

            match self.pending[idx] {
                b'\\' => {
                    escaped = true;
                    idx += 1;
                }
                b'\n' => {
                    self.reset_scan();
                    return Some((idx, 1, EolStyle::Lf));
                }
                b'\r' => {
                    if idx + 1 < self.pending.len() {
                        let eol_style = if self.pending[idx + 1] == b'\n' {
                            EolStyle::CrLf
                        } else {
                            EolStyle::Cr
                        };
                        let line_ending_len = if matches!(eol_style, EolStyle::CrLf) {
                            2
                        } else {
                            1
                        };
                        self.reset_scan();
                        return Some((idx, line_ending_len, eol_style));
                    }

                    self.scan_offset = idx;
                    self.scan_escaped = false;
                    return None;
                }
                _ => {
                    idx += 1;
                }
            }
        }

        self.scan_offset = idx;
        self.scan_escaped = escaped;
        None
    }

    fn reset_scan(&mut self) {
        self.scan_offset = 0;
        self.scan_escaped = false;
    }
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum EolStyle {
    #[default]
    Unknown,
    Lf,
    Cr,
    CrLf,
}

impl EolStyle {
    fn name(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Lf => "LF",
            Self::Cr => "CR",
            Self::CrLf => "CRLF",
        }
    }
}

struct TextRowParser {
    delimiter: u8,
    null_marker: Vec<u8>,
    field_count: usize,
}

impl TextRowParser {
    fn new(delimiter: u8, null_marker: Vec<u8>, field_count: usize) -> Self {
        Self {
            delimiter,
            null_marker,
            field_count,
        }
    }

    fn decode_record_into(
        &self,
        record: &[u8],
        field_oids: &[Oid],
        values: &mut VTableTuple,
    ) -> PgResult<()> {
        values.clear();
        let mut field_start = 0usize;
        let mut escaped = false;
        let mut has_escape = false;
        let mut field_count = 0usize;

        for (idx, byte) in record.iter().copied().enumerate() {
            if !escaped && byte == self.delimiter {
                self.push_value(record, field_start, idx, has_escape, field_oids, values)?;
                field_count += 1;
                field_start = idx + 1;
                has_escape = false;
                continue;
            }

            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
                has_escape = true;
            }
        }

        self.push_value(
            record,
            field_start,
            record.len(),
            has_escape,
            field_oids,
            values,
        )?;
        field_count += 1;
        if field_count != self.field_count {
            values.clear();
            return Err(PedanticError::new(
                PgErrorCode::BadCopyFileFormat,
                format!(
                    "COPY row has {} columns but expected {}",
                    field_count, self.field_count
                ),
            )
            .into());
        }

        Ok(())
    }

    fn push_value(
        &self,
        record: &[u8],
        start: usize,
        end: usize,
        has_escape: bool,
        field_oids: &[Oid],
        values: &mut VTableTuple,
    ) -> PgResult<()> {
        let raw = &record[start..end];
        let Some(oid) = field_oids.get(values.len()).copied() else {
            if has_escape {
                self.decode_text_field(raw)?;
            }
            return Ok(());
        };
        let pg_value = if raw == self.null_marker.as_slice() {
            PgValue::decode(None, oid, FieldFormat::Text)?
        } else if has_escape {
            let decoded = self.decode_text_field(raw)?;
            PgValue::decode(Some(decoded.as_slice()), oid, FieldFormat::Text)?
        } else {
            PgValue::decode(Some(raw), oid, FieldFormat::Text)?
        };
        let sbroad_value: SbroadValue = pg_value.try_into()?;
        values.push(sbroad_value);
        Ok(())
    }

    fn decode_text_field(&self, raw: &[u8]) -> PgResult<Vec<u8>> {
        let mut decoded = Vec::with_capacity(raw.len());
        let mut idx = 0usize;

        while idx < raw.len() {
            let byte = raw[idx];
            if byte != b'\\' {
                decoded.push(byte);
                idx += 1;
                continue;
            }

            idx += 1;
            if idx == raw.len() {
                return Err(PedanticError::new(
                    PgErrorCode::BadCopyFileFormat,
                    "COPY data ended inside an escape sequence",
                )
                .into());
            }

            let escaped = raw[idx];
            let decoded_byte = match escaped {
                b'b' => {
                    idx += 1;
                    b'\x08'
                }
                b'f' => {
                    idx += 1;
                    b'\x0c'
                }
                b'n' => {
                    idx += 1;
                    b'\n'
                }
                b'r' => {
                    idx += 1;
                    b'\r'
                }
                b't' => {
                    idx += 1;
                    b'\t'
                }
                b'v' => {
                    idx += 1;
                    b'\x0b'
                }
                b'\\' => {
                    idx += 1;
                    b'\\'
                }
                b'x' => {
                    let Some(first) = raw.get(idx + 1).copied() else {
                        return Err(PedanticError::new(
                            PgErrorCode::BadCopyFileFormat,
                            "COPY data ended inside a hexadecimal escape sequence",
                        )
                        .into());
                    };
                    let Some(mut value) = hex_value(first) else {
                        return Err(PedanticError::new(
                            PgErrorCode::BadCopyFileFormat,
                            format!(
                                "invalid COPY hexadecimal escape sequence: \\x{}",
                                char::from(first)
                            ),
                        )
                        .into());
                    };

                    idx += 2;
                    if let Some(second) = raw.get(idx).copied().and_then(hex_value) {
                        value = value * 16 + second;
                        idx += 1;
                    }
                    value
                }
                b'0'..=b'7' => {
                    let mut value = escaped - b'0';
                    idx += 1;
                    for _ in 0..2 {
                        let Some(next) = raw.get(idx).copied() else {
                            break;
                        };
                        if !(b'0'..=b'7').contains(&next) {
                            break;
                        }
                        value = value.saturating_mul(8).saturating_add(next - b'0');
                        idx += 1;
                    }
                    value
                }
                byte if byte == self.delimiter => {
                    idx += 1;
                    self.delimiter
                }
                other => {
                    idx += 1;
                    other
                }
            };

            decoded.push(decoded_byte);
        }

        Ok(decoded)
    }
}

fn parse_copy_delimiter(delimiter: &str) -> PgResult<u8> {
    let mut bytes = delimiter.as_bytes().iter().copied();
    let Some(byte) = bytes.next() else {
        return Err(PgError::FeatureNotSupported(format_smolstr!(
            "COPY delimiter must be a single-byte character"
        )));
    };
    if bytes.next().is_some() {
        return Err(PgError::FeatureNotSupported(format_smolstr!(
            "COPY delimiter must be a single-byte character"
        )));
    }
    Ok(byte)
}

fn copy_format_name(format: CopyFormat) -> &'static str {
    match format {
        CopyFormat::Text => "text",
        CopyFormat::Csv => "csv",
        CopyFormat::Binary => "binary",
    }
}

fn ends_with_unescaped_cr(data: &[u8]) -> bool {
    if data.last() != Some(&b'\r') {
        return false;
    }

    let mut escaped = false;
    for (idx, byte) in data.iter().enumerate() {
        if idx == data.len() - 1 {
            return !escaped;
        }

        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        }
    }

    false
}

fn pending_record_len(data: &[u8]) -> usize {
    if ends_with_unescaped_cr(data) {
        data.len().saturating_sub(1)
    } else {
        data.len()
    }
}

fn incoming_exceeds_record_limit(
    pending: &[u8],
    pending_escaped: bool,
    incoming: &[u8],
    record_byte_limit: usize,
) -> bool {
    if pending.len().saturating_add(incoming.len()) <= record_byte_limit {
        return false;
    }
    let mut record_len = pending.len();
    let mut escaped = pending_escaped;
    let mut awaiting_lf_after_cr = false;

    if ends_with_unescaped_cr(pending) {
        record_len = 0;
        escaped = false;
        awaiting_lf_after_cr = true;
    }

    if record_len > record_byte_limit {
        return true;
    }

    for byte in incoming.iter().copied() {
        if awaiting_lf_after_cr {
            awaiting_lf_after_cr = false;
            if byte == b'\n' {
                continue;
            }
        }

        if escaped {
            escaped = false;
            record_len = record_len.saturating_add(1);
        } else {
            match byte {
                b'\\' => {
                    escaped = true;
                    record_len = record_len.saturating_add(1);
                }
                b'\n' => {
                    record_len = 0;
                }
                b'\r' => {
                    record_len = 0;
                    awaiting_lf_after_cr = true;
                }
                _ => {
                    record_len = record_len.saturating_add(1);
                }
            }
        }

        if record_len > record_byte_limit {
            return true;
        }
    }

    false
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn require_positive_copy_option(name: &str, value: usize) -> PgResult<usize> {
    if value == 0 {
        return Err(PgError::FeatureNotSupported(format_smolstr!(
            "COPY {name} must be greater than zero"
        )));
    }
    Ok(value)
}

fn resolve_copy_flush_thresholds(spec: &CopySpec) -> PgResult<CopyFlushThresholdsByScope> {
    let session_rows = match spec.session_flush_rows {
        Some(value) => require_positive_copy_option("session_flush_rows", value)?,
        None => COPY_FALLBACK_FLUSH_ROWS,
    };
    let destination_rows = match spec.destination_flush_rows {
        Some(value) => require_positive_copy_option("destination_flush_rows", value)?,
        None => COPY_FALLBACK_FLUSH_ROWS,
    };
    let session_bytes = match spec.session_flush_bytes {
        Some(value) => Some(require_positive_copy_option("session_flush_bytes", value)?),
        None => Some(COPY_FALLBACK_FLUSH_BYTES),
    };
    let destination_bytes = match spec.destination_flush_bytes {
        Some(value) => Some(require_positive_copy_option(
            "destination_flush_bytes",
            value,
        )?),
        None => Some(COPY_FALLBACK_FLUSH_BYTES),
    };

    Ok(CopyFlushThresholdsByScope::new(
        CopyFlushThresholds::new(session_rows, session_bytes),
        CopyFlushThresholds::new(destination_rows, destination_bytes),
    ))
}

fn map_copy_target_flush_error(table_name: &SmolStr, error: CopyTargetError) -> PgError {
    match error {
        CopyTargetError::Storage(sql::errors::SbroadError::OutdatedStorageSchema) => {
            PedanticError::new(
                PgErrorCode::ObjectNotInPrerequisiteState,
                format!("target table schema changed during execution: {table_name}"),
            )
            .into()
        }
        other => map_copy_target_error(other),
    }
}

fn map_copy_target_error(error: CopyTargetError) -> PgError {
    match error {
        CopyTargetError::TableDoesNotExist { table } => PedanticError::new(
            PgErrorCode::UndefinedTable,
            format!("table does not exist: {table}"),
        )
        .into(),
        CopyTargetError::DuplicateColumn { column } => PedanticError::new(
            PgErrorCode::DuplicateColumn,
            format!("column \"{column}\" specified more than once"),
        )
        .into(),
        CopyTargetError::ColumnDoesNotExist { column } => PedanticError::new(
            PgErrorCode::UndefinedColumn,
            format!("column does not exist: {column}"),
        )
        .into(),
        CopyTargetError::SystemColumnInsertNotAllowed { column } => PedanticError::new(
            PgErrorCode::InvalidColumnReference,
            format!("system column \"{column}\" cannot be inserted"),
        )
        .into(),
        CopyTargetError::MissingRequiredColumn { column } => PedanticError::new(
            PgErrorCode::NotNullViolation,
            format!("NonNull column \"{column}\" must be specified"),
        )
        .into(),
        CopyTargetError::Internal(message) => {
            PedanticError::new(PgErrorCode::InternalError, message.to_string()).into()
        }
        CopyTargetError::FeatureNotSupported(message) => PgError::FeatureNotSupported(message),
        CopyTargetError::BucketRoutingStale { .. }
        | CopyTargetError::BucketRebalancingInProgress { .. } => {
            PedanticError::new(PgErrorCode::ObjectNotInPrerequisiteState, error.to_string()).into()
        }
        CopyTargetError::Picodata(crate::traft::error::Error::Cas(
            cas::Error::TableNotOperable { table },
        )) => PedanticError::new(
            PgErrorCode::ObjectNotInPrerequisiteState,
            format!("table {table} cannot be modified now as DDL operation is in progress"),
        )
        .into(),
        CopyTargetError::Picodata(error) => error.into(),
        CopyTargetError::Storage(error) => error.into(),
        CopyTargetError::MissingBucketId
        | CopyTargetError::MissingBucketRoute { .. }
        | CopyTargetError::NoSuchTier { .. } => {
            PedanticError::new(PgErrorCode::InternalError, error.to_string()).into()
        }
        CopyTargetError::Tarantool(error)
            if error.error_code() == TarantoolErrorCode::AccessDenied as u32 =>
        {
            PedanticError::new(PgErrorCode::InsufficientPrivilege, error.to_string()).into()
        }
        CopyTargetError::Tarantool(error) => error.into(),
    }
}

pub(crate) fn start_copy(spec: CopySpec) -> PgResult<(CopyStart, CopySession)> {
    let target = prepare_copy_target(
        spec.schema_name.as_ref(),
        &spec.table_name,
        &spec.columns,
        spec.conflict_policy,
    )
    .map_err(map_copy_target_error)?;
    let field_oids = target
        .field_types()
        .iter()
        .map(|field_type| super::storage::sbroad_type_to_pg(field_type).oid())
        .collect::<Vec<_>>();
    let flush_thresholds = resolve_copy_flush_thresholds(&spec)?;
    let row_byte_limit = require_positive_copy_option(
        "row_bytes",
        spec.row_bytes.unwrap_or(COPY_FALLBACK_ROW_BYTES),
    )?;
    let runtime = StorageRuntime::new();
    let start = CopyStart {
        column_count: field_oids.len(),
    };
    let session = CopySession::new(
        target,
        field_oids,
        runtime,
        spec.delimiter,
        spec.null_marker,
        spec.header,
        flush_thresholds,
        row_byte_limit,
    );
    PGPROTO_COPY_SESSIONS_STARTED_TOTAL.inc();

    Ok((start, session))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sharded_bucket_state_change_maps_to_55000() {
        let error = map_copy_target_error(CopyTargetError::BucketRoutingStale {
            tier_name: "tier1".into(),
            prepared_bucket_state_version: 1,
            live_current_bucket_state_version: 2,
            live_target_bucket_state_version: 2,
        });

        assert_eq!(
            error.info().code,
            PgErrorCode::ObjectNotInPrerequisiteState.as_str()
        );
    }

    #[test]
    fn sharded_bucket_rebalancing_maps_to_55000() {
        let error = map_copy_target_error(CopyTargetError::BucketRebalancingInProgress {
            tier_name: "tier1".into(),
            current_bucket_state_version: 1,
            target_bucket_state_version: 2,
        });

        assert_eq!(
            error.info().code,
            PgErrorCode::ObjectNotInPrerequisiteState.as_str()
        );
    }

    #[test]
    fn internal_sharded_route_corruption_maps_to_xx000() {
        let error = map_copy_target_error(CopyTargetError::MissingBucketRoute {
            tier_name: "tier1".into(),
            bucket_id: 42,
        });

        assert_eq!(error.info().code, PgErrorCode::InternalError.as_str());
    }

    #[test]
    fn escaped_cr_before_lf_keeps_lf_as_record_terminator() {
        let mut reader = TextRecordReader::with_capacity(8);

        reader.push(br"one\");
        assert!(reader.next_record().unwrap().is_none());

        reader.push(b"\r\ntwo\n");
        let record = reader.next_record().unwrap().unwrap();
        let next = reader.next_record().unwrap().unwrap();

        assert_eq!(record.as_ref(), b"one\\\r");
        assert_eq!(next.as_ref(), b"two");
    }
}

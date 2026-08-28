use bitflags::bitflags;
use smol_str::format_smolstr;
use sql_ir::errors::SbroadError;
use sql_ir::ir::node::{AnonymousBlock, BlockEntries};
use std::fmt;
use tarantool::msgpack;

use std::fmt::Write as _;

use sql_executor::executor::Port;

use serde::{Deserialize, Serialize};

use crate::explain::buckets::BoundedBuckets;
use crate::explain::utils::format_block_stage_label;
use crate::explain::utils::format_block_stage_number;

use crate::explain::utils::{format_sql, indent_custom, indent_with_prefix};
use crate::write_explain_header2;

/// Helper struct which is used for EXPLAIN (RAW) output generation.
#[derive(Clone, Copy)]
pub struct MotionInfo {
    ///  If subtree has segment motion, its buckets are calculated from the
    ///  contents of that motion virtual table. Save that to further reflect in
    ///  EXPLAIN (RAW).
    pub has_segment_motion: bool,
    ///  `SerializeAsEmpty` is a motion opcode. If it is present in subtree,
    ///  there could possibly be generated two different local SQLs. The meaning
    ///  of possible values:
    ///  * `None` - motion subtree does not contain `SerializeAsEmpty` opcode.
    ///  * `Some(true)` - the generated SQL from such subtree is going to be simple scan
    ///    of sharded table:
    ///    `SELECT "t"."a" FROM "t" UNION ALL select cast(null as int) as "b" where false`.
    ///  * `Some(false)` - the generated SQL performs UNION(UNION ALL) of global and
    ///    sharded tables:
    ///    `SELECT * FROM t UNION ALL SELECT * FROM g`
    pub has_serialize_as_empty_opcode: Option<bool>,
}

impl MotionInfo {
    // Queries in transactional blocks can not
    // have motions.
    pub fn new_for_transaction() -> Self {
        Self {
            has_segment_motion: false,
            has_serialize_as_empty_opcode: None,
        }
    }

    pub fn new_for_query(
        has_segment_motion: bool,
        has_serialize_as_empty_opcode: Option<bool>,
    ) -> Self {
        Self {
            has_segment_motion,
            has_serialize_as_empty_opcode,
        }
    }
}

/// Helper struct which holds the query execution location.
#[derive(Serialize, Deserialize, Debug)]
pub enum ExplainQueryLocation {
    /// Query is executed exactly on N replicasets.
    ConstFiltered { fraction: (usize, usize) },
    /// Query execution replicasets are computed in runtime. In case when it
    /// is possible to calculate an upper bound, estimation "<= N/M" is added.
    DynFiltered { fraction: Option<(usize, usize)> },
    /// Query is executed locally.
    Router,
    /// Query is executed on every replicaset.
    Whole,
}

impl std::fmt::Display for ExplainQueryLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExplainQueryLocation::ConstFiltered { fraction } => {
                write!(f, "CONST-FILTERED STORAGE, {}/{}", fraction.0, fraction.1)
            }
            ExplainQueryLocation::DynFiltered { fraction } if fraction.is_some() => {
                let fraction = fraction.unwrap();
                write!(f, "DYN-FILTERED STORAGE, <= {}/{}", fraction.0, fraction.1)
            }
            ExplainQueryLocation::DynFiltered { .. } => write!(f, "DYN-FILTERED STORAGE"),
            ExplainQueryLocation::Router => write!(f, "ROUTER"),
            ExplainQueryLocation::Whole => write!(f, "WHOLE STORAGE"),
        }
    }
}

pub const LINE_WIDTH: usize = 80;

#[derive(Serialize, Deserialize, Debug, Clone, msgpack::Encode, msgpack::Decode)]
pub struct RawExplainTuple {
    selectid: i64,
    order: i64,
    from: i64,
    detail: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RawExplainEntry {
    Multiple(Vec<QueryEntry>),
    Single(QueryEntry),
}

impl RawExplainEntry {
    fn decode_entry(mp: &[u8]) -> Result<RawExplainEntry, SbroadError> {
        let json_wrapped: Vec<String> = msgpack::decode(mp)
            .map_err(|err| SbroadError::Other(format_smolstr!("unable to decode JSON: {err}")))?;
        let json = json_wrapped[0].clone();

        let mut query_entry: RawExplainEntry = serde_json::de::from_str(&json).map_err(|err| {
            SbroadError::Other(format_smolstr!(
                "unable to decode RawExplainEntry from JSON: {err}"
            ))
        })?;

        // Provide a fallback for empty raw plans.
        match &mut query_entry {
            RawExplainEntry::Single(entry) => {
                if entry.tuples.as_ref().is_ok_and(Vec::is_empty) {
                    add_trivial_fallback(entry);
                }
            }
            RawExplainEntry::Multiple(entries) => {
                for entry in entries {
                    if entry.tuples.as_ref().is_ok_and(Vec::is_empty) {
                        add_trivial_fallback(entry);
                    }
                }
            }
        }

        Ok(query_entry)
    }
}

fn add_trivial_fallback(entry: &mut QueryEntry) {
    if let Ok(items) = &mut entry.tuples {
        items.push(RawExplainTuple {
            selectid: 0,
            order: 0,
            from: 0,
            detail: "TRIVIAL".into(),
        });
    }
}

pub fn decode_vdbe_plan<'p>(port: &impl Port<'p>) -> Result<Vec<RawExplainTuple>, SbroadError> {
    let mut tuples = Vec::with_capacity(port.size() as usize);

    for mp in port.iter() {
        let tuple = msgpack::decode::<RawExplainTuple>(mp).map_err(|err| {
            SbroadError::Other(format_smolstr!(
                "unable to decode RawExplainTuple from port: {err}"
            ))
        })?;

        tuples.push(tuple);
    }

    Ok(tuples)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryEntry {
    sql: String,
    location: ExplainQueryLocation,
    buckets: BoundedBuckets,
    params: Vec<String>,
    tuples: Result<Vec<RawExplainTuple>, String>,
}

impl QueryEntry {
    pub fn new(
        sql: String,
        location: ExplainQueryLocation,
        buckets: BoundedBuckets,
        params: Vec<String>,
        tuples: Result<Vec<RawExplainTuple>, String>,
    ) -> QueryEntry {
        QueryEntry {
            sql,
            location,
            buckets,
            params,
            tuples,
        }
    }
}

fn format_raw_plan_node(node: &str, should_fmt: bool) -> String {
    let mut node = node.to_owned();
    if should_fmt && node.len() > LINE_WIDTH {
        node = node.replace("USING", "\n USING");
        node = node.replace("(", "\n (");
    }

    node
}

fn format_raw_plan(tuples: &[RawExplainTuple], should_fmt: bool) -> String {
    let mut plan = String::new();

    let mut tuples = tuples.iter().peekable();
    while let Some(tuple) = tuples.next() {
        let has_next = tuples.peek().is_some();
        let sep = if has_next { "\n" } else { "" };

        let idx = tuple.selectid;
        let level = tuple.order.max(0) as usize + 1;
        let node = format_raw_plan_node(&tuple.detail, should_fmt);
        let prefix = format_smolstr!("[{idx}] ");

        write!(
            indent_custom(&mut plan, &mut indent_with_prefix(level * 2, prefix)),
            "{node}{sep}"
        )
        .unwrap();
    }

    plan
}

struct ExplainIndex(usize, Option<usize>);

impl std::fmt::Display for ExplainIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(idx) = self.1 {
            write!(f, "{}.{idx}.", self.0)
        } else {
            write!(f, "{}.", self.0)
        }
    }
}

/// Number and label of one transactional-block stage, worked out on the router
/// from the block's shape. See [`format_block_stage_number`] and
/// [`format_block_stage_label`].
#[derive(Debug)]
pub struct BlockStageHeader {
    number: String,
    label: String,
}

impl BlockStageHeader {
    pub(crate) fn from_anon_block(block: &AnonymousBlock) -> Result<Vec<Self>, SbroadError> {
        let stages = BlockEntries::new(&block.statements)
            .map(|entry| BlockStageHeader {
                number: format_block_stage_number(&entry.location),
                label: format_block_stage_label(&entry.location),
            })
            .collect();

        Ok(stages)
    }
}

fn write_raw_explain_entry(
    f: &mut fmt::Formatter<'_>,
    entry: &QueryEntry,
    idx: ExplainIndex,
    stage: Option<&BlockStageHeader>,
    format_options: RawExplainOptions,
) -> fmt::Result {
    let should_fmt = format_options.contains(RawExplainOptions::Fmt);
    let sql = format_sql(&entry.sql, &entry.params, should_fmt);
    let plan = match &entry.tuples {
        Ok(tuples) => format_raw_plan(tuples, should_fmt),
        Err(err) => err.clone(),
    };

    let source = &entry.location;
    match stage {
        Some(stage) => {
            let (number, label) = (&stage.number, &stage.label);
            write_explain_header2!(f, "{number} {label} ({source})")?;
        }
        None => {
            write_explain_header2!(f, "{idx} Query ({source})")?;
        }
    }
    write!(f, "\n{sql}\n\n")?;
    write!(f, "plan:\n{plan}")?;

    let show_buckets = format_options.contains(RawExplainOptions::ShowBuckets);
    if show_buckets {
        write!(f, "\n\n{}", entry.buckets)?;
    }

    Ok(())
}

bitflags! {
    /// Helper struct which specifies the options of `RawExplain` formatting.
    #[derive(Clone, Copy, Debug)]
    pub(crate) struct RawExplainOptions: u8 {
        const ShowBuckets = 1;
        const Fmt = 1 << 1;
    }
}

#[derive(Debug)]
pub(crate) struct RawExplain {
    entries: Vec<RawExplainEntry>,
    format_options: RawExplainOptions,
    /// Headers for a transactional block, one per entry in the same order.
    /// Empty for anything else, and deliberately also when the count does not
    /// match the entries -- better plain numbering than wrong labels.
    stages: Vec<BlockStageHeader>,
}

impl fmt::Display for RawExplain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entries = self.entries.iter().enumerate().peekable();
        while let Some((idx, entry)) = entries.next() {
            match entry {
                RawExplainEntry::Single(entry) => {
                    write_raw_explain_entry(
                        f,
                        entry,
                        ExplainIndex(idx + 1, None),
                        self.stages.get(idx),
                        self.format_options,
                    )?;
                }
                RawExplainEntry::Multiple(entries) => {
                    let mut entry_iter = entries.iter().enumerate().peekable();
                    while let Some((i, entry)) = entry_iter.next() {
                        write_raw_explain_entry(
                            f,
                            entry,
                            ExplainIndex(idx + 1, Some(i + 1)),
                            None,
                            self.format_options,
                        )?;

                        let has_next = entry_iter.peek().is_some();
                        if has_next {
                            write!(f, "\n\n")?;
                        }
                    }
                }
            };

            // Since raw explain entries don't include a trailing newline,
            // the first writeln! terminates the previous entry's last line,
            // and the second writeln! adds a blank separator line between entries.
            let has_next = entries.peek().is_some();
            if has_next {
                write!(f, "\n\n")?;
            }
        }

        Ok(())
    }
}

impl RawExplain {
    pub fn from_port<'p>(
        port: &mut impl Port<'p>,
        format_options: RawExplainOptions,
        stages: Vec<BlockStageHeader>,
    ) -> Result<RawExplain, SbroadError> {
        let mut explain_entries = Vec::new();
        for mp in port.iter() {
            let entry = RawExplainEntry::decode_entry(mp)?;
            explain_entries.push(entry);
        }

        Ok(Self {
            entries: explain_entries,
            format_options,
            stages,
        })
    }
}

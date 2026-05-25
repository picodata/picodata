use sql_dynfilter::{DynamicFilter, NullPolicy};

/// Adapter trait for encoding data into msgpack format
pub trait MsgpackEncode {
    fn encode_into(&self, w: &mut impl std::io::Write) -> std::io::Result<()>;
}

/// Storage-side directive for one dynamic filter — tells the executor
/// which columns to extract from each probe row, and how to handle
/// NULLs, before consulting `FilterView::contains`.
///
/// The coordinator builds and ships one `ApplySpec` per filter inside
/// the DQL packet; the storage-side executor uses it to locate the
/// JOIN-key columns by position in the row's msgpack `array` and feed
/// them — in the same byte order the build side used — into a
/// `TupleHasher`.
///
/// Positions are column indices into the storage's pre-projection row
/// layout (matches the SQL stmt's output schema). The null policy must
/// match the paired `BuildFilter::null_policy` exactly or the filter
/// is silently wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplySpec {
    pub key_positions: Vec<u32>,
    pub null_policy: NullPolicy,
}

impl ApplySpec {
    pub fn new(key_positions: Vec<u32>, null_policy: NullPolicy) -> Self {
        Self {
            key_positions,
            null_policy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ColumnType {
    Map = 0,
    Boolean,
    Datetime,
    Decimal,
    Double,
    Integer,
    String,
    Uuid,
    Any,
    Array,
    Scalar,
}

impl TryFrom<u8> for ColumnType {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let t = match value {
            0 => ColumnType::Map,
            1 => ColumnType::Boolean,
            2 => ColumnType::Datetime,
            3 => ColumnType::Decimal,
            4 => ColumnType::Double,
            5 => ColumnType::Integer,
            6 => ColumnType::String,
            7 => ColumnType::Uuid,
            8 => ColumnType::Any,
            9 => ColumnType::Array,
            10 => ColumnType::Scalar,
            _ => return Err(format!("Unknown column type: {value}")),
        };

        Ok(t)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DQLOptions {
    pub sql_motion_row_max: u64,
    pub sql_vdbe_opcode_max: u64,
}

pub trait DQLDataSource {
    fn get_table_schema_info(&self) -> impl ExactSizeIterator<Item = (u32, u64)>;

    fn get_index_schema_info(&self) -> impl ExactSizeIterator<Item = ([u32; 2], u64)>;

    fn get_plan_id(&self) -> u64;

    fn get_sender_id(&self) -> u64;

    fn get_request_id(&self) -> &str;

    fn get_vtables(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, impl ExactSizeIterator<Item = impl MsgpackEncode>)>;

    fn get_options(&self) -> DQLOptions;

    fn get_params(&self) -> impl MsgpackEncode;

    /// Whether the coordinator is sending dynamic filters with this packet.
    /// Gated by the `sql_dynamic_filter_pushdown` alter-system parameter.
    /// When `false`, `write_dql_packet_data` emits the 7-field layout
    /// (backward-compatible with older storages); when `true`, it emits
    /// 8 fields with the filters map as the trailing field.
    fn dynamic_filters_enabled(&self) -> bool {
        false
    }

    /// Dynamic filters keyed by stable `filter_id` (matches
    /// `BuildFilter::filter_id` / `ApplyFilter::filter_id` in the IR).
    /// Returned by reference so the encoder can call `encode_into`
    /// directly into the wire buffer — no intermediate `Vec<u8>`.
    ///
    /// The `ApplySpec` rides alongside the filter bytes so the storage
    /// can apply the filter without re-deriving column positions from
    /// the IR: storage executes a cached `SqlStmt` that does not
    /// contain `ApplyFilter` (which is a runtime-only node), so the
    /// wire packet is its only source for those positions.
    fn get_dynamic_filters(
        &self,
    ) -> impl ExactSizeIterator<Item = (u32, &DynamicFilter, &ApplySpec)> {
        std::iter::empty()
    }
}

pub trait DQLCacheMissDataSource {
    fn get_table_schema_info(&self) -> impl ExactSizeIterator<Item = (u32, u64)>;
    fn get_index_schema_info(&self) -> impl ExactSizeIterator<Item = ([u32; 2], u64)>;
    fn get_vtables_metadata(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, impl ExactSizeIterator<Item = (&str, ColumnType)>)>;
    fn get_sql(&self) -> &str;
}

pub(crate) mod test {
    use super::*;
    use std::collections::HashMap;
    use std::io::Write;

    pub(crate) struct TestParamEncoder<'e> {
        data: &'e [u64],
    }

    impl<'e> TestParamEncoder<'e> {
        pub fn new(data: &'e [u64]) -> Self {
            Self { data }
        }
    }

    impl MsgpackEncode for TestParamEncoder<'_> {
        fn encode_into(&self, w: &mut impl Write) -> std::io::Result<()> {
            rmp::encode::write_array_len(w, self.data.len() as u32)?;

            for param in self.data {
                rmp::encode::write_uint(w, *param)?;
            }

            Ok(())
        }
    }

    struct TestTupleEncoder<'e> {
        data: &'e Vec<u64>,
        pk: u64,
    }

    impl<'e> TestTupleEncoder<'e> {
        pub fn new(data: &'e Vec<u64>, pk: u64) -> Self {
            Self { data, pk }
        }
    }

    impl MsgpackEncode for TestTupleEncoder<'_> {
        fn encode_into(&self, w: &mut impl Write) -> std::io::Result<()> {
            rmp::encode::write_array_len(w, (self.data.len() + 1) as u32)?;

            for elem in self.data {
                rmp::encode::write_uint(w, *elem)?;
            }
            rmp::encode::write_uint(w, self.pk)?;

            Ok(())
        }
    }

    #[allow(dead_code)]
    pub struct TestDQLEncoderBuilder {
        encoder: TestDQLDataSource,
    }

    impl TestDQLEncoderBuilder {
        #[allow(dead_code)]
        pub fn new() -> Self {
            TestDQLEncoderBuilder {
                encoder: TestDQLDataSource::default(),
            }
        }

        #[allow(dead_code)]
        pub fn build(self) -> TestDQLDataSource {
            self.encoder
        }

        #[allow(dead_code)]
        pub fn set_schema_info(
            mut self,
            schema_info: (HashMap<u32, u64>, HashMap<[u32; 2], u64>),
        ) -> Self {
            self.encoder.schema_info = schema_info;
            self
        }
        #[allow(dead_code)]
        pub fn set_plan_id(mut self, plan_id: u64) -> Self {
            self.encoder.plan_id = plan_id;
            self
        }
        #[allow(dead_code)]
        pub fn set_sender_id(mut self, sender_id: u64) -> Self {
            self.encoder.sender_id = sender_id;
            self
        }
        #[allow(dead_code)]
        pub fn set_meta(mut self, meta: HashMap<String, Vec<(String, ColumnType)>>) -> Self {
            self.encoder.meta = meta;
            self
        }
        #[allow(dead_code)]
        pub fn set_sql(mut self, sql: String) -> Self {
            self.encoder.sql = sql;
            self
        }
        #[allow(dead_code)]
        pub fn set_request_id(mut self, request_id: String) -> Self {
            self.encoder.request_id = request_id;
            self
        }
        #[allow(dead_code)]
        pub fn set_vtables(mut self, vtables: HashMap<String, Vec<Vec<u64>>>) -> Self {
            self.encoder.vtables = vtables;
            self
        }
        #[allow(dead_code)]
        pub fn set_options(mut self, options: DQLOptions) -> Self {
            self.encoder.options = options;
            self
        }
        #[allow(dead_code)]
        pub fn set_params(mut self, params: Vec<u64>) -> Self {
            self.encoder.params = params;
            self
        }
    }

    #[derive(Default)]
    pub struct TestDQLDataSource {
        pub schema_info: (HashMap<u32, u64>, HashMap<[u32; 2], u64>),
        pub plan_id: u64,
        pub sender_id: u64,
        pub meta: HashMap<String, Vec<(String, ColumnType)>>,
        pub sql: String,
        pub request_id: String,
        pub vtables: HashMap<String, Vec<Vec<u64>>>,
        pub options: DQLOptions,
        pub params: Vec<u64>,
    }

    impl DQLDataSource for TestDQLDataSource {
        fn get_table_schema_info(&self) -> impl ExactSizeIterator<Item = (u32, u64)> {
            self.schema_info.0.iter().map(|(k, v)| (*k, *v))
        }

        fn get_index_schema_info(&self) -> impl ExactSizeIterator<Item = ([u32; 2], u64)> {
            self.schema_info.1.iter().map(|(k, v)| (*k, *v))
        }

        fn get_plan_id(&self) -> u64 {
            self.plan_id
        }

        fn get_sender_id(&self) -> u64 {
            self.sender_id
        }

        fn get_request_id(&self) -> &str {
            self.request_id.as_str()
        }

        fn get_vtables(
            &self,
        ) -> impl ExactSizeIterator<Item = (&str, impl ExactSizeIterator<Item = impl MsgpackEncode>)>
        {
            self.vtables.iter().map(|(k, v)| {
                (
                    k.as_str(),
                    v.iter()
                        .enumerate()
                        .map(|(pk, tuple)| TestTupleEncoder::new(tuple, pk as u64)),
                )
            })
        }

        fn get_options(&self) -> DQLOptions {
            self.options
        }

        fn get_params(&self) -> impl MsgpackEncode {
            TestParamEncoder::new(&self.params)
        }
    }

    impl DQLCacheMissDataSource for TestDQLDataSource {
        fn get_table_schema_info(&self) -> impl ExactSizeIterator<Item = (u32, u64)> {
            self.schema_info.0.iter().map(|(k, v)| (*k, *v))
        }
        fn get_index_schema_info(&self) -> impl ExactSizeIterator<Item = ([u32; 2], u64)> {
            self.schema_info.1.iter().map(|(k, v)| (*k, *v))
        }
        fn get_vtables_metadata(
            &self,
        ) -> impl ExactSizeIterator<Item = (&str, impl ExactSizeIterator<Item = (&str, ColumnType)>)>
        {
            self.meta
                .iter()
                .map(|(k, v)| (k.as_str(), v.iter().map(|(k, t)| (k.as_str(), *t))))
        }
        fn get_sql(&self) -> &str {
            &self.sql
        }
    }
}

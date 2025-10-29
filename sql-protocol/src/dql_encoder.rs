/// Adapter trait for encoding data into msgpack format
pub trait MsgpackEncode {
    fn encode_into(&self, w: &mut impl std::io::Write) -> std::io::Result<()>;
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

pub trait DQLDataSource {
    fn get_schema_info(&self) -> impl ExactSizeIterator<Item = (u32, u64)>;

    fn get_plan_id(&self) -> u64;

    fn get_sender_id(&self) -> &str;

    fn get_request_id(&self) -> &str;

    fn get_vtables(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, impl ExactSizeIterator<Item = impl MsgpackEncode>)>;

    fn get_options(&self) -> [u64; 2];

    fn get_params(&self) -> impl MsgpackEncode;
}

pub trait DQLCacheMissDataSource {
    fn get_schema_info(&self) -> impl ExactSizeIterator<Item = (u32, u64)>;
    fn get_vtables_metadata(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, impl ExactSizeIterator<Item = (&str, ColumnType)>)>;
    fn get_sql(&self) -> &str;
}

pub(crate) mod test {
    use super::*;
    use std::collections::HashMap;
    use std::io::Write;

    struct TestParamEncoder<'e> {
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
        pub fn set_schema_info(mut self, schema_info: HashMap<u32, u64>) -> Self {
            self.encoder.schema_info = schema_info;
            self
        }
        #[allow(dead_code)]
        pub fn set_plan_id(mut self, plan_id: u64) -> Self {
            self.encoder.plan_id = plan_id;
            self
        }
        #[allow(dead_code)]
        pub fn set_sender_id(mut self, sender_id: String) -> Self {
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
        pub fn set_options(mut self, options: [u64; 2]) -> Self {
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
        pub schema_info: HashMap<u32, u64>,
        pub plan_id: u64,
        pub sender_id: String,
        pub meta: HashMap<String, Vec<(String, ColumnType)>>,
        pub sql: String,
        pub request_id: String,
        pub vtables: HashMap<String, Vec<Vec<u64>>>,
        pub options: [u64; 2],
        pub params: Vec<u64>,
    }

    impl DQLDataSource for TestDQLDataSource {
        fn get_schema_info(&self) -> impl ExactSizeIterator<Item = (u32, u64)> {
            self.schema_info.iter().map(|(k, v)| (*k, *v))
        }

        fn get_plan_id(&self) -> u64 {
            self.plan_id
        }

        fn get_sender_id(&self) -> &str {
            self.sender_id.as_str()
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

        fn get_options(&self) -> [u64; 2] {
            self.options
        }

        fn get_params(&self) -> impl MsgpackEncode {
            TestParamEncoder::new(&self.params)
        }
    }

    impl DQLCacheMissDataSource for TestDQLDataSource {
        fn get_schema_info(&self) -> impl ExactSizeIterator<Item = (u32, u64)> {
            self.schema_info.iter().map(|(k, v)| (*k, *v))
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

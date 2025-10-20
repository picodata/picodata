use smol_str::SmolStr;

pub trait MsgpackWriter {
    fn write_current(&self, w: impl std::io::Write) -> std::io::Result<()>;
    fn next(&mut self) -> Option<()>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
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

pub trait DQLEncoder {
    fn get_schema_info(&self) -> impl ExactSizeIterator<Item = (&u32, &u64)>;

    fn get_plan_id(&self) -> u64;

    fn get_sender_id(&self) -> &str;

    fn get_request_id(&self) -> &str;

    fn get_vtables_metadata(
        &self,
    ) -> impl ExactSizeIterator<
        Item = (
            SmolStr,
            impl ExactSizeIterator<Item = (&SmolStr, ColumnType)>,
        ),
    >;

    fn get_vtables(
        &self,
        plan_id: u64,
    ) -> impl ExactSizeIterator<Item = (SmolStr, impl MsgpackWriter)>;

    fn get_options(&self) -> [u64; 2];

    fn get_params(&self) -> impl MsgpackWriter;

    fn get_sql(&self) -> &SmolStr;
}

pub(crate) mod test {
    use super::*;
    use std::collections::HashMap;
    use std::io::Write;

    #[derive(Clone)]
    struct TestParamIterator<'mi> {
        current: Option<&'mi u64>,
        iter: std::slice::Iter<'mi, u64>,
    }

    impl<'mi> TestParamIterator<'mi> {
        pub fn new(iter: std::slice::Iter<'mi, u64>) -> Self {
            Self {
                current: None,
                iter,
            }
        }
    }

    impl MsgpackWriter for TestParamIterator<'_> {
        fn write_current(&self, mut w: impl Write) -> std::io::Result<()> {
            if let Some(v) = self.current {
                rmp::encode::write_uint(&mut w, *v)?;
            }

            Ok(())
        }

        fn next(&mut self) -> Option<()> {
            self.current = self.iter.next();

            self.current.map(|_| ())
        }

        fn len(&self) -> usize {
            self.iter.len()
        }
    }

    struct TestVTableWriter<'vtw> {
        pk: u64,
        current: Option<&'vtw Vec<u64>>,
        iter: std::slice::Iter<'vtw, Vec<u64>>,
    }

    impl<'vtw> TestVTableWriter<'vtw> {
        fn new(iter: std::slice::Iter<'vtw, Vec<u64>>) -> Self {
            Self {
                pk: 0,
                current: None,
                iter,
            }
        }
    }

    impl MsgpackWriter for TestVTableWriter<'_> {
        fn write_current(&self, mut w: impl Write) -> std::io::Result<()> {
            let Some(elem) = self.current else {
                return Ok(());
            };

            rmp::encode::write_array_len(&mut w, (elem.len() + 1) as u32)?;

            for elem in elem {
                rmp::encode::write_uint(&mut w, *elem)?;
            }
            rmp::encode::write_uint(&mut w, self.pk)?;

            Ok(())
        }

        fn next(&mut self) -> Option<()> {
            let is_first = self.current.is_none();
            self.current = self.iter.next();

            self.current?;

            if !is_first {
                self.pk += 1;
            }

            Some(())
        }

        fn len(&self) -> usize {
            self.iter.len()
        }
    }

    #[allow(dead_code)]
    pub struct TestDQLEncoderBuilder {
        encoder: TestDQLEncoder,
    }

    impl TestDQLEncoderBuilder {
        #[allow(dead_code)]
        pub fn new() -> Self {
            TestDQLEncoderBuilder {
                encoder: TestDQLEncoder::default(),
            }
        }

        #[allow(dead_code)]
        pub fn build(self) -> TestDQLEncoder {
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
        pub fn set_meta(mut self, meta: HashMap<SmolStr, Vec<(SmolStr, ColumnType)>>) -> Self {
            self.encoder.meta = meta;
            self
        }
        #[allow(dead_code)]
        pub fn set_sql(mut self, sql: SmolStr) -> Self {
            self.encoder.sql = sql;
            self
        }
        #[allow(dead_code)]
        pub fn set_request_id(mut self, request_id: String) -> Self {
            self.encoder.request_id = request_id;
            self
        }
        #[allow(dead_code)]
        pub fn set_vtables(mut self, vtables: HashMap<SmolStr, Vec<Vec<u64>>>) -> Self {
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
    pub struct TestDQLEncoder {
        pub schema_info: HashMap<u32, u64>,
        pub plan_id: u64,
        pub sender_id: String,
        pub meta: HashMap<SmolStr, Vec<(SmolStr, ColumnType)>>,
        pub sql: SmolStr,
        pub request_id: String,
        pub vtables: HashMap<SmolStr, Vec<Vec<u64>>>,
        pub options: [u64; 2],
        pub params: Vec<u64>,
    }

    impl DQLEncoder for TestDQLEncoder {
        fn get_schema_info(&self) -> impl ExactSizeIterator<Item = (&u32, &u64)> {
            self.schema_info.iter()
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

        fn get_vtables_metadata(
            &self,
        ) -> impl ExactSizeIterator<
            Item = (
                SmolStr,
                impl ExactSizeIterator<Item = (&SmolStr, ColumnType)>,
            ),
        > {
            self.meta
                .iter()
                .map(|(k, v)| (k.clone(), v.iter().map(|(k, t)| (k, *t))))
        }

        fn get_vtables(
            &self,
            _plan_id: u64,
        ) -> impl ExactSizeIterator<Item = (SmolStr, impl MsgpackWriter)> {
            self.vtables
                .iter()
                .map(|(k, v)| (k.clone(), TestVTableWriter::new(v.iter())))
        }

        fn get_options(&self) -> [u64; 2] {
            self.options
        }

        fn get_params(&self) -> impl MsgpackWriter {
            TestParamIterator::new(self.params.iter())
        }

        fn get_sql(&self) -> &SmolStr {
            &self.sql
        }
    }
}

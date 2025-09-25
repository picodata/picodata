use smol_str::SmolStr;

pub trait MsgpackWriter {
    fn write_current(&self, w: impl std::io::Write) -> std::io::Result<()>;
    fn next(&mut self) -> Option<()>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait ProtocolEncoder {
    fn get_schema_info(&self) -> impl ExactSizeIterator<Item = (&u32, &u64)>;

    fn get_plan_id(&self) -> u64;

    fn get_sender_id(&self) -> String;

    fn get_request_id(&self) -> String;

    fn get_vtables(
        &self,
        plan_id: u64,
    ) -> impl ExactSizeIterator<Item = (SmolStr, impl MsgpackWriter)>;

    fn get_options(&self) -> [u64; 2];

    fn get_params(&self) -> impl MsgpackWriter;
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

    pub struct TestEncoder {
        pub schema_info: HashMap<u32, u64>,
        pub plan_id: u64,
        pub sender_id: String,
        pub request_id: String,
        pub vtables: HashMap<SmolStr, Vec<Vec<u64>>>,
        pub options: [u64; 2],
        pub params: Vec<u64>,
    }

    impl ProtocolEncoder for TestEncoder {
        fn get_schema_info(&self) -> impl ExactSizeIterator<Item = (&u32, &u64)> {
            self.schema_info.iter()
        }

        fn get_plan_id(&self) -> u64 {
            self.plan_id
        }

        fn get_sender_id(&self) -> String {
            self.sender_id.clone()
        }

        fn get_request_id(&self) -> String {
            self.request_id.clone()
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
    }
}

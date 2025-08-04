use std::{
    io::{self, Write},
    slice::from_raw_parts,
};

use crate::sql::cluster_proto::{decode_msgpack_res, DecodeResult};
use ::tarantool::ffi::sql::{Ibuf, PortC};
use sbroad::executor::vtable::VirtualTable;
use sbroad::ir::value::Value;

/// A writer that stores msgpack bytes into Tarantool PortC
pub struct PortWriter<'p> {
    port: &'p mut PortC,
}

impl<'p> PortWriter<'p> {
    #[inline]
    pub fn new(port: &'p mut PortC) -> Self {
        Self { port }
    }
}

impl Write for PortWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        unsafe { self.port.add_mp(buf) };
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A writer that stores msgpack bytes into a VirtualTable
pub struct VTableWriter<'v> {
    vtable: &'v mut VirtualTable,
}

impl<'v> VTableWriter<'v> {
    #[inline]
    pub fn new(vtable: &'v mut VirtualTable) -> Self {
        Self { vtable }
    }
}

impl Write for VTableWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Decode a single tuple encoded as msgpack array and append to the virtual table
        let tuple: Vec<Value> = tarantool::msgpack::decode(buf)
            .map_err(|e| io::Error::other(format!("failed to decode msgpack: {e:?}")))?;
        self.vtable.add_tuple(tuple);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Process a single msgpack buffer: returns number of DQL rows written, or 0 on miss/DML
/// For DML operations, returns the row count in the dml_count parameter
pub fn write_from_slice<W: Write>(
    writer: &mut W,
    data: &[u8],
    dml_count: &mut u64,
) -> io::Result<usize> {
    let res = decode_msgpack_res(data).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("decode_msgpack_res error: {e}"),
        )
    })?;

    match res {
        // DQL response: write each tuple slice
        DecodeResult::Dql(iter) => {
            let mut count = 0;
            for slice in iter {
                writer.write_all(slice)?;
                count += 1;
            }
            Ok(count)
        }

        // Cache miss: signal upstream to retry
        DecodeResult::Miss => Ok(0),

        // DML: accumulate, defer writing
        DecodeResult::RowCnt(n) => {
            *dml_count = dml_count.saturating_add(n);
            Ok(0)
        }
    }
}

/// Write the total DML count as a single-element array
pub fn flush_dml<W: Write>(writer: &mut W, total: u64) -> io::Result<()> {
    let mut tmp = Vec::new();
    // array len = 1
    rmp::encode::write_array_len(&mut tmp, 1).map_err(io::Error::other)?;
    // the integer
    rmp::encode::write_uint(&mut tmp, total).map_err(io::Error::other)?;
    writer.write_all(&tmp)?;
    Ok(())
}

/// # Safety
#[inline]
pub unsafe fn write_from_ibuf<W: Write>(
    ibuf: *const Ibuf,
    writer: &mut W,
    dml_count: &mut u64,
) -> io::Result<usize> {
    let start = (*ibuf).rpos;
    let end = (*ibuf).wpos;
    let len = end.offset_from(start) as usize;
    let data = from_raw_parts(start, len);
    write_from_slice(writer, data, dml_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sbroad::executor::vtable::VirtualTable;
    use sbroad::ir::value::Value;
    use tarantool::datetime::Datetime;
    use tarantool::decimal::Decimal;
    use tarantool::uuid::Uuid;
    use time::macros::datetime;

    #[test]
    #[cfg(feature = "standalone_decimal")]
    fn test_write_dql_full_row() {
        // {
        //   "dql": [
        //     [5, "foo", true, dt, uuid, dec, 7],
        //     [8, "bar", false, dt, uuid, dec, 9]
        //   ]
        // }
        let part1 = b"\x81\xA3dql\x92\x97\x05\xA3foo\xC3";
        let part2 = b"\x07\x97\x08\xA3bar\xC2";
        let part3 = b"\x09";

        let dt = datetime!(2023-11-11 02:03:19.35421 -03);
        let dt_slice = rmp_serde::to_vec::<Datetime>(&dt.into()).unwrap();
        let uuid_slice = rmp_serde::to_vec(&Uuid::nil()).unwrap();
        let dec_slice = rmp_serde::to_vec(&Decimal::from(666)).unwrap();

        let mut buf = Vec::new();
        buf.extend_from_slice(part1);
        buf.extend_from_slice(&dt_slice);
        buf.extend_from_slice(&uuid_slice);
        buf.extend_from_slice(&dec_slice);
        buf.extend_from_slice(part2);
        buf.extend_from_slice(&dt_slice);
        buf.extend_from_slice(&uuid_slice);
        buf.extend_from_slice(&dec_slice);
        buf.extend_from_slice(part3);

        let mut vtable = VirtualTable::new();
        let mut writer = VTableWriter::new(&mut vtable);
        let mut dml_count = 0;
        let count = write_from_slice(&mut writer, &buf, &mut dml_count).unwrap();
        assert_eq!(count, 2);

        // We won't compare entire rows here; just ensure two entries and correct first fields
        let tuples = vtable.get_tuples();
        assert_eq!(tuples.len(), 2);
        let expected = vec![
            vec![
                Value::Integer(5),
                Value::String("foo".into()),
                Value::Boolean(true),
                Value::Datetime(dt.into()),
                Value::Uuid(Uuid::nil()),
                Value::Decimal(Box::new(Decimal::from(666))),
                Value::Integer(7),
            ],
            vec![
                Value::Integer(8),
                Value::String("bar".into()),
                Value::Boolean(false),
                Value::Datetime(dt.into()),
                Value::Uuid(Uuid::nil()),
                Value::Decimal(Box::new(Decimal::from(666))),
                Value::Integer(9),
            ],
        ];

        assert_eq!(tuples, expected);
    }

    #[test]
    fn test_write_dml() {
        // { "dml": [567] }
        let buf = b"\x81\xA3dml\xCD\x02\x37";
        let mut vtable = VirtualTable::new();
        let mut writer = VTableWriter::new(&mut vtable);
        let mut dml_count = 0;
        let count = write_from_slice(&mut writer, buf, &mut dml_count).unwrap();

        assert_eq!(count, 0);
        assert_eq!(dml_count, 567);

        // flush the DML count
        flush_dml(&mut writer, dml_count).unwrap();
        let tuples = vtable.get_tuples();
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0], vec![Value::Integer(567)]);
    }

    #[test]
    fn test_write_miss() {
        // { "miss": nil }
        let buf = b"\x81\xA4miss\xC0";
        let mut vtable = VirtualTable::new();
        let mut writer = VTableWriter::new(&mut vtable);
        let mut dml_count = 0;
        let count = write_from_slice(&mut writer, buf, &mut dml_count).unwrap();
        assert_eq!(count, 0);
        assert!(vtable.get_tuples().is_empty());
    }
}

mod tarantool_tests {
    use super::*;
    use tarantool::ffi::sql::Port;

    #[tarantool::test]
    fn test_write_dql_to_port() {
        // { "dql": [ [1,"one"], [2,"two"] ] }
        let buf = b"\x81\xA3dql\x92\x92\x01\xA3one\x92\x02\xA3two";
        let mut port = Port::new_port_c();
        let port_c = unsafe { port.as_mut_port_c() };
        let mut writer = PortWriter::new(port_c);

        let mut dml_count = 0;
        let count = write_from_slice(&mut writer, buf, &mut dml_count).unwrap();
        assert_eq!(count, 2);
        assert_eq!(port_c.size(), 2);

        let slices: Vec<&[u8]> = port_c.iter().collect();
        assert_eq!(slices[0], b"\x92\x01\xA3one");
        assert_eq!(slices[1], b"\x92\x02\xA3two");
    }

    #[tarantool::test]
    fn test_write_dml_to_port() {
        // { "dml": [567] }
        let buf = b"\x81\xA3dml\xCD\x02\x37";
        let mut port = Port::new_port_c();
        let port_c = unsafe { port.as_mut_port_c() };

        let mut writer = PortWriter::new(port_c);
        let mut dml_count = 0;
        let count = write_from_slice(&mut writer, buf, &mut dml_count).unwrap();

        assert_eq!(count, 0);
        assert_eq!(dml_count, 567);

        flush_dml(&mut writer, dml_count).unwrap();
        assert_eq!(port_c.size(), 1);

        let slices: Vec<&[u8]> = port_c.iter().collect();
        assert_eq!(slices[0], b"\x91\xCD\x02\x37");
    }

    #[tarantool::test]
    fn test_write_miss_to_port() {
        // { "miss": nil }
        let buf = b"\x81\xA4miss\xC0";
        let mut port = Port::new_port_c();
        let port_c = unsafe { port.as_mut_port_c() };

        let mut writer = PortWriter::new(port_c);
        let mut dml_count = 0;
        let count = write_from_slice(&mut writer, buf, &mut dml_count).unwrap();

        assert_eq!(count, 0);
        assert_eq!(port_c.size(), 0);
    }
}

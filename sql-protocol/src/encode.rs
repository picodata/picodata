use crate::msgpack::ByteCounter;
use crate::query_plan::PlanBlockIter;
use rmp::decode::read_int;
use rmp::encode::{write_array_len, write_map_len, write_str, write_uint};
use std::io::{Cursor, Error as IoError, Result as IoResult, Write};

/// Write sql_dispatch result of a DML query as a msgpack.
pub fn dispatch_write_dml_response<'bytes>(
    writer: &mut impl Write,
    mut port: impl Iterator<Item = &'bytes [u8]>,
) -> IoResult<()> {
    // Take the first msgpack that contains the count of changed rows.
    let count_mp = match port.next() {
        Some(mp) => mp,
        None => {
            return Err(IoError::other(
                "Expected at least one msgpack with changed row count in the port",
            ))
        }
    };

    let count: u64 = read_int(&mut Cursor::new(count_mp))
        .map_err(|e| IoError::other(format!("Failed to decode changed row count: {e}")))?;

    // Write [{"row_count": count}] as a msgpack.
    write_array_len(writer, 1)?;
    write_changed(writer, count)?;
    Ok(())
}

/// Write sql_dispatch result of an EXPLAIN query as a msgpack.
/// The port must contain at least one msgpack with an explained plan.
/// If there are multiple msgpacks, it means that explain has been
/// split into multiple lines.
pub fn dispatch_write_explain_response<'bytes>(
    writer: &mut impl Write,
    tuples: u32,
    port: impl Iterator<Item = &'bytes [u8]>,
) -> IoResult<()> {
    // Write the explained plan msgpack inside a single element array
    // (for iproto compatibility).
    write_array_len(writer, 1)?;

    // Write all the explain lines as an array of strings.
    write_array_len(writer, tuples)?;
    let len = copy_tuples(writer, port)?;
    if len != tuples as usize {
        return Err(IoError::other(format!(
            "Expected {tuples} explain lines, but got {len}",
        )));
    }
    Ok(())
}

pub fn dispatch_write_query_plan_response<'bytes>(
    writer: &mut impl Write,
    port: impl Iterator<Item = &'bytes [u8]>,
) -> IoResult<()> {
    // Write the explained plan msgpack inside a single element array
    // (for iproto compatibility).
    write_array_len(writer, 1)?;

    // As we split each plan block into lines, we have to materialize
    // all blocks first to calculate the amount of entries in msgpack
    // array.
    let blocks: Vec<String> = PlanBlockIter::new(port)
        .map(|b| b.to_string())
        .collect::<Vec<String>>();
    let amount =
        u32::try_from(blocks.iter().flat_map(|s| s.split('\n')).count()).map_err(IoError::other)?;
    write_array_len(writer, amount)?;
    for line in blocks.iter().flat_map(|s| s.split('\n')) {
        write_str(writer, line)?;
    }
    Ok(())
}

/// Write sql_dispatch result of a DQL query as a msgpack.
pub fn dispatch_write_dql_response<'bytes>(
    writer: &mut impl Write,
    tuples: u32,
    mut port: impl Iterator<Item = &'bytes [u8]>,
) -> IoResult<()> {
    // Take the first msgpack that contains metadata
    let meta_mp = match port.next() {
        Some(mp) => mp,
        None => {
            return Err(IoError::other(
                "Expected at least one msgpack with metadata in the port",
            ));
        }
    };

    // Write a single element array.
    write_array_len(writer, 1)?;

    // Write a map with two key-value pairs ("metadata", metadata) and ("rows", array of tuples).
    write_map_len(writer, 2)?;

    // First pair: "metadata" -> metadata
    write_str(writer, "metadata")?;
    writer.write_all(meta_mp)?;

    // Second pair: "rows" -> array of tuples
    write_str(writer, "rows")?;
    write_array_len(writer, tuples)?;
    let len = copy_tuples(writer, port)?;
    if len != tuples as usize {
        return Err(IoError::other(format!(
            "Expected {tuples} tuples, but got {len}",
        )));
    }

    Ok(())
}

/// Write metadata columns as a msgpack.
pub fn write_metadata<'m>(
    writer: &mut impl Write,
    metadata: impl Iterator<Item = (&'m str, &'m str)> + Clone,
    length: u32,
) -> IoResult<()> {
    fn dump<'m>(
        mut writer: &mut impl Write,
        metadata: impl Iterator<Item = (&'m str, &'m str)> + Clone,
        length: u32,
    ) -> IoResult<()> {
        write_array_len(&mut writer, length)?;

        // For each metadata column
        for (column_name, column_type) in metadata {
            // Map with 2 entries ("name" and "type")
            write_map_len(&mut writer, 2)?;

            // "name"
            write_str(&mut writer, "name")?;
            write_str(&mut writer, column_name)?;

            // "type"
            write_str(&mut writer, "type")?;
            write_str(&mut writer, column_type)?;
        }
        Ok(())
    }
    let mut bc = ByteCounter::default();
    dump(&mut bc, metadata.clone(), length)
        .map_err(|e| IoError::other(format!("Failed to count metadata bytes: {e}")))?;
    let mut buf = Vec::with_capacity(bc.bytes());
    dump(&mut buf, metadata, length)
        .map_err(|e| IoError::other(format!("Failed to dump metadata: {e}")))?;

    writer.write_all(&buf)?;
    Ok(())
}

pub fn execute_write_dql_response<'bytes>(
    writer: &mut impl Write,
    length: u32,
    tuples: impl Iterator<Item = &'bytes [u8]>,
) -> Result<(), IoError> {
    writer.write_all(b"\x81")?;
    write_str(writer, "dql")?;

    write_array_len(writer, length)?;
    let count = copy_tuples(writer, tuples)?;
    if count != length as usize {
        return Err(IoError::other(format!(
            "Expected {length} tuples, but got {count}",
        )));
    }

    Ok(())
}

pub fn execute_write_dml_response(writer: &mut impl Write, changed: u64) -> Result<(), IoError> {
    writer.write_all(b"\x81")?;
    write_str(writer, "dml")?;
    write_uint(writer, changed)?;

    Ok(())
}

pub fn execute_write_miss_response(writer: &mut impl Write) -> Result<(), IoError> {
    writer.write_all(b"\x81")?;
    write_str(writer, "miss")?;
    writer.write_all(b"\x00")?;
    Ok(())
}

#[inline]
fn write_changed(writer: &mut impl Write, changed: u64) -> IoResult<()> {
    write_map_len(writer, 1)?;
    write_str(writer, "row_count")?;
    write_uint(writer, changed)?;
    Ok(())
}

/// Copy tuple byte slices from some iterator into a writer.
#[inline]
fn copy_tuples<'bytes>(
    writer: &mut impl Write,
    iter: impl Iterator<Item = &'bytes [u8]>,
) -> IoResult<usize> {
    let mut count = 0;
    for slice in iter {
        writer.write_all(slice)?;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::SqlExecute;

    #[test]
    fn test_dispatch_write_dml_response() {
        let count_mp = b"\xcd\x01\x2C"; // 300 in u16
        let mut buf = Vec::new();
        dispatch_write_dml_response(&mut buf, &mut vec![count_mp.as_ref()].into_iter()).unwrap();
        assert_eq!(buf.as_slice(), b"\x91\x81\xA9row_count\xCD\x01\x2C");
    }

    #[test]
    fn test_dispatch_write_dml_response_no_mp() {
        let mut buf = Vec::new();
        let mut empty = std::iter::empty::<&[u8]>();
        let err = dispatch_write_dml_response(&mut buf, &mut empty).unwrap_err();
        assert!(err
            .to_string()
            .contains("Expected at least one msgpack with changed row count"));
    }

    #[test]
    fn test_write_changed() {
        // row_count = 0
        let mut buf = Vec::new();
        write_changed(&mut buf, 0).unwrap();
        assert_eq!(buf.as_slice(), b"\x81\xA9row_count\x00");

        // row_count = 42
        let mut buf = Vec::new();
        write_changed(&mut buf, 42).unwrap();
        assert_eq!(buf.as_slice(), b"\x81\xA9row_count\x2A");

        // row_count = 4294967296
        let mut buf = Vec::new();
        write_changed(&mut buf, 4294967296).unwrap();
        assert_eq!(
            buf.as_slice(),
            b"\x81\xA9row_count\xcf\x00\x00\x00\x01\x00\x00\x00\x00"
        );
    }

    #[test]
    fn test_dispatch_write_explain_response() {
        let port = vec![b"\xa7explain".as_ref(), b"\xa4plan".as_ref()];
        let mut buf = Vec::new();
        dispatch_write_explain_response(&mut buf, 2, port.into_iter()).unwrap();
        assert_eq!(buf.as_slice(), b"\x91\x92\xa7explain\xa4plan");
    }

    #[test]
    fn test_dispatch_write_dql_response() {
        // Metadata: [("id", "integer"), ("name", "text")]
        // Tuples: [ [1,"one"], [2,"two"] ]
        let metadata = b"\x92\
            \x82\xa4name\xa2id\xa4type\xa7integer\
            \x82\xa4name\xa4name\xa4type\xa4text";
        let tuples = [b"\x92\x01\xA3one".as_ref(), b"\x92\x02\xA3two".as_ref()];
        let mut buf = Vec::new();
        dispatch_write_dql_response(
            &mut buf,
            2,
            &mut vec![metadata, tuples[0], tuples[1]].into_iter(),
        )
        .unwrap();
        assert_eq!(
            buf.as_slice(),
            [
                b"\x91\x82\xa8metadata".as_slice(),
                metadata,
                b"\xa4rows\x92",
                tuples[0],
                tuples[1]
            ]
            .concat(),
        );
    }

    #[test]
    fn test_dispatch_write_dql_response_no_tuples() {
        // Metadata: [("id", "integer"), ("name", "text")]
        let metadata = b"\x92\
            \x82\xa4name\xa2id\xa4type\xa7integer\
            \x82\xa4name\xa4name\xa4type\xa4text";
        let mut buf = Vec::new();
        dispatch_write_dql_response(&mut buf, 0, &mut vec![&metadata[..]].into_iter()).unwrap();
        assert_eq!(
            buf.as_slice(),
            [
                b"\x91\x82\xa8metadata".as_slice(),
                metadata,
                b"\xa4rows\x90"
            ]
            .concat(),
        );
    }

    #[test]
    fn test_dispatch_write_dql_response_no_metadata() {
        let mut buf = Vec::new();
        let mut empty = std::iter::empty::<&[u8]>();
        let err = dispatch_write_dql_response(&mut buf, 2, &mut empty).unwrap_err();
        assert!(err
            .to_string()
            .contains("Expected at least one msgpack with metadata"));
    }

    #[test]
    fn test_write_metadata() {
        let metadata = [("id", "integer"), ("name", "text")];
        let mut buf = Vec::new();
        write_metadata(&mut buf, metadata.iter().cloned(), 2).unwrap();
        assert_eq!(
            buf.as_slice(),
            b"\x92\x82\xa4name\xa2id\xa4type\xa7integer\x82\xa4name\xa4name\xa4type\xa4text"
        );
    }

    #[test]
    fn test_copy_tuples() {
        // { "dql": [ [1,"one"], [2,"two"] ] }
        let buf = b"\x81\xA3dql\x92\x92\x01\xA3one\x92\x02\xA3two";
        let response = crate::decode::execute_read_response(buf).unwrap();
        let SqlExecute::Dql(iter) = response else {
            panic!("Expected DQL response");
        };
        let mut out = Vec::new();
        let count = copy_tuples(&mut out, iter.into_iter().map(|r| r.unwrap())).unwrap();
        assert_eq!(count, 2);
        assert_eq!(out.as_slice(), b"\x92\x01\xA3one\x92\x02\xA3two");
    }

    #[test]
    fn test_execute_write_dql_response() {
        // {
        //     "dql" : [
        //         [5, ["Online", 10], "buz"],
        //         [56, ["Offline", 0], "bar"]
        //     ]
        // }
        let port: Vec<&[u8]> = vec![
            b"\x93\x05\x92\xa6Online\x0a\xa3buz".as_ref(),
            b"\x93\x38\x92\xa7Offline\x00\xa3bar".as_ref(),
        ];
        let mut obuf: Vec<u8> = Vec::new();
        execute_write_dql_response(&mut obuf, port.len() as u32, port.into_iter()).unwrap();
        assert_eq!(
            obuf,
            b"\x81\xa3dql\x92\
            \x93\x05\x92\xa6Online\x0a\xa3buz\
            \x93\x38\x92\xa7Offline\x00\xa3bar"
        );
    }

    #[test]
    fn test_execute_write_dml_response() {
        let mut obuf: Vec<u8> = Vec::new();
        execute_write_dml_response(&mut obuf, 567).unwrap();
        assert_eq!(obuf, b"\x81\xa3dml\xcd\x02\x37");
    }

    #[test]
    fn test_execute_write_miss_response() {
        let mut obuf: Vec<u8> = Vec::new();
        execute_write_miss_response(&mut obuf).unwrap();
        assert_eq!(obuf, b"\x81\xa4miss\x00");
    }
}

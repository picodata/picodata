use std::io::{Read, Seek, SeekFrom};

use byteorder::{BigEndian, ReadBytesExt};

use crate::Result;

pub fn skip_value(cur: &mut (impl Read + Seek)) -> Result<()> {
    use rmp::Marker;

    match rmp::decode::read_marker(cur)? {
        Marker::FixPos(_) | Marker::FixNeg(_) | Marker::Null | Marker::True | Marker::False => {}
        Marker::U8 | Marker::I8 => {
            cur.seek(SeekFrom::Current(1))?;
        }
        Marker::U16 | Marker::I16 => {
            cur.seek(SeekFrom::Current(2))?;
        }
        Marker::U32 | Marker::I32 | Marker::F32 => {
            cur.seek(SeekFrom::Current(4))?;
        }
        Marker::U64 | Marker::I64 | Marker::F64 => {
            cur.seek(SeekFrom::Current(8))?;
        }
        Marker::FixStr(len) => {
            cur.seek(SeekFrom::Current(len as i64))?;
        }
        Marker::Str8 | Marker::Bin8 => {
            let len = cur.read_u8()?;
            cur.seek(SeekFrom::Current(len as i64))?;
        }
        Marker::Str16 | Marker::Bin16 => {
            let len = cur.read_u16::<BigEndian>()?;
            cur.seek(SeekFrom::Current(len as i64))?;
        }
        Marker::Str32 | Marker::Bin32 => {
            let len = cur.read_u32::<BigEndian>()?;
            cur.seek(SeekFrom::Current(len as i64))?;
        }
        Marker::FixArray(len) => {
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::Array16 => {
            let len = cur.read_u16::<BigEndian>()?;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::Array32 => {
            let len = cur.read_u32::<BigEndian>()?;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::FixMap(len) => {
            let len = len * 2;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::Map16 => {
            let len = cur.read_u16::<BigEndian>()? * 2;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::Map32 => {
            let len = cur.read_u32::<BigEndian>()? * 2;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::FixExt1 => {
            cur.seek(SeekFrom::Current(2))?;
        }
        Marker::FixExt2 => {
            cur.seek(SeekFrom::Current(3))?;
        }
        Marker::FixExt4 => {
            cur.seek(SeekFrom::Current(5))?;
        }
        Marker::FixExt8 => {
            cur.seek(SeekFrom::Current(9))?;
        }
        Marker::FixExt16 => {
            cur.seek(SeekFrom::Current(17))?;
        }
        Marker::Ext8 => {
            let len = cur.read_u8()?;
            cur.seek(SeekFrom::Current(len as i64 + 1))?;
        }
        Marker::Ext16 => {
            let len = cur.read_u16::<BigEndian>()?;
            cur.seek(SeekFrom::Current(len as i64 + 1))?;
        }
        Marker::Ext32 => {
            let len = cur.read_u32::<BigEndian>()?;
            cur.seek(SeekFrom::Current(len as i64 + 1))?;
        }
        Marker::Reserved => {
            return Err(rmp::decode::ValueReadError::TypeMismatch(Marker::Reserved).into())
        }
    }
    Ok(())
}


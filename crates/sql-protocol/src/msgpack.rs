use rmp::decode::{RmpRead, ValueReadError};
use rmp::Marker;
use std::io::{Cursor, Error, Result, Write};

#[derive(Default)]
pub struct ByteCounter(usize);

impl ByteCounter {
    pub fn bytes(&self) -> usize {
        self.0
    }
}

impl Write for ByteCounter {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

pub fn skip_value(rd: &mut Cursor<&[u8]>) -> Result<()> {
    let marker = rmp::decode::read_marker(rd).map_err(|e| {
        Error::other(format!(
            "unsuccesfull marker read: {}",
            ValueReadError::from(e)
        ))
    })?;

    match marker {
        Marker::Reserved
        | Marker::Null
        | Marker::True
        | Marker::False
        | Marker::FixPos(_)
        | Marker::FixNeg(_) => {}
        Marker::U8 | Marker::I8 => shift_pos(rd, 1)?,
        Marker::U16 | Marker::I16 => shift_pos(rd, 2)?,
        Marker::U32 | Marker::I32 => shift_pos(rd, 4)?,
        Marker::U64 | Marker::I64 => shift_pos(rd, 8)?,
        Marker::F32 => shift_pos(rd, 4)?,
        Marker::F64 => shift_pos(rd, 8)?,
        Marker::FixStr(len) => shift_pos(rd, len as u64)?,
        Marker::Str8 => {
            let len = rd
                .read_data_u8()
                .map_err(|e| Error::other(format!("u8 expected: {e}")))?;
            shift_pos(rd, len as u64)?;
        }
        Marker::Str16 => {
            let len = rd
                .read_data_u16()
                .map_err(|e| Error::other(format!("u16 expected: {e}")))?;
            shift_pos(rd, len as u64)?;
        }
        Marker::Str32 => {
            let len = rd
                .read_data_u32()
                .map_err(|e| Error::other(format!("u32 expected: {e}")))?;
            shift_pos(rd, len as u64)?;
        }
        Marker::FixArray(len) => {
            shift_array_data(rd, len as u64)?;
        }
        Marker::Array16 => {
            let len = rd
                .read_data_u16()
                .map_err(|e| Error::other(format!("u16 expected: {e}")))?;
            shift_array_data(rd, len as u64)?;
        }
        Marker::Array32 => {
            let len = rd
                .read_data_u32()
                .map_err(|e| Error::other(format!("u32 expected: {e}")))?;
            shift_array_data(rd, len as u64)?;
        }
        Marker::FixMap(len) => shift_map_data(rd, len as u64)?,
        Marker::Map16 => {
            let len = rd
                .read_data_u16()
                .map_err(|e| Error::other(format!("u16 expected: {e}")))?;
            shift_map_data(rd, len as u64)?;
        }
        Marker::Map32 => {
            let len = rd
                .read_data_u32()
                .map_err(|e| Error::other(format!("u32 expected: {e}")))?;
            shift_map_data(rd, len as u64)?;
        }
        Marker::Bin8 => {
            let len = rd
                .read_data_u8()
                .map_err(|e| Error::other(format!("u8 expected: {e}")))?;
            shift_pos(rd, len as u64)?;
        }
        Marker::Bin16 => {
            let len = rd
                .read_data_u16()
                .map_err(|e| Error::other(format!("u16 expected: {e}")))?;
            shift_pos(rd, len as u64)?;
        }
        Marker::Bin32 => {
            let len = rd
                .read_data_u32()
                .map_err(|e| Error::other(format!("u32 expected: {e}")))?;
            shift_pos(rd, len as u64)?;
        }
        Marker::FixExt1 => shift_pos(rd, 2)?,
        Marker::FixExt2 => shift_pos(rd, 3)?,
        Marker::FixExt4 => shift_pos(rd, 5)?,
        Marker::FixExt8 => shift_pos(rd, 9)?,
        Marker::FixExt16 => shift_pos(rd, 17)?,
        Marker::Ext8 => {
            let len =
                rd.read_data_u8()
                    .map_err(|e| Error::other(format!("u8 expected: {e}")))? as u64;
            shift_pos(rd, len + 1)?;
        }
        Marker::Ext16 => {
            let len = rd
                .read_data_u16()
                .map_err(|e| Error::other(format!("u16 expected: {e}")))?
                as u64;
            shift_pos(rd, len + 1)?;
        }
        Marker::Ext32 => {
            let len = rd
                .read_data_u32()
                .map_err(|e| Error::other(format!("u32 expected: {e}")))?
                as u64;
            shift_pos(rd, len + 1)?;
        }
    };

    Ok(())
}

#[inline]
pub(crate) fn shift_pos(rd: &mut Cursor<&[u8]>, len: u64) -> Result<()> {
    let pos = rd.position();
    let slice_len = rd.get_ref().len() as u64;
    if len + pos > slice_len {
        return Err(Error::other("out of bound attempt to shift position"));
    }

    rd.set_position(pos + len);

    Ok(())
}

fn shift_map_data(rd: &mut Cursor<&[u8]>, mut len: u64) -> Result<()> {
    while len > 0 {
        skip_value(rd)?;
        skip_value(rd)?;
        len -= 1;
    }

    Ok(())
}

fn shift_array_data(rd: &mut Cursor<&[u8]>, mut len: u64) -> Result<()> {
    while len > 0 {
        skip_value(rd)?;
        len -= 1;
    }

    Ok(())
}

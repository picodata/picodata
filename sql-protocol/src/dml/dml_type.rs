use crate::message_type::write_request_header;
use crate::message_type::MessageType::{LocalDML, DML};

#[repr(u8)]
pub(crate) enum DMLType {
    Insert = 0,
    Update,
    Delete,
}

impl TryFrom<u8> for DMLType {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(DMLType::Insert),
            1 => Ok(DMLType::Update),
            2 => Ok(DMLType::Delete),
            _ => Err(format!("Unknown DML type: {value}")),
        }
    }
}

pub(crate) fn write_dml_header(
    w: &mut impl std::io::Write,
    dml_type: DMLType,
    request_id: &str,
) -> Result<(), std::io::Error> {
    write_request_header(w, DML, request_id)?;
    rmp::encode::write_array_len(w, 2)?;
    rmp::encode::write_pfix(w, dml_type as u8)?;

    Ok(())
}

pub(crate) fn write_dml_with_sql_header(
    w: &mut impl std::io::Write,
    dml_type: DMLType,
    request_id: &str,
) -> Result<(), std::io::Error> {
    write_request_header(w, LocalDML, request_id)?;
    rmp::encode::write_array_len(w, 2)?;
    rmp::encode::write_pfix(w, dml_type as u8)?;

    Ok(())
}

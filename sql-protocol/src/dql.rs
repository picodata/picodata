use crate::dql_encoder::{
    ApplySpec, ColumnType, DQLCacheMissDataSource, DQLDataSource, DQLOptions, MsgpackEncode,
};
use crate::error::ProtocolError;
use crate::iterators::{MsgpackMapIterator, TupleIterator};
use crate::message_type::write_request_header;
use crate::message_type::MessageType::DQL;
use crate::msgpack::{skip_value, ByteCounter};
use rmp::decode::{read_array_len, read_bin_len, read_int, read_map_len, read_pfix, read_str_len};
use rmp::encode::{write_array_len, write_bin_len, write_map_len, write_pfix, write_str, write_uint};
use sql_dynfilter::{DynamicFilter, NullPolicy};
use std::fmt;
use std::fmt::Formatter;
use std::io::{Cursor, Write};
use std::str::from_utf8;

pub fn write_dql_packet(
    w: &mut impl Write,
    data: &impl DQLDataSource,
) -> Result<(), std::io::Error> {
    write_request_header(w, DQL, data.get_request_id())?;

    write_dql_packet_data(w, data)?;

    Ok(())
}

pub(crate) fn write_dql_packet_data(
    w: &mut impl Write,
    data: &impl DQLDataSource,
) -> Result<(), std::io::Error> {
    let with_filters = data.dynamic_filters_enabled();
    let field_count = if with_filters {
        DQL_PACKET_FIELD_COUNT_WITH_FILTERS
    } else {
        DQL_PACKET_FIELD_COUNT
    };
    write_array_len(w, field_count as u32)?;

    write_schema_info(w, data.get_table_schema_info())?;
    write_index_schema_info(w, data.get_index_schema_info())?;

    write_plan_id(w, data.get_plan_id())?;

    let sender_id = data.get_sender_id();
    write_sender_id(w, sender_id)?;

    write_vtables(w, data.get_vtables())?;

    write_options(w, data.get_options())?;

    let params = data.get_params();
    write_params(w, params)?;

    if with_filters {
        write_dynamic_filters(w, data.get_dynamic_filters())?;
    }

    Ok(())
}

/// Wire layout for the 8th DQL field: `map<u32 filter_id, array<3>{ bin
/// filter_bytes, array<u32> key_positions, u8 null_policy }>`. The
/// `ApplySpec` (positions + null_policy) rides alongside filter bytes
/// because the storage executes a cached `SqlStmt` that does not carry
/// the runtime-only `ApplyFilter` IR node — the wire packet is the
/// storage's only source for those positions.
pub(crate) fn write_dynamic_filters<'a>(
    w: &mut impl Write,
    filters: impl ExactSizeIterator<Item = (u32, &'a DynamicFilter, &'a ApplySpec)>,
) -> Result<(), std::io::Error> {
    write_map_len(w, filters.len() as u32)?;
    for (id, filter, spec) in filters {
        write_uint(w, id as u64)?;
        write_array_len(w, 3)?;
        write_bin_len(w, filter.encoded_len() as u32)?;
        filter.encode_into(w)?;
        write_array_len(w, spec.key_positions.len() as u32)?;
        for pos in &spec.key_positions {
            write_uint(w, *pos as u64)?;
        }
        write_pfix(w, spec.null_policy as u8)?;
    }
    Ok(())
}

pub(crate) fn write_schema_info(
    w: &mut impl Write,
    schema_info: impl ExactSizeIterator<Item = (u32, u64)>,
) -> Result<(), std::io::Error> {
    write_map_len(w, schema_info.len() as u32)?;
    for (key, value) in schema_info {
        write_uint(w, key as u64)?;
        write_uint(w, value)?;
    }

    Ok(())
}

pub(crate) fn write_index_schema_info(
    w: &mut impl Write,
    schema_info: impl ExactSizeIterator<Item = ([u32; 2], u64)>,
) -> Result<(), std::io::Error> {
    write_map_len(w, schema_info.len() as u32)?;
    for (key, value) in schema_info {
        write_array_len(w, 2)?;
        write_uint(w, key[0] as u64)?;
        write_uint(w, key[1] as u64)?;
        write_uint(w, value)?;
    }

    Ok(())
}

pub(crate) fn write_plan_id(w: &mut impl Write, plan_id: u64) -> Result<(), std::io::Error> {
    rmp::encode::write_u64(w, plan_id).map_err(std::io::Error::from)
}

pub(crate) fn write_sender_id(w: &mut impl Write, sender_id: u64) -> Result<(), std::io::Error> {
    write_uint(w, sender_id)
        .map(|_| ())
        .map_err(std::io::Error::from)
}
pub(crate) fn write_vtables<'a>(
    w: &mut impl Write,
    vtables: impl ExactSizeIterator<Item = (&'a str, impl ExactSizeIterator<Item = impl MsgpackEncode>)>,
) -> Result<(), std::io::Error> {
    write_map_len(w, vtables.len() as u32)?;

    for (key, tuples) in vtables {
        write_str(w, key)?;
        write_tuples(w, tuples)?;
    }

    Ok(())
}
pub(crate) fn write_tuples(
    w: &mut impl Write,
    tuples: impl ExactSizeIterator<Item = impl MsgpackEncode>,
) -> Result<(), std::io::Error> {
    write_array_len(w, tuples.len() as u32)?;
    for tuple in tuples {
        let mut tuple_counter = ByteCounter::default();
        tuple.encode_into(&mut tuple_counter)?;
        rmp::encode::write_bin_len(w, tuple_counter.bytes() as u32)?;
        tuple.encode_into(w)?;
    }

    Ok(())
}

pub(crate) fn write_options(w: &mut impl Write, options: DQLOptions) -> Result<(), std::io::Error> {
    write_array_len(w, 2)?;
    write_uint(w, options.sql_motion_row_max)?;
    write_uint(w, options.sql_vdbe_opcode_max)?;

    Ok(())
}
pub(crate) fn write_params(
    w: &mut impl Write,
    params: impl MsgpackEncode,
) -> Result<(), std::io::Error> {
    params.encode_into(w)?;

    Ok(())
}

#[derive(PartialEq, Debug, Copy, Clone)]
#[repr(u8)]
enum DQLState {
    TableSchemaInfo = 0,
    IndexSchemaInfo,
    PlanId,
    SenderId,
    Vtables,
    Options,
    Params,
    Filters,
    End,
}

pub enum DQLResult<'a> {
    TableSchemaInfo(MsgpackMapIterator<'a, u32, u64>),
    IndexSchemaInfo(MsgpackMapIterator<'a, [u32; 2], u64>),
    PlanId(u64),
    SenderId(u64),
    Vtables(MsgpackMapIterator<'a, &'a str, TupleIterator<'a>>),
    Options(DQLOptions),
    Params(&'a [u8]),
    /// Map of `filter_id (u32) -> encoded filter bytes`. Yielded only
    /// when the packet has the 8-field layout. Each `&[u8]` borrows
    /// directly from the wire buffer; the consumer feeds it to
    /// `sql_dynfilter::FilterView::decode`.
    DynamicFilters(MsgpackMapIterator<'a, u32, WireDynamicFilter<'a>>),
}

impl fmt::Display for DQLResult<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DQLResult::IndexSchemaInfo(_) => f.write_str("IndexSchemaInfo"),
            DQLResult::TableSchemaInfo(_) => f.write_str("TableSchemaInfo"),
            DQLResult::PlanId(_) => f.write_str("PlanId"),
            DQLResult::SenderId(_) => f.write_str("SenderId"),
            DQLResult::Vtables(_) => f.write_str("Vtables"),
            DQLResult::Options(_) => f.write_str("Options"),
            DQLResult::Params(_) => f.write_str("Params"),
            DQLResult::DynamicFilters(_) => f.write_str("DynamicFilters"),
        }
    }
}

const DQL_PACKET_FIELD_COUNT: usize = 7;
const DQL_PACKET_FIELD_COUNT_WITH_FILTERS: usize = 8;
pub struct DQLPacketPayloadIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: DQLState,
    has_filters: bool,
}

impl<'a> DQLPacketPayloadIterator<'a> {
    pub fn new(raw_payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(raw_payload);

        let l = read_array_len(&mut cursor)?;
        let has_filters = match l as usize {
            DQL_PACKET_FIELD_COUNT => false,
            DQL_PACKET_FIELD_COUNT_WITH_FILTERS => true,
            _ => {
                return Err(ProtocolError::DecodeError(format!(
                    "DQL package is invalid: expected package array length {DQL_PACKET_FIELD_COUNT} or {DQL_PACKET_FIELD_COUNT_WITH_FILTERS}, got {l}"
                )));
            }
        };

        Ok(Self {
            raw_payload: cursor,
            state: DQLState::TableSchemaInfo,
            has_filters,
        })
    }

    fn get_table_schema_info(&mut self) -> Result<MsgpackMapIterator<'a, u32, u64>, ProtocolError> {
        debug_assert_eq!(self.state, DQLState::TableSchemaInfo);
        let schema_info = get_schema_info(&mut self.raw_payload)?;
        self.state = DQLState::IndexSchemaInfo;

        Ok(schema_info)
    }

    fn get_index_schema_info(
        &mut self,
    ) -> Result<MsgpackMapIterator<'a, [u32; 2], u64>, ProtocolError> {
        debug_assert_eq!(self.state, DQLState::IndexSchemaInfo);
        let schema_info = get_index_schema_info(&mut self.raw_payload)?;
        self.state = DQLState::PlanId;

        Ok(schema_info)
    }

    fn get_plan_id(&mut self) -> Result<u64, ProtocolError> {
        debug_assert_eq!(self.state, DQLState::PlanId);
        let plan_id = get_plan_id(&mut self.raw_payload)?;
        self.state = DQLState::SenderId;
        Ok(plan_id)
    }

    fn get_sender_id(&mut self) -> Result<u64, ProtocolError> {
        debug_assert_eq!(self.state, DQLState::SenderId);
        let sender_id = get_sender_id(&mut self.raw_payload)?;
        self.state = DQLState::Vtables;
        Ok(sender_id)
    }

    fn get_vtables(
        &mut self,
    ) -> Result<MsgpackMapIterator<'a, &'a str, TupleIterator<'a>>, ProtocolError> {
        debug_assert_eq!(self.state, DQLState::Vtables);
        let vtables = get_vtables(&mut self.raw_payload)?;
        self.state = DQLState::Options;
        Ok(vtables)
    }

    fn get_options(&mut self) -> Result<DQLOptions, ProtocolError> {
        debug_assert_eq!(self.state, DQLState::Options);
        let options = get_options(&mut self.raw_payload)?;
        self.state = DQLState::Params;
        Ok(options)
    }

    fn get_params(&mut self) -> Result<&'a [u8], ProtocolError> {
        debug_assert_eq!(self.state, DQLState::Params);
        let params = get_params(&mut self.raw_payload)?;
        self.state = if self.has_filters {
            DQLState::Filters
        } else {
            DQLState::End
        };
        Ok(params)
    }

    fn get_filters(
        &mut self,
    ) -> Result<MsgpackMapIterator<'a, u32, WireDynamicFilter<'a>>, ProtocolError> {
        debug_assert_eq!(self.state, DQLState::Filters);
        let filters = get_filters(&mut self.raw_payload)?;
        self.state = DQLState::End;
        Ok(filters)
    }
}

pub(crate) fn get_schema_info<'a>(
    raw_payload: &mut Cursor<&'a [u8]>,
) -> Result<MsgpackMapIterator<'a, u32, u64>, ProtocolError> {
    let l = read_map_len(raw_payload)?;

    let start = raw_payload.position() as usize;
    for _ in 0..l * 2 {
        skip_value(raw_payload).map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
    }
    let end = raw_payload.position() as usize;

    Ok(MsgpackMapIterator::new(
        &raw_payload.get_ref()[start..end],
        l,
        |r| read_int(r).map_err(|err| ProtocolError::DecodeError(err.to_string())),
        |r| read_int(r).map_err(|err| ProtocolError::DecodeError(err.to_string())),
    ))
}

pub(crate) fn get_index_schema_info<'a>(
    raw_payload: &mut Cursor<&'a [u8]>,
) -> Result<MsgpackMapIterator<'a, [u32; 2], u64>, ProtocolError> {
    let l = read_map_len(raw_payload)?;

    let start = raw_payload.position() as usize;

    // Skip all keys and values from map.
    for _ in 0..l * 2 {
        skip_value(raw_payload).map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
    }
    let end = raw_payload.position() as usize;

    let index_schema_decoder = |r: &mut Cursor<&'a [u8]>| -> Result<[u32; 2], ProtocolError> {
        let l =
            read_array_len(r).map_err(|err| ProtocolError::DecodeError(err.to_string()))? as usize;
        debug_assert_eq!(l, 2);
        let table_id = read_int(r).map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        let index_id = read_int(r).map_err(|err| ProtocolError::DecodeError(err.to_string()))?;

        let pk = [table_id, index_id];
        Ok(pk)
    };

    Ok(MsgpackMapIterator::new(
        &raw_payload.get_ref()[start..end],
        l,
        index_schema_decoder,
        |r| read_int(r).map_err(|err| ProtocolError::DecodeError(err.to_string())),
    ))
}

pub(crate) fn get_plan_id(raw_payload: &mut Cursor<&[u8]>) -> Result<u64, ProtocolError> {
    let plan_id = rmp::decode::read_u64(raw_payload)?;
    Ok(plan_id)
}

pub(crate) fn get_sender_id(raw_payload: &mut Cursor<&[u8]>) -> Result<u64, ProtocolError> {
    let sender_id = read_int(raw_payload)?;
    Ok(sender_id)
}

pub(crate) fn get_vtables<'a>(
    raw_payload: &mut Cursor<&'a [u8]>,
) -> Result<MsgpackMapIterator<'a, &'a str, TupleIterator<'a>>, ProtocolError> {
    let l = read_map_len(raw_payload)?;
    let start = raw_payload.position() as usize;
    for _ in 0..l * 2 {
        skip_value(raw_payload).map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
    }
    let end = raw_payload.position() as usize;

    let table_name_decoder = |r: &mut Cursor<&'a [u8]>| -> Result<&'a str, ProtocolError> {
        let l = read_str_len(r)?;
        let start = r.position() as usize;
        let end = start + l as usize;
        let vtable_name = from_utf8(&r.get_ref()[start..end])
            .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        r.set_position(end as u64);
        Ok(vtable_name)
    };

    let tuple_iterator_decoder =
        |r: &mut Cursor<&'a [u8]>| -> Result<TupleIterator<'a>, ProtocolError> {
            let l = read_array_len(r)? as usize;
            let start = r.position() as usize;
            for _ in 0..l {
                skip_value(r).map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
            }
            let end = r.position() as usize;

            Ok(TupleIterator::new(&r.get_ref()[start..end], l))
        };

    Ok(MsgpackMapIterator::new(
        &raw_payload.get_ref()[start..end],
        l,
        table_name_decoder,
        tuple_iterator_decoder,
    ))
}

pub(crate) fn get_options(raw_payload: &mut Cursor<&[u8]>) -> Result<DQLOptions, ProtocolError> {
    let options = read_array_len(raw_payload)?;
    let (sql_motion_row_max, sql_vdbe_opcode_max) = match options {
        2 => {
            let sql_motion_row_max = read_int(raw_payload)?;
            let sql_vdbe_opcode_max = read_int(raw_payload)?;
            (sql_motion_row_max, sql_vdbe_opcode_max)
        }
        _ => {
            return Err(ProtocolError::DecodeError(format!(
                "DQL package is invalid: expected to have options array length 2, got {options}"
            )));
        }
    };
    Ok(DQLOptions {
        sql_motion_row_max,
        sql_vdbe_opcode_max,
    })
}

pub(crate) fn get_params<'a>(
    raw_payload: &mut Cursor<&'a [u8]>,
) -> Result<&'a [u8], ProtocolError> {
    let start = raw_payload.position() as usize;
    skip_value(raw_payload).map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
    let end = raw_payload.position() as usize;
    Ok(&raw_payload.get_ref()[start..end])
}

/// Decoded payload of one dynamic-filter map entry. Borrowed against the
/// wire buffer — `bytes` feeds `sql_dynfilter::FilterView::decode`, the
/// rest goes into `key_hash` on the storage side.
#[derive(Debug, Clone)]
pub struct WireDynamicFilter<'a> {
    pub bytes: &'a [u8],
    pub key_positions: Vec<u32>,
    pub null_policy: NullPolicy,
}

pub(crate) fn get_filters<'a>(
    raw_payload: &mut Cursor<&'a [u8]>,
) -> Result<MsgpackMapIterator<'a, u32, WireDynamicFilter<'a>>, ProtocolError> {
    let l = read_map_len(raw_payload)?;
    let start = raw_payload.position() as usize;
    for _ in 0..l * 2 {
        skip_value(raw_payload).map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
    }
    let end = raw_payload.position() as usize;

    let key_decoder = |r: &mut Cursor<&'a [u8]>| -> Result<u32, ProtocolError> {
        read_int::<u32, _>(r).map_err(|err| ProtocolError::DecodeError(err.to_string()))
    };

    let value_decoder = |r: &mut Cursor<&'a [u8]>| -> Result<WireDynamicFilter<'a>, ProtocolError> {
        let arr_len = read_array_len(r)
            .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        if arr_len != 3 {
            return Err(ProtocolError::DecodeError(format!(
                "DynamicFilter entry: expected array<3>, got array<{arr_len}>"
            )));
        }
        let bin_len = read_bin_len(r)? as usize;
        let bytes_start = r.position() as usize;
        let bytes_end = bytes_start + bin_len;
        r.set_position(bytes_end as u64);
        let bytes = &r.get_ref()[bytes_start..bytes_end];

        let pos_len = read_array_len(r)
            .map_err(|err| ProtocolError::DecodeError(err.to_string()))? as usize;
        let mut key_positions = Vec::with_capacity(pos_len);
        for _ in 0..pos_len {
            key_positions.push(
                read_int::<u32, _>(r)
                    .map_err(|err| ProtocolError::DecodeError(err.to_string()))?,
            );
        }
        let np_tag = read_pfix(r)
            .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        let null_policy = NullPolicy::try_from(np_tag)
            .map_err(|_| ProtocolError::DecodeError(format!("invalid NullPolicy tag {np_tag}")))?;
        Ok(WireDynamicFilter {
            bytes,
            key_positions,
            null_policy,
        })
    };

    Ok(MsgpackMapIterator::new(
        &raw_payload.get_ref()[start..end],
        l,
        key_decoder,
        value_decoder,
    ))
}

impl<'a> Iterator for DQLPacketPayloadIterator<'a> {
    type Item = Result<DQLResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            DQLState::TableSchemaInfo => {
                Some(self.get_table_schema_info().map(DQLResult::TableSchemaInfo))
            }
            DQLState::IndexSchemaInfo => {
                Some(self.get_index_schema_info().map(DQLResult::IndexSchemaInfo))
            }
            DQLState::PlanId => Some(self.get_plan_id().map(DQLResult::PlanId)),
            DQLState::SenderId => Some(self.get_sender_id().map(DQLResult::SenderId)),
            DQLState::Vtables => Some(self.get_vtables().map(DQLResult::Vtables)),
            DQLState::Options => Some(self.get_options().map(DQLResult::Options)),
            DQLState::Params => Some(self.get_params().map(DQLResult::Params)),
            DQLState::Filters => Some(self.get_filters().map(DQLResult::DynamicFilters)),
            DQLState::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let total = if self.has_filters {
            DQL_PACKET_FIELD_COUNT_WITH_FILTERS
        } else {
            DQL_PACKET_FIELD_COUNT
        };
        let size = total.saturating_sub(self.state as usize);
        (size, Some(size))
    }
}

pub fn write_dql_cache_miss_packet(
    w: &mut impl Write,
    data: &impl DQLCacheMissDataSource,
) -> Result<(), std::io::Error> {
    write_array_len(w, DQL_CACHE_MISS_PACKET_FIELD_COUNT as u32)?;

    let table_schema_info = data.get_table_schema_info();
    write_schema_info(w, table_schema_info)?;

    let index_schema_info = data.get_index_schema_info();
    write_index_schema_info(w, index_schema_info)?;

    let vtables_metadata = data.get_vtables_metadata();
    write_map_len(w, vtables_metadata.len() as u32)?;
    for (key, columns) in vtables_metadata {
        write_str(w, key)?;
        write_array_len(w, columns.len() as u32)?;
        for (column, ty) in columns {
            write_array_len(w, 2)?;
            write_str(w, column)?;
            rmp::encode::write_pfix(w, ty as u8)?;
        }
    }

    let sql = data.get_sql();
    write_str(w, sql)?;

    Ok(())
}

#[derive(PartialEq, Debug, Copy, Clone)]
#[repr(u8)]
enum DQLCacheMissState {
    TableSchemaInfo = 0,
    IndexSchemaInfo,
    VtablesMetadata,
    Sql,
    End,
}

pub enum DQLCacheMissResult<'a> {
    TableSchemaInfo(MsgpackMapIterator<'a, u32, u64>),
    IndexSchemaInfo(MsgpackMapIterator<'a, [u32; 2], u64>),
    VtablesMetadata(MsgpackMapIterator<'a, &'a str, Vec<(&'a str, ColumnType)>>),
    Sql(&'a str),
}

impl fmt::Display for DQLCacheMissResult<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DQLCacheMissResult::TableSchemaInfo(_) => f.write_str("TableSchemaInfo"),
            DQLCacheMissResult::IndexSchemaInfo(_) => f.write_str("IndexSchemaInfo"),
            DQLCacheMissResult::VtablesMetadata(_) => f.write_str("VtablesMetadata"),
            DQLCacheMissResult::Sql(_) => f.write_str("Sql"),
        }
    }
}

const DQL_CACHE_MISS_PACKET_FIELD_COUNT: usize = 4;
pub struct DQLCacheMissPayloadIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: DQLCacheMissState,
}

impl<'a> DQLCacheMissPayloadIterator<'a> {
    pub fn new(raw_payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(raw_payload);

        let l = read_array_len(&mut cursor)?;
        if l != DQL_CACHE_MISS_PACKET_FIELD_COUNT as u32 {
            return Err(ProtocolError::DecodeError(format!(
                "DQL package is invalid: expected to have package array length {DQL_CACHE_MISS_PACKET_FIELD_COUNT}, got {l}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: DQLCacheMissState::TableSchemaInfo,
        })
    }

    fn get_table_schema_info(&mut self) -> Result<MsgpackMapIterator<'a, u32, u64>, ProtocolError> {
        debug_assert_eq!(self.state, DQLCacheMissState::TableSchemaInfo);
        let schema_info = get_schema_info(&mut self.raw_payload)?;
        self.state = DQLCacheMissState::IndexSchemaInfo;
        Ok(schema_info)
    }

    fn get_index_schema_info(
        &mut self,
    ) -> Result<MsgpackMapIterator<'a, [u32; 2], u64>, ProtocolError> {
        debug_assert_eq!(self.state, DQLCacheMissState::IndexSchemaInfo);
        let schema_info = get_index_schema_info(&mut self.raw_payload)?;
        self.state = DQLCacheMissState::VtablesMetadata;
        Ok(schema_info)
    }

    #[allow(clippy::type_complexity)]
    fn get_vtables_metadata(
        &mut self,
    ) -> Result<MsgpackMapIterator<'a, &'a str, Vec<(&'a str, ColumnType)>>, ProtocolError> {
        debug_assert_eq!(self.state, DQLCacheMissState::VtablesMetadata);
        let l = read_map_len(&mut self.raw_payload)?;
        let start = self.raw_payload.position() as usize;
        for _ in 0..l * 2 {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        self.state = DQLCacheMissState::Sql;

        let table_name_decoder = |r: &mut Cursor<&'a [u8]>| -> Result<&str, ProtocolError> {
            let l = read_str_len(r)?;
            let start = r.position() as usize;
            let end = start + l as usize;
            let vtable_name = from_utf8(&r.get_ref()[start..end])
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
            r.set_position(end as u64);
            Ok(vtable_name)
        };

        let metadata_decoder =
            |r: &mut Cursor<&'a [u8]>| -> Result<Vec<(&str, ColumnType)>, ProtocolError> {
                let l = read_array_len(r)? as usize;
                let mut res = Vec::with_capacity(l);
                for _ in 0..l {
                    let l = read_array_len(r)? as usize;
                    if l != 2 {
                        return Err(ProtocolError::DecodeError(format!(
                            "DQL Cache Miss package is invalid: expected to have array length 2, got {l}"
                        )));
                    }

                    let l = read_str_len(r)?;
                    let start = r.position() as usize;
                    let end = start + l as usize;
                    let column_name = from_utf8(&r.get_ref()[start..end])
                        .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
                    r.set_position(end as u64);

                    let ct = rmp::decode::read_pfix(r)?;
                    let ct = ColumnType::try_from(ct)
                        .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;

                    res.push((column_name, ct));
                }

                Ok(res)
            };

        Ok(MsgpackMapIterator::new(
            &self.raw_payload.get_ref()[start..end],
            l,
            table_name_decoder,
            metadata_decoder,
        ))
    }

    fn get_sql(&mut self) -> Result<&'a str, ProtocolError> {
        debug_assert_eq!(self.state, DQLCacheMissState::Sql);
        let l = read_str_len(&mut self.raw_payload)?;
        let start = self.raw_payload.position() as usize;
        let end = start + l as usize;
        let sql = from_utf8(&self.raw_payload.get_ref()[start..end])
            .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        self.raw_payload.set_position(end as u64);
        self.state = DQLCacheMissState::End;
        Ok(sql)
    }
}

impl<'a> Iterator for DQLCacheMissPayloadIterator<'a> {
    type Item = Result<DQLCacheMissResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            DQLCacheMissState::TableSchemaInfo => match self.get_table_schema_info() {
                Ok(schema_info) => Some(Ok(DQLCacheMissResult::TableSchemaInfo(schema_info))),
                Err(err) => Some(Err(err)),
            },
            DQLCacheMissState::IndexSchemaInfo => match self.get_index_schema_info() {
                Ok(schema_info) => Some(Ok(DQLCacheMissResult::IndexSchemaInfo(schema_info))),
                Err(err) => Some(Err(err)),
            },
            DQLCacheMissState::VtablesMetadata => match self.get_vtables_metadata() {
                Ok(vtables_metadata) => {
                    Some(Ok(DQLCacheMissResult::VtablesMetadata(vtables_metadata)))
                }
                Err(err) => Some(Err(err)),
            },
            DQLCacheMissState::Sql => match self.get_sql() {
                Ok(sql) => Some(Ok(DQLCacheMissResult::Sql(sql))),
                Err(err) => Some(Err(err)),
            },
            DQLCacheMissState::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = DQL_CACHE_MISS_PACKET_FIELD_COUNT.saturating_sub(self.state as usize);
        (size, Some(size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dql_encoder::test::TestDQLEncoderBuilder;
    use crate::dql_encoder::ColumnType;

    use std::collections::HashMap;
    use std::str::from_utf8;

    #[test]
    fn test_encode_dql() {
        let data = TestDQLEncoderBuilder::new()
            .set_plan_id(5264743718663535479)
            .set_request_id("14e84334-71df-4e69-8c85-dc2707a390c6".to_string())
            .set_schema_info((HashMap::from([(12, 138)]), HashMap::from([([12, 12], 138)])))
            .set_sender_id(42)
            .set_vtables(HashMap::from([(
                "TMP_1302_".to_string(),
                vec![vec![1, 2, 3], vec![3, 2, 1]],
            )]))
            .set_options(DQLOptions {
                sql_motion_row_max: 123,
                sql_vdbe_opcode_max: 456,
            })
            .set_params(vec![138, 123, 432])
            .build();

        let mut writer = Vec::new();

        write_dql_packet(&mut writer, &data).unwrap();
        let expected: &[u8] = b"\x93\xd9$14e84334-71df-4e69-8c85-dc2707a390c6\x00\x97\x81\x0c\xcc\x8a\x81\x92\x0c\x0c\xcc\x8a\xcfI\x10 \x84\xb0h\xbbw\x2a\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";

        assert_eq!(writer, expected);
    }

    #[test]
    fn test_execute_dql_cache_hit() {
        let mut data: &[u8] = b"\x93\xd9$14e84334-71df-4e69-8c85-dc2707a390c6\x00\x97\x81\x0c\xcc\x8a\x81\x92\x0c\x0c\xcc\x8a\xcfI\x10 \x84\xb0h\xbbw\x2a\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";

        let l = read_array_len(&mut data).unwrap();
        assert_eq!(l, 3);
        let str_len = read_str_len(&mut data).unwrap();
        let (request_id, new_data) = data.split_at(str_len as usize);
        let request_id = from_utf8(request_id).unwrap();
        assert_eq!(request_id, "14e84334-71df-4e69-8c85-dc2707a390c6");
        data = new_data;
        let msg_type = rmp::decode::read_pfix(&mut data).unwrap();
        assert_eq!(msg_type, DQL as u8);

        let package = DQLPacketPayloadIterator::new(data).unwrap();

        for elem in package {
            match elem.unwrap() {
                DQLResult::TableSchemaInfo(schema_info) => {
                    assert_eq!(schema_info.len(), 1);
                    for res in schema_info {
                        let (t_id, version) = res.unwrap();
                        assert_eq!(t_id, 12);
                        assert_eq!(version, 138);
                    }
                }
                DQLResult::IndexSchemaInfo(schema_info) => {
                    assert_eq!(schema_info.len(), 1);
                    for res in schema_info {
                        let (t_id, version) = res.unwrap();
                        assert_eq!(t_id, [12, 12]);
                        assert_eq!(version, 138);
                    }
                }
                DQLResult::PlanId(plan_id) => {
                    assert_eq!(plan_id, 5264743718663535479);
                }
                DQLResult::SenderId(sender_id) => {
                    assert_eq!(sender_id, 42);
                }
                DQLResult::Vtables(vtables) => {
                    for result in vtables {
                        let (name, tuples) = result.unwrap();
                        assert_eq!(name, "TMP_1302_");
                        assert_eq!(tuples.len(), 2);
                        let mut actual = Vec::with_capacity(2);
                        for tuple in tuples {
                            let tuple = tuple.unwrap();
                            actual.push(tuple);
                        }
                        let expected = vec![[148, 1, 2, 3, 0], [148, 3, 2, 1, 1]];
                        assert_eq!(actual, expected);
                    }
                }
                DQLResult::Options(options) => {
                    assert_eq!(options.sql_motion_row_max, 123);
                    assert_eq!(options.sql_vdbe_opcode_max, 456);
                }
                DQLResult::Params(params) => {
                    let expected = vec![147, 204, 138, 123, 205, 1, 176];
                    assert_eq!(params, expected.as_slice());
                }
                DQLResult::DynamicFilters(_) => {
                    unreachable!("legacy 7-field packet has no DynamicFilters")
                }
            }
        }
    }

    #[test]
    fn test_encode_dql_cache_miss() {
        let data = TestDQLEncoderBuilder::new()
            .set_schema_info((HashMap::from([(12, 138)]), HashMap::from([([12, 12], 138)])))
            .set_meta(HashMap::from([(
                "TMP_1302_".to_string(),
                vec![
                    ("a".to_string(), ColumnType::Integer),
                    ("b".to_string(), ColumnType::Integer),
                ],
            )]))
            .set_sql("select * from TMP_1302_;".to_string())
            .build();

        let mut writer = Vec::new();
        write_dql_cache_miss_packet(&mut writer, &data).unwrap();
        let expected: &[u8] = b"\x94\x81\x0c\xcc\x8a\x81\x92\x0c\x0c\xcc\x8a\x81\xa9TMP_1302_\x92\x92\xa1a\x05\x92\xa1b\x05\xb8select * from TMP_1302_;";
        assert_eq!(writer, expected);
    }

    #[test]
    fn test_handle_dql_cache_miss() {
        let data: &[u8] = b"\x94\x81\x0c\xcc\x8a\x81\x92\x0c\x0c\xcc\x8a\x81\xa9TMP_1302_\x92\x92\xa1a\x05\x92\xa1b\x05\xb8select * from TMP_1302_;";

        let package = DQLCacheMissPayloadIterator::new(data).unwrap();

        for elem in package {
            match elem.unwrap() {
                DQLCacheMissResult::TableSchemaInfo(schema_info) => {
                    assert_eq!(schema_info.len(), 1);
                    for res in schema_info {
                        let (t_id, ver) = res.unwrap();
                        assert_eq!(t_id, 12);
                        assert_eq!(ver, 138);
                    }
                }
                DQLCacheMissResult::IndexSchemaInfo(schema_info) => {
                    assert_eq!(schema_info.len(), 1);
                    for res in schema_info {
                        let (t_id, ver) = res.unwrap();
                        assert_eq!(t_id, [12, 12]);
                        assert_eq!(ver, 138);
                    }
                }
                DQLCacheMissResult::VtablesMetadata(vtables_metadata) => {
                    assert_eq!(vtables_metadata.len(), 1);
                    for res in vtables_metadata {
                        let (table_name, columns) = res.unwrap();
                        assert_eq!(table_name, "TMP_1302_");
                        let expected = vec![("a", ColumnType::Integer), ("b", ColumnType::Integer)];
                        assert_eq!(columns, expected);
                    }
                }
                DQLCacheMissResult::Sql(sql) => {
                    assert_eq!(sql, "select * from TMP_1302_;");
                }
            }
        }
    }

    /// 8-field DQL packet round-trip. Confirms that a packet emitted
    /// with non-empty filters decodes back into the same filter map and
    /// that the surrounding fields still parse correctly.
    #[test]
    fn test_dql_packet_with_filters_roundtrip() {
        use crate::dql_encoder::test::TestDQLDataSource;
        use crate::dql_encoder::ApplySpec;
        use sql_dynfilter::{DynamicFilter, NullPolicy};

        struct FilteredDQL {
            inner: TestDQLDataSource,
            filters: Vec<(u32, DynamicFilter, ApplySpec)>,
        }
        impl DQLDataSource for FilteredDQL {
            fn get_table_schema_info(&self) -> impl ExactSizeIterator<Item = (u32, u64)> {
                DQLDataSource::get_table_schema_info(&self.inner)
            }
            fn get_index_schema_info(&self) -> impl ExactSizeIterator<Item = ([u32; 2], u64)> {
                DQLDataSource::get_index_schema_info(&self.inner)
            }
            fn get_plan_id(&self) -> u64 {
                self.inner.get_plan_id()
            }
            fn get_sender_id(&self) -> u64 {
                self.inner.get_sender_id()
            }
            fn get_request_id(&self) -> &str {
                self.inner.get_request_id()
            }
            fn get_vtables(
                &self,
            ) -> impl ExactSizeIterator<
                Item = (&str, impl ExactSizeIterator<Item = impl MsgpackEncode>),
            > {
                self.inner.get_vtables()
            }
            fn get_options(&self) -> DQLOptions {
                self.inner.get_options()
            }
            fn get_params(&self) -> impl MsgpackEncode {
                self.inner.get_params()
            }
            fn dynamic_filters_enabled(&self) -> bool {
                true
            }
            fn get_dynamic_filters(
                &self,
            ) -> impl ExactSizeIterator<Item = (u32, &DynamicFilter, &ApplySpec)> {
                self.filters.iter().map(|(id, f, spec)| (*id, f, spec))
            }
        }

        let inner = TestDQLEncoderBuilder::new()
            .set_plan_id(42)
            .set_request_id("rid-1".to_string())
            .set_schema_info((HashMap::new(), HashMap::new()))
            .set_sender_id(7)
            .set_vtables(HashMap::new())
            .set_options(DQLOptions {
                sql_motion_row_max: 1,
                sql_vdbe_opcode_max: 2,
            })
            .set_params(vec![])
            .build();

        // Build two small finalized filters to encode.
        let mut f1 = DynamicFilter::new(2);
        f1.insert(0x1111_2222_3333_4444u128);
        f1.insert(0x5555_6666_7777_8888u128);
        f1.finalize().unwrap();
        let mut f2 = DynamicFilter::new(1);
        f2.insert(0xCAFEBABE_DEADBEEFu128);
        f2.finalize().unwrap();
        let spec1 = ApplySpec::new(vec![2u32, 5u32], NullPolicy::Skip);
        let spec2 = ApplySpec::new(vec![0u32], NullPolicy::Insert);
        let filters = vec![(7u32, f1, spec1.clone()), (42u32, f2, spec2.clone())];

        let data = FilteredDQL { inner, filters };

        let mut buf = Vec::new();
        write_dql_packet(&mut buf, &data).unwrap();

        // Skip the request header (array_len=3, request_id str, type byte).
        let mut cursor: &[u8] = &buf;
        let l = read_array_len(&mut cursor).unwrap();
        assert_eq!(l, 3);
        let rid_len = read_str_len(&mut cursor).unwrap();
        cursor = &cursor[rid_len as usize..];
        let _msg_type = rmp::decode::read_pfix(&mut cursor).unwrap();

        let pkg = DQLPacketPayloadIterator::new(cursor).unwrap();
        let mut saw_filters = false;
        for elem in pkg {
            if let DQLResult::DynamicFilters(map) = elem.unwrap() {
                saw_filters = true;
                let mut ids: Vec<u32> = Vec::new();
                for entry in map {
                    let (id, wire) = entry.unwrap();
                    // The bytes field must decode as a FilterView.
                    let view = sql_dynfilter::FilterView::decode(wire.bytes).unwrap();
                    if id == 7 {
                        assert!(view.contains(0x1111_2222_3333_4444u128));
                        assert!(view.contains(0x5555_6666_7777_8888u128));
                        assert_eq!(wire.key_positions, vec![2u32, 5u32]);
                        assert_eq!(wire.null_policy, NullPolicy::Skip);
                    } else if id == 42 {
                        assert!(view.contains(0xCAFEBABE_DEADBEEFu128));
                        assert_eq!(wire.key_positions, vec![0u32]);
                        assert_eq!(wire.null_policy, NullPolicy::Insert);
                    } else {
                        panic!("unexpected filter id {id}");
                    }
                    ids.push(id);
                }
                ids.sort();
                assert_eq!(ids, vec![7u32, 42u32]);
            }
        }
        assert!(saw_filters, "iterator did not yield DynamicFilters");
    }

    /// New decoder must still accept a 7-field packet (forward
    /// tolerance for storages that have not been upgraded yet).
    #[test]
    fn test_dql_packet_legacy_7_fields_still_decodes() {
        let data: &[u8] = b"\x97\x81\x0c\xcc\x8a\x81\x92\x0c\x0c\xcc\x8a\xcfI\x10 \x84\xb0h\xbbw\x2a\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";
        let pkg = DQLPacketPayloadIterator::new(data).unwrap();
        let mut count = 0;
        for elem in pkg {
            elem.unwrap();
            count += 1;
        }
        assert_eq!(count, 7);
    }
}

use ::tarantool::ffi::sql::{Obuf, Port, PortC, PortVTable};
use std::io::Write;
use std::os::raw::c_int;
use std::ptr::NonNull;
use tarantool::tlua::ffi::lua_State;

use crate::sql::router_port::ObufWriter;
use rmp::encode::RmpWrite;

#[derive(Debug)]
pub enum ResultType {
    DML,
    DQL,
    MISS,
}

impl ResultType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResultType::DML => "dml",
            ResultType::DQL => "dql",
            ResultType::MISS => "miss",
        }
    }
}

pub fn serialize_res<'tuple>(
    tuple_arr: &mut impl Iterator<Item = &'tuple [u8]>,
    len: u32,
    wr: &mut impl Write,
    result_type: ResultType,
) -> Result<(), tarantool::error::Error> {
    let res_type = result_type.as_str();
    wr.write_bytes(b"\x81")?;
    rmp::encode::write_str(wr, res_type)?;

    if let ResultType::MISS = result_type {
        wr.write_bytes(b"\xC0")?;
        return Ok(());
    }

    if let ResultType::DQL = result_type {
        rmp::encode::write_array_len(wr, len)?;
    }

    for msgpack_tuple in tuple_arr {
        wr.write_bytes(msgpack_tuple)?;
    }

    Ok(())
}

unsafe extern "C" fn dump_dql_msgpack(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    let mut w = ObufWriter(out);

    serialize_res(
        &mut port_c.iter(),
        port_c.size() as u32,
        &mut w,
        ResultType::DQL,
    )
    .expect("dump_dql failed!");

    port_c.size()
}

unsafe extern "C" fn dump_dml_msgpack(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    let mut w = ObufWriter(out);

    serialize_res(
        &mut port_c.iter(),
        port_c.size() as u32,
        &mut w,
        ResultType::DML,
    )
    .expect("dump_dml failed!");

    port_c.size()
}

unsafe extern "C" fn dump_miss_msgpack(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    let mut w = ObufWriter(out);

    serialize_res(
        &mut port_c.iter(),
        port_c.size() as u32,
        &mut w,
        ResultType::MISS,
    )
    .expect("dump_miss failed!");

    port_c.size()
}

/// # Safety
#[no_mangle]
unsafe extern "C" fn dump_storage_lua(_port: *mut Port, _l: *mut lua_State, _is_flat: bool) {
    unimplemented!();
}

pub static DQL_VTAB: PortVTable = PortVTable::new(dump_dql_msgpack, dump_storage_lua);
pub static DML_VTAB: PortVTable = PortVTable::new(dump_dml_msgpack, dump_storage_lua);
pub static MISS_VTAB: PortVTable = PortVTable::new(dump_miss_msgpack, dump_storage_lua);

#[cfg(test)]
mod tests {
    use crate::sql::storage_port::serialize_res;

    #[test]
    fn test_serialize_dql() {
        // {
        //     "dql" : [
        //         [5, ["Online", 10], "buz"],
        //         [56, ["Offline", 0], "bar"]
        //     ]
        // }
        let tuple_arr: Vec<&[u8]> = vec![
            b"\x93\x05\x92\xA6Online\x0A\xA3buz",
            b"\x93\x38\x92\xA7Offline\x00\xA3bar",
        ];
        let mut iter = tuple_arr.iter().copied();
        let mut wr: Vec<u8> = Vec::new();
        serialize_res(
            &mut iter,
            tuple_arr.len() as u32,
            &mut wr,
            super::ResultType::DQL,
        )
        .unwrap();

        assert_eq!(
            wr,
            b"\x81\xA3dql\x92\x93\x05\x92\xA6Online\x0A\xA3buz\x93\x38\x92\xA7Offline\x00\xA3bar"
        );
    }

    #[test]
    fn test_serialize_dml() {
        // {
        //     "dml": 567
        // }

        let tuple_arr: Vec<&[u8]> = vec![b"\xCD\x02\x37"];
        let mut iter = tuple_arr.iter().copied();
        let mut wr: Vec<u8> = Vec::new();
        serialize_res(
            &mut iter,
            tuple_arr.len() as u32,
            &mut wr,
            super::ResultType::DML,
        )
        .unwrap();

        assert_eq!(wr, b"\x81\xA3dml\xCD\x02\x37");
    }

    #[test]
    fn test_serialize_miss() {
        // {
        //     "miss": null
        // }
        let tuple_arr: Vec<&[u8]> = vec![b"\xC0"];
        let mut iter = tuple_arr.iter().copied();
        let mut wr: Vec<u8> = Vec::new();
        serialize_res(
            &mut iter,
            tuple_arr.len() as u32,
            &mut wr,
            super::ResultType::MISS,
        )
        .unwrap();

        assert_eq!(wr, b"\x81\xA4miss\xC0");
    }
}

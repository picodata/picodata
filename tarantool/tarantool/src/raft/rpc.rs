use std::ffi::{c_void, CStr};
use std::net::SocketAddr;
use std::path::Path;

use serde::{Serialize, Deserialize};

use crate::error::Error;
use crate::session;
use crate::space::{FuncMetadata, Privilege, Space, SystemSpace};
use crate::tuple::Encode;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    #[serde(rename = "bootstrap")]
    Bootstrap(BootstrapMsg),
    #[serde(rename = "propose")]
    Propose,
    #[serde(rename = "raft")]
    Raft { data: Vec<u8> },
}

impl Encode for Request {}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Response {
    #[serde(rename = "bootstrap")]
    Bootstrap(BootstrapMsg),
    #[serde(rename = "ack")]
    Ack,
}

impl Encode for Response {}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct BootstrapMsg {
    pub from_id: u64,
    pub nodes: Vec<(u64, Vec<SocketAddr>)>,
}

#[allow(unused)]
pub fn init_stored_proc(function_name: &str) -> Result<(), Error> {
    // get library metadata
    let mut module_info = libc::Dl_info {
        dli_fname: std::ptr::null(),
        dli_sname: std::ptr::null(),
        dli_fbase: std::ptr::null_mut(),
        dli_saddr: std::ptr::null_mut(),
    };
    unsafe { libc::dladdr(init_stored_proc as *const c_void, &mut module_info) };

    // extract name from library metadata
    let module_abs_path = unsafe { CStr::from_ptr(module_info.dli_fname) }
        .to_str()
        .unwrap();
    let module_name = Path::new(module_abs_path)
        .file_stem()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let stored_proc_name = [module_name.as_str(), function_name].join(".");

    let mut func_sys_space: Space = SystemSpace::Func.into();
    let idx = func_sys_space
        .index("name")
        .expect("System space \"_func\" not found");

    if idx.get(&(stored_proc_name.as_str(),))?.is_none() {
        // resolve new func id: get max id + increment
        let id = match func_sys_space.primary_key().max(&())? {
            None => 1,
            Some(t) => t.into_struct::<(u32,)>()?.0 + 1, // decode: Result -> Tuple[u32] -> (u32,) -> u32
        };

        // create new func record
        let datetime = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
        let owner = session::uid()? as u32;
        func_sys_space.insert(&FuncMetadata {
            id,
            owner,
            name: stored_proc_name,
            setuid: 0,
            language: "C".to_string(),
            body: "".to_string(),
            routine_type: "function".to_string(),
            param_list: vec![],
            returns: "any".to_string(),
            aggregate: "none".to_string(),
            sql_data_access: "none".to_string(),
            is_deterministic: false,
            is_sandboxed: false,
            is_null_call: true,
            exports: vec!["LUA".to_string()],
            opts: Default::default(),
            comment: "".to_string(),
            created: datetime.to_string(),
            last_altered: datetime.to_string(),
        })?;

        // grant guest to execute new func
        let mut priv_sys_space: Space = SystemSpace::Priv.into();
        priv_sys_space.insert(&Privilege {
            grantor: 1,
            grantee: 0,
            object_type: "function".to_string(),
            object_id: id,
            privilege: 4,
        })?;
    }

    Ok(())
}

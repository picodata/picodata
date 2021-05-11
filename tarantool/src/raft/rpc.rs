use std::collections::BTreeMap;
use std::ffi::{c_void, CStr};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs};
use std::path::Path;
use std::ptr::null_mut;

use ipnetwork::{Ipv4Network, Ipv6Network};

use crate::error::Error;
use crate::session;
use crate::space::{FuncMetadata, Privilege, Space, SystemSpace};
use crate::tuple::AsTuple;

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

impl AsTuple for Request {}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Response {
    #[serde(rename = "bootstrap")]
    Bootstrap(BootstrapMsg),
    #[serde(rename = "ack")]
    Ack,
}

impl AsTuple for Response {}

#[derive(Debug, Serialize, Deserialize)]
pub struct BootstrapMsg {
    pub from: u64,
    pub nodes: BTreeMap<u64, SocketAddr>,
}

pub fn self_addr(listen_addr_config: &str) -> Result<Vec<SocketAddr>, Error> {
    let listen_addrs = match listen_addr_config.parse::<u16>() {
        Ok(port) => vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            port,
        ))],
        _ => listen_addr_config.to_socket_addrs()?.collect(),
    };

    let mut if_addrs = null_mut::<libc::ifaddrs>();
    let res = unsafe { libc::getifaddrs(&mut if_addrs as *mut _) };
    if res < 0 {
        return Err(io::Error::last_os_error().into());
    }

    let mut result = Vec::<SocketAddr>::new();
    let mut current_if_addr = if_addrs;
    while !current_if_addr.is_null() {
        unsafe {
            let ifa_addr = (*current_if_addr).ifa_addr;
            let netmask = (*current_if_addr).ifa_netmask;
            current_if_addr = (*current_if_addr).ifa_next;

            if !(ifa_addr.is_null() || netmask.is_null()) {
                let addr_family = (*ifa_addr).sa_family as i32;
                let network = match addr_family {
                    libc::AF_INET => {
                        // is a valid IP4 Address
                        let addr = (*(ifa_addr as *const _ as *const SocketAddrV4)).ip();
                        let netmask = (*(netmask as *const _ as *const SocketAddrV4)).ip();
                        ipnetwork::IpNetwork::V4(
                            Ipv4Network::with_netmask(*addr, *netmask).unwrap(),
                        )
                    }
                    libc::AF_INET6 => {
                        // is a valid IP6 Address
                        let addr = (*(ifa_addr as *const _ as *const SocketAddrV6)).ip();
                        let netmask = (*(netmask as *const _ as *const SocketAddrV6)).ip();
                        ipnetwork::IpNetwork::V6(
                            Ipv6Network::with_netmask(*addr, *netmask).unwrap(),
                        )
                    }
                    _ => continue,
                };

                for listen_addr in listen_addrs.iter() {
                    let is_matches = match listen_addr.ip() {
                        IpAddr::V4(ip) if ip.is_unspecified() => true,
                        listen_addr => network.contains(listen_addr),
                    };

                    if is_matches {
                        result.push(SocketAddr::new(network.ip(), listen_addr.port()));
                    }
                }
            }
        }
    }

    unsafe {
        libc::freeifaddrs(if_addrs);
    }
    Ok(result)
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
        let id = match func_sys_space.primary_key().max(&Vec::<()>::new())? {
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

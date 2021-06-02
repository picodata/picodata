use std::borrow::Cow;
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs};
use std::ptr::null_mut;

use ipnetwork::{Ipv4Network, Ipv6Network};

use crate::error::Error;

use super::{Conn, ConnOptions};

#[derive(Default)]
pub struct ConnectionPoll {
    connections: Vec<Conn>,
    ids_index: HashMap<u64, usize>,
    addrs_index: HashMap<SocketAddr, usize>,
    options: ConnOptions,
}

impl ConnectionPoll {
    pub fn connect_or_get(
        &mut self,
        id: Option<u64>,
        addrs: &Vec<SocketAddr>,
    ) -> Result<&Conn, Error> {
        let inner_id = if let Some(id) = id {
            self.ids_index.get(&id)
        } else {
            let mut inner_id = None;
            for addr in addrs {
                inner_id = self.addrs_index.get(addr);
                if inner_id.is_some() {
                    break;
                }
            }
            inner_id
        };

        let inner_id = match inner_id {
            Some(inner_id) => *inner_id,
            None => {
                let inner_id = self.connections.len();
                self.connections
                    .push(Conn::new(addrs.as_slice(), self.options.clone(), None)?);

                if let Some(id) = id {
                    self.ids_index.insert(id, inner_id);
                }

                for addr in addrs {
                    self.addrs_index.insert(addr.clone(), inner_id);
                }

                inner_id
            }
        };

        Ok(self.connections.get(inner_id).unwrap())
    }
}

pub fn get_local_addrs() -> Result<Vec<SocketAddr>, Error> {
    let listen_addr_config = unsafe { get_listen_addr_config() };
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

unsafe fn get_listen_addr_config<'a>() -> Cow<'a, str> {
    use crate::ffi::lua::{
        luaT_state, lua_getfield, lua_getglobal, lua_gettop, lua_settop, lua_tostring,
    };
    use std::ffi::{CStr, CString};

    let l = luaT_state();
    let top_idx = lua_gettop(l);

    let s = CString::new("box").unwrap();
    lua_getglobal(l, s.as_ptr());

    let s = CString::new("cfg").unwrap();
    lua_getfield(l, -1, s.as_ptr());

    let s = CString::new("listen").unwrap();
    lua_getfield(l, -1, s.as_ptr());
    let listen_addr_str = CStr::from_ptr(lua_tostring(l, -1)).to_string_lossy();

    lua_settop(l, top_idx);

    listen_addr_str
}

//! Utilities to use when writing picodata tarantool tests.
//!
//! NOTE: this code makes assumptions about the testing environment. Do not use outside of tests.

use smol_str::SmolStr;

/// Returns the binary protocol listen address of the current tarantool instance.
/// It is the first listener in the config.
pub fn listen_address() -> SmolStr {
    let lua = tarantool::lua_state();
    lua.eval("return (box.info.listen[1] or box.info.listen)")
        .unwrap()
}

/// Returns the binary protocol port of the current tarantool instance.
/// It is the first port in the config.
pub fn listen_port() -> u16 {
    let listen = listen_address();
    let (_address, port) = listen.rsplit_once(':').unwrap();
    port.parse().unwrap()
}

/// Returns the TLS binary protocol listen address of the current tarantool instance.
/// It is the second listener in the config.
pub fn tls_listen_address() -> SmolStr {
    let lua = tarantool::lua_state();
    lua.eval("return box.info.listen[2]").unwrap()
}

/// Returns the TLS binary protocol port of the current tarantool instance.
/// It is the second port in the config.
pub fn tls_listen_port() -> u16 {
    let listen = tls_listen_address();
    let (_address, port) = listen.rsplit_once(':').unwrap();
    port.parse().unwrap()
}

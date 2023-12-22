use crate::{
    tarantool_main,
    util::{unwrap_or_terminate, validate_and_complete_unix_socket_path},
};

use super::args;

fn connect_and_start_interacitve_console(args: args::Admin) -> Result<(), String> {
    let endpoint = validate_and_complete_unix_socket_path(&args.socket_path)?;

    tarantool::lua_state()
        .exec_with(
            r#"local code, arg = ...
            return load(code, '@src/connect.lua')(arg)"#,
            (include_str!("connect.lua"), endpoint),
        )
        .map_err(|e| e.to_string())?;

    Ok(())
}

pub fn main(args: args::Admin) -> ! {
    let rc = tarantool_main!(
        args.tt_args().unwrap(),
        callback_data: args,
        callback_data_type: args::Admin,
        callback_body: {
            unwrap_or_terminate(connect_and_start_interacitve_console(args));
            std::process::exit(0)
        }
    );

    std::process::exit(rc);
}

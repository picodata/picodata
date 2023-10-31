use crate::{
    tarantool_main,
    util::{unwrap_or_terminate, validate_and_complete_unix_socket_path},
};

use super::args::{self, Address};

pub(crate) fn get_password_from_file(path: &str) -> Result<String, String> {
    let content = std::fs::read_to_string(path).map_err(|e| {
        format!(r#"can't read password from password file by "{path}", reason: {e}"#)
    })?;

    let password = content
        .lines()
        .next()
        .ok_or("Empty password file".to_string())?
        .trim();

    if password.is_empty() {
        return Ok(String::new());
    }

    Ok(password.into())
}

fn connect_and_start_interacitve_console(args: args::Connect) -> Result<(), String> {
    let endpoint = if args.address_as_socket {
        validate_and_complete_unix_socket_path(&args.address)?
    } else {
        let address = args
            .address
            .parse::<Address>()
            .map_err(|msg| format!("invalid format of address argument: {msg}"))?;
        let user = address.user.unwrap_or(args.user);

        let password = if user == args::DEFAULT_USERNAME {
            String::new()
        } else if let Some(path) = args.password_file {
            get_password_from_file(&path)?
        } else {
            let prompt = format!("Enter password for {user}: ");
            crate::util::prompt_password(&prompt)
                .map_err(|e| format!("\nFailed to prompt for a password: {e}"))?
        };

        format!(
            "{}:{}@{}:{}?auth_type={}",
            user, password, address.host, address.port, args.auth_method
        )
    };

    tarantool::lua_state()
        .exec_with(
            r#"local code, arg = ...
            return load(code, '@src/connect.lua')(arg)"#,
            (include_str!("connect.lua"), endpoint),
        )
        .map_err(|e| e.to_string())?;

    Ok(())
}

pub fn main(args: args::Connect) -> ! {
    let rc = tarantool_main!(
        args.tt_args().unwrap(),
        callback_data: args,
        callback_data_type: args::Connect,
        callback_body: {
            unwrap_or_terminate(connect_and_start_interacitve_console(args));
            std::process::exit(0)
        }
    );

    std::process::exit(rc);
}

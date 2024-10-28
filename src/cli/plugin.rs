use std::{collections::HashMap, fs::read_to_string};

use crate::{
    cli::{
        args::{Plugin, ServiceConfigUpdate},
        connect::determine_credentials_and_connect,
        console::ReplError,
    },
    schema::PICO_SERVICE_USER_NAME,
};

use rmpv::{Utf8String, Value};
use tarantool::{
    auth::AuthMethod,
    network::{AsClient, Client},
};

fn fetch_current_parameters(
    client: &Client,
    plugin_name: &str,
    plugin_version: &str,
) -> Vec<Vec<Value>> {
    const ROWS_KEY_LENGTH: usize = 5;
    const ROWS_KEY_VALUE: [u8; ROWS_KEY_LENGTH] = [0xA4, 0x72, 0x6F, 0x77, 0x73];

    let query = "SELECT * FROM _pico_plugin_config;";
    let raw_response =
        ::tarantool::fiber::block_on(client.call(".proc_sql_dispatch", &(query, Vec::<()>::new())))
            .expect("_pico_plugin_config table should be always available")
            .to_vec();

    let rows_msgpack_pos = raw_response
        .windows(ROWS_KEY_LENGTH)
        .position(|window| window == ROWS_KEY_VALUE)
        .expect("msgpack representation of values from _pico_plugin_config cannot be invalid");
    let positioned_response = &raw_response[rows_msgpack_pos + ROWS_KEY_LENGTH..];

    let unfiltered_response: Vec<Vec<Value>> =
        tarantool::tuple::Decode::decode(positioned_response)
            .expect("correctly positioned response decode cannot fail");
    unfiltered_response
        .into_iter()
        .filter(|tuple| {
            tuple[0] == Value::String(Utf8String::from(plugin_name))
                && tuple[1] == Value::String(Utf8String::from(plugin_version))
        })
        .collect()
}

fn current_equal_to_new(current_parameters: &[Vec<Value>], new_key: &str, new_val: &str) -> bool {
    #[inline]
    fn strip_single_double_quotes(param: &str) -> &str {
        param
            .strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))
            .unwrap_or(param)
    }

    let new_key = strip_single_double_quotes(new_key);
    let new_val = strip_single_double_quotes(new_val);

    current_parameters.iter().any(|current_set| {
        let curr_key = current_set[3].to_string();
        let curr_key = strip_single_double_quotes(&curr_key);
        if curr_key != new_key {
            return false;
        }

        let curr_val = current_set[4].to_string();
        let curr_val = strip_single_double_quotes(&curr_val);

        let keys_equal = curr_key == new_key;
        let vals_equal = curr_val == new_val;
        keys_equal && vals_equal
    })
}

fn main_impl(args: Plugin) -> Result<(), ReplError> {
    match args {
        Plugin::Configure(cfg) => {
            type ConfigRepr = HashMap<String, HashMap<String, serde_yaml::Value>>;

            let ServiceConfigUpdate {
                mut address,
                plugin_name,
                plugin_version,
                config_file,
                service_names,
                password_file,
            } = cfg;
            address.user = None; // ignore username, we will connect as `pico_service`
            let password_file = password_file.as_ref().and_then(|path| path.to_str());

            let config_raw = read_to_string(config_file)?;
            let config_values: ConfigRepr = serde_yaml::from_str(&config_raw)
                .map_err(|err| ReplError::Other(err.to_string()))?;

            let (client, _) = determine_credentials_and_connect(
                &address,
                Some(PICO_SERVICE_USER_NAME),
                password_file,
                AuthMethod::ChapSha1,
                10, /* timeout */
            )
            .map_err(|err| ReplError::Other(err.to_string()))?;

            let current_parameters =
                fetch_current_parameters(&client, &plugin_name, &plugin_version);

            // services values update logic
            let mut queries = Vec::new();
            match service_names {
                // create queries to update all services from user config file
                None => {
                    let query_prefix = format!("ALTER PLUGIN {plugin_name} {plugin_version} SET");

                    for (service_name, service_parameters) in config_values {
                        for (new_key, new_val) in service_parameters {
                            let new_val = serde_json::to_string(&new_val)
                                .map_err(|err| ReplError::Other(err.to_string()))?;
                            if !current_equal_to_new(&current_parameters, &new_key, &new_val) {
                                queries.push(format!(
                                    "{query_prefix} {service_name}.{new_key}='{new_val}';"
                                ));
                            }
                        }
                    }
                }
                // create queries to update only specified services in a flag from user config file
                Some(service_names) => {
                    let query_prefix = format!("ALTER PLUGIN {plugin_name} {plugin_version} SET");

                    for service_name in service_names {
                        let config_subvalues = config_values.get(&service_name).ok_or_else(|| {
                            ReplError::Other(
                                format!("service {service_name} from `--service-names` parameter not found in a new configuration file")
                            )
                        })?;

                        for (new_key, new_val) in config_subvalues {
                            let new_val = serde_json::to_string(new_val)
                                .map_err(|err| ReplError::Other(err.to_string()))?;
                            if !current_equal_to_new(&current_parameters, new_key, &new_val) {
                                queries.push(format!(
                                    "{query_prefix} {service_name}.{new_key}='{new_val}';"
                                ));
                            }
                        }
                    }
                }
            }

            if queries.is_empty() {
                Err(ReplError::Other("no values to update".into()))?;
            } else {
                // update values from created queries
                queries.iter().for_each(|query| {
                    ::tarantool::fiber::block_on(
                        client.call(".proc_sql_dispatch", &(query, Vec::<()>::new())),
                    )
                    .expect("updating existing and correct values should be fine");
                })
            }
        }
    }

    Ok(())
}

pub fn main(args: Plugin) -> ! {
    let tt_args = args.tt_args().unwrap();
    super::tarantool::main_cb(&tt_args, || -> Result<(), ReplError> {
        if let Err(error) = main_impl(args) {
            println!("{error}");
            std::process::exit(1);
        }
        std::process::exit(0)
    })
}

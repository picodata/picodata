use crate::cli;
use crate::cli::args::{Plugin, ServiceConfigUpdate};
use crate::cli::util::{Credentials, RowSet};
use crate::schema::PluginConfigRecord;
use crate::sql::proc_sql_dispatch;

use std::collections::HashMap;
use std::fs::read_to_string;
use std::time::Duration;

use tarantool::fiber;
use tarantool::network::{AsClient, Client};
use tarantool::tuple::Decode;

fn fetch_current_parameters(
    client: &Client,
    plugin_name: &str,
    plugin_version: &str,
) -> cli::Result<Vec<PluginConfigRecord>> {
    let query = r#"SELECT * FROM _pico_plugin_config WHERE plugin=? AND version=?;"#;
    let params = [plugin_name, plugin_version];

    let response_raw =
        fiber::block_on(client.call(crate::proc_name!(proc_sql_dispatch), &(query, params)))?;
    let response_full = response_raw.decode::<Vec<Vec<RowSet>>>()?;

    let response_content = response_full
        .first()
        .expect("select should return at least empty response array")
        .first()
        .ok_or("no rows to modify in _pico_plugin_config table")?;
    let response_records = &response_content.rows;
    let response_length = response_records.len();

    let mut result = Vec::with_capacity(response_length);
    response_records.iter().for_each(|tuple| {
        let record = rmp_serde::encode::to_vec(tuple)
            .expect("values from _pico_plugin_config should be serializable");
        let record: PluginConfigRecord = Decode::decode(&record)
            .expect("values from _pico_plugin_config should be deserializable");
        result.push(record);
    });
    Ok(result)
}

/// # Description
///
/// Compares passed keys and value with a set of current plugin
/// config parameters.
///
/// # Warning
///
/// Passed value should be a string, converted from JSON!
fn current_equal_to_new(
    current_parameters: &[PluginConfigRecord],
    new_key: &str,
    new_value: &str,
) -> bool {
    current_parameters.iter().any(|parameter_record| {
        let current_key = &parameter_record.key;
        let current_value = serde_json::to_string(&parameter_record.value)
            .expect("yaml to json deserialization should not have failed");
        current_key == new_key && current_value == new_value
    })
}

fn create_update_queries(
    query_prefix: &str,
    service_name: &str,
    old_parameters: &[PluginConfigRecord],
    new_parameters: &HashMap<String, serde_yaml::Value>,
) -> cli::Result<(Vec<String>, Vec<String>)> {
    let mut updated_parameters = Vec::new();
    let mut update_queries = Vec::new();

    for (new_key, new_value) in new_parameters {
        let new_value = serde_json::to_string(new_value)?;

        let parameter_changed = !current_equal_to_new(old_parameters, new_key, &new_value);
        if parameter_changed {
            updated_parameters.push(format!("{service_name}.{new_key}"));
            update_queries.push(format!(
                r#"{query_prefix} "{service_name}"."{new_key}"='{new_value}';"#
            ));
        }
    }

    Ok((updated_parameters, update_queries))
}

fn main_impl(args: Plugin) -> cli::Result<()> {
    match args {
        Plugin::Configure(cfg) => {
            type ConfigRepr = HashMap<String, HashMap<String, serde_yaml::Value>>;

            let ServiceConfigUpdate {
                peer_address,
                plugin_name,
                plugin_version,
                config_file,
                service_names,
                timeout,
                ..
            } = &cfg;

            // validate config first for better ux
            let config_string = read_to_string(config_file)?;
            let config_values: ConfigRepr = serde_yaml::from_str(&config_string)?;

            // setup credentials and options for the connection
            let credentials = Credentials::try_from(&cfg)?;
            let timeout = Some(Duration::from_secs(*timeout));
            let client = credentials
                .connect(peer_address, timeout)
                .map_err(crate::traft::error::Error::other)?;

            // setup buffers and current parameters to update them
            let query_prefix = format!(r#"ALTER PLUGIN "{plugin_name}" {plugin_version} SET"#);
            let mut updated_parameters = Vec::new();
            let mut update_queries = Vec::new();
            let current_parameters =
                fetch_current_parameters(&client, plugin_name, plugin_version)?;

            // user specified (with a flag) a list of services to update from a config file
            if let Some(service_names) = service_names {
                for service_name in service_names {
                    let service_parameters = config_values.get(service_name).ok_or_else(|| {
                        format!("service {service_name} from `--service-names` parameter not found in a new config")
                    })?;

                    let (parameters, queries) = create_update_queries(
                        &query_prefix,
                        service_name,
                        &current_parameters,
                        service_parameters,
                    )?;
                    updated_parameters.extend_from_slice(&parameters);
                    update_queries.extend_from_slice(&queries);
                }
            // user have not specified anything so we update all services from a config file
            } else {
                for (service_name, service_parameters) in config_values {
                    let (parameters, queries) = create_update_queries(
                        &query_prefix,
                        &service_name,
                        &current_parameters,
                        &service_parameters,
                    )?;
                    updated_parameters.extend_from_slice(&parameters);
                    update_queries.extend_from_slice(&queries);
                }
            }

            // run all update queries if they were created
            assert_eq!(updated_parameters.len(), update_queries.len());

            // quite common situation, return correct code
            if update_queries.is_empty() {
                println!("no values to update");
                return Ok(());
            }

            for update_query in update_queries {
                fiber::block_on(client.call(
                    crate::proc_name!(proc_sql_dispatch),
                    &(update_query, Vec::<()>::new()),
                ))
                .expect("updating existing and correct parameters of plugins should be fine");
            }

            // output success message for better ux
            println!("new configuration for plugin '{plugin_name}' successfully applied: {updated_parameters:?}");
        }
    }

    Ok(())
}

pub fn main(args: Plugin) -> ! {
    let tt_args = args.tt_args().unwrap();
    super::tarantool::main_cb(&tt_args, || -> cli::Result<()> {
        if let Err(error) = main_impl(args) {
            eprintln!("{error}");
            std::process::exit(1);
        }
        std::process::exit(0)
    })
}

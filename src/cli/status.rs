use crate::cli;
use crate::cli::args;
use crate::cli::util::{Credentials, RowSet};
use crate::info::{proc_instance_info, InstanceInfo};
use crate::instance::StateVariant;
use crate::sql::proc_sql_dispatch;
use crate::traft;

use std::cmp::max;
use std::collections::BTreeMap;
use std::time::Duration;

use comfy_table::{ContentArrangement, Table};
use rmpv::Value;
use tarantool::network::AsClient;

pub fn main(args: args::Status) -> ! {
    let tt_args = args.tt_args().unwrap();
    super::tarantool::main_cb(&tt_args, || -> cli::Result<()> {
        if let Err(error) = main_impl(args) {
            eprintln!("{error}");
            std::process::exit(1);
        }
        std::process::exit(0)
    })
}

fn table_without_graphic() -> Table {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Disabled);

    // remove all pseudo graphic
    table.remove_style(comfy_table::TableComponent::BottomBorder);
    table.remove_style(comfy_table::TableComponent::BottomBorderIntersections);
    table.remove_style(comfy_table::TableComponent::BottomLeftCorner);
    table.remove_style(comfy_table::TableComponent::BottomRightCorner);
    table.remove_style(comfy_table::TableComponent::HeaderLines);
    table.remove_style(comfy_table::TableComponent::HorizontalLines);
    table.remove_style(comfy_table::TableComponent::LeftBorder);
    table.remove_style(comfy_table::TableComponent::LeftBorderIntersections);
    table.remove_style(comfy_table::TableComponent::LeftHeaderIntersection);
    table.remove_style(comfy_table::TableComponent::MiddleHeaderIntersections);
    table.remove_style(comfy_table::TableComponent::MiddleIntersections);
    table.remove_style(comfy_table::TableComponent::RightBorder);
    table.remove_style(comfy_table::TableComponent::RightBorderIntersections);
    table.remove_style(comfy_table::TableComponent::RightHeaderIntersection);
    table.remove_style(comfy_table::TableComponent::TopBorder);
    table.remove_style(comfy_table::TableComponent::TopBorderIntersections);
    table.remove_style(comfy_table::TableComponent::TopRightCorner);
    table.remove_style(comfy_table::TableComponent::TopLeftCorner);
    table.remove_style(comfy_table::TableComponent::VerticalLines);

    table
}

fn set_width_for_columns(table: &mut Table, name_length: usize, state_length: usize) {
    const SPACES_BETWEEN_COLUMNS: u16 = 3;
    for (index, column) in table.column_iter_mut().enumerate() {
        match index {
            // length of the name can be whatever you want
            0 => column.set_constraint(comfy_table::ColumnConstraint::Absolute(
                comfy_table::Width::Fixed(name_length as u16 + SPACES_BETWEEN_COLUMNS),
            )),
            // state - values: {Online, Offline, Online?, Offline?, Expelled, Expelled?}
            1 => column.set_constraint(comfy_table::ColumnConstraint::Absolute(
                comfy_table::Width::Fixed(state_length as u16 + SPACES_BETWEEN_COLUMNS),
            )),
            // uuid - uuid length is fixed, so we can be sure that
            // problem with len overflow couldn't occur
            2 => column.set_constraint(comfy_table::ColumnConstraint::Absolute(
                comfy_table::Width::Fixed(36 + SPACES_BETWEEN_COLUMNS),
            )),
            // uri - it's last column, so width shouldn't be set
            3 => column.set_constraint(comfy_table::ColumnConstraint::ContentWidth),
            _ => unreachable!(),
        };
    }
}

fn main_impl(args: args::Status) -> cli::Result<()> {
    // setup credentials and options for the connection
    let credentials = Credentials::try_from(&args)?;
    let timeout = Some(Duration::from_secs(args.timeout));
    let client = credentials
        .connect(&args.peer_address, &args.tls, timeout)
        .map_err(traft::error::Error::other)?;

    let instance_info =
        ::tarantool::fiber::block_on(client.call(crate::proc_name!(proc_instance_info), &()))?
            .decode::<Vec<InstanceInfo>>()?;

    // Response always wrapped in MP_ARRAY
    let Some(instance_info) = instance_info.first() else {
        return Err("Invalid form of response".to_string().into());
    };

    println!(" CLUSTER NAME: {}", instance_info.cluster_name);
    println!(" CLUSTER UUID: {}", instance_info.cluster_uuid);

    let mut response = ::tarantool::fiber::block_on(client.call(
        crate::proc_name!(proc_sql_dispatch),
        &(
            r#"
            SELECT name, state, target_state, "uuid", uri, fd, tier FROM
                (SELECT PI.name name,
                       PI.current_state state,
                       PI.target_state target_state,
                       PI."uuid",
                       PA.address uri,
                       PI.tier,
                       PI.failure_domain fd
                FROM _pico_peer_address PA
                JOIN _pico_instance PI
                     ON PA.raft_id = PI.raft_id
                WHERE connection_type = 'iproto'
                ORDER BY tier)
            ORDER BY name ASC"#,
            Vec::<()>::new(),
        ),
    ))?
    .decode::<Vec<Vec<RowSet>>>()?;

    // Response always wrapped in MP_ARRAY
    let Some(response) = response.first_mut() else {
        return Err("Invalid form of response".to_string().into());
    };

    // There is only one query
    let Some(response) = response.first_mut() else {
        return Err("Invalid form of respone".to_string().into());
    };

    let find_idx_of_column = |column_name: &str| {
        response
            .metadata
            .iter()
            .enumerate()
            .find_map(|(index, column_description)| {
                (column_description.name == column_name).then_some(index)
            })
            .expect("sql completed, so shouldn't be a problem to find existing columns")
    };

    let current_state_index = find_idx_of_column("state");
    let target_state_index = find_idx_of_column("target_state");

    // Transforms state column from MP_ARRAY to MP_STRING removing incarnation.
    {
        // Value in column *_state represented as ["Online/Offline", int(incarnation)]
        let state_as_string = |state_column_value: &rmpv::Value| {
            let state_column_value = state_column_value
                .as_array()
                .expect("type of state column is array");

            let state_value = state_column_value[0]
                .as_str()
                .expect("type of state value is string")
                .to_string();

            state_value
        };

        for row in response.rows.iter_mut() {
            let target_state = state_as_string(&row[target_state_index]);
            let current_state = state_as_string(&row[current_state_index]);
            let current_state = if current_state != target_state {
                current_state + "?"
            } else {
                current_state
            };

            row[current_state_index] = current_state.into();

            // Remove target state column.
            row.remove(target_state_index);
        }
    }

    // Remove 'expelled' instances.
    response.rows.retain(|row| {
        let current_state_value = row[current_state_index]
            .as_str()
            .expect("should be converted to string");

        current_state_value != StateVariant::Expelled.as_str()
    });

    // Sort by state.
    {
        #[rustfmt::skip]
        response.rows.sort_by(|left, right| {
            let left_current_state_value =  left[current_state_index].as_str().expect("should be converted to string");
            let right_current_state_value = right[current_state_index].as_str().expect("should be converted to string");

            // Sort by state value in descending order, because "Offline" should be lower than "Online",
            left_current_state_value.cmp(right_current_state_value).reverse()
        });
    }

    // map from tier/fd -> instances info
    let mut aggregated_by_tier_fd: BTreeMap<String, Vec<Vec<Value>>> = BTreeMap::new();

    // The column name shouldn't be trimmed.
    let mut name_max_size_initial = 5;
    let mut state_max_size_initial = 6;

    // Fill `aggregated_by_tier_fd` and also remove tier and fd from table.
    for row in &response.rows {
        let (tier, other_cols) = row.split_last().expect("there is much more than 1 column");
        let (fd, other_cols) = other_cols
            .split_last()
            .expect("there is much more than 2 column");
        let tier = tier
            .as_str()
            .expect("column type `tier` is string")
            .to_string();

        let name = other_cols.first().expect("name is the first column");
        let name = name.as_str().expect("type of name column is string");
        name_max_size_initial = max(name_max_size_initial, name.len());

        let state = other_cols
            .get(current_state_index)
            .expect("state column index is precalculated");
        let state = state.as_str().expect("type of state column is string");
        state_max_size_initial = max(state_max_size_initial, state.len());

        let mut fd = fd
            .as_map()
            .expect("failure_domain represented as map in _pico_instance")
            .clone();

        fd.sort_by_key(|(key, _)| {
            key.as_str()
                .expect("key-value in failure_domain is string type")
                .to_string()
        });

        let values_of_fd = fd
            .iter()
            .map(|(_, value)| {
                value
                    .as_str()
                    .expect("key-value in failure_domain is string type")
            })
            .collect::<Vec<_>>();

        let joined_fds = values_of_fd.join(":");

        let tier_fd = if joined_fds.is_empty() {
            tier
        } else {
            tier + "/" + &joined_fds
        };

        aggregated_by_tier_fd
            .entry(tier_fd)
            .or_default()
            .push(other_cols.into());
    }

    // Remove `tier`, `failure_domain` and `target_state` from metadata.
    let metadata = {
        let extra_columns_indexes = [2, 5, 6];
        let mut acc = Vec::new();
        for (index, column) in response.metadata.iter().enumerate() {
            if !extra_columns_indexes.contains(&index) {
                acc.push(column);
            }
        }

        acc
    };

    for (tier_and_fd, rows) in aggregated_by_tier_fd.iter() {
        let mut table = table_without_graphic();

        table.set_header(&metadata);

        set_width_for_columns(&mut table, name_max_size_initial, state_max_size_initial);

        for row in rows {
            let formatted_row: Vec<String> = row
                .iter()
                .map(|v| {
                    // If cell is Utf8String, then we format it as plain string with no quotes.
                    if let rmpv::Value::String(s) = v {
                        s.as_str().unwrap().to_string()
                    } else {
                        v.to_string()
                    }
                })
                .collect();

            table.add_row(formatted_row);
        }

        // All meta-information must be separated
        // by a space at the beginning of the line.
        let table = table.to_string();
        let mut splitted = table.split("\n").map(String::from).collect::<Vec<_>>();

        for (index, line) in splitted.iter_mut().enumerate() {
            // Table header is the first line in table output.
            if index == 0 {
                line.remove(name_max_size_initial + 1);
            } else {
                *line = line.split_at(1).1.to_string();
            }
        }

        let table = splitted.join("\n");

        println!(" TIER/DOMAIN: {tier_and_fd}");
        println!();
        println!("{table}");
        println!();
    }

    Ok(())
}

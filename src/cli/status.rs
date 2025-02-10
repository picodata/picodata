use std::time::Duration;

use tarantool::{auth::AuthMethod, network::AsClient};

use crate::{
    cli::connect::determine_credentials_and_connect,
    info::{proc_instance_info, InstanceInfo},
    schema::PICO_SERVICE_USER_NAME,
    sql::proc_sql_dispatch,
};

use super::{args, connect::RowSet};

pub fn main(args: args::Status) -> ! {
    let tt_args = args.tt_args().unwrap();
    super::tarantool::main_cb(&tt_args, || -> Result<(), Box<dyn std::error::Error>> {
        if let Err(error) = main_impl(args) {
            eprintln!("{error}");
            std::process::exit(1);
        }
        std::process::exit(0)
    })
}

fn sort_by_state(
    rows: &mut [Vec<rmpv::Value>],
    current_state_index: usize,
    target_state_index: usize,
) {
    // Value in column *_state represented as ["Online/Offline", int(incarnation)]
    fn state_and_incarnation(state_column_value: &rmpv::Value) -> (String, u64) {
        let state_column_value = state_column_value
            .as_array()
            .expect("type of state column is array");

        let state_value = state_column_value[0]
            .as_str()
            .expect("type of state value is string")
            .to_string();

        let incarnation = state_column_value[1]
            .as_u64()
            .expect("type of incarnation is u64");

        (state_value, incarnation)
    }

    // Sort by current and target status
    #[rustfmt::skip]
    rows.sort_by(|left, right| {
        let (left_current_state_value, left_current_state_incarnation) = state_and_incarnation(&left[current_state_index]);
        let (right_current_state_value, right_current_state_incarnation) = state_and_incarnation(&right[current_state_index]);

        let (left_target_state_value, left_target_state_incarnation) = state_and_incarnation(&left[target_state_index]);
        let (right_target_state_value, right_target_state_incarnation) = state_and_incarnation(&right[target_state_index]);

        // Sort by state value in descending order, because "Offline" should be lower than "Online",
        // only then by incarnation
        left_current_state_value.cmp(&right_current_state_value).reverse().then_with(||{
            left_target_state_value.cmp(&right_target_state_value).reverse()
        }).then_with(|| {
            left_current_state_incarnation.cmp(&right_current_state_incarnation)
        }).then_with(|| {
            left_target_state_incarnation.cmp(&right_target_state_incarnation)
        })
    });
}

fn main_impl(args: args::Status) -> Result<(), Box<dyn std::error::Error>> {
    let password_file = args.password_file.as_ref().and_then(|path| path.to_str());
    let (client, _) = determine_credentials_and_connect(
        &args.peer_address,
        Some(PICO_SERVICE_USER_NAME),
        password_file,
        AuthMethod::ChapSha1,
        Duration::from_secs(args.timeout),
    )?;

    let instance_info =
        ::tarantool::fiber::block_on(client.call(crate::proc_name!(proc_instance_info), &()))?
            .decode::<Vec<InstanceInfo>>()?;

    // Response always wrapped in MP_ARRAY
    let Some(instance_info) = instance_info.first() else {
        return Err("Invalid form of response".to_string().into());
    };

    println!("\n");
    println!("CLUSTER NAME: {}", instance_info.cluster_name);
    println!("\n");

    let mut response = ::tarantool::fiber::block_on(client.call(
        crate::proc_name!(proc_sql_dispatch),
        &(
            r#"
            SELECT * FROM
                (SELECT PI.name instance_name,
                       PI.current_state,
                       PI.target_state,
                       PI."uuid" instance_uuid,
                       PI.replicaset_uuid,
                       PI.tier, PA.address uri
                FROM _pico_peer_address PA
                JOIN _pico_instance PI ON PA.raft_id = PI.raft_id
                WHERE connection_type = 'iproto')
            ORDER BY instance_name"#,
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

    let current_state_index = find_idx_of_column("current_state");
    let target_state_index = find_idx_of_column("target_state");

    sort_by_state(&mut response.rows, current_state_index, target_state_index);

    println!("{response}");

    Ok(())
}

#[cfg(test)]
mod tests {
    use rmpv::Value;

    use super::sort_by_state;

    #[test]
    fn custom_sorting_of_instances_states() {
        let first = Value::String("first".into());
        let second = Value::String("second".into());

        let online_state_with_incarnation = |incarnation: usize| {
            Value::Array(vec![
                Value::String("Online".into()),
                Value::Integer(incarnation.into()),
            ])
        };
        let offline_state_with_incarnation = |incarnation: usize| {
            Value::Array(vec![
                Value::String("Offline".into()),
                Value::Integer(incarnation.into()),
            ])
        };

        // sql output row format: [_, _, ..., current_state, ..., target_state, ..]
        // current/target_state position determined by second and third args of `sort_by_state`
        // function
        let sort = |table: &mut Vec<Vec<Value>>| {
            sort_by_state(table, 1, 2);
        };

        // one instance
        let mut table = vec![vec![
            first.clone(),
            online_state_with_incarnation(0),
            online_state_with_incarnation(0),
        ]];

        sort(&mut table);

        assert!(table[0][0] == first);

        // two active instances
        let mut table = vec![
            vec![
                first.clone(),
                online_state_with_incarnation(0),
                online_state_with_incarnation(0),
            ],
            vec![
                second.clone(),
                online_state_with_incarnation(0),
                online_state_with_incarnation(0),
            ],
        ];

        sort(&mut table);

        assert!(table[0][0] == first);
        assert!(table[1][0] == second);

        // two active instances with different incarnation
        let mut table = vec![
            vec![
                first.clone(),
                online_state_with_incarnation(1),
                online_state_with_incarnation(1),
            ],
            vec![
                second.clone(),
                online_state_with_incarnation(0),
                online_state_with_incarnation(0),
            ],
        ];

        sort(&mut table);

        assert!(table[0][0] == second);
        assert!(table[1][0] == first);

        // two offline instances
        let mut table = vec![
            vec![
                first.clone(),
                offline_state_with_incarnation(0),
                offline_state_with_incarnation(0),
            ],
            vec![
                second.clone(),
                offline_state_with_incarnation(0),
                offline_state_with_incarnation(0),
            ],
        ];

        sort(&mut table);

        assert!(table[0][0] == first);
        assert!(table[1][0] == second);

        // two offline instances with different incarnation
        let mut table = vec![
            vec![
                first.clone(),
                offline_state_with_incarnation(1),
                offline_state_with_incarnation(1),
            ],
            vec![
                second.clone(),
                offline_state_with_incarnation(0),
                offline_state_with_incarnation(0),
            ],
        ];

        sort(&mut table);

        assert!(table[0][0] == second);
        assert!(table[1][0] == first);

        // first instance is inactive
        let mut table = vec![
            vec![
                first.clone(),
                offline_state_with_incarnation(0),
                online_state_with_incarnation(0),
            ],
            vec![
                second.clone(),
                online_state_with_incarnation(0),
                online_state_with_incarnation(0),
            ],
        ];

        sort(&mut table);

        assert!(table[0][0] == second);
        assert!(table[1][0] == first);

        // first instance is going inactive
        let mut table = vec![
            vec![
                first.clone(),
                online_state_with_incarnation(0),
                offline_state_with_incarnation(0),
            ],
            vec![
                second.clone(),
                online_state_with_incarnation(0),
                online_state_with_incarnation(0),
            ],
        ];

        sort(&mut table);

        assert!(table[0][0] == second);
        assert!(table[1][0] == first);
    }
}

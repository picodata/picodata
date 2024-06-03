//! Lua API exported as `_G.pico`
//!

use crate::cas::{self, compare_and_swap};
use crate::config::PicodataConfig;
use crate::instance::InstanceId;
use crate::schema::{self, CreateTableParams, ADMIN_ID};
use crate::traft::error::Error;
use crate::traft::op::{self, Op};
use crate::traft::{self, node, RaftIndex, RaftTerm};
use crate::util::str_eq;
use crate::util::INFINITY;
use crate::util::{duration_from_secs_f64_clamped, effective_user_id};
use crate::{rpc, sync, tlog};
use ::tarantool::fiber;
use ::tarantool::session;
use ::tarantool::tlua;
use ::tarantool::tlua::{LuaState, LuaThread, PushOneInto, Void};
use ::tarantool::transaction::transaction;
use ::tarantool::tuple::Decode;
use ::tarantool::vclock::Vclock;
use indoc::indoc;
use std::time::Duration;

#[inline(always)]
fn luamod_set<V>(l: &LuaThread, name: &str, help: &str, value: V)
where
    V: PushOneInto<LuaState>,
    V::Err: Into<Void>,
{
    let luamod: tlua::LuaTable<_> = l.get("pico").unwrap();
    luamod.set(name, value);
    let help_table: tlua::LuaTable<_> = luamod.get_or_create_metatable();
    help_table.set(name, help);
}

#[inline(always)]
fn luamod_set_help_only(l: &LuaThread, name: &str, help: &str) {
    let luamod: tlua::LuaTable<_> = l.get("pico").unwrap();
    let help_table: tlua::LuaTable<_> = luamod.get_or_create_metatable();
    help_table.set(name, help);
}

pub(crate) fn setup(config: &PicodataConfig) {
    let l = ::tarantool::lua_state();
    l.exec(include_str!("luamod.lua")).unwrap();

    luamod_set(
        &l,
        "PICODATA_VERSION",
        indoc! {"
        pico.PICODATA_VERSION
        =====================

        A string variable (not a function) containing Picodata version
        which follows the Calendar Versioning convention with the
        `YY.0M.MICRO` scheme:

            https://calver.org/#scheme

        Example:

            picodata> pico.PICODATA_VERSION
            ---
            - 24.2.4
            ...
        "},
        {
            const _: () = assert!(str_eq(env!("CARGO_PKG_VERSION"), "24.2.4"));
            crate::info::PICODATA_VERSION
        },
    );

    luamod_set(
        &l,
        "LUA_API_VERSION",
        indoc! {"
        pico.LUA_API_VERSION
        ====================

        A string variable (not a function) contatining version of the Lua API
        which follows the Semantic Versioning convention:

            https://semver.org/

        The functions marked as Internal API only affect the PATCH version.

        Example:

            picodata> pico.LUA_API_VERSION
            ---
            - 3.1.0
            ...
        "},
        "3.1.0",
    );

    let config = config.clone();
    luamod_set(
        &l,
        "config",
        indoc! {"
        pico.config()
        =========

        Returns a Lua table containing picodata configuration which is
        gathered from the configuration file, environment variables and/or
        command line arguments.

        The content of the table is not strictly defined and may depend on circumstances.

        Returns:

            (table)

        Example:

            picodata> pico.config()
            ---
            - cluster:
                cluster_id: demo
            - instance:
                log_level: info
                listen: localhost:3301
                data_dir: .
                peers:
                  - localhost:3301
            ...
        "},
        tlua::Function::new(move || -> PicodataConfig {
            // FIXME: currently it only contains explicitly specified parameters,
            // but default parameters are omitted
            config.clone()
        }),
    );

    luamod_set(
        &l,
        "whoami",
        indoc! {"
        pico.whoami()
        =============

        Returns the identifiers of the current instance.

        Returns:

            (table)

        Fields:

            - raft_id (number)
            - cluster_id (string)
            - instance_id (string)
            - tier (string)

        Example:

            picodata> pico.whoami()
            ---
            - raft_id: 1
              cluster_id: demo
              instance_id: i1
              tier: storage
            ...
        "},
        tlua::function0(|| -> traft::Result<_> {
            let node = traft::node::global()?;
            let info = crate::info::InstanceInfo::try_get(node, None)?;

            Ok(tlua::AsTable((
                ("raft_id", info.raft_id),
                ("cluster_id", info.cluster_id),
                ("instance_id", info.instance_id),
                ("tier", info.tier),
            )))
        }),
    );

    luamod_set(
        &l,
        "instance_info",
        indoc! {"
        pico.instance_info([instance_id])
        =================================

        Provides general information for the given instance.

        Params:

            1. instance_id (optional string), default: id of the current instance

        Returns:

            (table)
            or
            (nil, string) in case of an error

        Fields:

            - raft_id (number)
            - advertise_address (string)
            - instance_id (string)
            - instance_uuid (string)
            - replicaset_id (string)
            - replicaset_uuid (string)
            - current_grade (table),
                `{variant = string, incarnation = number}`, where variant is one of
                'Offline' | 'Online' | 'Expelled'
            - target_grade (table),
                `{variant = string, incarnation = number}`, where variant is one of
                'Offline' | 'Replicated' | 'ShardingInitialized' | 'Online' | 'Expelled'
            - tier (string)

        Example:

            picodata> pico.instance_info()
            ---
            - raft_id: 1
              advertise_address: localhost:3301
              instance_id: i1
              instance_uuid: 68d4a766-4144-3248-aeb4-e212356716e4
              tier: storage
              replicaset_id: r1
              replicaset_uuid: e0df68c5-e7f9-395f-86b3-30ad9e1b7b07
              current_grade:
                variant: Online
                incarnation: 26
              target_grade:
                variant: Online
                incarnation: 26
            ...
        "},
        tlua::function1(|iid: Option<InstanceId>| -> traft::Result<_> {
            let node = traft::node::global()?;
            let info = crate::info::InstanceInfo::try_get(node, iid.as_ref())?;

            Ok(tlua::AsTable((
                ("raft_id", info.raft_id),
                ("advertise_address", info.advertise_address),
                ("instance_id", info.instance_id.0),
                ("instance_uuid", info.instance_uuid),
                ("replicaset_id", info.replicaset_id),
                ("replicaset_uuid", info.replicaset_uuid),
                ("current_grade", info.current_grade),
                ("target_grade", info.target_grade),
                ("tier", info.tier),
            )))
        }),
    );

    ///////////////////////////////////////////////////////////////////////////
    luamod_set(
        &l,
        "raft_status",
        indoc! {"
        pico.raft_status()
        ==================

        Returns the raft node status of the current instance.

        Returns:

            (table)
            or
            (nil, string) in case of an error, if the raft node is
                not initialized yet

        Fields:

            - id (number)
            - term (number)
            - leader_id (number | nil),
                absent if there's no leader in the current term
                or it's unknown yet
            - raft_state (string),
                one of 'Follower' | 'Candidate' | 'Leader' | 'PreCandidate'

        Example:

            picodata> pico.raft_status()
            ---
            - term: 27
              leader_id: 1
              raft_state: Leader
              id: 1
            ...
        "},
        tlua::function0(|| traft::node::global().map(|n| n.status())),
    );

    luamod_set(
        &l,
        "raft_tick",
        indoc! {"
        pico.raft_tick(n_times)
        =======================

        Internal API. Makes the raft node to 'tick' `n_times`. It shouldn't
        be used except for tests and dirty hacks. See `src/luamod.rs` for
        the details.

        Params:

            1. n_times (number)

        Returns:

            (true)
            or
            (nil, string) in case of an error

        "},
        tlua::function1(|n_times: u32| -> traft::Result<bool> {
            traft::node::global()?.tick_and_yield(n_times);
            Ok(true)
        }),
    );

    // raft index
    ///////////////////////////////////////////////////////////////////////////

    luamod_set(
        &l,
        "raft_get_index",
        indoc! {"
        pico.raft_get_index()
        =====================

        Returns the current applied raft index.

        Returns:

            (number)
            or
            (nil, string) in case of an error, if the raft node is
                not initialized yet
        "},
        tlua::function0(|| traft::node::global().map(|n| n.get_index())),
    );
    luamod_set(
        &l,
        "raft_read_index",
        indoc! {"
        pico.raft_read_index(timeout)
        =============================

        Performs the quorum read operation.

        It works the following way:

        1. The instance forwards a request (`MsgReadIndex`) to a raft
           leader. In case there's no leader at the moment, the function
           returns the error 'raft: proposal dropped'.
        2. Raft leader tracks its `commit_index` and broadcasts a
           heartbeat to followers to make certain that it's still a
           leader.
        3. As soon as the heartbeat is acknowledged by the quorum, the
           leader returns that index to the instance.
        4. The instance awaits when the index is applied. If timeout
           expires beforehand, the function returns the error 'timeout'.

        Params:

            1. timeout (number), in seconds

        Returns:

            (number)
            or
            (nil, string) in case of an error
        "},
        tlua::function1(|timeout: f64| -> traft::Result<RaftIndex> {
            traft::node::global()?.read_index(duration_from_secs_f64_clamped(timeout))
        }),
    );
    luamod_set(
        &l,
        "raft_wait_index",
        indoc! {"
        pico.raft_wait_index(target, timeout)
        =====================================

        Waits for the `target` index to be applied to the storage locally.

        Returns current applied raft index. It can be equal to or
        greater than the requested one. If timeout expires beforehand,
        the function returns an error.

        Params:

            1. target (number)
            2. timeout (number), in seconds

        Returns:

            (number)
            or
            (nil, string) in case of an error
        "},
        tlua::function2(
            |target: RaftIndex, timeout: f64| -> traft::Result<RaftIndex> {
                let node = traft::node::global()?;
                node.wait_index(target, duration_from_secs_f64_clamped(timeout))
            },
        ),
    );

    // sql
    ///////////////////////////////////////////////////////////////////////////
    luamod_set_help_only(
        &l,
        "sql",
        indoc! {r#"
        pico.sql(query, [params], [options])
        =========================

        Executes a cluster-wide SQL query.

        1. The query is parsed and validated to build a distributed
           query plan on the current instance (router).
        2. The query plan is dispatched to the target instances (storages)
           slice-by-slice in a bottom-up manner. All intermediate results
           are stored in the router's memory.

        Params:

            1. query (string)
            2. params (table), optional
            3. options (table), optional
                `{traceable = boolean, query_id = string}`

        Returns:

            (table DqlResult) if query retrieves data
            or
            (table DmlResult) if query modifies data
            or
            (nil, string) in case of an error

        table DqlResult:

            - metadata (table),
                `{{name = string, type = string}, ...}`, an array of column
                definitions of the table.
            - rows (table),
                `{row, ...}`, essentially the result of the query.

        table DmlResult:

            - row_count (number), the number of rows modified, inserted or
            deleted by the query.

        Example:

            picodata> -- Create a sharded table 'wonderland'.
            picodata> pico.sql([[
                create table "wonderland" (
                    "property" text not null,
                    "value" integer,
                    primary key ("property")
                ) using memtx distributed by ("property")
                option (timeout = 3.0)
            ]])
            ---
            - row_count: 1
            ...

            picodata> -- Drop 'wonderland' table.
            picodata> pico.sql([[
                drop table "wonderland"
                option (timeout = 3.0)
            ]])
            ---
            - row_count: 1
            ...

            picodata> -- Insert a row into the 'wonderland' table using parameters
            picodata> -- as an optional argument.
            picodata> pico.sql(
                [[insert into "wonderland" ("property", "value") values (?, ?)]],
                {"dragon", 13}
            )
            ---
            - row_count: 1
            ...

            picodata> -- Select a row from the 'wonderland' table.
            picodata> pico.sql(
                [[select * from "wonderland" where "property" = 'dragon']]
            )
            ---
            - metadata:
                - {'name': 'property', 'type': 'string'}
                - {'name': 'value', 'type': 'integer'}
              rows:
                - ['dragon', 13]
        "#},
    );

    // vclock
    ///////////////////////////////////////////////////////////////////////////
    luamod_set_help_only(
        &l,
        "table Vclock",
        indoc! {"
        table Vclock
        ============

        Vclock is a mapping of replica id (number) to its LSN (number).

        The meaning of these concepts is explained below.

        To ensure data persistence, Tarantool records updates to the
        database in the so-called write-ahead log (WAL) files. Each record
        in the WAL represents a single Tarantool data-change request such as
        `INSERT`, `UPDATE`, or `DELETE`, and is assigned a monotonically
        growing log sequence number (LSN).

        Enabling replication makes all replicas in a replica set to exchange
        their records, each with it's own LSN. Together, LSNs from different
        replicas form a vector clock — vclock. Vclock defines the database
        state of an instance.

        The zero vclock component is special, it's used for tracking local
        changes that aren't replicated.

        Example:

            {[0] = 2, [1] = 101}
            {[0] = 148, [1] = 9086, [3] = 2}
        "},
    );
    luamod_set(
        &l,
        "get_vclock",
        indoc! {"
        pico.get_vclock()
        =================

        Returns the current vclock from Tarantool `box.info.vclock` API.

        Returns:

            (table Vclock), see pico.help('table Vclock')

        Example:

            picodata> pico.get_vclock()
            ---
            - 0: 2
              1: 101
            ...
        "},
        tlua::function0(Vclock::current),
    );
    luamod_set(
        &l,
        "wait_vclock",
        indoc! {"
        pico.wait_vclock(target, timeout)
        =================================

        Waits until Tarantool vclock reaches the `target` value.

        Returns the actual vclock. It can be equal to or greater than the
        target one. If timeout expires beforehand, the function returns an
        error.

        Params:

            1. target (table Vclock), see pico.help('table Vclock')
            2. timeout (number), in seconds

        Returns:

            (table Vclock)
            or
            (nil, string) in case of an error
        "},
        tlua::function2(|target: Vclock, timeout: f64| -> traft::Result<Vclock> {
            sync::wait_vclock(target, duration_from_secs_f64_clamped(timeout))
        }),
    );

    // propose
    ///////////////////////////////////////////////////////////////////////////
    luamod_set(
        &l,
        "raft_propose_nop",
        indoc! {"
        pico.raft_propose_nop()
        =======================

        Internal API. Proposes and waits for Op::Nop to be applied.
        It shouldn't be used except for tests.
        "},
        tlua::function0(|| {
            traft::node::global()?.propose_and_wait(Op::Nop, Duration::from_secs(1))
        }),
    );

    luamod_set(
        &l,
        "raft_propose",
        // TODO: Provide a more Lua friendly interface for `Op` and then document it
        // or maybe mark this function `internal`
        indoc! {"
        pico.raft_propose(op)
        ============================

        Internal API, see src/luamod.rs for the details.

        Params:

            1. op (table)

        Returns:

            (number)
            or
            (nil, string) in case of an error
        "},
        tlua::function1(|lua: tlua::StaticLua| -> traft::Result<RaftIndex> {
            use tlua::{AnyLuaString, AsLua, LuaError, LuaTable};
            let t: LuaTable<_> = AsLua::read(&lua).map_err(|(_, e)| LuaError::from(e))?;
            let mp: AnyLuaString = lua
                .eval_with("return require 'msgpack'.encode(...)", &t)
                .map_err(LuaError::from)?;
            let op: Op = Decode::decode(mp.as_bytes())?;

            let node = traft::node::global()?;
            let mut node_impl = node.node_impl();
            let entry_id = node_impl.propose_async(op)?;
            node.main_loop.wakeup();
            // Release the lock
            drop(node_impl);
            Ok(entry_id.index)
        }),
    );

    luamod_set(
        &l,
        "_schema_change_cas_request",
        indoc! {"
        pico._schema_change_cas_request(op, index, timeout)
        ============================

        Internal API, see src/luamod.rs for the details.

        Params:

            1. op (table | string)
            2. index (number)
            3. timeout (number)

        Returns:

            (number, number) raft index, raft term
            or
            (nil, string) in case of an error
        "},
        tlua::Function::new(
            |lua: tlua::StaticLua| -> traft::Result<(RaftIndex, RaftTerm)> {
                use tlua::{AnyLuaString, AsLua, LuaError, LuaTable};

                let mp: AnyLuaString;
                let t: Option<LuaTable<_>> = (&lua).read_at(1).ok();
                if let Some(t) = t {
                    // We do [lua value -> msgpack -> rust -> msgpack]
                    // instead of [lua value -> rust -> msgpack]
                    // because despite what it may seem this is much simpler.
                    // (The final [-> msgpack] is when we eventually do the rpc).
                    // The transmition medium is always msgpack.
                    mp = lua
                        .eval_with("return require 'msgpack'.encode(...)", &t)
                        .map_err(LuaError::from)?;
                } else {
                    let s: Option<AnyLuaString> = (&lua).read_at(1).ok();
                    if let Some(s) = s {
                        mp = s;
                    } else {
                        return Err(Error::other("op should be a table or a string"));
                    }
                }

                let op: Op = Decode::decode(mp.as_bytes())?;
                if !op.is_schema_change() {
                    return Err(Error::other(
                        "only schema changing operations are supported",
                    ));
                }

                let index: RaftIndex = (&lua).read_at(2).map_err(|(_, e)| LuaError::from(e))?;

                let timeout: f64 = (&lua).read_at(3).map_err(|(_, e)| LuaError::from(e))?;
                let timeout = duration_from_secs_f64_clamped(timeout);

                let node = node::global()?;
                let term = raft::Storage::term(&node.raft_storage, index)?;
                let predicate = cas::Predicate {
                    index,
                    term,
                    ranges: cas::schema_change_ranges().into(),
                };

                let res = compare_and_swap(op, predicate, effective_user_id(), timeout)?;
                Ok(res)
            },
        ),
    );

    ///////////////////////////////////////////////////////////////////////////
    #[rustfmt::skip]
    luamod_set(
        &l,
        "exit",
        indoc! {"
        pico.exit([code])
        =================

        Terminates the picodata process with the supplied exit code.

        Params:

            1. code (optional number), default: 0
        "},
        tlua::function1(|code: Option<i32>| {
            crate::tarantool::exit(code.unwrap_or(0))
        }),
    );

    ///////////////////////////////////////////////////////////////////////////
    luamod_set(
        &l,
        "expel",
        indoc! {"
        pico.expel(instance_id)
        ======================

        Expels an instance with instance_id from the cluster. The instance will
        keep on running though. If restarted, it will not join the cluster.

        Params:

            1. instance_id (string)

        Returns:

            (true)
            or
            (nil, string) in case of an error
        "},
        tlua::function1(|instance_id: InstanceId| -> traft::Result<bool> {
            let raft_storage = &traft::node::global()?.raft_storage;
            let cluster_id = raft_storage.cluster_id()?;
            fiber::block_on(rpc::network_call_to_leader(&rpc::expel::Request {
                instance_id,
                cluster_id,
            }))?;
            Ok(true)
        }),
    );

    // log
    ///////////////////////////////////////////////////////////////////////////
    l.get::<tlua::LuaTable<_>, _>("pico")
        .unwrap()
        .set("log", &[()]);
    #[rustfmt::skip]
    l.exec_with(
        "pico.log.highlight_key = ...",
        tlua::function2(|key: String, color: Option<String>| -> Result<(), String> {
            let color = match color.as_deref() {
                None            => None,
                Some("red")     => Some(tlog::Color::Red),
                Some("green")   => Some(tlog::Color::Green),
                Some("blue")    => Some(tlog::Color::Blue),
                Some("cyan")    => Some(tlog::Color::Cyan),
                Some("yellow")  => Some(tlog::Color::Yellow),
                Some("magenta") => Some(tlog::Color::Magenta),
                Some("white")   => Some(tlog::Color::White),
                Some("black")   => Some(tlog::Color::Black),
                Some(other) => {
                    return Err(format!("unknown color: {other:?}"))
                }
            };
            tlog::highlight_key(key, color);
            Ok(())
        }),
    )
    .unwrap();
    l.exec_with(
        "pico.log.clear_highlight = ...",
        tlua::function0(tlog::clear_highlight),
    )
    .unwrap();

    ///////////////////////////////////////////////////////////////////////////
    #[derive(::tarantool::tlua::LuaRead, Default, Clone, Copy)]
    enum Justify {
        Left,
        #[default]
        Center,
        Right,
    }
    #[derive(::tarantool::tlua::LuaRead)]
    struct RaftLogOpts {
        justify_contents: Option<Justify>,
        max_width: Option<usize>,
    }
    luamod_set(
        &l,
        "raft_log",
        indoc! {"
        pico.raft_log([opts])
        ====================================

        Inspect raft log contents in human readable format. The contents are
        returned as a table of strings similarly to how fselect works for
        spaces.

        opt.justify_contents can be used to control how contents are justified.

        If opts.max_width is specified the output will not be wider than this
        many printable characters. If not specified the terminal width is
        used, unless it is called in context of a remote session.

        Params:

            1. opts (table)
                - justify_contents (string), one of 'center' | 'left' | 'right', default: 'center'
                - max_width (number), default for remote context: 80

        Returns:

            (table)

        Example:

            pico.raft_log({justify_contents = 'center', max_width = 100})
        "},
        tlua::function1(
            |opts: Option<RaftLogOpts>| -> traft::Result<Option<Vec<String>>> {
                let mut justify_contents = Default::default();
                let mut max_width = None;
                if let Some(opts) = opts {
                    justify_contents = opts.justify_contents.unwrap_or_default();
                    max_width = opts.max_width;
                }
                let remote_ctx =
                    crate::tarantool::eval("return box.session.peer() ~= nil").unwrap();
                let max_width = crate::unwrap_some_or!(
                    max_width,
                    if remote_ctx {
                        80
                    } else {
                        // Need to subtract 4, because this is how many symbols
                        // are prepended by the console when output format is yaml.
                        crate::util::screen_size().1 as usize - 5
                    }
                );

                let header = ["index", "term", "contents"];
                let [index, term, contents] = header;
                let mut rows = vec![];
                let mut col_widths = header.map(|h| h.len());
                let node = traft::node::global()?;
                let entries = node
                    .all_traft_entries()
                    .map_err(|e| traft::error::Error::Other(Box::new(e)))?;
                for entry in entries {
                    let row = [
                        entry.index.to_string(),
                        entry.term.to_string(),
                        entry.payload().to_string(),
                    ];
                    for i in 0..col_widths.len() {
                        col_widths[i] = col_widths[i].max(row[i].len());
                    }
                    rows.push(row);
                }
                let [iw, tw, mut cw] = col_widths;

                let total_width = 1 + header.len() + col_widths.iter().sum::<usize>();
                if total_width > max_width {
                    match cw.checked_sub(total_width - max_width) {
                        Some(new_cw) if new_cw > 0 => cw = new_cw,
                        _ => {
                            return Err(traft::error::Error::other("screen too small"));
                        }
                    }
                }

                use std::io::Write;
                let mut buf: Vec<u8> = Vec::with_capacity(512);
                let write_contents = move |buf: &mut Vec<u8>, contents: &str| match justify_contents
                {
                    Justify::Left => writeln!(buf, "{contents: <cw$}|"),
                    Justify::Center => writeln!(buf, "{contents: ^cw$}|"),
                    Justify::Right => writeln!(buf, "{contents: >cw$}|"),
                };

                let row_sep = |buf: &mut Vec<u8>| {
                    match justify_contents {
                        Justify::Left => {
                            // NOTE: here and later a special unicode character \u{200b} is used.
                            // This is a ZERO WIDTH SPACE and it helps with the tarantool's console.
                            // The way it works is that tarantool's console when printing the values
                            // returned from a function will surround string values with quotes if
                            // for instance they start with a '|' pipe character, which is our case.
                            // Adding a space before '|' doesn't help but a ZERO WIDTH SPACE
                            // for what ever reason does. So this is basically a crutch,
                            // but if it's good enough for tarantool developers, it's good enough for us.
                            writeln!(buf, "\u{200b}+{0:-^iw$}+{0:-^tw$}+{0:-<cw$}+", "")
                        }
                        Justify::Center => {
                            writeln!(buf, "\u{200b}+{0:-^iw$}+{0:-^tw$}+{0:-^cw$}+", "")
                        }
                        Justify::Right => {
                            writeln!(buf, "\u{200b}+{0:-^iw$}+{0:-^tw$}+{0:->cw$}+", "")
                        }
                    }
                    .unwrap()
                };
                row_sep(&mut buf);
                write!(buf, "\u{200b}|{index: ^iw$}|{term: ^tw$}|").unwrap();
                write_contents(&mut buf, contents).unwrap();
                row_sep(&mut buf);
                for [index, term, contents] in rows {
                    if contents.len() <= cw {
                        write!(buf, "\u{200b}|{index: ^iw$}|{term: ^tw$}|").unwrap();
                        write_contents(&mut buf, &contents).unwrap();
                    } else {
                        write!(buf, "\u{200b}|{index: ^iw$}|{term: ^tw$}|").unwrap();
                        write_contents(&mut buf, &contents[..cw]).unwrap();
                        let mut rest = &contents[cw..];
                        while !rest.is_empty() {
                            let clamped_cw = usize::min(rest.len(), cw);
                            write!(buf, "\u{200b}|{blank: ^iw$}|{blank: ^tw$}|", blank = "~",)
                                .unwrap();
                            write_contents(&mut buf, &rest[..clamped_cw]).unwrap();
                            rest = &rest[clamped_cw..];
                        }
                    }
                }
                row_sep(&mut buf);

                let s = String::from_utf8_lossy(&buf);
                let mut res = vec![];
                for line in s.lines() {
                    res.push(line.into());
                }
                Ok(Some(res))
            },
        ),
    );

    ///////////////////////////////////////////////////////////////////////////
    luamod_set(
        &l,
        "raft_compact_log",
        indoc! {"
        pico.raft_compact_log(up_to)
        ============================

        Trims raft log up to the given index (excluding the index itself).
        Returns the new first_index after the log compaction. Returns when
        transaction is committed to the local storage.

        Params:

            1. up_to (number)

        Returns:

            (number)
        "},
        {
            tlua::function1(|up_to: RaftIndex| -> traft::Result<RaftIndex> {
                let raft_storage = &node::global()?.raft_storage;
                let ret = transaction(|| raft_storage.compact_log(up_to));
                Ok(ret?)
            })
        },
    );

    ///////////////////////////////////////////////////////////////////////////
    luamod_set(
        &l,
        "cas",
        indoc! {"
        pico.cas(dml[, predicate])
        ==========================

        Performs a clusterwide compare-and-swap operation. Works for global
        tables only.

        E.g. it checks the `predicate` on leader and, if no conflicting entries
        were found, appends the new entry to the raft log and returns its index
        (uncommitted yet).
        The `predicate` consists of three parts:
        - index
        - term
        - ranges

        If `predicate` is not supplied, it will be auto generated with `index`
        and `term` taken from the current instance and with empty `ranges`.
        Omitting `ranges` implies a blind write, therefore such usage is
        discouraged.

        Returns when the operation is appended to the raft log on a leader.
        Returns the index of the corresponding Op::Dml.

        Params:

            1. dml (table)
                - kind (string), one of 'insert' | 'replace' | 'update' | 'delete'
                - table (string)
                - tuple (optional table), mandatory for insert and replace, see [1, 2]
                - key (optional table), mandatory for update and delete, see [3, 4]
                - ops (optional table), mandatory for update see [3]

            2. predicate (optional table)
                - index (optional number), default: current applied index
                - term (optional number), default: current term
                - ranges (optional table {table CasRange,...}), see pico.help('table CasRange'),
                    default: {} (empty table)

        Returns:

            (number)
            or
            (nil, string) in case of an error

        See also:

            [1]: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/insert/
            [2]: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/replace/
            [3]: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/update/
            [4]: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/delete/

        Example:

            -- Assuming there exists a table 'friends_of_peppa' with two
            -- fields: id (unsigned) and name (string) and corresponding
            -- unique indexes.

            -- Insert a tuple {1, 'Suzy'} into this table. This will fail
            -- if the term of the current instance is outdated.
            pico.cas({
                kind = 'insert',
                table = 'friends_of_peppa',
                tuple = {1, 'Suzy'},
            })

            -- Add Rebecca, but only if no other friends were added after Suzy.
            pico.cas({
                kind = 'replace',
                table = 'friends_of_peppa',
                tuple = {2, 'Rebecca'},
            }, {
                ranges = {{
                    table = 'friends_of_peppa',
                    key_min = { kind = 'excluded', key = {1,} },
                    key_max = { kind = 'unbounded' },
                }},
            })

            -- Check that there were no other updates, and update the second
            -- Peppa friend, replacing 'Rebecca' with 'Emily'.
            pico.cas({
                kind = 'update',
                table = 'friends_of_peppa',
                key = {2},
                ops = {{'=', 2, 'Emily'}},
            }, {
                ranges = {{
                    table = 'friends_of_peppa',
                    key_min = { kind = 'included', key = {2} },
                    key_max = { kind = 'included', key = {2} },
                }},
            })

            -- Delete the second Peppa friend, specifying index and term
            -- explicitly. It's necessary when there are some yielding
            -- operations between reading and writing.
            index, term =
                assert(pico.raft_get_index()),
                assert(pico.raft_status()).term
            emily = box.space.friends_of_peppa.index.name:get('Emily')
            fiber.sleep(1) -- do something yielding

            pico.cas({
                kind = 'delete',
                table = 'friends_of_peppa',
                key = {emily.id},
            }, {
                index = index,
                term = term,
                ranges = {{
                    table = 'friends_of_peppa',
                    key_min = { kind = 'included', key = {emily.id} },
                    key_max = { kind = 'included', key = {emily.id} },
                }},
            })
        "},
        tlua::function2(
            |op: op::DmlInLua,
             predicate: Option<cas::PredicateInLua>|
             -> traft::Result<RaftIndex> {
                // su is needed here because for cas execution we need to consult with system spaces like `_raft_state`
                // and the user executing cas request may not (even shouldnt) have access to these spaces
                let su = session::su(ADMIN_ID)?;

                let op = op::Dml::from_lua_args(op, su.original_user_id)
                    .map_err(traft::error::Error::other)?;
                let predicate = cas::Predicate::from_lua_args(predicate.unwrap_or_default())?;
                let (index, _) = compare_and_swap(
                    op.into(),
                    predicate,
                    su.original_user_id,
                    Duration::from_secs(3),
                )?;
                Ok(index)
            },
        ),
    );

    luamod_set(
        &l,
        "batch_cas",
        indoc! {"
        pico.batch_cas(ops[, predicate])
        ======================
            The same as pico.cas, but allows multiple dml operations.
        "},
        tlua::function2(
            |arg: op::BatchDmlInLua,
             predicate: Option<cas::PredicateInLua>|
             -> traft::Result<RaftIndex> {
                // su is needed here because for cas execution we need to consult with system spaces like `_raft_state`
                // and the user executing cas request may not (even shouldnt) have access to these spaces
                let su = session::su(ADMIN_ID)?;

                let mut dmls = Vec::with_capacity(arg.ops.len());
                for op in arg.ops {
                    dmls.push(
                        op::Dml::from_lua_args(op, su.original_user_id)
                            .map_err(traft::error::Error::other)?,
                    )
                }
                let predicate = cas::Predicate::from_lua_args(predicate.unwrap_or_default())?;
                let (index, _) = compare_and_swap(
                    Op::BatchDml { ops: dmls },
                    predicate,
                    su.original_user_id,
                    Duration::from_secs(3),
                )?;
                Ok(index)
            },
        ),
    );

    luamod_set_help_only(
        &l,
        "table CasRange",
        indoc! {"
        table CasRange
        ==============

        CasRange is a Lua table describing a range to be used as a
        compare-and-swap predicate, see pico.help('cas')

        Fields:

            - table (string)
            - key_min (table CasBound), see pico.help('table CasBound')
            - key_max (table CasBound)

        Example:

            local unbounded = { kind = 'unbounded' }
            local including_1 = { kind = 'included', key = {1,} }
            local excluding_3 = { kind = 'excluded', key = {3,} }

            local range_a = {
                table = 'friends_of_peppa',
                key_min = unbounded,
                key_max = unbounded,
            }

            -- [1, 3)
            local range_a = {
                table = 'friends_of_peppa',
                key_min = including_1,
                key_max = excluding_3,
            }
        "},
    );
    luamod_set_help_only(
        &l,
        "table CasBound",
        indoc! {"
        table CasBound
        ==============

        A Lua table representing a range bound (either min or max) used in
        CasRange, see pico.help('table CasRange')

        Fields:

            - kind (string), one of 'included' | 'excluded' | 'unbounded'
            - key (optional table), mandatory for included and excluded
        "},
    );

    luamod_set(
        &l,
        "_check_create_table_opts",
        indoc! {"
        pico._check_create_table_opts(opts)
        =================================

        Internal API, see src/luamod.rs for the details.

        Params:

            1. opts (table)

        Returns:

            (true)
            or
            (nil, string) in case of an error
        "},
        tlua::function1(|params: CreateTableParams| -> traft::Result<bool> {
            params.validate()?;
            Ok(true)
        }),
    );
    luamod_set(
        &l,
        "_make_create_table_op_if_needed",
        indoc! {"
        pico._make_create_table_op_if_needed(opts)
        =================================

        Internal API, see src/luamod.rs for the details.

        Params:

            1. opts (table), space create opts

        Returns:

            (string) raft op encoded as msgpack
            or
            (nil) in case no operation is needed
            or
            (nil, string) in case of conflict
        "},
        tlua::function1(
            |mut params: CreateTableParams| -> traft::Result<Option<tlua::AnyLuaString>> {
                let storage = &node::global()?.storage;
                if params.space_exists()? {
                    return Ok(None);
                }
                params.choose_id_if_not_specified()?;
                params.test_create_space(storage)?;
                let ddl = params.into_ddl()?;
                let schema_version = storage.properties.next_schema_version()?;
                let op = Op::DdlPrepare {
                    schema_version,
                    ddl,
                };
                // FIXME: this is stupid, we serialize op into msgpack just to
                // pass it via lua into another rust callback where it will be
                // deserialized back, just to get serialized once again for
                // the rpc (not to mention that inside the rpc it will once
                // again be serialized to be put into the raft log). It is
                // however much better than converting this op to a lua value
                // and back. Anyway this shouldn't be a perf problem because
                // this is not a very hot function.
                let mp = rmp_serde::to_vec_named(&op).expect("raft op shouldn't fail to serialize");
                Ok(Some(tlua::AnyLuaString(mp)))
            },
        ),
    );
    luamod_set(
        &l,
        "abort_ddl",
        indoc! {"
        pico.abort_ddl([timeout])
        =======================

        Aborts a pending schema change.

        Returns an index of the corresponding Op::DdlAbort raft entry.
        Returns an error if there is no pending schema change operation.

        Params:

            1. timeout (optional number), in seconds, default: infinity

        Returns:

            (number)
            or
            (nil, string) in case of an error
        "},
        {
            tlua::function1(|timeout: Option<f64>| -> traft::Result<RaftIndex> {
                let timeout = if let Some(timeout) = timeout {
                    duration_from_secs_f64_clamped(timeout)
                } else {
                    INFINITY
                };
                schema::abort_ddl(timeout)
            })
        },
    );
    luamod_set(
        &l,
        "wait_ddl_finalize",
        indoc! {"
        pico.wait_ddl_finalize(index, [opts])
        =======================

        Waits for the ddl operation at given raft index to be finalized.

        Returns raft index of the finilizing entry.

        Params:

            1. index (number), raft index
            2. opts (optional table)
                - timeout (optional number), in seconds, default: infinity

        Returns:

            (number) raft index
            or
            (nil, string) in case of an error
        "},
        {
            #[derive(::tarantool::tlua::LuaRead)]
            struct Opts {
                timeout: Option<f64>,
            }
            tlua::Function::new(
                |index: RaftIndex, opts: Option<Opts>| -> traft::Result<RaftIndex> {
                    let mut timeout = INFINITY;
                    if let Some(opts) = opts {
                        if let Some(t) = opts.timeout {
                            timeout = duration_from_secs_f64_clamped(t);
                        }
                    }
                    let commit_index = schema::wait_for_ddl_commit(index, timeout)?;
                    Ok(commit_index)
                },
            )
        },
    );
    luamod_set_help_only(
        &l,
        "table TableField",
        indoc! {"
        table TableField
        ================

        A Lua table describing a field in a table, see [1].

        Fields:

            - name (string)
            - type (string), see [2]
            - is_nullable (boolean)

        Example:

            {name = 'id', type = 'unsigned', is_nullable = false}
            {name = 'value', type = 'unsigned', is_nullable = false}

        See also:

            [1]: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/format/
            [2]: https://docs.rs/tarantool/latest/tarantool/space/enum.FieldType.html
        "},
    );
    luamod_set(
        &l,
        "raft_term",
        indoc! {"
        pico.raft_term([index])
        =======================

        Returns term of the raft entry at the given index or the current term if
        no argument was specified.

        Params:

            1. index (optional number), raft index

        Returns:

            (number)
            or
            (nil, string) in case of an error
        "},
        {
            tlua::function1(|index: Option<RaftIndex>| -> traft::Result<RaftTerm> {
                let node = node::global()?;
                let term = if let Some(index) = index {
                    raft::Storage::term(&node.raft_storage, index)?
                } else {
                    node.status().term
                };
                Ok(term)
            })
        },
    );

    luamod_set(
        &l,
        "_is_retriable_error_message",
        indoc! {"
        pico._is_retriable_error_message(msg)
        ============================

        Internal API, see src/luamod.rs for the details.

        Params:

            1. msg (string)

        Returns:

            (bool)
        "},
        tlua::Function::new(|msg: String| -> bool {
            crate::traft::error::is_retriable_error_message(&msg)
        }),
    );

    #[cfg(feature = "error_injection")]
    luamod_set(
        &l,
        "_inject_error",
        indoc! {"
        pico._inject_error(error, enable)

        Internal API, see src/luamod.rs for the details.

        Params:

            1. error (string)
            2. enable (bool)
        "},
        {
            tlog!(Info, "error injection enabled");
            tlua::Function::new(|error: String, enable: bool| {
                crate::error_injection::enable(&error, enable);
            })
        },
    );
}

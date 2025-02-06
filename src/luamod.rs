//! Lua API exported as `_G.pico`
//!

use crate::cas;
use crate::config::PicodataConfig;
use crate::instance::InstanceName;
use crate::plugin;
use crate::plugin::InheritOpts;
use crate::plugin::PluginIdentifier;
use crate::plugin::TopologyUpdateOpKind;
use crate::rpc;
use crate::schema::{self, ADMIN_ID};
use crate::sync;
#[allow(unused_imports)]
use crate::tlog;
use crate::traft::op::{self, Op};
use crate::traft::{self, node, RaftIndex, RaftTerm};
use crate::util::duration_from_secs_f64_clamped;
use crate::util::INFINITY;
use ::tarantool::error::BoxError;
use ::tarantool::fiber;
use ::tarantool::msgpack::ViaMsgpack;
use ::tarantool::session;
use ::tarantool::tlua;
use ::tarantool::tlua::{LuaState, LuaThread, PushOneInto, Void};
use ::tarantool::transaction::transaction;
use ::tarantool::tuple::Decode;
use ::tarantool::vclock::Vclock;
use indoc::formatdoc;
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
    let help_table: tlua::LuaTable<_> = luamod.metatable();
    help_table.set(name, help);
}

#[inline(always)]
fn luamod_set_help_only(l: &LuaThread, name: &str, help: &str) {
    let luamod: tlua::LuaTable<_> = l.get("pico").unwrap();
    let help_table: tlua::LuaTable<_> = luamod.metatable();
    help_table.set(name, help);
}

pub(crate) fn setup() {
    let l = ::tarantool::lua_state();
    l.exec(include_str!("luamod.lua")).unwrap();

    luamod_set(
        &l,
        "PICODATA_VERSION",
        &formatdoc! {"
        pico.PICODATA_VERSION
        =====================

        A string variable (not a function) containing Picodata version
        which follows the Calendar Versioning convention with the
        `YY.0M.MICRO` scheme:

            https://calver.org/#scheme

        Example:

            picodata> pico.PICODATA_VERSION
            ---
            - {version}
            ...
        ",
        version = env!("CARGO_PKG_VERSION")
        },
        crate::info::PICODATA_VERSION,
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
            - 5.0.0
            ...
        "},
        "5.0.0",
    );

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
                cluster_name: demo
            - instance:
                log_level: info
                listen: 127.0.0.1:3301
                instance_dir: .
                peers:
                  - 127.0.0.1:3301
            ...
        "},
        tlua::Function::new(move || -> ViaMsgpack<rmpv::Value> {
            ViaMsgpack(PicodataConfig::get().parameters_with_sources_as_rmpv())
        }),
    );

    luamod_set(
        &l,
        "vshard_config",
        indoc! {"
        pico.vshard_config()
        =========

        Returns a Lua table containing current vshard configuration.

        Returns:

            (table)

        "},
        tlua::Function::new(move || -> traft::Result<_> {
            let node = node::global()?;
            let tier = node
                .raft_storage
                .tier()?
                .expect("tier for instance should exists");
            let config = crate::vshard::VshardConfig::from_storage(&node.storage, &tier)?;
            Ok(config)
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
            - cluster_name (string)
            - instance_name (string)
            - tier (string)

        Example:

            picodata> pico.whoami()
            ---
            - raft_id: 1
              cluster_name: demo
              instance_name: i1
              tier: storage
            ...
        "},
        tlua::function0(|| -> traft::Result<_> {
            let node = traft::node::global()?;
            let info = crate::info::InstanceInfo::try_get(node, None)?;

            Ok(tlua::AsTable((
                ("raft_id", info.raft_id),
                ("cluster_name", info.cluster_name),
                ("instance_name", info.name),
                ("tier", info.tier),
            )))
        }),
    );

    luamod_set(
        &l,
        "instance_info",
        indoc! {"
        pico.instance_info([instance name])
        =================================

        Provides general information for the given instance.

        Params:

            1. instance name (optional string), default: id of the current instance

        Returns:

            (table)
            or
            (nil, string) in case of an error

        Fields:

            - raft_id (number)
            - advertise_address (string)
            - name (string)
            - uuid (string)
            - replicaset_name (string)
            - replicaset_uuid (string)
            - current_state (table),
                `{variant = string, incarnation = number}`, where variant is one of
                'Offline' | 'Online' | 'Expelled'
            - target_state (table),
                `{variant = string, incarnation = number}`, where variant is one of
                'Offline' | 'Replicated' | 'ShardingInitialized' | 'Online' | 'Expelled'
            - tier (string)
            - picodata_version (string)

        Example:

            picodata> pico.instance_info()
            ---
            - raft_id: 1
              advertise_address: 127.0.0.1:3301
              name: i1
              uuid: 68d4a766-4144-3248-aeb4-e212356716e4
              tier: storage
              replicaset_name: r1
              replicaset_uuid: e0df68c5-e7f9-395f-86b3-30ad9e1b7b07
              current_state:
                variant: Online
                incarnation: 26
              target_state:
                variant: Online
                incarnation: 26
            ...
        "},
        tlua::function1(|iid: Option<InstanceName>| -> traft::Result<_> {
            let node = traft::node::global()?;
            let info = crate::info::InstanceInfo::try_get(node, iid.as_ref())?;

            Ok(tlua::AsTable((
                ("raft_id", info.raft_id),
                ("iproto_advertise", info.iproto_advertise),
                ("name", info.name.0),
                ("uuid", info.uuid),
                ("replicaset_name", info.replicaset_name),
                ("replicaset_uuid", info.replicaset_uuid),
                ("current_state", info.current_state),
                ("target_state", info.target_state),
                ("tier", info.tier),
                ("picodata_version", info.picodata_version),
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

    ///////////////////////////////////////////////////////////////////////////
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

    ///////////////////////////////////////////////////////////////////////////
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

        Returns:

            (number) raft index of the nop entry
        "},
        tlua::function0(|| traft::node::global()?.propose_nop()),
    );

    luamod_set(
        &l,
        "raft_propose",
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
        #[allow(clippy::unused_unit)] // Needed to suppress dependency_on_unit_never_type_fallback
        tlua::function1(|code: Option<i32>| -> () {
            crate::tarantool::exit(code.unwrap_or(0));
        }),
    );

    ///////////////////////////////////////////////////////////////////////////
    luamod_set(
        &l,
        "expel",
        indoc! {"
        pico.expel(instance_name)
        ======================

        Expels an instance with instance_name from the cluster. The instance will
        keep on running though. If restarted, it will not join the cluster.

        Params:

            1. instance_name (string)
            2. opts (table)
                - timeout (number)

        Returns:

            (true)
            or
            (nil, string) in case of an error
        "},
        tlua::Function::new(
            |instance_name: InstanceName, opts: Option<PicoExpelOptions>| -> traft::Result<bool> {
                let mut timeout = Duration::from_secs(3600);
                if let Some(opts) = opts {
                    if let Some(t) = opts.timeout {
                        timeout = Duration::from_secs_f64(t);
                    }
                }

                let node = traft::node::global()?;
                let raft_storage = &node.raft_storage;
                let instance = node.storage.instances.get(&instance_name)?;
                let cluster_name = raft_storage.cluster_name()?;
                fiber::block_on(rpc::network_call_to_leader(
                    crate::proc_name!(rpc::expel::proc_expel),
                    &rpc::expel::Request {
                        instance_uuid: instance.uuid,
                        cluster_name,
                        timeout,
                    },
                ))?;
                Ok(true)
            },
        ),
    );
    #[derive(tlua::LuaRead)]
    struct PicoExpelOptions {
        timeout: Option<f64>,
    }

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
        remote: Option<String>,
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
                - remote (string), address of a remote picodata instance from
                  which the _raft_log space contents will be fetched.

        Returns:

            (table)

        Example:

            pico.raft_log({justify_contents = 'center', max_width = 100})
        "},
        tlua::function1(
            |opts: Option<RaftLogOpts>| -> traft::Result<Option<Vec<String>>> {
                let mut justify_contents = Default::default();
                let mut opts_max_width = None;
                let mut address = None;
                if let Some(opts) = opts {
                    justify_contents = opts.justify_contents.unwrap_or_default();
                    opts_max_width = opts.max_width;
                    address = opts.remote;
                }

                let remote_ctx: bool =
                    crate::tarantool::eval("return box.session.peer() ~= nil").unwrap();
                let terminal_width = crate::util::screen_size().1 as usize;
                let mut max_width = 80;
                if let Some(width) = opts_max_width {
                    max_width = width;
                } else if !remote_ctx && terminal_width >= 5 {
                    // Need to subtract 5, because this is how many symbols
                    // are prepended by the console when output format is yaml.
                    max_width = terminal_width - 5;
                }

                let header = ["index", "term", "contents"];
                let [index, term, contents] = header;
                let mut rows = vec![];
                let mut col_widths = header.map(|h| h.len());

                let entries;
                if let Some(address) = &address {
                    let f = crate::rpc::network_call_raw(
                        address,
                        "LUA",
                        &["return box.space._raft_log:select()"],
                    );
                    entries = fiber::block_on(f)?;
                } else {
                    let node = traft::node::global()?;
                    entries = node
                        .all_traft_entries()
                        .map_err(|e| traft::error::Error::Other(Box::new(e)))?;
                }

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
                            writeln!(buf, "+{0:-^iw$}+{0:-^tw$}+{0:-<cw$}+", "")
                        }
                        Justify::Center => {
                            writeln!(buf, "+{0:-^iw$}+{0:-^tw$}+{0:-^cw$}+", "")
                        }
                        Justify::Right => {
                            writeln!(buf, "+{0:-^iw$}+{0:-^tw$}+{0:->cw$}+", "")
                        }
                    }
                    .unwrap()
                };
                row_sep(&mut buf);
                // NOTE: here and later we use the special uincode \u{01c0} symbol.
                // It looks like this `ǀ` (exactly like `|`). We do this,
                // because tarantool's yaml handler has special behavior when it
                // sees a pipe character at the start of the string. This is the
                // same thing as what tarantool's `index:fselect()` is doing.
                write!(buf, "\u{01c0}{index: ^iw$}|{term: ^tw$}|").unwrap();
                write_contents(&mut buf, contents).unwrap();
                row_sep(&mut buf);
                for [index, term, contents] in rows {
                    if contents.chars().count() <= cw {
                        write!(buf, "\u{01c0}{index: ^iw$}|{term: ^tw$}|").unwrap();
                        write_contents(&mut buf, &contents).unwrap();
                    } else {
                        // use crate::util::TokenType::*;
                        let mut lexer = crate::util::Lexer::new(&contents);
                        let first_token = *lexer.peek_token().expect("contents is not empyt");

                        // Do some extra handling for BatchDml entries so that they look better
                        let mut args_on_separate_rows = false;
                        let mut paren_depth = 0;
                        if first_token.text == "BatchDml" {
                            args_on_separate_rows = true;
                        }

                        let mut prev_token = first_token;
                        loop {
                            let mut start = 0;
                            let mut end = 0;
                            let mut utf8_len = 0;
                            let mut first = true;
                            while let Some(&token) = lexer.peek_token() {
                                if lexer.token_counter % 1000 == 0 {
                                    // Yield to other fibers so that the event loop is not blocked in case of huge raft log.
                                    fiber::reschedule();
                                }

                                let mut added_chars = token.utf8_count;
                                if first {
                                    first = false;
                                    start = token.start;
                                    end = token.start;
                                } else {
                                    // also count the spaces between this and previous token
                                    added_chars += token.start - prev_token.end;
                                }

                                // XXX: currently if a token is longer then the
                                // allotted width, we'll just ingore the contraint
                                // and make an ugly row. We could subdivide such tokens
                                // even further, but I don't want to make this code
                                // even more complicated than it already is...
                                let new_token_will_overflow = utf8_len + added_chars > cw;
                                let added_at_least_one_token = start != end;
                                if new_token_will_overflow && added_at_least_one_token {
                                    break;
                                }

                                if args_on_separate_rows
                                    && added_at_least_one_token
                                    // This depth doesn't take into account the current ")" itself
                                    && paren_depth == 1
                                    && token.text == ")"
                                {
                                    break;
                                }

                                // At this point the new token is added to the current piece
                                utf8_len += added_chars;
                                end = token.end;
                                _ = lexer.next_token();

                                // If we had `defer` this would've been so much cleaner...
                                let prev_token_ = prev_token;
                                // Must be assigned before `break`
                                prev_token = token;

                                match token.text {
                                    "(" => paren_depth += 1,
                                    ")" => paren_depth -= 1,
                                    _ => {}
                                }

                                if args_on_separate_rows && paren_depth == 1 {
                                    if token.text == "(" {
                                        break;
                                    }

                                    if prev_token_.text == ")" {
                                        break;
                                    }
                                }
                            }

                            if start == 0 {
                                write!(buf, "\u{01c0}{index: ^iw$}|{term: ^tw$}|").unwrap();
                            } else {
                                write!(buf, "\u{01c0}{blank: ^iw$}|{blank: ^tw$}|", blank = "~",)
                                    .unwrap();
                            }

                            write_contents(&mut buf, &contents[start..end]).unwrap();
                            if end == contents.len() {
                                break;
                            }
                        }
                    }
                }
                row_sep(&mut buf);

                let s = String::from_utf8_lossy(&buf);
                let mut res = vec![];
                for line in s.lines() {
                    // replace all spaces with the non-breaking space character
                    // to prevent tarantool's yaml handler from breaking the
                    // rows which we tried so hard to format correctly
                    let line = line.replace(' ', "\u{a0}");
                    res.push(line);
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
                let node = node::global()?;
                let applied = node.get_index();
                let up_to = up_to.min(applied + 1);
                let raft_storage = &node.raft_storage;
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
                let req = crate::cas::Request::new(op, predicate, su.original_user_id)?;
                let deadline = fiber::clock().saturating_add(Duration::from_secs(3));
                let res = cas::compare_and_swap(&req, false, false, deadline)?;
                let (index, _) = res.no_retries()?;
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
                let req = crate::cas::Request::new(
                    Op::BatchDml { ops: dmls },
                    predicate,
                    su.original_user_id,
                )?;
                let deadline = fiber::clock().saturating_add(Duration::from_secs(3));
                let res = cas::compare_and_swap(&req, false, false, deadline)?;
                let (index, _) = res.no_retries()?;
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
                let deadline = fiber::clock().saturating_add(timeout);
                schema::abort_ddl(deadline)
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

    ///////////////////////////////////////////////////////////////////////////
    #[rustfmt::skip]
    luamod_set(
        &l,
        "install_plugin",
        indoc! {"
        pico.install_plugin(name, version, [opts])
        =================

        Install a new plugin on cluster.

        Params:

            1. name - plugin name, manifest with same name must exists in plugin_dir
            2. version - plugin version
            3. opts (optional table)
                - timeout (optional number), in seconds, default: 10
                - if_not_exists (optional bool), default: false
                - inherit_config (optional bool), inherit config from previous installed version, default: false
                - inherit_topology (optional bool), inherit topology from previous installed version, default: false
        "},
        {
            #[derive(::tarantool::tlua::LuaRead)]
            struct Opts {
                timeout: Option<f64>,
                if_not_exists: Option<bool>,
                inherit_config: Option<bool>,
                inherit_topology: Option<bool>,
            }
            tlua::function3(|name: String, version: String, opts: Option<Opts>| -> traft::Result<()> {
                let mut timeout = Duration::from_secs(10);
                let mut if_not_exists = false;
                let mut inherit_opts = InheritOpts::default();
                if let Some(opts) = opts {
                    if_not_exists = opts.if_not_exists.unwrap_or_default();
                    inherit_opts.config = opts.inherit_config.unwrap_or_default();
                    inherit_opts.topology = opts.inherit_topology.unwrap_or_default();
                    if let Some(t) = opts.timeout {
                        timeout = duration_from_secs_f64_clamped(t);
                    }
                }
                plugin::create_plugin(PluginIdentifier::new(name, version), timeout, if_not_exists, inherit_opts)
            })
        },
    );

    ///////////////////////////////////////////////////////////////////////////
    #[rustfmt::skip]
    luamod_set(
        &l,
        "enable_plugin",
        indoc! {"
        pico.enable_plugin(name, version, [opts])
        =================

        Enable plugin in cluster.

        Params:

            1. name - plugin name, plugin should already be installed with `pico.install_plugin` command
            2. version - plugin version
            3. opts (optional table)
                - on_start_timeout (optional number), in seconds, default: 5
                - timeout (optional number), in seconds, default: 10
        "},
        {
            #[derive(::tarantool::tlua::LuaRead)]
            struct Opts {
                timeout: Option<f64>,
                on_start_timeout: Option<f64>,
            }
            tlua::function3(|name: String, version: String, opts: Option<Opts>| -> traft::Result<()> {
                let mut on_start_timeout = Duration::from_secs(5);
                let mut timeout = Duration::from_secs(10);
                if let Some(opts) = opts {
                    if let Some(t) = opts.on_start_timeout {
                        on_start_timeout = duration_from_secs_f64_clamped(t);
                    }
                    if let Some(t) = opts.timeout {
                        timeout = duration_from_secs_f64_clamped(t);
                    }
                }
                plugin::enable_plugin(&PluginIdentifier::new(name, version), on_start_timeout, timeout)
            })
        },
    );

    ///////////////////////////////////////////////////////////////////////////
    #[rustfmt::skip]
    luamod_set(
        &l,
        "service_append_tier",
        indoc! {"
        pico.service_append_tier(plugin_name, plugin_version, service_name, tier, [opts])
        =================

        Append service to a tier, this will enable service on all instances with coressponding tier.

        Params:

            1. plugin_name - plugin name, plugin should already be installed with `pico.install_plugin` command
            2. plugin_version - plugin version
            3. service_name - service name
            4. tier - tier where service must be enabled
            5. opts (optional table)
                - timeout (optional number), in seconds, default: 10
        "},
        {
            #[derive(::tarantool::tlua::LuaRead)]
            struct Opts {
                timeout: Option<f64>,
            }
            tlua::function5(|plugin_name: String, plugin_version: String, service_name: String, tier: String, opts: Option<Opts>| -> traft::Result<()> {
                let mut timeout = Duration::from_secs(10);
                if let Some(opts) = opts {
                    if let Some(t) = opts.timeout {
                        timeout = duration_from_secs_f64_clamped(t);
                    }
                }
                plugin::update_service_tiers(
                    &PluginIdentifier::new(plugin_name, plugin_version),
                    &service_name,
                    &tier,
                    TopologyUpdateOpKind::Add,
                    timeout
                )
            })
        },
    );

    ///////////////////////////////////////////////////////////////////////////
    #[rustfmt::skip]
    luamod_set(
        &l,
        "service_remove_tier",
        indoc! {"
        pico.service_remove_tier(plugin_name, plugin_version, service_name, tier, [opts])
        =================

        Remove service from tier, this will disable service on all instances with coressponding tier.

        Params:

            1. plugin_name - plugin name, plugin should already be installed with `pico.install_plugin` command
            2. plugin_version - plugin version
            3. service_name - service name
            4. tier - tier where service must be disabled
            5. opts (optional table)
                - timeout (optional number), in seconds, default: 10
        "},
        {
            #[derive(::tarantool::tlua::LuaRead)]
            struct Opts {
                timeout: Option<f64>,
            }
            tlua::function5(|plugin_name: String, plugin_version: String, service_name: String, tier: String, opts: Option<Opts>| -> traft::Result<()> {
                let mut timeout = Duration::from_secs(10);
                if let Some(opts) = opts {
                    if let Some(t) = opts.timeout {
                        timeout = duration_from_secs_f64_clamped(t);
                    }
                }
                plugin::update_service_tiers(
                    &PluginIdentifier::new(plugin_name, plugin_version),
                    &service_name,
                    &tier,
                    TopologyUpdateOpKind::Remove,
                    timeout
                )
            })
        },
    );

    ///////////////////////////////////////////////////////////////////////////
    #[rustfmt::skip]
    luamod_set(
        &l,
        "disable_plugin",
        indoc! {"
        pico.disable_plugin(name, version, [opts])
        =================

        Disable plugin on cluster, `on_stop` callbacks will be called.

        Params:

            1. name - plugin name to be disabled
            2. version - plugin version to be disabled
            3. opts (optional table)
                - timeout (optional number), in seconds, default: 10
        "},
        {
            #[derive(::tarantool::tlua::LuaRead)]
            struct Opts {
                timeout: Option<f64>,
            }
            tlua::function3(|name: String, version: String, opts: Option<Opts>| -> traft::Result<()> {
                let mut timeout = Duration::from_secs(10);
                if let Some(opts) = opts {
                    if let Some(t) = opts.timeout {
                        timeout = duration_from_secs_f64_clamped(t);
                    }
                }
                plugin::disable_plugin(&PluginIdentifier::new(name, version), timeout)
            })
        },
    );

    ///////////////////////////////////////////////////////////////////////////
    #[rustfmt::skip]
    luamod_set(
        &l,
        "remove_plugin",
        indoc! {"
        pico.remove_plugin(name, version, [opts])
        =================

        Remove a plugin.

        Params:

            1. name - plugin name to be removed from a system
            2. version - plugin version to be removed from a system
            3. opts (optional table)
                - timeout (optional number), in seconds, default: 10
        "},
        {
            #[derive(::tarantool::tlua::LuaRead)]
            struct Opts {
                timeout: Option<f64>,
            }
            tlua::function3(|name: String, version: String, opts: Option<Opts>| -> traft::Result<()> {
                let mut timeout = Duration::from_secs(10);
                if let Some(opts) = opts {
                    if let Some(t) = opts.timeout {
                        timeout = duration_from_secs_f64_clamped(t);
                    }
                }
                plugin::drop_plugin(&PluginIdentifier::new(name, version),  false, false, timeout)
            })
        },
    );

    ///////////////////////////////////////////////////////////////////////////
    #[rustfmt::skip]
    luamod_set(
        &l,
        "migration_up",
        indoc! {"
        pico.migration_up(name, version, [opts])
        =================

        Up plugin migration.

        Params:

            1. name - plugin name, manifest with same name must exists in plugin_dir
            2. version - plugin version
            3. opts (optional table)
                - timeout (optional number), in seconds, default: 10
                - rollback_timeout (optional number), in seconds, default: 10
        "},
        {
            #[derive(::tarantool::tlua::LuaRead)]
            struct Opts {
                timeout: Option<f64>,
                rollback_timeout: Option<f64>,
            }
            tlua::function3(|name: String, version: String, opts: Option<Opts>| -> traft::Result<()> {
                let mut timeout = Duration::from_secs(10);
                let mut rollback_timeout = Duration::from_secs(10);
                if let Some(opts) = opts {
                    if let Some(t) = opts.timeout {
                        timeout = duration_from_secs_f64_clamped(t);
                    }
                    if let Some(t) = opts.rollback_timeout {
                        rollback_timeout = duration_from_secs_f64_clamped(t);
                    }
                }
                plugin::migration_up(&PluginIdentifier::new(name, version), timeout, rollback_timeout)
            })
        },
    );

    ///////////////////////////////////////////////////////////////////////////
    #[rustfmt::skip]
    luamod_set(
        &l,
        "migration_down",
        indoc! {"
        pico.migration_down(name, version, [opts])
        =================

        DOWN plugin migration.

        Params:

            1. name - plugin name, manifest with same name must exists in plugin_dir
            2. version - plugin version
            3. opts (optional table)
                - timeout (optional number), in seconds, default: 10
        "},
        {
            #[derive(::tarantool::tlua::LuaRead)]
            struct Opts {
                timeout: Option<f64>,
            }
            tlua::function3(|name: String, version: String, opts: Option<Opts>| -> traft::Result<()> {
                let mut timeout = Duration::from_secs(10);
                if let Some(opts) = opts {
                    if let Some(t) = opts.timeout {
                        timeout = duration_from_secs_f64_clamped(t);
                    }
                }
                plugin::migration_down(PluginIdentifier::new(name, version), timeout)
            })
        },
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

#[no_mangle]
pub extern "C" fn pico_internal_initialize_dummy() -> i32 {
    if !crate::tarantool::is_box_configured() {
        tlog!(Error, "run box.cfg {{ ... }} first");
        BoxError::new(
            crate::error_code::ErrorCode::Uninitialized,
            "run box.cfg { ... } first",
        )
        .set_last();
        return -1;
    }

    node::Node::for_tests();
    setup();

    0
}

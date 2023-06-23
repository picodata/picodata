//! Lua API exported as `_G.pico`
//!

use std::time::Duration;

use crate::instance::InstanceId;
use crate::schema::{self, CreateSpaceParams};
use crate::traft::op::{self, Op};
use crate::traft::{self, node, RaftIndex};
use crate::util::str_eq;
use crate::{args, compare_and_swap, rpc, sync, tlog};
use ::tarantool::fiber;
use ::tarantool::tlua;
use ::tarantool::tlua::{LuaState, LuaThread, PushOneInto, Void};
use ::tarantool::transaction::start_transaction;
use ::tarantool::tuple::Decode;
use ::tarantool::vclock::Vclock;
use indoc::indoc;

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

pub(crate) fn setup(args: &args::Run) {
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
            - 23.06.0
            ...
        "},
        {
            const _: () = assert!(str_eq(env!("CARGO_PKG_VERSION"), "23.6.0"));
            "23.06.0"
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
            - 1.0.0
            ...
        "},
        "1.0.0",
    );

    luamod_set(
        &l,
        "args",
        indoc! {"
        pico.args
        =========

        A Lua table (not a function) containing the command-line arguments
        specified at the instance startup. The content of the table is not
        strictly defined and may depend on circumstances.

        Example:

            picodata> pico.args
            ---
            - log_level: info
              listen: localhost:3301
              data_dir: .
              peers:
              - localhost:3301
            ...
        "},
        args,
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

        Example:

            picodata> pico.whoami()
            ---
            - raft_id: 1
              cluster_id: demo
              instance_id: i1
            ...
        "},
        tlua::function0(|| -> traft::Result<_> {
            let node = traft::node::global()?;
            let raft_storage = &node.raft_storage;

            Ok(tlua::AsTable((
                ("raft_id", raft_storage.raft_id()?),
                ("cluster_id", raft_storage.cluster_id()?),
                ("instance_id", raft_storage.instance_id()?),
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

        Example:

            picodata> pico.instance_info()
            ---
            - raft_id: 1
              advertise_address: localhost:3301
              instance_id: i1
              instance_uuid: 68d4a766-4144-3248-aeb4-e212356716e4
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
            let iid = iid.unwrap_or(node.raft_storage.instance_id()?.unwrap());
            let instance = node.storage.instances.get(&iid)?;
            let peer_address = node
                .storage
                .peer_addresses
                .get(instance.raft_id)?
                .unwrap_or_else(|| "<unknown>".into());

            Ok(tlua::AsTable((
                ("raft_id", instance.raft_id),
                ("advertise_address", peer_address),
                ("instance_id", instance.instance_id.0),
                ("instance_uuid", instance.instance_uuid),
                ("replicaset_id", instance.replicaset_id),
                ("replicaset_uuid", instance.replicaset_uuid),
                ("current_grade", instance.current_grade),
                ("target_grade", instance.target_grade),
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

            1. timeout (number), in seconds

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
            traft::node::global()?.read_index(Duration::from_secs_f64(timeout))
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
                node.wait_index(target, Duration::from_secs_f64(timeout))
            },
        ),
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
        tlua::function2(
            |target: Vclock, timeout: f64| -> Result<Vclock, sync::TimeoutError> {
                sync::wait_vclock(target, Duration::from_secs_f64(timeout))
            },
        ),
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
        tlua::function1(|lua: tlua::LuaState| -> traft::Result<RaftIndex> {
            use tlua::{AnyLuaString, AsLua, LuaError, LuaTable};
            let lua = unsafe { tlua::Lua::from_static(lua) };
            let t: LuaTable<_> = AsLua::read(&lua).map_err(|(_, e)| LuaError::from(e))?;
            let mp: AnyLuaString = lua
                .eval_with("return require 'msgpack'.encode(...)", &t)
                .map_err(LuaError::from)?;
            let op: Op = Decode::decode(mp.as_bytes())?;

            let node = traft::node::global()?;
            let mut node_impl = node.node_impl();
            let index = node_impl.propose(op)?;
            node.main_loop.wakeup();
            // Release the lock
            drop(node_impl);
            Ok(index)
        }),
    );

    luamod_set(
        &l,
        "raft_propose_mp",
        indoc! {"
        pico.raft_propose_mp(op_bytes)
        ==============================

        Internal API, see src/luamod.rs for the details.

        Params:

            1. op_bytes (string), encoded with msgpack

        Returns:

            ()
            or
            (nil, string) in case of an error
        "},
        tlua::function1(|op: tlua::AnyLuaString| -> traft::Result<()> {
            let op: Op = Decode::decode(op.as_bytes())?;
            traft::node::global()?.propose_and_wait(op, Duration::from_secs(1))
        }),
    );

    ///////////////////////////////////////////////////////////////////////////
    luamod_set(
        &l,
        "raft_timeout_now",
        indoc! {"
        pico.raft_timeout_now()
        =======================

        Internal API. Causes this instance to artificially timeout on waiting
        for a heartbeat from raft leader. The instance then will start a new
        election and transition to a 'PreCandidate' state.

        This function yields. It returns when the raft node changes it's state.

        Later the instance will likely become a leader, unless there are some
        impediments, e.g. the loss of quorum or split-vote.

        Example log:

            received MsgTimeoutNow from 3 and starts an election
                to get leadership., from: 3, term: 4, raft_id: 3
            
            starting a new election, term: 4, raft_id: 3
            
            became candidate at term 5, term: 5, raft_id: 3
            
            broadcasting vote request, to: [4, 1], log_index: 54,
                log_term: 4, term: 5, type: MsgRequestVote, raft_id: 3
            
            received votes response, term: 5, type: MsgRequestVoteResponse, 
                approvals: 2, rejections: 0, from: 4, vote: true, raft_id: 3
        
            became leader at term 5, term: 5, raft_id: 3
        "},
        tlua::function0(|| -> traft::Result<()> {
            traft::node::global()?.timeout_now();
            Ok(())
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
        return_string: Option<bool>,
        justify_contents: Option<Justify>,
    }
    luamod_set(
        &l,
        "raft_log",
        indoc! {"
        pico.raft_log([opts])
        ====================================

        Internal API.

        If `return_string` is true, returns a string with formatted contents of
        raft log. Otherwise, prints the formatted raft log contents to the
        standard output.

        Example:

            pico.raft_log({justify_contents = 'center', return_string = false})
        "},
        tlua::function1(
            |opts: Option<RaftLogOpts>| -> traft::Result<Option<String>> {
                let mut return_string = false;
                let mut justify_contents = Default::default();
                if let Some(opts) = opts {
                    return_string = opts.return_string.unwrap_or(false);
                    justify_contents = opts.justify_contents.unwrap_or_default();
                }
                let header = ["index", "term", "lc", "contents"];
                let [index, term, lc, contents] = header;
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
                        entry
                            .lc()
                            .map(|lc| lc.to_string())
                            .unwrap_or_else(String::new),
                        entry.payload().to_string(),
                    ];
                    for i in 0..col_widths.len() {
                        col_widths[i] = col_widths[i].max(row[i].len());
                    }
                    rows.push(row);
                }
                let [iw, tw, lw, mut cw] = col_widths;

                let total_width = 1 + header.len() + col_widths.iter().sum::<usize>();
                let cols = if return_string {
                    256
                } else {
                    crate::util::screen_size().1 as usize
                };
                if total_width > cols {
                    match cw.checked_sub(total_width - cols) {
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
                            writeln!(buf, "+{0:-^iw$}+{0:-^tw$}+{0:-^lw$}+{0:-<cw$}+", "")
                        }
                        Justify::Center => {
                            writeln!(buf, "+{0:-^iw$}+{0:-^tw$}+{0:-^lw$}+{0:-^cw$}+", "")
                        }
                        Justify::Right => {
                            writeln!(buf, "+{0:-^iw$}+{0:-^tw$}+{0:-^lw$}+{0:->cw$}+", "")
                        }
                    }
                    .unwrap()
                };
                row_sep(&mut buf);
                write!(buf, "|{index: ^iw$}|{term: ^tw$}|{lc: ^lw$}|").unwrap();
                write_contents(&mut buf, contents).unwrap();
                row_sep(&mut buf);
                for [index, term, lc, contents] in rows {
                    if contents.len() <= cw {
                        write!(buf, "|{index: ^iw$}|{term: ^tw$}|{lc: ^lw$}|").unwrap();
                        write_contents(&mut buf, &contents).unwrap();
                    } else {
                        write!(buf, "|{index: ^iw$}|{term: ^tw$}|{lc: ^lw$}|").unwrap();
                        write_contents(&mut buf, &contents[..cw]).unwrap();
                        let mut rest = &contents[cw..];
                        while !rest.is_empty() {
                            let clamped_cw = usize::min(rest.len(), cw);
                            write!(
                                buf,
                                "|{blank: ^iw$}|{blank: ^tw$}|{blank: ^lw$}|",
                                blank = "~",
                            )
                            .unwrap();
                            write_contents(&mut buf, &rest[..clamped_cw]).unwrap();
                            rest = &rest[clamped_cw..];
                        }
                    }
                }
                row_sep(&mut buf);
                if return_string {
                    Ok(Some(String::from_utf8_lossy(&buf).into()))
                } else {
                    std::io::stdout().write_all(&buf).unwrap();
                    Ok(None)
                }
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
                let ret = start_transaction(|| raft_storage.compact_log(up_to));
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
        spaces only.

        E.g. it checks the `predicate` on leader and if no conflicting entries
        were found appends the `op` to the raft log and returns its index. If
        predicate is not supplied, it will be auto generated with `index` and
        `term` taken from the current instance and with empty `ranges`.

        Returns when the operation is appended to the raft log on a leader.
        Returns the index of the corresponding Op::Dml.

        Params:

            1. dml (table)
                - kind (string), one of 'insert' | 'replace' | 'update' | 'delete'
                - space (stringLua)
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

            -- Assuming there exists a space 'friends_of_peppa' with two
            -- fields: id (unsigned) and name (string) and corresponding
            -- unique indexes.

            -- Insert a tuple {1, 'Suzy'} into this space. This will fail
            -- if the term of the current instance is outdated.
            pico.cas({
                kind = 'insert',
                space = 'friends_of_peppa',
                tuple = {1, 'Suzy'},
            })

            -- Add Rebecca, but only if no other friends were added after Suzy.
            pico.cas({
                kind = 'replace',
                space = 'friends_of_peppa',
                tuple = {2, 'Rebecca'},
            }, {
                ranges = {{
                    space = 'friends_of_peppa',
                    key_min = { kind = 'excluded', key = {1,} },
                    key_max = { kind = 'unbounded' },
                }},
            })

            -- Check that there were no other updates, and update the second
            -- Peppa friend, replacing 'Rebecca' with 'Emily'.
            pico.cas({
                kind = 'update',
                space = 'friends_of_peppa',
                key = {2},
                ops = {'=', 2, 'Emily'},
            }, {
                ranges = {{
                    space = 'friends_of_peppa',
                    key_min = { kind = 'included', key = {2} },
                    key_max = { kind = 'included', key = {2} },
                }},
            })

            -- Delete the second Peppa friend, specifying index and term
            -- explicitly. It's necessary when there are some yielding
            -- operations between reading and writing.
            index, term = {
                assert(pico.raft_get_index()),
                assert(pico.raft_status()).term,
            }
            emily = box.space.friends_of_peppa.index.name:get('Emily')
            fiber.sleep(1) -- do something yielding

            pico.cas({
                kind = 'delete',
                space = 'friends_of_peppa',
                key = {emily.id},
            }, {
                index = index,
                term = term,
                ranges = {{
                    space = 'friends_of_peppa',
                    key_min = { kind = 'included', key = {emily.id} },
                    key_max = { kind = 'included', key = {emily.id} },
                }},
            })
        "},
        tlua::function2(
            |op: op::DmlInLua,
             predicate: Option<rpc::cas::PredicateInLua>|
             -> traft::Result<RaftIndex> {
                let op = op::Dml::from_lua_args(op).map_err(traft::error::Error::other)?;
                let predicate = rpc::cas::Predicate::from_lua_args(predicate.unwrap_or_default())?;
                let (index, _) = compare_and_swap(op.into(), predicate)?;
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

            - space (string)
            - key_min (table CasBound), see pico.help('table CasBound')
            - key_max (table CasBound)

        Example:

            local unbounded = { kind = 'unbounded' }
            local including_1 = { kind = 'included', key = {1,} }
            local excluding_3 = { kind = 'excluded', key = {3,} }

            local range_a = {
                space = 'friends_of_peppa',
                key_min = unbounded,
                key_max = unbounded,
            }

            -- [1, 3)
            local range_a = {
                space = 'friends_of_peppa',
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
        "create_space",
        indoc! {"
        pico.create_space(opts)
        =======================

        Creates a space.

        Returns when the space is created globally and becomes operable on the
        current instance. Returns the index of the corresponding Op::DdlCommit
        raft entry. It's necessary for syncing with other instances.

        Params:

            1. opts (table)
                - name (string)
                - format (table {table SpaceField,...}), see pico.help('table SpaceField')
                - primary_key (table {string,...}), with field names
                - id (optional number), default: implicitly generated
                - distribution (string), one of 'global' | 'sharded'
                    in case it's sharded, either `by_field` (for explicit sharding)
                    or `sharding_key`+`sharding_fn` (for implicit sharding) options
                    must be supplied.
                - by_field (optional string), usually 'bucket_id'
                - sharding_key (optional table {string,...}) with field names
                - sharding_fn (optional string), only default 'murmur3' is supported for now
                - timeout (number), in seconds

        Returns:

            (number)
            or
            (nil, string) in case of an error

        Example:

            -- Creates a global space 'friends_of_peppa' with two fields:
            -- id (unsigned) and name (string).
            pico.create_space({
                name = 'friends_of_peppa',
                format = {
                    {name = 'id', type = 'unsigned', is_nullable = false},
                    {name = 'name', type = 'string', is_nullable = false},
                },
                primary_key = {'id'},
                distribution = 'global',
                timeout = 3,
            })

            -- Global spaces are updated with compare-and-swap, see pico.help('cas')
            pico.cas({
                kind = 'insert',
                space = 'friends_of_peppa',
                key = {1, 'Suzy'},
            })

            -- Creates an implicitly sharded space 'wonderland' with two fields:
            -- property (string) and value (any).
            pico.create_space({
                name = 'wonderland',
                format = {
                    {name = 'property', type = 'string', is_nullable = false},
                    {name = 'value', type = 'any', is_nullable = true}
                },
                primary_key = {'property'},
                distribution = 'sharded',
                sharding_key = {'property'},
                timeout = 3,
            })

            -- Sharded spaces are updated via vshard api, see [1]
            local bucket_id = vshard.router.bucket_id_mpcrc32('unicorns')
            vshard.router.callrw(bucket_id, 'box.space.wonderland:insert', {{'unicorns', 12}})

        See also:

            [1]: https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/vshard_router/
        "},
        {
            tlua::function1(|params: CreateSpaceParams| -> traft::Result<RaftIndex> {
                let timeout = Duration::from_secs_f64(params.timeout);
                let storage = &node::global()?.storage;
                let params = params.validate(storage)?;
                // TODO: check space creation and rollback
                // box.begin() box.schema.space.create() box.rollback()
                let op = params.into_ddl(storage)?;
                let index = schema::prepare_ddl(op, timeout)?;
                let commit_index = schema::wait_for_ddl_commit(index, timeout)?;
                Ok(commit_index)
            })
        },
    );
    luamod_set(
        &l,
        "abort_ddl",
        indoc! {"
        pico.abort_ddl(timeout)
        =======================

        Aborts a pending schema change.

        Returns an index of the corresponding Op::DdlAbort raft entry.
        Returns an error if there is no pending schema change operation.

        Params:

            1. timeout (number), in seconds

        Returns:

            (number)
            or
            (nil, string) in case of an error
        "},
        {
            tlua::function1(|timeout: f64| -> traft::Result<RaftIndex> {
                schema::abort_ddl(Duration::from_secs_f64(timeout))
            })
        },
    );
    luamod_set_help_only(
        &l,
        "table SpaceField",
        indoc! {"
        table SpaceField
        ================

        A Lua table describing a field in a space, see [1].

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
}

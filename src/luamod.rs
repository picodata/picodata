//! Lua API exported as `_G.pico`
//!

use std::time::Duration;

use crate::instance::InstanceId;
use crate::schema::{self, CreateSpaceParams};
use crate::traft::op::{self, Op};
use crate::traft::{self, node, RaftIndex};
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
        "VERSION",
        indoc! {"
        pico.VERSION
        ============

        A string variable (not a function) contatining Picodata version
        which follows the Calendar Versioning convention with the
        `YY.0M.MICRO` scheme.

            https://calver.org/#scheme

        Example

            tarantool> pico.VERSION
            ---
            - 22.11.0
            ...
        "},
        env!("CARGO_PKG_VERSION"),
    );

    luamod_set(
        &l,
        "args",
        indoc! {"
        pico.args
        =========

        A Lua table (not a function) containing the command-line arguments
        specified at instance startup. The content of the table is not
        strictly defined and may depend on circumstances.

        Example:

            tarantool> pico.args
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

        Returns a table containing the following instance identifiers:

        - raft_id (number)
        - cluster_id (string)
        - instance_id (string)

        Example

            tarantool> pico.whoami()
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

        Returns a table containing the following instance information:

        - raft_id (number)
        - advertised_address (string)
        - instance_id (string)
        - instance_uuid (string)
        - replicaset_id (string)
        - replicaset_uuid (string)
        - current_grade (table)
        - target_grade (table)

        # Params
        1. instance_id - number
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

    luamod_set(
        &l,
        "raft_status",
        indoc! {"
        pico.raft_status()
        ==================

        Returns a table `RaftStatus`. See pico.help(\"RaftStatus\")

        - id (number)
        - leader_id (number)
        - term (number)
        - raft_state (string)
        "},
        tlua::function0(|| traft::node::global().map(|n| n.status())),
    );
    luamod_set_help_only(
        &l,
        "RaftStatus",
        indoc! {"
        This table contains the following fields:
        - id (number)
        - leader_id (number)
        - term (number)
        - raft_state (string)
        "},
    );

    luamod_set(
        &l,
        "raft_tick",
        indoc! {"
        pico.raft_tick(n_times)
        ======================

        # Params
        1. timeout - number
        "},
        tlua::function1(|n_times: u32| -> traft::Result<()> {
            traft::node::global()?.tick_and_yield(n_times);
            Ok(())
        }),
    );

    luamod_set(
        &l,
        "raft_get_index",
        indoc! {"
        pico.raft_get_index()
        ======================

        Returns current applied raft index (number).
        "},
        tlua::function0(|| -> traft::Result<RaftIndex> {
            let node = traft::node::global()?;
            Ok(node.get_index())
        }),
    );
    luamod_set(
        &l,
        "raft_read_index",
        indoc! {"
        pico.raft_read_index(timeout)
        ============================
        Performs the quorum read operation.
       
        If works the following way:
       
        1. The instance forwards a request (`MsgReadIndex`) to a raft
           leader. In case there's no leader at the moment, the function
           returns `Err(ProposalDropped)`.
        2. Raft leader tracks its `commit_index` and broadcasts a
           heartbeat to followers to make certain that it's still a
           leader.
        3. As soon as the heartbeat is acknowlenged by the quorum, the
           function returns that index.
        4. The instance awaits when the index is applied. If timeout
           expires beforehand, the function returns an error.
       
        Returns current applied raft index (number).

        # Params
        1. timeout - number
        "},
        tlua::function1(|timeout: f64| -> traft::Result<RaftIndex> {
            traft::node::global()?.read_index(Duration::from_secs_f64(timeout))
        }),
    );
    luamod_set(
        &l,
        "raft_wait_index",
        indoc! {"
        pico.raft_wait_index(target_index, timeout)
        ===========================================
        Waits for target_index to be applied to the storage locally.
       
        Returns current applied raft index. It can be equal to or
        greater than the target one. If timeout expires beforehand, the
        function returns an error.

        # Params
        1. target_vclock - table
        2. timeout - number
        "},
        tlua::function2(
            |target: RaftIndex, timeout: f64| -> traft::Result<RaftIndex> {
                let node = traft::node::global()?;
                node.wait_index(target, Duration::from_secs_f64(timeout))
            },
        ),
    );
    luamod_set(
        &l,
        "get_vclock",
        indoc! {"
        pico.get_vclock()
        ==================
        Obtains current vclock from Tarantool `box.info.vclock` API.

        Returns a Vclock (table). See pico.help(\"Vclock\")
        "},
        tlua::function0(Vclock::current),
    );
    luamod_set_help_only(
        &l,
        "Vclock",
        indoc! {"
        Vclock is a mapping of replica id (number) to its LSN (number).

        The meaning of these concepts is explained below.
       
        To ensure data persistence, Tarantool records updates to the
        database in the so-called write-ahead log (WAL) files. Each record
        in the WAL represents a single Tarantool data-change request such as
        `INSERT`, `UPDATE`, or `DELETE`, and is assigned a monotonically
        growing log sequence number (LSN).
       
        Enabling replication makes all replicas in a replica set to exchange
        their records, each with it's own LSN. Together, LSNs from different
        replicas form a vector clock (vclock). Vclock defines the database
        state of an instance.
        "},
    );
    luamod_set(
        &l,
        "wait_vclock",
        indoc! {"
        pico.wait_vclock(target_vclock, timeout)
        ========================================
        Wait until Tarantool vclock reaches the target_vclock. Returns the
        actual vclock (table). It can be equal to or greater than the target one.

        # Params
        1. target_vclock - table. See pico.help(\"Vclock\")
        2. timeout - number
        "},
        tlua::function2(
            |target: Vclock, timeout: f64| -> Result<Vclock, sync::TimeoutError> {
                sync::wait_vclock(target, Duration::from_secs_f64(timeout))
            },
        ),
    );
    luamod_set(
        &l,
        "raft_propose_nop",
        indoc! {"
        pico.raft_propose_nop()
        =======================

        Proposes and waits for Op::Nop to be applied.
        "},
        tlua::function0(|| {
            traft::node::global()?.propose_and_wait(Op::Nop, Duration::from_secs(1))
        }),
    );

    luamod_set(
        &l,
        "raft_propose",
        indoc! {"
        pico.raft_propose(operation)
        ============================

        Proposes operation to raft and returns its index (number).
        Returned index should be supplied to `pico.wait_index`
        manually if it's necessary.

        # Params
        1. operation - table. See pico.help(\"Op\")
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
    luamod_set_help_only(
        &l,
        "Op",
        // TODO: provide more detailed description of each op
        indoc! {"
        Op is an operation on the raft state machine.

        See traft::op::Op in source code.
        "},
    );
    luamod_set(
        &l,
        "raft_propose_mp",
        indoc! {"
        pico.raft_propose_mp(operation_bytes)
        =====================================

        Proposes operation to raft and returns its index (number).
        "},
        tlua::function1(|op: tlua::AnyLuaString| -> traft::Result<()> {
            let op: Op = Decode::decode(op.as_bytes())?;
            traft::node::global()?.propose_and_wait(op, Duration::from_secs(1))
        }),
    );
    luamod_set(
        &l,
        "raft_timeout_now",
        indoc! {"
        pico.raft_timeout_now()
        =======================
        "},
        tlua::function0(|| -> traft::Result<()> {
            traft::node::global()?.timeout_now();
            Ok(())
        }),
    );
    #[rustfmt::skip]
    luamod_set(
        &l,
        "exit",
        indoc! {"
        pico.exit([code])
        =================

        Terminate the picodata process with the supplied code.

        # Params
        1. code - number, default: 0
        "},
        tlua::function1(|code: Option<i32>| {
            crate::tarantool::exit(code.unwrap_or(0))
        }),
    );
    luamod_set(
        &l,
        "expel",
        indoc! {"
        pico.expel(instance_id)
        ======================

        Expells an instance with instance_id from the cluster.

        # Params
        1. instance_id - number
        "},
        tlua::function1(|instance_id: InstanceId| -> traft::Result<()> {
            let raft_storage = &traft::node::global()?.raft_storage;
            let cluster_id = raft_storage.cluster_id()?;
            fiber::block_on(rpc::network_call_to_leader(&rpc::expel::Request {
                instance_id,
                cluster_id,
            }))?;
            Ok(())
        }),
    );
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
        pico.raft_log()
        pico.raft_log { 
            return_string=...,    -- boolean
            justify_contents=...  -- string
        }
        =======================================================

        If return_string is true, returns a string with formatted contents of raft log.
        If false, prints the formatted raft log contents to the standard output.
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
    luamod_set(
        &l,
        "raft_compact_log",
        indoc! {"
        pico.raft_compact_log(up_to_index)
        =================================

        Trims raft log up to the given index (excluding the index
        itself). Returns the new first_index (number) after the log compaction.

        # Params
        1. up_to_index - number
        "},
        {
            tlua::function1(|up_to: RaftIndex| -> traft::Result<RaftIndex> {
                let raft_storage = &node::global()?.raft_storage;
                let ret = start_transaction(|| raft_storage.compact_log(up_to));
                Ok(ret?)
            })
        },
    );
    luamod_set(
        &l,
        "cas",
        indoc! {"
        pico.cas(dml, [predicate])
        ========================

        Performs a clusterwide compare and swap operation.
       
        E.g. it checks the `predicate` on leader and if no conflicting entries were found
        appends the `op` to the raft log and returns its index (number). If predicate
        is not supplied, it will be auto generated with `index` and `term` taken from the
        current instance and with empty `ranges`.

        # Params
        1. dml - table. See pico.help(\"Dml\")
        2. predicate - table. See pico.help(\"Predicate\")
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
    // TODO: describe how to fill these fields for different opts
    luamod_set_help_only(
        &l,
        "Dml",
        indoc! {"
        A table describing a dml operation. Has these fields:
        - space (string)
        - kind (string, one of 'insert' | 'replace' | 'update' | 'delete')
        - tuple (optional, table)
        - key (optional, table)
        - ops (optional, table)
        "},
    );
    // TODO: describe ranges
    luamod_set_help_only(
        &l,
        "Predicate",
        indoc! {"
        Predicate that will be checked by the leader, before accepting the proposed op.

        Has these fields:
        - index (optional, number)
        - term (optional, number)
        - ranges (optional, table)

        If some fields are not supplied, they will be autogenerated. `index` and `term` taken from the
        raft state on the instance which sends this operation and `ranges` left as an empty
        vector.
        "},
    );
    luamod_set(
        &l,
        "create_space",
        indoc! {"
        pico.create_space {
            id = ...,             -- number
            name = ...,           -- string
            format = ...,         -- table of Field. See pico.help(\"Field\")
            primary_key = ...,    -- table of string
            distribution = ...,   -- string, one of 'global' | 'sharded'
            timeout = ...,    -- number, in seconds
        }
        ========================

        Creates a space. Returns a raft index (number) at which a newly created space
        has to exist on all peers.
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
        ========================

        Aborts a pending DDL operation.

        Returns an index of the corresponding DdlAbort raft entry, or an error if
        there is no pending DDL operation.

        # Params
        1. timeout - number, in seconds
        "},
        {
            tlua::function1(|timeout: f64| -> traft::Result<RaftIndex> {
                schema::abort_ddl(Duration::from_secs_f64(timeout))
            })
        },
    );
    luamod_set_help_only(
        &l,
        "Field",
        indoc! {"
        Has these fields:
        - name (number)
        - type (string)
        - is_nullable (boolean)
        "},
    );
}

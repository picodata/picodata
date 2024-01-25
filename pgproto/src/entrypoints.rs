use crate::{
    client::ClientId,
    error::{PgError, PgResult},
    storage::{
        describe::{PortalDescribe, QueryType, StatementDescribe},
        result::ExecuteResult,
        value::PgValue,
    },
};
use postgres_types::Oid;
use serde::Deserialize;
use serde_json::Value;
use std::cell::RefCell;
use tarantool::tlua::{LuaFunction, LuaThread, PushGuard};

type Row = Vec<PgValue>;

#[derive(Deserialize)]
struct RawExecuteResult {
    describe: PortalDescribe,
    // tuple in the same format as tuples returned from pico.sql
    result: Value,
}

struct DqlResult {
    rows: Vec<Row>,
    is_finished: bool,
}

fn parse_dql(res: Value) -> PgResult<DqlResult> {
    #[derive(Deserialize)]
    struct RawDqlResult {
        rows: Vec<Vec<Value>>,
        is_finished: bool,
    }

    let res: RawDqlResult = serde_json::from_value(res)?;
    let rows: PgResult<Vec<Row>> = res
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(PgValue::try_from).collect())
        .collect();

    rows.map(|rows| DqlResult {
        rows,
        is_finished: res.is_finished,
    })
}

fn parse_dml(res: Value) -> PgResult<usize> {
    #[derive(Deserialize)]
    struct RawDmlResult {
        row_count: usize,
    }

    let res: RawDmlResult = serde_json::from_value(res)?;
    Ok(res.row_count)
}

fn parse_explain(res: Value) -> PgResult<DqlResult> {
    #[derive(Deserialize)]
    struct RawExplainResult {
        rows: Vec<Value>,
        is_finished: bool,
    }

    let res: RawExplainResult = serde_json::from_value(res)?;
    let rows: PgResult<Vec<Row>> = res
        .rows
        .into_iter()
        // every row must be a vector
        .map(|val| Ok(vec![PgValue::try_from(val)?]))
        .collect();

    rows.map(|rows| DqlResult {
        rows,
        is_finished: res.is_finished,
    })
}

fn execute_result_from_json(json: &str) -> PgResult<ExecuteResult> {
    let raw: RawExecuteResult = serde_json::from_str(json)?;
    match raw.describe.query_type() {
        QueryType::Dql => {
            let res = parse_dql(raw.result)?;
            Ok(ExecuteResult::new(res.rows, raw.describe, res.is_finished))
        }
        QueryType::Explain => {
            let res = parse_explain(raw.result)?;
            Ok(ExecuteResult::new(res.rows, raw.describe, res.is_finished))
        }
        QueryType::Acl | QueryType::Ddl => Ok(ExecuteResult::empty(0, raw.describe)),
        QueryType::Dml => Ok(ExecuteResult::empty(parse_dml(raw.result)?, raw.describe)),
    }
}

type Entrypoint = LuaFunction<PushGuard<LuaThread>>;

/// List of lua functions from sbroad that implement PG protcol API.
pub struct Entrypoints {
    /// Handler for a Query message.
    /// First, it closes an unnamed portal and statement, just like PG does when gets a Query messsage.
    /// After that the extended pipeline is executed on unnamed portal and statement: parse + bind + describe + execute.
    /// It returns the query result (in the same format as pico.sql) and description.
    /// We need the description here for the command tag (CREATE TABLE, SELECT, etc)
    /// that is required to make a CommandComplete message as the response to the Query message,
    /// so pico.sql is not enough.
    ///
    /// No resources to be free after the call.
    simple_query: Entrypoint,

    /// Handler for a Parse message.
    /// Create a statement from a query query and store it in the sbroad storage using the given id and name as a key.
    /// In case of conflicts the strategy is the same with PG.
    ///
    /// The statement lasts until it is explicitly closed.
    parse: Entrypoint,

    /// Handler for a Bind message.
    /// Copy the sources statement, create a portal by binding parameters to it and stores the portal in the sbroad storage.
    /// In case of conflicts the strategy is the same with PG.
    ///
    /// The portal lasts until it is explicitly closed or executed.
    bind: Entrypoint,

    /// Handler for an Execute message.
    ///
    /// Remove a portal from the sbroad storage, run it til the end and return the result.
    execute: Entrypoint,

    /// Handler for a Describe message.
    /// Get a statement description.
    describe_statement: Entrypoint,

    /// Handler for a Describe message.
    /// Get a portal description.
    describe_portal: Entrypoint,

    /// Handler for a Close message.
    /// Close a portal. It's not an error to close a nonexistent portal.
    close_portal: Entrypoint,

    /// Handler for a Close message.
    /// Close a statement with its portals. It's not an error to close a nonexistent statement.
    close_statement: Entrypoint,

    /// Close client portals by the given client id.
    close_client_portals: Entrypoint,

    /// Close client statements with its portals by the given client id.
    close_client_statements: Entrypoint,
}

impl Entrypoints {
    fn new() -> PgResult<Self> {
        let simple_query = LuaFunction::load(
            tarantool::lua_state(),
            "
            local function close_unnamed(client_id)
                pico.pg_close_stmt(client_id, '')
                pico.pg_close_portal(client_id, '')
            end

            local function parse_and_execute_unnamed(client_id, sql)
                local res, err = pico.pg_parse(client_id, '', sql, {})
                if res == nil then
                    return nil, err
                end

                -- {}, {} => no parameters, default result encoding (text)
                local res, err = pico.pg_bind(client_id, '', '', {}, {})
                if res == nil then
                    return nil, err
                end

                local desc, err = pico.pg_describe_portal(client_id, '')
                if desc == nil then
                    return nil, err
                end

                -- -1 == fetch all
                local res, err = pico.pg_execute(client_id, '', -1)
                if res == nil then
                    return nil, err
                end

                return {['describe'] = desc, ['result'] = res}
            end

            local client_id, sql = ...

            -- Strictly speaking, this is a part of an extended query protocol.
            -- When a query message is received, PG closes an unnamed portal and statement
            -- and runs the extended query pipeline on them (parse + bind + execute).
            close_unnamed(client_id)

            local res, err = parse_and_execute_unnamed(client_id, sql)

            -- After the execution, the portal and statement must be closed.
            close_unnamed(client_id)

            if res == nil then
                error(err)
            end

            return require('json').encode(res)
        ",
        )?;

        let close_client_statements = LuaFunction::load(
            tarantool::lua_state(),
            "
            -- closing a statement closes its portals,
            -- so then we close all the statements we close all the portals too.
            pico.pg_close_client_stmts(...)
            ",
        )?;

        let parse = LuaFunction::load(
            tarantool::lua_state(),
            "
            local res, err = pico.pg_parse(...)
            if res == nil then
                error(err)
            end
            ",
        )?;

        let bind = LuaFunction::load(
            tarantool::lua_state(),
            "
            local res, err = pico.pg_bind(...)
            if res == nil then
                error(err)
            end
            ",
        )?;

        let execute = LuaFunction::load(
            tarantool::lua_state(),
            "
            local id, portal = ...
            local desc, err = pico.pg_describe_portal(id, portal)
            if desc == nil then
                error(err)
            end

            -- -1 == fetch all
            local res, err = pico.pg_execute(id, portal, -1)
            if res == nil then
                error(err)
            end

            return require('json').encode({['describe'] = desc, ['result'] = res})
            ",
        )?;

        let describe_portal = LuaFunction::load(
            tarantool::lua_state(),
            "
            local res, err = pico.pg_describe_portal(...)
            if res == nil then
                error(err)
            end
            return require('json').encode(res)
            ",
        )?;

        let describe_statement = LuaFunction::load(
            tarantool::lua_state(),
            "
            local res, err = pico.pg_describe_stmt(...)
            if res == nil then
                error(err)
            end
            return require('json').encode(res)
            ",
        )?;

        let close_portal = LuaFunction::load(
            tarantool::lua_state(),
            "
            local res, err = pico.pg_close_portal(...)
            if res == nil then
                error(err)
            end
            ",
        )?;

        let close_statement = LuaFunction::load(
            tarantool::lua_state(),
            "
            local res, err = pico.pg_close_stmt(...)
            if res == nil then
                error(err)
            end
            ",
        )?;

        let close_client_portals = LuaFunction::load(
            tarantool::lua_state(),
            "
            local res, err = pico.pg_close_client_portals(...)
            if res == nil then
                error(err)
            end
            ",
        )?;

        Ok(Self {
            simple_query,
            close_client_statements,
            parse,
            bind,
            execute,
            describe_portal,
            describe_statement,
            close_portal,
            close_statement,
            close_client_portals,
        })
    }

    /// Handler for a Query message. See self.simple_query for the details.
    pub fn simple_query(&self, client_id: ClientId, sql: &str) -> PgResult<ExecuteResult> {
        let json: String = self
            .simple_query
            .call_with_args((client_id, sql))
            .map_err(|e| PgError::TarantoolError(e.into()))?;
        execute_result_from_json(&json)
    }

    /// Handler for a Parse message. See self.parse for the details.
    pub fn parse(
        &self,
        client_id: ClientId,
        name: &str,
        sql: &str,
        param_oids: &[Oid],
    ) -> PgResult<()> {
        self.parse
            .call_with_args((client_id, name, sql, param_oids))
            .map_err(|e| PgError::TarantoolError(e.into()))
    }

    /// Handler for a Bind message. See self.bind for the details.
    pub fn bind(
        &self,
        id: ClientId,
        statement: &str,
        portal: &str,
        params: Vec<PgValue>,
        result_format: &[i16],
    ) -> PgResult<()> {
        self.bind
            .call_with_args((id, statement, portal, params, result_format))
            .map_err(|e| PgError::TarantoolError(e.into()))
    }

    /// Handler for an Execute message. See self.execute for the details.
    pub fn execute(&self, id: ClientId, portal: &str) -> PgResult<ExecuteResult> {
        let json: String = self
            .execute
            .call_with_args((id, portal))
            .map_err(|e| PgError::TarantoolError(e.into()))?;
        execute_result_from_json(&json)
    }

    /// Handler for a Describe message. See self.describe_portal for the details.
    pub fn describe_portal(&self, client_id: ClientId, portal: &str) -> PgResult<PortalDescribe> {
        let json: String = self
            .describe_portal
            .call_with_args((client_id, portal))
            .map_err(|e| PgError::TarantoolError(e.into()))?;
        let describe = serde_json::from_str(&json)?;
        Ok(describe)
    }

    /// Handler for a Describe message. See self.describe_statement for the details.
    pub fn describe_statement(
        &self,
        client_id: ClientId,
        statement: &str,
    ) -> PgResult<StatementDescribe> {
        let json: String = self
            .describe_statement
            .call_with_args((client_id, statement))
            .map_err(|e| PgError::TarantoolError(e.into()))?;
        let describe = serde_json::from_str(&json)?;
        Ok(describe)
    }

    /// Handler for a Close message. See self.close_portal for the details.
    pub fn close_portal(&self, id: ClientId, portal: &str) -> PgResult<()> {
        self.close_portal
            .call_with_args((id, portal))
            .map_err(|e| PgError::TarantoolError(e.into()))
    }

    /// Handler for a Close message. See self.close_statement for the details.
    pub fn close_statement(&self, client_id: ClientId, statement: &str) -> PgResult<()> {
        self.close_statement
            .call_with_args((client_id, statement))
            .map_err(|e| PgError::TarantoolError(e.into()))
    }

    /// Close all the client statements and portals. See self.close_client_statements for the details.
    pub fn close_client_statements(&self, client_id: ClientId) -> PgResult<()> {
        self.close_client_statements
            .call_with_args(client_id)
            .map_err(|e| PgError::TarantoolError(e.into()))
    }

    /// Close client statements with its portals.
    pub fn close_client_portals(&self, client_id: ClientId) -> PgResult<()> {
        self.close_client_portals
            .call_with_args(client_id)
            .map_err(|e| PgError::TarantoolError(e.into()))
    }
}

thread_local! {
    pub static PG_ENTRYPOINTS: RefCell<Entrypoints> = RefCell::new(Entrypoints::new().unwrap())
}

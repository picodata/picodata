use super::describe::Describe;
use serde::de::DeserializeOwned;

use crate::error::PgResult;

#[derive(Debug)]
pub struct Handle(usize);

impl Handle {
    /// Parse the statement and store its description in the engine;
    /// then return an opaque handle (descriptor) to that description.
    pub fn prepare(query: &str) -> PgResult<Handle> {
        let code = format!(
            "
            local res, err = pico.pg_parse([[{query}]])

            if res == nil then
                error(err)
            end

            return res
        "
        );
        let desc: usize = tarantool::lua_state().eval(&code)?;
        Ok(desc.into())
    }

    /// Execute query and get a field from result.
    pub fn execute_and_get<T: DeserializeOwned>(&self, name: &str) -> PgResult<T> {
        let desc = self.0;
        let code = format!(
            "
            local res, err = pico.pg_execute({desc})

            if res == nil then
                error(err)
            end

            return require('json').encode(res['{name}'])
        "
        );
        let raw_result = tarantool::lua_state().eval::<String>(&code)?;
        let result: T = serde_json::from_str(&raw_result)?;
        Ok(result)
    }

    /// Execute query and get result.
    pub fn execute<T: DeserializeOwned>(&self) -> PgResult<T> {
        let desc = self.0;
        let code = format!(
            "
            local res, err = pico.pg_execute({desc})

            if res == nil then
                error(err)
            end

            return require('json').encode(res)
        "
        );
        let raw_result = tarantool::lua_state().eval::<String>(&code)?;
        let result: T = serde_json::from_str(&raw_result)?;
        Ok(result)
    }

    /// Bind parameters and optimize query.
    pub fn bind(&self) -> PgResult<()> {
        let desc = self.0;
        let code = format!(
            "
            local res, err = pico.pg_bind({desc}, {{}})

            if res == nil then
                error(err)
            end
        "
        );
        tarantool::lua_state().eval::<()>(&code)?;
        Ok(())
    }

    /// Get query description.
    pub fn describe(&self) -> PgResult<Describe> {
        let desc = self.0;
        let code = format!(
            "
            local res, err = pico.pg_describe({desc})

            if res == nil then
                error(err)
            end

            return require('json').encode(res)
        "
        );
        let describe_str = tarantool::lua_state().eval::<String>(&code)?;
        let describe = serde_json::from_str(&describe_str)?;
        Ok(describe)
    }

    fn close(&self) -> PgResult<()> {
        let desc = self.0;
        let code = format!(
            "
            local res, err = pico.pg_close({desc})

            if res == nil then
                error(err)
            end
        "
        );
        tarantool::lua_state().eval(&code)?;
        Ok(())
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.close().unwrap()
    }
}

impl From<usize> for Handle {
    fn from(value: usize) -> Self {
        Handle(value)
    }
}

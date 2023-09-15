use super::describe::Describe;
use super::handle::Handle;
use super::portal::Portal;
use crate::error::PgResult;
use std::rc::Rc;

#[derive(Debug)]
struct StatementImpl {
    handle: Handle,
    describe: Describe,
}

#[derive(Debug, Clone)]
pub struct Statement(Rc<StatementImpl>);

impl Statement {
    /// Prepare statement from query.
    pub fn prepare(query: &str) -> PgResult<Statement> {
        let handle = Handle::prepare(query)?;
        let describe = handle.describe()?;
        let stmt = StatementImpl { handle, describe };

        Ok(Statement(stmt.into()))
    }

    /// Create a portal by binding parameters to statement.
    pub fn bind(&self) -> PgResult<Portal> {
        self.0.handle.bind()?;
        Ok(Portal::new(self.clone()))
    }

    /// Get the handle representing the statement.
    pub fn handle(&self) -> &Handle {
        &self.0.handle
    }

    /// Get statement description.
    pub fn describe(&self) -> &Describe {
        &self.0.describe
    }
}

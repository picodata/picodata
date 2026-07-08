//! Executor and SQL-backend (IR-to-SQL serialization) layers of the
//! distributed SQL stack.

pub mod backend;
pub mod executor;
#[cfg(feature = "mock")]
pub mod test_helpers;

pub use sql_ir::{collection, crit, debug, error, fatal, info, system, verbose, warn};
pub use sql_ir::{errors, ir, log, utils};

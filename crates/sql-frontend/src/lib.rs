//! SQL frontend of the distributed SQL stack: pest grammar, AST and
//! the AST-to-IR populator.

#[macro_use]
extern crate pest_derive;

pub mod frontend;

pub use sql_ir::{collection, crit, debug, error, fatal, info, system, verbose, warn};
pub use sql_ir::{errors, ir, log, utils};

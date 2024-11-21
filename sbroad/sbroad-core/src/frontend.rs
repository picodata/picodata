//! Frontend module.
//!
//! A list of different frontend implementations
//! to build the intermediate representation (IR).

use crate::errors::SbroadError;
use crate::executor::engine::Metadata;
use crate::ir::Plan;

pub trait Ast {
    /// Get `Plan` from `Ast`.
    ///
    /// # Errors
    /// - Unable to fill `Ast` from pest pairs
    /// - Unable to resolve metadata
    fn transform_into_plan<M>(query: &str, metadata: &M) -> Result<Plan, SbroadError>
    where
        M: Metadata + Sized;
}

pub mod sql;

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FilterDecodeError {
    #[error("filter buffer too short: need {need} bytes, got {got}")]
    Truncated { need: usize, got: usize },

    #[error("unknown filter type tag: {0}")]
    UnknownTag(u8),

    #[error("filter array length zero")]
    EmptyArray,

    #[error("filter array length mismatch: header says {header}, payload has {payload}")]
    ArrayLengthMismatch { header: u32, payload: usize },
}

#[derive(Debug, Error)]
pub enum FilterBuildError {
    #[error("XOR-filter construction did not converge after {0} attempts")]
    NotConverged(u32),
}

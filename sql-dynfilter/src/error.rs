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
    /// `xorf::BinaryFuse8` reported a construction failure — usually
    /// duplicate keys (which we dedup before calling) but in rare cases
    /// the random hypergraph fails to peel. The xorf message is kept
    /// verbatim so a future caller can surface it.
    #[error("BinaryFuse8 construction failed: {0}")]
    FuseBuildFailed(String),
}

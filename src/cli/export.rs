mod catalog;
mod diagnostics;
mod output;
mod render;

#[derive(Debug, thiserror::Error)]
enum ExportError {
    #[error("failed writing the dump to {path}: {source}")]
    Output {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed writing the dump: {0}")]
    Write(#[from] std::io::Error),

    #[error("{0}")]
    Unsupported(String),
}

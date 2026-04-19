use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum FreestilerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Geometry parsing failed: {0}")]
    GeometryParse(String),

    #[error("Tile encoding failed: {0}")]
    TileEncoding(String),

    #[error("No tiles generated")]
    NoTilesGenerated,

    #[error("No valid features found{0}")]
    NoFeatures(String),

    #[error("CRS mismatch: {0}")]
    CrsMismatch(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("File error: {path}: {message}")]
    File { path: PathBuf, message: String },

    #[error("{0}")]
    Other(String),
}

impl FreestilerError {
    pub fn other(msg: impl Into<String>) -> Self {
        FreestilerError::Other(msg.into())
    }

    pub fn db(msg: impl Into<String>) -> Self {
        FreestilerError::Database(msg.into())
    }

    pub fn geom(msg: impl Into<String>) -> Self {
        FreestilerError::GeometryParse(msg.into())
    }

    pub fn encoding(msg: impl Into<String>) -> Self {
        FreestilerError::TileEncoding(msg.into())
    }
}

impl From<FreestilerError> for String {
    fn from(err: FreestilerError) -> String {
        err.to_string()
    }
}

pub type Result<T> = std::result::Result<T, FreestilerError>;

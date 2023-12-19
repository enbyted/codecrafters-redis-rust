pub mod error;
pub mod resp;

pub type Result<T> = std::result::Result<T, error::Error>;

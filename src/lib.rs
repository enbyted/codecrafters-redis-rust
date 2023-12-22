pub mod error;
pub mod resp;
pub mod rdb;

pub type Result<T> = std::result::Result<T, error::Error>;

pub mod error;
pub mod rdb;
pub mod resp;
pub mod store;
pub mod stream;

pub type Result<T> = std::result::Result<T, error::Error>;

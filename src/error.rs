use std::{str::Utf8Error, num::ParseIntError};

use thiserror::{self, Error};

use crate::resp::Type;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error")]
    AsyncIoError(#[from] tokio::io::Error),
    #[error("Context: {0}")]
    Context(String, #[source] Box<Error>),

    #[error("Invalid UTF-8 sequence")]
    Utf8Error(#[from] Utf8Error),
    #[error("Parse error (int)")]
    PraseIntError(#[from] ParseIntError),
    #[error("Unknown type specifier {:?} ({})", char::from_u32(* .0 as u32), .0)]
    UnknownTypeSpecifier(u8),
    #[error("Invalid CR LF terminator {:?}, {:?} ({}, {})", char::from_u32(* .0 as u32), char::from_u32(* .1 as u32), .0, .1)]
    InvalidCrLfTerminator(u8, u8),

    #[error("Unexpected type when parsing command {0:?}")]
    UnexpectedCommandType(Type),

    #[error("Not yet implemented")]
    Unimplemented,
    #[error("Unimplemented command '{0}'")]
    UnimplementedCommand(String),
}

pub trait WithContext<T, E> {
    fn context(self, context: &str) -> std::result::Result<T, E>;
}

impl<T> WithContext<T, Error> for std::result::Result<T, Error> {
    fn context(self, context: &str) -> Self {
        self.map_err(|err| Error::Context(context.into(), Box::new(err)))
    }
}

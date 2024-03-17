use std::{fmt::Display, num::ParseIntError, str::Utf8Error};

use thiserror::{self, Error};

use crate::{
    resp::Type,
    stream::{InsertionError, ItemIdParseError},
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error")]
    AsyncIoError(#[from] tokio::io::Error),
    #[error("{0}")]
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

    #[error("Missing argument {1} in {0} command")]
    MissingArgument(&'static str, &'static str),
    #[error("Unexpected argument '{0}'")]
    UnexpectedArgument(String),

    #[error("Parse error {0:?}")]
    ParseError(nom::Err<nom::error::Error<Vec<u8>>>),
    #[error("Parse error {0:?}")]
    VerboseParseError(nom::Err<nom::error::VerboseError<Vec<u8>>>),

    #[error("Expected '{0}', but got something else")]
    ExpectedOtherType(&'static str),

    #[error("Failed to insert into stream: {0}")]
    StreamInsertError(#[from] InsertionError),

    #[error("Failed to parse item id: {0}")]
    ItemIdParseError(#[from] ItemIdParseError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    Generic,
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Generic => write!(f, "ERR"),
        }
    }
}

impl Error {
    pub fn with_trace(&self) -> PrintTrace<'_> {
        PrintTrace(self)
    }

    pub fn kind(&self) -> ErrorKind {
        ErrorKind::Generic
    }

    pub fn redis_error_message(&self, cmd: &str) -> String {
        match self {
            Self::StreamInsertError(InsertionError::IdIsNotGreaterThanHighestStored(_)) => format!(
                "The ID specified in {cmd} is equal or smaller than the target stream top item"
            ),
            Self::ItemIdParseError(ItemIdParseError::TooLow) => {
                format!("The ID specified in {cmd} must be greater than 0-0")
            }
            other => format!("Internal Error in {cmd}: {other}"),
        }
    }

    pub fn is_fatal(&self) -> bool {
        match self {
            Self::AsyncIoError(_) | Self::Context(_, _) => true,
            _ => false,
        }
    }
}

impl From<nom::Err<nom::error::Error<&[u8]>>> for Error {
    fn from(value: nom::Err<nom::error::Error<&[u8]>>) -> Self {
        match value {
            nom::Err::Incomplete(needed) => Self::ParseError(nom::Err::Incomplete(needed)),
            nom::Err::Error(err) => Self::ParseError(nom::Err::Error(nom::error::Error::new(
                err.input.to_vec(),
                err.code,
            ))),
            nom::Err::Failure(err) => Self::ParseError(nom::Err::Failure(nom::error::Error::new(
                err.input.to_vec(),
                err.code,
            ))),
        }
    }
}

fn to_owned_verbose_error(
    error: &(&[u8], nom::error::VerboseErrorKind),
) -> (Vec<u8>, nom::error::VerboseErrorKind) {
    (error.0.to_vec(), error.1.clone())
}

impl From<nom::Err<nom::error::VerboseError<&[u8]>>> for Error {
    fn from(value: nom::Err<nom::error::VerboseError<&[u8]>>) -> Self {
        match value {
            nom::Err::Incomplete(needed) => Self::VerboseParseError(nom::Err::Incomplete(needed)),
            nom::Err::Error(err) => {
                Self::VerboseParseError(nom::Err::Error(nom::error::VerboseError {
                    errors: err.errors.iter().map(to_owned_verbose_error).collect(),
                }))
            }
            nom::Err::Failure(err) => {
                Self::VerboseParseError(nom::Err::Failure(nom::error::VerboseError {
                    errors: err.errors.iter().map(to_owned_verbose_error).collect(),
                }))
            }
        }
    }
}

pub trait WithContext<T, E> {
    fn context(self, context: &str) -> std::result::Result<T, E>;
}

impl<T> WithContext<T, Error> for std::result::Result<T, Error> {
    fn context(self, context: &str) -> Self {
        self.map_err(|err| Error::Context(context.into(), Box::new(err)))
    }
}

pub struct PrintTrace<'a>(&'a Error);

impl Display for PrintTrace<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.0))?;

        if let Some(cause) = std::error::Error::source(self.0) {
            f.write_str("\nCaused by:\n")?;
            let mut cause = Some(cause);
            loop {
                if let Some(err) = cause {
                    f.write_fmt(format_args!("  {err}"))?;
                    cause = err.source();
                } else {
                    break;
                }
            }
        }
        Ok(())
    }
}

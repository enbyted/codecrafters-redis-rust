use std::{pin::Pin, future::Future};
use std::str;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{Result, error::Error};

#[derive(Debug, Clone)]
pub enum Type {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Type>),
}


type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
trait FutureExt: Future {
    fn boxed<'a>(self) -> BoxFuture<'a, Self::Output> where Self: Sized + Send + 'a, {
        Box::pin(self)
    }
}

impl<T, U: std::future::Future<Output = T>> FutureExt for U {}

impl Type {
    pub async fn parse(stream: &mut Pin<&mut (impl AsyncRead + Unpin + Send)>) -> Result<Type> {
        let ident = stream.as_mut().read_u8().await?;
        match char::from(ident) {
            '+' => Self::parse_simple_string(stream).await,
            '$' => Self::parse_bulk_string(stream).await,
            '*' => Self::parse_array(stream).await,
            _ => Err(Error::UnknownTypeSpecifier(ident))
        }
    }

    fn parse_array<'a>(stream: &'a mut Pin<&mut (impl AsyncRead + Unpin + Send)>) -> BoxFuture<'a, Result<Type>> {
        async move {
            let len = Type::parse_usize(stream).await?;
            let mut buffer = Vec::with_capacity(len);
            for _ in 0..len {
                let ty = Type::parse(stream).await?;
                buffer.push(ty);
            }

            Ok(Type::Array(buffer))
        }.boxed()
    }

    async fn parse_simple_string(stream: &mut Pin<&mut impl AsyncRead>) -> Result<Type> {
        let buffer = Self::read_until_crlf(stream).await?;

        Ok(Type::SimpleString(str::from_utf8(&buffer)?.into()))
    }

    async fn parse_bulk_string(stream: &mut Pin<&mut impl AsyncRead>) -> Result<Type> {
        let len = Self::parse_usize(stream).await?;
        let mut buffer = Vec::with_capacity(len);
        buffer.resize(len, 0);
        stream.read_exact(&mut buffer).await?;

        let mut terminator = [0; 2];
        stream.read_exact(&mut terminator).await?;
        if terminator != *b"\r\n" {
            return Err(Error::InvalidCrLfTerminator(terminator[0], terminator[1]));
        }

        Ok(Type::BulkString(str::from_utf8(&buffer)?.into()))
    }

    async fn parse_usize(stream: &mut Pin<&mut impl AsyncRead>) -> Result<usize> {
        let len = Self::read_until_crlf(stream).await?;
        let len: usize = str::from_utf8(&len)?.parse()?;
        Ok(len)
    }

    async fn read_until_crlf(stream: &mut Pin<&mut impl AsyncRead>) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        loop {
            let byte = stream.read_u8().await?;
            if byte == b'\r' {
                let byte = stream.read_u8().await?;
                if byte == b'\n' {
                    break;
                } else {
                    return Err(Error::InvalidCrLfTerminator(b'\r', byte));
                }
            } else {
                buffer.push(byte);
            }
        }

        Ok(buffer)
    }
}

impl Type {
    pub async fn write(&self, stream: &mut (impl AsyncWrite + Unpin)) -> Result<()> {
        match self {
            Type::SimpleString(str) => Self::write_simple_string(stream, &str).await,
            Type::BulkString(_) => todo!(),
            Type::Array(_) => todo!(),
        }
    }

    async fn write_simple_string(stream: &mut (impl AsyncWrite + Unpin), value: &str) -> Result<()> {
        let mut stream = Pin::new(stream);
        // TODO: Check that the string does not contain any CR or LF
        stream.write_u8(b'+').await?;
        stream.write_all(value.as_bytes()).await?;
        stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
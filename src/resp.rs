use std::str;
use std::{future::Future, pin::Pin};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{error::Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Type>),
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
trait FutureExt: Future {
    fn boxed<'a>(self) -> BoxFuture<'a, Self::Output>
    where
        Self: Sized + Send + 'a,
    {
        Box::pin(self)
    }
}

impl<T, U: std::future::Future<Output = T>> FutureExt for U {}

type PinnedRead<'a> = Pin<&'a mut (dyn AsyncRead + Unpin + Send)>;
type PinnedWrite<'a> = Pin<&'a mut (dyn AsyncWrite + Unpin + Send)>;

impl Type {
    pub async fn parse(stream: &mut Pin<&mut (dyn AsyncRead + Unpin + Send)>) -> Result<Type> {
        let ident = stream.as_mut().read_u8().await?;
        match char::from(ident) {
            '+' => Self::parse_simple_string(stream).await,
            '$' => Self::parse_bulk_string(stream).await,
            '*' => Self::parse_array(stream).await,
            _ => Err(Error::UnknownTypeSpecifier(ident)),
        }
    }

    fn parse_array<'a>(
        stream: &'a mut Pin<&mut (dyn AsyncRead + Unpin + Send)>,
    ) -> BoxFuture<'a, Result<Type>> {
        async move {
            let len = Type::parse_usize(stream).await?;
            let mut buffer = Vec::with_capacity(len);
            for _ in 0..len {
                let ty = Type::parse(stream).await?;
                buffer.push(ty);
            }

            Ok(Type::Array(buffer))
        }
        .boxed()
    }

    async fn parse_simple_string(stream: &mut Pin<&mut (impl AsyncRead + ?Sized)>) -> Result<Type> {
        let buffer = Self::read_until_crlf(stream).await?;

        Ok(Type::SimpleString(str::from_utf8(&buffer)?.into()))
    }

    async fn parse_bulk_string(stream: &mut Pin<&mut (impl AsyncRead + ?Sized)>) -> Result<Type> {
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

    async fn parse_usize(stream: &mut Pin<&mut (impl AsyncRead + ?Sized)>) -> Result<usize> {
        let len = Self::read_until_crlf(stream).await?;
        let len: usize = str::from_utf8(&len)?.parse()?;
        Ok(len)
    }

    async fn read_until_crlf(stream: &mut Pin<&mut (impl AsyncRead + ?Sized)>) -> Result<Vec<u8>> {
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
    pub async fn write(&self, stream: &mut (dyn AsyncWrite + Unpin + Send)) -> Result<()> {
        self.write_impl(&mut Pin::new(stream)).await
    }

    async fn write_impl(&self, stream: &mut PinnedWrite<'_>) -> Result<()> {
        match self {
            Type::SimpleString(str) => Self::write_simple_string(stream, &str).await,
            Type::BulkString(str) => Self::write_bulk_string(stream, str).await,
            Type::Array(items) => Self::write_array(stream, items).await,
        }
    }

    async fn write_simple_string(stream: &mut PinnedWrite<'_>, value: &str) -> Result<()> {
        // TODO: Check that the string does not contain any CR or LF
        stream.write_u8(b'+').await?;
        stream.write_all(value.as_bytes()).await?;
        stream.write_all(b"\r\n").await?;

        Ok(())
    }

    async fn write_bulk_string(stream: &mut PinnedWrite<'_>, value: &str) -> Result<()> {
        stream.write_u8(b'$').await?;
        stream.write_all(value.len().to_string().as_bytes()).await?;
        stream.write_all(b"\r\n").await?;
        stream.write_all(value.as_bytes()).await?;
        stream.write_all(b"\r\n").await?;

        Ok(())
    }

    fn write_array<'a>(
        stream: &'a mut PinnedWrite<'_>,
        value: &'a [Type],
    ) -> BoxFuture<'a, Result<()>> {
        async move {
            stream.write_u8(b'*').await?;
            stream.write_all(value.len().to_string().as_bytes()).await?;
            stream.write_all(b"\r\n").await?;

            for item in value {
                item.write(stream).await?;
            }
            Ok(())
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use std::pin::Pin;

    use crate::resp::Type;

    #[tokio::test]
    async fn parse_simple_string() {
        let input = b"+This is a test string\r\n";
        let mut input = &input[..];
        let parsed = Type::parse(&mut Pin::new(&mut input))
            .await
            .expect("Input was formatted correctly, parse should succeed");
        assert_eq!(parsed, Type::SimpleString("This is a test string".into()));
    }

    #[tokio::test]
    async fn write_simple_string() {
        let mut buffer = Vec::<u8>::new();
        Type::SimpleString("Test string".into())
            .write(&mut buffer)
            .await
            .expect("Write should succeed");
        assert_eq!(buffer, b"+Test string\r\n");
    }

    #[tokio::test]
    async fn parse_bulk_string() {
        let input = b"$8\r\ntest foo\r\n";
        let mut input = &input[..];
        let parsed = Type::parse(&mut Pin::new(&mut input)).await.expect("");
        assert_eq!(parsed, Type::BulkString("test foo".into()));
    }

    #[tokio::test]
    async fn write_bulk_string() {
        let mut buffer = Vec::<u8>::new();
        Type::BulkString("Test string".into())
            .write(&mut buffer)
            .await
            .expect("Write should succeed");
        assert_eq!(buffer, b"$11\r\nTest string\r\n");
    }

    #[tokio::test]
    async fn parse_array() {
        let input = b"*3\r\n+OK1\r\n+OK2\r\n+OK3\r\n";
        let mut input = &input[..];
        let parsed = Type::parse(&mut Pin::new(&mut input)).await.expect("");
        assert_eq!(
            parsed,
            Type::Array(vec![
                Type::SimpleString("OK1".into()),
                Type::SimpleString("OK2".into()),
                Type::SimpleString("OK3".into())
            ])
        );
    }

    #[tokio::test]
    async fn write_array() {
        let mut buffer = Vec::<u8>::new();
        let mut items = Vec::new();
        items.push(Type::SimpleString("Test1".into()));
        items.push(Type::BulkString("Test2\n".into()));
        Type::Array(items)
            .write(&mut buffer)
            .await
            .expect("Write should succeed");
        assert_eq!(buffer, b"*2\r\n+Test1\r\n$6\r\nTest2\n\r\n");
    }
}

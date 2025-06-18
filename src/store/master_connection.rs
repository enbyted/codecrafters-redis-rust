use tokio::net::TcpStream;

use crate::{
    error::{Error, WithContext},
    resp::Type,
    Result,
};

pub(super) struct MasterConnection {
    stream: TcpStream,
    listening_port: u16,
    replication_id: Option<String>,
    replication_offset: i64,
}

impl MasterConnection {
    pub fn new(stream: TcpStream, listening_port: u16) -> Self {
        Self {
            stream,
            listening_port,
            replication_id: None,
            replication_offset: -1,
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        self.send_initial_ping().await?;
        self.send_replconf("listening-port", self.listening_port)
            .await?;
        self.send_replconf("capa", "psync2").await?;
        self.send_psync().await?;
        Ok(())
    }

    async fn send_psync(&mut self) -> Result<()> {
        let reply = self
            .execute_command(Type::Array(vec![
                Type::BulkString("PSYNC".to_string()),
                Type::BulkString(
                    self.replication_id
                        .clone()
                        .unwrap_or_else(|| "?".to_string()),
                ),
                Type::BulkString(self.replication_offset.to_string()),
            ]))
            .await?;
        match reply {
            Type::SimpleString(reply) if reply.starts_with("FULLRESYNC") => {
                let mut parts = reply.split_ascii_whitespace();
                parts.next(); // skip "FULLRESYNC"
                self.replication_id = Some(
                    parts
                        .next()
                        .ok_or_else(|| Error::InvalidPsyncReplyFormat(reply.clone()))?
                        .to_string(),
                );
                self.replication_offset = parts
                    .next()
                    .ok_or_else(|| Error::InvalidPsyncReplyFormat(reply.clone()))?
                    .parse::<i64>()
                    .map_err(Error::from)
                    .context("Parsing PSYNC replication offset")?;
                Ok(())
            }
            reply => Err(Error::UnexpectedReply {
                reply,
                expected: "FULLRESYNC",
            }),
        }
    }

    async fn send_replconf(&mut self, key: impl ToString, value: impl ToString) -> Result<()> {
        let reply = self
            .execute_command(Type::Array(vec![
                Type::BulkString("REPLCONF".to_string()),
                Type::BulkString(key.to_string()),
                Type::BulkString(value.to_string()),
            ]))
            .await?;

        match reply {
            Type::SimpleString(reply) if reply == "OK" => Ok(()),
            reply => Err(Error::UnexpectedReply {
                reply,
                expected: "OK",
            }),
        }
    }

    async fn send_initial_ping(&mut self) -> Result<()> {
        eprintln!("Sending initial ping to master");
        let reply = self
            .execute_command(Type::Array(vec![Type::BulkString("PING".to_string())]))
            .await?;
        match reply {
            Type::SimpleString(reply) => {
                if reply == "PONG" {
                    Ok(())
                } else {
                    Err(Error::UnexpectedReply {
                        reply: Type::SimpleString(reply),
                        expected: "PONG",
                    })
                }
            }
            reply => Err(Error::UnexpectedReply {
                reply,
                expected: "PONG",
            }),
        }
    }

    async fn execute_command(&mut self, command: Type) -> Result<Type> {
        command.write(&mut self.stream).await?;
        let reply = Type::parse(&mut self.stream).await?;
        eprintln!("Received reply: {reply:?}");
        Ok(reply)
    }
}

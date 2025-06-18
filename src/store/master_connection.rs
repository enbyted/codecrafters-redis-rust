use tokio::net::TcpStream;

use crate::{error::Error, resp::Type, Result};

pub(super) struct MasterConnection {
    stream: TcpStream,
    listening_port: u16,
}

impl MasterConnection {
    pub fn new(stream: TcpStream, listening_port: u16) -> Self {
        Self {
            stream,
            listening_port,
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        self.send_initial_ping().await?;
        self.send_replconf("listening-port", self.listening_port)
            .await?;
        self.send_replconf("capa", "psync2").await?;
        Ok(())
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

use tokio::net::TcpStream;

use crate::{error::Error, resp::Type, Result};

pub(super) struct MasterConnection {
    stream: TcpStream,
}

impl MasterConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub async fn init(&mut self) -> Result<()> {
        self.send_initial_ping().await?;
        Ok(())
    }

    async fn send_initial_ping(&mut self) -> Result<()> {
        eprintln!("Sending initial ping to master");
        Type::Array(vec![Type::BulkString("PING".to_string())])
            .write(&mut self.stream)
            .await?;
        let reply = Type::parse(&mut self.stream).await?;
        eprintln!("Received reply: {reply:?}");
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
}

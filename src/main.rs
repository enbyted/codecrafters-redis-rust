use anyhow;
use redis_starter_rust::error::{Error, WithContext};
use redis_starter_rust::resp::Type;
use redis_starter_rust::Result;
use std::error::Error as StdError;
use std::net::SocketAddr;
use tokio;
use tokio::net::{TcpListener, TcpStream};

struct Client(TcpStream, SocketAddr);

impl Client {
    fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        Self(stream, addr)
    }

    async fn run(mut self) -> () {
        eprintln!("Client {} connected", self.1);
        let res = self.run_int().await.context(&format!("Client {}", self.1));
        if let Err(error) = res {
            eprintln!("[ERROR] {}", error);
            if let Some(cause) = error.source() {
                eprintln!();
                eprintln!("Caused by:");
                let mut cause = Some(cause);
                loop {
                    if let Some(err) = cause {
                        eprintln!("  {err}");
                        cause = err.source();
                    } else {
                        break;
                    }
                }
            }
        }
    }

    async fn run_int(&mut self) -> Result<()> {
        loop {
            let cmd = self.read_command().await.context("Reading command")?;
            eprintln!("Received CMD: {:?}", &cmd);

            let mut iter = cmd.iter();
            match iter.next().map(|s| s.as_str()) {
                Some("ping") => {
                    let reply = if let Some(arg) = iter.next() {
                        Type::BulkString(arg.clone())
                    } else {
                        Type::SimpleString("PONG".into())
                    };

                    reply.write(&mut self.0).await?;
                }
                Some(cmd) => return Err(Error::UnimplementedCommand(cmd.into())),
                None => todo!(),
            }
        }
    }

    async fn read_command(&mut self) -> Result<Vec<String>> {
        let parsed = Type::parse(&mut std::pin::Pin::new(&mut self.0)).await.context("Parsing command")?;
        if let Type::Array(cmds) = parsed {
            let ret = cmds
                .into_iter()
                .map(|c| match c {
                    Type::BulkString(str) => Ok(str.to_ascii_lowercase()),
                    Type::SimpleString(str) => Ok(str.to_ascii_lowercase()),
                    other => Err(Error::UnexpectedCommandType(other)),
                })
                .collect::<Result<_>>()
                .context("Unwrapping parsed command")?;

            Ok(ret)
        } else {
            Err(Error::UnexpectedCommandType(parsed))
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let address = "127.0.0.1:6379";

    let listener = TcpListener::bind(address).await?;
    eprintln!("Listening on {address}");

    loop {
        let (stream, addr) = listener.accept().await?;
        tokio::spawn(Client::new(stream, addr).run());
    }
}

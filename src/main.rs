use anyhow;
use redis_starter_rust::error::{Error, WithContext};
use redis_starter_rust::resp::Type;
use redis_starter_rust::Result;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct DataStore(Arc<Mutex<HashMap<String, String>>>);

impl DataStore {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    pub async fn set(&self, key: String, value: String) -> Option<String> {
        self.0.clone().lock_owned().await.insert(key, value)
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        self.0.lock().await.get(key).map(|v| v.clone())
    }
}

struct Client {
    stream: TcpStream,
    addr: SocketAddr,
    store: DataStore,
}

impl Client {
    fn new(stream: TcpStream, addr: SocketAddr, store: DataStore) -> Self {
        Self {
            stream,
            addr,
            store,
        }
    }

    async fn run(mut self) -> () {
        eprintln!("Client {} connected", self.addr);
        let res = self
            .run_int()
            .await
            .context(&format!("Client {}", self.addr));
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

            let mut args = cmd.into_iter();
            let cmd = args.next();
            match cmd.as_ref().map(|s| s.as_str()) {
                Some("ping") => self.handle_ping(args).await?,
                Some("echo") => self.handle_echo(args).await?,
                Some("get") => self.handle_get(args).await?,
                Some("set") => self.handle_set(args).await?,
                Some(cmd) => return Err(Error::UnimplementedCommand(cmd.into())),
                None => todo!(),
            }
        }
    }

    async fn handle_get(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let key = args.next().ok_or(Error::MissingArgument("get", "key"))?;

        self.store
            .get(&key)
            .await
            .map_or(Type::Null, |s| Type::BulkString(s))
            .write(&mut self.stream)
            .await
    }

    async fn handle_set(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let key = args.next().ok_or(Error::MissingArgument("set", "key"))?;
        let value = args.next().ok_or(Error::MissingArgument("set", "value"))?;

        self.store.set(key, value).await;
        Type::SimpleString("OK".into())
            .write(&mut self.stream)
            .await
    }

    async fn handle_echo(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let reply = args.next().unwrap_or_default();

        Type::BulkString(reply).write(&mut self.stream).await?;
        Ok(())
    }

    async fn handle_ping(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let reply = if let Some(arg) = args.next() {
            Type::BulkString(arg)
        } else {
            Type::SimpleString("PONG".into())
        };

        reply.write(&mut self.stream).await?;
        Ok(())
    }

    async fn read_command(&mut self) -> Result<Vec<String>> {
        let parsed = Type::parse(&mut std::pin::Pin::new(&mut self.stream))
            .await
            .context("Parsing command")?;
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

    let store = DataStore::new();

    loop {
        let (stream, addr) = listener.accept().await?;
        tokio::spawn(Client::new(stream, addr, store.clone()).run());
    }
}

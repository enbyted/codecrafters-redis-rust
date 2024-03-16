use anyhow;
use redis_starter_rust::error::{Error, WithContext};
use redis_starter_rust::resp::Type;
use redis_starter_rust::stream::{ItemData, ItemId, Stream};
use redis_starter_rust::{rdb, Result};
use std::collections::HashMap;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{env, path};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::{self, fs};

#[derive(Debug, Clone)]
enum Value {
    String(String),
    Stream(Stream),
}

impl Value {
    pub fn kind(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::Stream(_) => "stream",
        }
    }

    pub fn as_stream_mut(&mut self) -> Option<&mut Stream> {
        match self {
            Value::Stream(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct DataValue {
    value: Value,
    expires_at: Option<SystemTime>,
}

#[derive(Debug, Clone)]
struct DataStore {
    data: Arc<Mutex<HashMap<String, DataValue>>>,
    config: Arc<HashMap<String, String>>,
}

impl DataStore {
    pub fn new(config: HashMap<String, String>) -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(config),
        }
    }

    pub async fn load_from_rdb(&mut self) -> Result<()> {
        match (self.config.get("dir"), self.config.get("dbfilename")) {
            (Some(dir), Some(file)) => {
                let path = path::Path::new(dir).join(file);
                let data = Self::read_file(&path)
                    .await
                    .context(format!("File path {path:?}").as_str())?;
                eprintln!("RDB file data: {data:?}");

                let parsed = rdb::Database::parse(&data)?;

                let mut data = self.data.lock().await;
                for (key, value) in parsed.keys() {
                    let value = match value {
                        rdb::OwnedValue::String(s) => s.clone(),
                        rdb::OwnedValue::Integer(v) => v.to_string(),
                    };

                    data.insert(
                        key.clone(),
                        DataValue {
                            value: Value::String(value),
                            expires_at: None,
                        },
                    );
                }
                let now = SystemTime::now();
                for (key, (value, expires_at)) in parsed.expiring() {
                    let expires_at = expires_at.clone();

                    if expires_at < now {
                        continue;
                    }

                    let value = match value {
                        rdb::OwnedValue::String(s) => s.clone(),
                        rdb::OwnedValue::Integer(v) => v.to_string(),
                    };

                    data.insert(
                        key.clone(),
                        DataValue {
                            value: Value::String(value),
                            expires_at: Some(expires_at),
                        },
                    );
                }
            }
            (Some(_), None) => eprintln!("Not loading database, `dbfilename` not provided"),
            (None, Some(_)) => eprintln!("Not loading database, `dir` not provided"),
            (None, None) => eprintln!("Not loading database, `dir` and `dbfilename` not provided"),
        }
        Ok(())
    }

    async fn read_file(path: &PathBuf) -> Result<Vec<u8>> {
        Ok(fs::read(path).await?)
    }

    pub async fn set(
        &self,
        key: String,
        value: Value,
        expires_at: Option<SystemTime>,
    ) -> Option<Value> {
        let now = SystemTime::now();

        self.data
            .clone()
            .lock_owned()
            .await
            .insert(key, DataValue { value, expires_at })
            .and_then(|v| match v.expires_at {
                Some(expires_at) if expires_at <= now => None,
                _ => Some(v.value),
            })
    }

    pub async fn get(&self, key: &str) -> Option<Value> {
        let now = SystemTime::now();

        self.data
            .lock()
            .await
            .get(key)
            .and_then(|v| match v.expires_at {
                Some(expires_at) if expires_at <= now => None,
                _ => Some(v.value.clone()),
            })
    }

    pub async fn insert_stream_item(
        &self,
        key: String,
        id: Option<ItemId>,
        data: ItemData,
    ) -> Result<ItemId> {
        Ok(self
            .data
            .lock()
            .await
            .entry(key)
            .or_insert_with(|| DataValue {
                value: Value::Stream(Stream::new()),
                expires_at: None,
            })
            .value
            .as_stream_mut()
            .ok_or(Error::ExpectedOtherType("stream"))?
            .insert(id, data)?)
    }

    pub async fn keys(&self) -> Vec<String> {
        self.data.lock().await.keys().map(|k| k.clone()).collect()
    }

    pub fn get_config(&self, key: &str) -> Option<&str> {
        self.config.get(key).map(|s| s.as_str())
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
            eprintln!("[ERROR] {}", error.with_trace());
        }
    }

    async fn run_int(&mut self) -> Result<()> {
        loop {
            let cmd = self.read_command().await.context("Reading command")?;
            eprintln!("Received CMD: {:?}", &cmd);
            let command = cmd
                .first()
                .map(|s| s.as_str())
                .unwrap_or("")
                .to_ascii_uppercase();

            if let Err(err) = self.run_command(cmd).await {
                if err.is_fatal() {
                    return Err(err);
                } else {
                    Type::SimpleString(err.to_redis_error(&command))
                        .write(&mut self.stream)
                        .await?;
                }
            }
        }
    }

    async fn run_command(&mut self, cmd: Vec<String>) -> Result<()> {
        let mut args = cmd.into_iter();
        let cmd = args.next().map(|s| s.to_ascii_lowercase());

        match cmd.as_ref().map(|s| s.as_str()) {
            Some("ping") => self.handle_ping(args).await?,
            Some("echo") => self.handle_echo(args).await?,
            Some("get") => self.handle_get(args).await?,
            Some("type") => self.handle_type(args).await?,
            Some("set") => self.handle_set(args).await?,
            Some("xadd") => self.handle_xadd(args).await?,
            Some("keys") => self.handle_keys(args).await?,
            Some("config") => self.handle_config(args).await?,
            Some(cmd) => return Err(Error::UnimplementedCommand(cmd.into())),
            None => todo!(),
        }

        Ok(())
    }

    async fn handle_config(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let subcmd = args.next().map(|s| s.to_ascii_lowercase());
        match subcmd.as_ref().map(|v| v.as_str()) {
            Some("get") => self.handle_config_get(args).await,
            Some(cmd) => Err(Error::UnimplementedCommand(format!("CONFIG {cmd}"))),
            None => todo!(),
        }
    }

    async fn handle_config_get(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let key = args
            .next()
            .ok_or(Error::MissingArgument("config get", "key"))?
            .to_ascii_lowercase();

        self.store
            .get_config(&key)
            .map_or(Type::NullString, |s| {
                Type::Array(vec![Type::BulkString(key), Type::BulkString(s.into())])
            })
            .write(&mut self.stream)
            .await
    }

    async fn handle_keys(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let key = args.next().ok_or(Error::MissingArgument("keys", "key"))?;
        if key != "*" {
            return Err(Error::Unimplemented).context("Only `KEYS *` is implemented");
        }

        let keys = self
            .store
            .keys()
            .await
            .into_iter()
            .map(|k| Type::BulkString(k))
            .collect();

        Type::Array(keys).write(&mut self.stream).await?;

        Ok(())
    }

    async fn handle_get(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let key = args.next().ok_or(Error::MissingArgument("get", "key"))?;

        self.store
            .get(&key)
            .await
            .map_or(Ok(Type::NullString), |s| match s {
                Value::String(x) => Ok(Type::BulkString(x)),
                _ => Err(Error::Unimplemented),
            })?
            .write(&mut self.stream)
            .await
    }

    async fn handle_type(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let key = args.next().ok_or(Error::MissingArgument("get", "key"))?;

        self.store
            .get(&key)
            .await
            .map_or(Type::SimpleString("none".into()), |value| {
                Type::SimpleString(value.kind().to_owned())
            })
            .write(&mut self.stream)
            .await
    }

    async fn handle_set(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let key = args.next().ok_or(Error::MissingArgument("set", "key"))?;
        let value = args.next().ok_or(Error::MissingArgument("set", "value"))?;

        let expires_at = match (
            args.next().map(|v| v.to_ascii_lowercase()),
            args.next().and_then(|v| v.parse::<u64>().ok()),
        ) {
            (Some(cmd), Some(arg)) if cmd == "px" => {
                Some(SystemTime::now() + Duration::from_millis(arg))
            }
            _ => None,
        };

        self.store.set(key, Value::String(value), expires_at).await;
        Type::SimpleString("OK".into())
            .write(&mut self.stream)
            .await
    }

    async fn handle_xadd(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let key = args.next().ok_or(Error::MissingArgument("xadd", "key"))?;
        let id = args.next().ok_or(Error::MissingArgument("xadd", "id"))?;
        let mut items = HashMap::new();

        let id = if id == "*" {
            None
        } else {
            Some(ItemId::try_from(id.as_str())?)
        };

        while let Some(key) = args.next() {
            let value = args.next().ok_or(Error::MissingArgument("xadd", "value"))?;
            items.insert(key, value);
        }

        let id = self.store.insert_stream_item(key, id, items).await?;
        Type::SimpleString(id.to_string())
            .write(&mut self.stream)
            .await?;

        Ok(())
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
                    Type::BulkString(str) => Ok(str),
                    Type::SimpleString(str) => Ok(str),
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
    let mut config = HashMap::new();
    let args: Vec<_> = env::args().skip(1).collect();

    for val in args.chunks_exact(2) {
        let key = &val[0];
        let value = val[1].trim();
        if !key.starts_with("--") {
            eprintln!("Invalid config option {key}");
            anyhow::bail!("Arugment parsing failed");
        }
        let key = key[2..].trim();
        eprintln!("Config '{key}' = '{value}'");
        config.insert(key.into(), value.into());
    }

    let address = "127.0.0.1:6379";

    let listener = TcpListener::bind(address).await?;
    eprintln!("Listening on {address}");

    let mut store = DataStore::new(config);
    eprintln!("Trying to read data from persistent store");
    if let Err(err) = store.load_from_rdb().await {
        eprintln!("Error reading from storage: {}", err.with_trace());
    }

    loop {
        let (stream, addr) = listener.accept().await?;
        tokio::spawn(Client::new(stream, addr, store.clone()).run());
    }
}

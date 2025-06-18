use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::Bound,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{net::TcpStream, sync::mpsc};

use crate::{
    error::{Error, WithContext},
    resp::Type,
    store::{DataStore, Value},
    stream::{Item, ItemId},
    Result,
};

pub struct Client {
    stream: TcpStream,
    addr: SocketAddr,
    store: DataStore,
}

impl Client {
    pub fn new(stream: TcpStream, addr: SocketAddr, store: DataStore) -> Self {
        Self {
            stream,
            addr,
            store,
        }
    }

    pub async fn run(mut self) -> () {
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
                    Type::SimpleError(err.kind(), err.redis_error_message(&command))
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
            Some("xrange") => self.handle_xrange(args).await?,
            Some("xread") => self.handle_xread(args).await?,
            Some("keys") => self.handle_keys(args).await?,
            Some("config") => self.handle_config(args).await?,
            Some("info") => self.handle_info(args).await?,
            Some("replconf") => self.handle_replconf(args).await?,
            Some(cmd) => return Err(Error::UnimplementedCommand(cmd.into())),
            None => todo!(),
        }

        Ok(())
    }

    async fn handle_replconf(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let key = args
            .next()
            .ok_or(Error::MissingArgument("replconf", "key"))?;
        let value = args
            .next()
            .ok_or(Error::MissingArgument("replconf", "value"))?;
        eprintln!("REPLCONF {key} {value}");
        // self.store.replconf(key, value).await;
        Type::SimpleString("OK".into())
            .write(&mut self.stream)
            .await?;
        Ok(())
    }

    async fn handle_info(&mut self, _args: impl Iterator<Item = String>) -> Result<()> {
        let info = self.store.info().await;
        let resp = format!(
            "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
            info.role(),
            info.replication_id()
                .iter()
                .fold(String::new(), |s, v| format!("{s}{v:02x}")),
            info.replication_offset()
        );

        Type::BulkString(resp).write(&mut self.stream).await?;

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

        while let Some(key) = args.next() {
            let value = args.next().ok_or(Error::MissingArgument("xadd", "value"))?;
            items.insert(key, value);
        }

        let id = self
            .store
            .insert_stream_item(key, id.as_str().try_into()?, items)
            .await?;
        Type::BulkString(id.to_string())
            .write(&mut self.stream)
            .await?;

        Ok(())
    }

    async fn handle_xrange(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let key = args.next().ok_or(Error::MissingArgument("xrange", "key"))?;
        let start = args
            .next()
            .ok_or(Error::MissingArgument("xrange", "start"))?;
        let end = args.next().ok_or(Error::MissingArgument("xrange", "end"))?;

        let start = if start == "-" {
            Bound::Unbounded
        } else {
            Bound::Included(start.as_str().try_into()?)
        };

        let end = if end == "+" {
            Bound::Unbounded
        } else {
            Bound::Included(end.as_str().try_into()?)
        };

        self.store
            .get_ref(&key, move |value| -> Result<_> {
                let value = value
                    .as_stream()
                    .ok_or(Error::ExpectedOtherType("stream"))?;

                let range = value.range(start, end).map(|v| v.into()).collect();
                Ok(Type::Array(range))
            })
            .await
            .unwrap_or(Ok(Type::NullString))?
            .write(&mut self.stream)
            .await?;

        Ok(())
    }

    async fn handle_xread(&mut self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let mut streams = Vec::new();
        let mut block = None;
        let now: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .try_into()?;

        fn parse_item_id(value: &str, now: u64) -> Result<ItemId> {
            if value == "$" {
                Ok(ItemId::new(now, 0))
            } else {
                Ok(value.try_into()?)
            }
        }

        while let Some(arg) = args.next() {
            match arg.to_ascii_uppercase().as_str() {
                "STREAMS" => {
                    let args: Vec<_> = args.collect();
                    if args.len() % 2 != 0 {
                        return Err(Error::MissingArgument("xread", "item_id"));
                    }

                    let half_point = args.len() / 2;
                    for i in 0..half_point {
                        streams.push((args[i].clone(), parse_item_id(&args[i + half_point], now)?));
                    }

                    break;
                }
                "BLOCK" => {
                    let duration = args
                        .next()
                        .ok_or(Error::MissingArgument("xread", "block duration"))?;
                    block = Some(u64::from_str_radix(&duration, 10)?);
                }
                _ => {
                    return Err(Error::UnexpectedArgument(arg));
                }
            }
        }

        let mut resp = Vec::new();

        for (key, start) in &streams {
            let values = self
                .store
                .get_ref(&key, move |value| -> Result<_> {
                    let value = value
                        .as_stream()
                        .ok_or(Error::ExpectedOtherType("stream"))?;

                    let range = value
                        .range(Bound::Excluded(*start), Bound::Unbounded)
                        .map(|v| v.into())
                        .collect();
                    Ok(Type::Array(range))
                })
                .await
                .unwrap_or(Ok(Type::NullString))?;

            match values {
                Type::Array(arr) if arr.is_empty() => {}
                values => resp.push(Type::Array(vec![Type::BulkString(key.clone()), values])),
            }
        }

        if resp.is_empty() && block.is_some() {
            let block = block.expect("Just check that it is some");
            let (tx, mut rx) = mpsc::channel(1);

            for (key, _) in streams {
                self.store.notify_on_stream_insert(key, tx.clone()).await?;
            }

            if block > 0 {
                let timeout = tokio::time::sleep(Duration::from_millis(block));

                tokio::select! {
                    Some((key, id, data)) = rx.recv() => {
                        resp.push(Type::Array(vec![
                            Type::BulkString(key),
                            Type::Array(vec![Item::new(id, &data).into()]),
                        ]));
                    }
                    _ = timeout => {
                    }
                }
            } else if let Some((key, id, data)) = rx.recv().await {
                resp.push(Type::Array(vec![
                    Type::BulkString(key),
                    Type::Array(vec![Item::new(id, &data).into()]),
                ]));
            }
        }

        let resp = if resp.is_empty() {
            Type::NullString
        } else {
            Type::Array(resp)
        };

        resp.write(&mut self.stream).await?;

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

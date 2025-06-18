use anyhow;
use redis_starter_rust::client::Client;
use redis_starter_rust::store::{DataStore, Role};
use std::collections::HashMap;

use std::env;
use tokio;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut config = HashMap::new();
    let mut args = env::args().skip(1);

    while let Some(key) = args.next() {
        if !key.starts_with("--") {
            eprintln!("Invalid config option {key}");
            anyhow::bail!("Argument parsing failed");
        }

        let key = key[2..].trim();
        match key {
            "replicaof" => {
                let host = args.next();
                let port = args.next();
                anyhow::ensure!(host.is_some(), "option '{key}' requires 2 arguments");
                anyhow::ensure!(port.is_some(), "option '{key}' requires 2 arguments");
                let host = host.unwrap();
                let port = port.unwrap();
                config.insert(key.into(), format!("{host}:{port}"));
            }
            _ => {
                let value = args.next();
                anyhow::ensure!(value.is_some(), "option '{key}' requires a value");
                config.insert(key.into(), value.unwrap().trim().into());
            }
        }
    }

    let port = config
        .get("port")
        .map(String::as_str)
        .unwrap_or_else(|| "6379");
    let address = format!("127.0.0.1:{port}");

    let listener = TcpListener::bind(&address).await?;
    eprintln!("Listening on {address}");

    let role = if let Some(master) = config.get("replicaof") {
        Role::Slave(master.clone())
    } else {
        Role::Master
    };

    let mut store = DataStore::new(config, role);
    if let Err(err) = store.init().await {
        eprintln!("Initialization failed: {}", err.with_trace());
    }

    loop {
        let (stream, addr) = listener.accept().await?;
        tokio::spawn(Client::new(stream, addr, store.clone()).run());
    }
}

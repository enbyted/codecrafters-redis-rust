use anyhow;
use redis_starter_rust::client::Client;
use redis_starter_rust::store::DataStore;
use std::collections::HashMap;

use std::env;
use tokio;
use tokio::net::TcpListener;

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

use std::net::SocketAddr;
use tokio::net::{TcpStream, TcpListener};
use tokio;
use anyhow;

struct Client(TcpStream, SocketAddr);

impl Client {
    fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        Self(stream, addr)
    }

    async fn run(self) {
        eprintln!("Client {} connected", self.1);
        todo!();
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

mod jobserver;
use jobserver::*;

mod client_handler;
use client_handler::*;

use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};
use fxhash::FxHashSet as HashSet;
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::sync::Mutex;

async fn handle(stream: TcpStream, server: Arc<Mutex<JobServer>>) -> Result<()> {
    let (read, write) = stream.into_split();
    let read = BufReader::new(read);
    let in_progress: HashSet<u64> = Default::default();

    let mut client_handler = ClientHandler {
        server,
        read,
        write,
        in_progress,
    };

    client_handler.run().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let list = TcpListener::bind("0.0.0.0:4567").await?;
    let server = Arc::new(Mutex::new(JobServer::default()));
    loop {
        let (stream, _) = list.accept().await?;
        tokio::spawn(handle(stream, server.clone()));
    }
}

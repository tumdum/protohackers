use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn handle(mut stream: TcpStream) -> Result<()> {
    loop {
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        stream.write_all(&buf[..n]).await?;
    }

    stream.shutdown().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let list = TcpListener::bind("0.0.0.0:4567").await?;
    loop {
        let (stream, _) = list.accept().await?;
        handle(stream).await?;
    }
}

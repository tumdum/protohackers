use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn handle(mut stream: TcpStream) -> Result<()> {
    let mut data: Vec<(i32, i32)> = vec![];
    loop {
        let mut buf = [0u8; 9];
        if stream.read_exact(&mut buf).await? != buf.len() {
            break;
        }
        match buf[0] {
            b'I' => {
                let timestamp = i32::from_be_bytes(buf[1..=4].try_into()?);
                let price = i32::from_be_bytes(buf[5..].try_into()?);
                data.push((timestamp, price));
            }
            b'Q' => {
                let mintime = i32::from_be_bytes(buf[1..=4].try_into()?);
                let maxtime = i32::from_be_bytes(buf[5..].try_into()?);
                let subset: Vec<i32> = data
                    .iter()
                    .filter(|(t, _)| *t >= mintime && *t <= maxtime)
                    .map(|(_, price)| *price)
                    .collect();
                let n = subset.len() as i64;
                let sum: i64 = subset.iter().map(|v| *v as i64).sum();
                let mean = if n == 0 { 0 } else { (sum / n) as i32 };
                let mean = mean.to_be_bytes();
                stream.write_all(&mean).await?;
            }
            _ => break,
        }
    }

    stream.shutdown().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let list = TcpListener::bind("0.0.0.0:4567").await?;
    loop {
        let (stream, _) = list.accept().await?;
        tokio::spawn(handle(stream));
    }
}

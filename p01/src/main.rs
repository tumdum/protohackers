use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Number;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Deserialize)]
struct Request {
    method: String,
    number: Number,
}

impl Request {
    fn is_valid(&self) -> bool {
        self.method == "isPrime"
    }
}

fn is_prime(n: u64) -> bool {
    if n == 1 || n == 0 {
        return false;
    }
    if n == 2 {
        return true;
    }
    let max = (n as f64).sqrt() as u64 + 1;
    (2..=max).all(|d| n % d != 0)
}

#[derive(Debug, Serialize)]
struct Response {
    method: String,
    prime: bool,
}

async fn handle(stream: TcpStream) -> Result<()> {
    let mut stream = BufStream::new(stream);
    loop {
        let mut line = String::new();
        if 0 == stream.read_line(&mut line).await? {
            break;
        }
        let req = match serde_json::from_str::<Request>(&line) {
            Ok(req) if req.is_valid() => req,
            _ => {
                stream.write_all(b"malformed\n").await?;
                break;
            }
        };
        let prime = req.number.as_u64().map(is_prime).unwrap_or_default();
        let resp = Response {
            prime,
            method: "isPrime".to_owned(),
        };
        let resp = format!("{}\n", serde_json::to_string(&resp)?);
        stream.write_all(resp.as_bytes()).await?;
        stream.flush().await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_valid() {
        let input = r#"{"method":"isPrime","number":123}"#;
        let _req: Request = serde_json::from_str(&input).unwrap();

        let input = r#"{"method":"isPrime","number":123.2}"#;
        let _req: Request = serde_json::from_str(&input).unwrap();
    }

    #[test]
    fn prime() {
        assert!(!is_prime(0));
        assert!(!is_prime(1));
        assert!(is_prime(2));
        assert!(is_prime(3));
        assert!(!is_prime(4));
        assert!(is_prime(5));
        assert!(!is_prime(6));
        assert!(is_prime(7));
        assert!(!is_prime(8));
        assert!(!is_prime(9));
        assert!(!is_prime(10));
        assert!(is_prime(11));
        assert!(!is_prime(12));
        assert!(is_prime(13));
        assert!(!is_prime(14));
        assert!(!is_prime(15));
        assert!(!is_prime(16));
        assert!(is_prime(17));
    }
}

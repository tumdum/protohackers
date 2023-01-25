use crate::cipher::Cipher;
use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

pub struct InsecureSocket {
    r: BufReader<OwnedReadHalf>,
    w: OwnedWriteHalf,
    cipher: Cipher,
    r_bytes: usize,
    w_bytes: usize,
}

impl InsecureSocket {
    pub async fn new(tcp: TcpStream) -> Result<Self> {
        let (r, w) = tcp.into_split();
        let mut r = BufReader::new(r);
        let mut buf = vec![];
        let n = r.read_until(0, &mut buf).await?;
        let cipher = Cipher::new(&buf[..n])?;
        Ok(Self {
            r,
            w,
            cipher,
            r_bytes: 0,
            w_bytes: 0,
        })
    }

    pub async fn read_line(&mut self) -> Result<String> {
        let mut buf = String::new();
        loop {
            let b = self.r.read_u8().await?;
            let b = self.cipher.decode_one(self.r_bytes, b)?;
            self.r_bytes += 1;
            if b == b'\n' {
                break;
            } else {
                buf.push(b as char);
            }
        }
        Ok(buf)
    }

    pub async fn write_line(&mut self, mut line: String) -> Result<()> {
        line.push('\n');
        let encoded_bytes = self.cipher.encode(self.w_bytes, line.as_bytes())?;
        self.w.write_all(&encoded_bytes).await?;
        self.w.flush().await?;
        self.w_bytes += encoded_bytes.len();
        Ok(())
    }
}

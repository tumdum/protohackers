use crate::isl::InsecureSocket;
use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};

mod cipher;
mod isl;

fn find_best(s: &str) -> String {
    let s: Vec<_> = s
        .split(',')
        .map(|s| {
            let mut s = s.split("x ");
            let n: usize = s.next().unwrap().parse().unwrap();
            let name = s.next().unwrap();
            (n, name)
        })
        .collect();

    let (n, name) = s.iter().max_by_key(|(n, _)| n).unwrap();
    format!("{n}x {name}")
}

async fn handle(stream: TcpStream) -> Result<()> {
    let mut isl = InsecureSocket::new(stream).await?;

    loop {
        let line = isl.read_line().await?;
        let reply = find_best(&line);
        isl.write_line(reply).await?;
    }
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
    fn test_find_best() {
        let input = "4x dog,5x car";
        let expected = "5x car";
        assert_eq!(expected, find_best(input));
    }
}

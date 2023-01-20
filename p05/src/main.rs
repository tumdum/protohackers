use anyhow::{bail, Result};
use regex::Regex;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

async fn read_next_line(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<String> {
    let mut line = String::new();
    if 0 == r.read_line(&mut line).await? {
        bail!("no message");
    }
    Ok(line)
}

async fn write_next_line(w: &mut (impl AsyncWriteExt + Unpin), msg: &str) -> Result<()> {
    w.write_all(msg.as_bytes()).await?;
    Ok(w.flush().await?)
}

async fn handle(stream: TcpStream, re: Regex) -> Result<()> {
    let (client_read, mut client_write) = stream.into_split();
    let mut client_read = BufReader::new(client_read);
    let real_server = TcpStream::connect("chat.protohackers.com:16963").await?;
    let (server_read, mut server_write) = real_server.into_split();
    let mut server_read = BufReader::new(server_read);

    tokio::spawn({
        let re = re.clone();
        async move {
            loop {
                match read_next_line(&mut server_read).await {
                    Ok(query) => {
                        let query = rep(&re, &query);
                        let _ = write_next_line(&mut client_write, &query).await;
                    }
                    Err(_) => break,
                }
            }
        }
    });

    loop {
        let line = read_next_line(&mut client_read).await?;
        let line = rep(&re, &line);
        write_next_line(&mut server_write, &line).await?;
    }
}

fn rep(re: &Regex, s: &str) -> String {
    use regex::Captures;

    fn aux<'a, 'b>(c: &'a Captures<'b>) -> String {
        let s = match c.name("before") {
            Some(s) => s.as_str().to_owned(),
            None => String::new(),
        };
        let s = format!("{s}{}", TONY);
        match c.name("rest") {
            Some(rest) => format!("{s}{}", rest.as_str()),
            None => s,
        }
    }
    re.replace_all(s, aux).to_string()
}

#[tokio::main]
async fn main() -> Result<()> {
    let list = TcpListener::bind("0.0.0.0:4567").await?;
    let re = Regex::new(r#"(?P<before>[ ])?(?P<coin>7[[:alnum:]]{25,34})(?P<rest>[ \n])"#)?;

    loop {
        let (stream, _) = list.accept().await?;
        tokio::spawn(handle(stream, re.clone()));
    }
}

const TONY: &str = "7YWHMfk9JZe0LM0g1ZauHuiSxhI";

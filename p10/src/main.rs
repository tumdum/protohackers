use anyhow::{bail, Result};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

async fn read_next_line(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<String> {
    let mut line = String::new();
    if 0 == r.read_line(&mut line).await? {
        bail!("no message");
    }
    Ok(line)
}

async fn write_next_line(w: &mut (impl AsyncWriteExt + Unpin), msg: &str) -> Result<()> {
    let msg = format!("{msg}\n");
    w.write_all(msg.as_bytes()).await?;
    Ok(w.flush().await?)
}

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Stat {
    File { path: String, revision: u64 },
    Dir(String),
}

impl Stat {
    fn path(&self) -> &str {
        match self {
            Self::File { path, .. } => &path,
            Self::Dir(path) => &path,
        }
    }
}

type Content = Vec<u8>;

#[derive(Debug, Default)]
struct State {
    files: HashMap<String, Vec<Content>>,
}

impl State {
    fn put(&mut self, path: String, content: Vec<u8>) -> Result<u64> {
        match self.files.entry(path) {
            Occupied(mut e) => {
                if e.get().last().unwrap() == &content {
                    return Ok(e.get().len() as u64);
                }
                e.get_mut().push(content);
                Ok(e.get().len() as u64)
            }
            Vacant(e) => {
                e.insert(vec![content]);
                Ok(1)
            }
        }
    }

    fn list(&self, path: &str) -> BTreeSet<Stat> {
        let listing: Vec<(String, u64)> = self
            .files
            .iter()
            .filter(|(name, _)| name.starts_with(&path))
            .map(|(name, contents_vec)| {
                (
                    name.strip_prefix(&path).unwrap().to_owned(),
                    contents_vec.len() as u64,
                )
            })
            .collect();
        let files: Vec<(String, u64)> = listing
            .iter()
            .filter(|(name, _)| !name.contains('/'))
            .cloned()
            .collect();
        let mut dirs: BTreeSet<Stat> = listing
            .iter()
            .filter(|(name, _)| name.contains('/'))
            .map(|(name, _)| {
                let idx = name.chars().position(|c| c == '/').unwrap();
                Stat::Dir(name[..=idx].to_owned())
            })
            .collect();
        dirs.extend(
            files
                .into_iter()
                .map(|(path, revision)| Stat::File { path, revision }),
        );
        dirs
    }
}

fn strip_prefix(s: &str, prefix: &str) -> Option<String> {
    let s_up = s.to_ascii_uppercase();
    let prefix_up = prefix.to_ascii_uppercase();
    if s_up.starts_with(&prefix_up) {
        Some(s.chars().skip(prefix.chars().count()).collect())
    } else {
        None
    }
}

fn valid_file_name(name: &str) -> bool {
    if name.contains("//") {
        return false;
    }
    if !name.starts_with("/") {
        return false;
    }
    if name.chars().last() == Some('/') {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '/' || c == '_' || c == '-')
}

fn is_text(content: &[u8]) -> bool {
    content
        .iter()
        .all(|c| c.is_ascii_graphic() || c.is_ascii_whitespace())
}

async fn handle(stream: TcpStream, addr: SocketAddr, state: Arc<Mutex<State>>) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);

    loop {
        write_next_line(&mut write, "READY").await?;
        let line = read_next_line(&mut read).await?.trim().to_owned();
        if let Some(name_and_len) = strip_prefix(&line, "PUT ") {
            let args: Vec<_> = name_and_len.split(' ').collect();
            if args.len() != 2 {
                write_next_line(&mut write, "ERR wrong number of arguments").await?;
                continue;
            }
            let name = args[0];
            let len: i32 = args[1].parse()?;
            if !valid_file_name(&name) {
                write_next_line(&mut write, "ERR illegal file name").await?;
                continue;
            }

            let mut buf = vec![0u8; len as usize];
            read.read_exact(&mut buf).await?;
            if !is_text(&buf) {
                write_next_line(&mut write, "ERR illegal file content").await?;
                continue;
            }

            let revision = state.lock().await.put(name.to_owned(), buf)?;

            write_next_line(&mut write, &format!("OK r{revision}")).await?;
        } else if let Some(name) = strip_prefix(&line, "GET ") {
            let args: Vec<&str> = name.split(" r").collect();
            if args.len() > 2 {
                write_next_line(
                    &mut write,
                    &format!("ERR invalid usage: GET file [revision]"),
                )
                .await?;
                continue;
            }
            let (name, rev): (&str, Option<u64>) = if args.len() > 1 {
                let rev = match args[1].parse() {
                    Ok(rev) => rev,
                    Err(_) => {
                        write_next_line(
                            &mut write,
                            &format!("ERR invalid usage: GET file [revision]"),
                        )
                        .await?;
                        continue;
                    }
                };
                (args[0], Some(rev))
            } else {
                (args[0], None)
            };
            let content_vec = state.lock().await.files.get(name).cloned();
            match content_vec {
                Some(content) => {
                    if let Some(rev) = rev {
                        let content = match content.get(rev as usize - 1) {
                            Some(content) => content,
                            None => {
                                write_next_line(&mut write, &format!("ERR no such revision"))
                                    .await?;
                                continue;
                            }
                        };
                        write_next_line(&mut write, &format!("OK {}", content.len())).await?;
                        write.write_all(content).await?;
                    } else {
                        let content = &content[content.len() - 1];
                        write_next_line(&mut write, &format!("OK {}", content.len())).await?;
                        write.write_all(content).await?;
                    }
                }
                None => {
                    write_next_line(&mut write, "ERR no such file").await?;
                }
            }
        } else if let Some(path) = strip_prefix(&line, "LIST ") {
            if !path.starts_with('/') {
                write_next_line(&mut write, "ERR dir name").await?;
                continue;
            }
            let path = if path.chars().last() != Some('/') {
                format!("{path}/")
            } else {
                path.to_owned()
            };
            let mut listing: Vec<Stat> = state.lock().await.list(&path).into_iter().collect();
            listing.sort_unstable_by(|a, b| a.path().cmp(b.path()));
            write_next_line(&mut write, &format!("OK {}", listing.len())).await?;
            for entry in listing {
                match entry {
                    Stat::File { path, revision } => {
                        write_next_line(&mut write, &format!("{path} r{revision}")).await?;
                    }
                    Stat::Dir(path) => {
                        write_next_line(&mut write, &format!("{path} DIR")).await?;
                    }
                }
            }
        } else if line == "HELP" {
            write_next_line(&mut write, "OK usage: HELP|GET|PUT|LIST").await?;
        } else {
            write_next_line(&mut write, &format!("ERR illegal method: {line}")).await?;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let list = TcpListener::bind("0.0.0.0:4567").await?;
    let state = Arc::new(Mutex::new(State::default()));
    loop {
        let (stream, addr) = list.accept().await?;
        tokio::spawn(handle(stream, addr, state.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_listing() -> Result<()> {
        let mut state = State::default();

        state.put("/a".to_owned(), vec![])?;
        state.put("/b".to_owned(), vec![])?;
        state.put("/c/d".to_owned(), vec![])?;

        let expected = [
            Stat::File {
                path: "a".to_owned(),
                revision: 1,
            },
            Stat::File {
                path: "b".to_owned(),
                revision: 1,
            },
            Stat::Dir("c/".to_owned()),
        ];

        assert_eq!(
            expected.into_iter().collect::<BTreeSet<_>>(),
            state.list("/")
        );
        Ok(())
    }
}

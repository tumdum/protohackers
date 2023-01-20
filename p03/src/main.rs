use anyhow::{bail, Result};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{
    broadcast::{channel, Sender},
    Mutex,
};

#[derive(Debug, Default)]
struct State {
    users: HashSet<String>,
}

#[derive(Clone, Debug)]
enum Event {
    NewUser(String),
    UserQuit(String),
    Message { from: String, content: String },
}

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

async fn handle(stream: TcpStream, s: Sender<Event>, state: Arc<Mutex<State>>) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);

    write_next_line(&mut write, "name?").await?;

    let name = read_next_line(&mut read).await?.trim().to_owned();

    if !name.chars().all(|c| c.is_ascii_alphanumeric()) || name.is_empty() {
        return Ok(());
    }

    state.lock().await.users.insert(name.clone());

    let resp = format!(
        "* {:?}",
        state
            .lock()
            .await
            .users
            .iter()
            .filter(|u| **u != name)
            .collect::<Vec<_>>()
    );
    write_next_line(&mut write, &resp).await?;

    let mut r = s.subscribe();
    s.send(Event::NewUser(name.clone()))?;

    let handle = tokio::spawn({
        let s = s.clone();
        let name = name.clone();
        let state = state.clone();

        async move {
            loop {
                match read_next_line(&mut read).await {
                    Err { .. } => {
                        s.send(Event::UserQuit(name.clone())).unwrap();
                        state.lock().await.users.remove(&name);
                        return;
                    }
                    Ok(line) => {
                        let line = line.trim();
                        s.send(Event::Message {
                            from: name.clone(),
                            content: line.to_owned(),
                        })
                        .unwrap();
                    }
                }
            }
        }
    });

    loop {
        match r.recv().await? {
            Event::UserQuit(user) if user != name => {
                let resp = format!("* {user} has quit the room");
                write_next_line(&mut write, &resp).await?;
            }
            Event::UserQuit(_) => break,
            Event::NewUser(new_user) if new_user != name => {
                let resp = format!("* {new_user} has entered the room");
                write_next_line(&mut write, &resp).await?;
            }
            Event::Message { from, content } if from != name => {
                let resp = format!("[{from}] {content}");
                write_next_line(&mut write, &resp).await?;
            }
            Event::NewUser(_) => {}
            Event::Message { .. } => {}
        }
    }

    write.shutdown().await?;
    Ok(handle.await?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let (s, _r) = channel(100);
    let state = Arc::new(Mutex::new(State::default()));
    let list = TcpListener::bind("0.0.0.0:4567").await?;
    loop {
        let (stream, _) = list.accept().await?;
        tokio::spawn(handle(stream, s.clone(), state.clone()));
    }
}

use anyhow::{anyhow, bail, Result};
use async_channel::{unbounded, Receiver, Sender};
use bstr::ByteSlice;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::select;
use tokio::sync::Mutex;

#[derive(Debug, PartialEq)]
enum Message {
    Connect {
        session: u64,
    },
    Data {
        session: u64,
        pos: u64,
        data: Vec<u8>,
    },
    Ack {
        session: u64,
        len: u64,
    },
    Close {
        session: u64,
    },
}

impl Message {
    fn parse(b: &[u8]) -> Result<Message> {
        if b.last() != Some(&b'/') {
            bail!("missing / at the end");
        }
        if let Some(s) = b.strip_prefix(b"/connect/") {
            let s = &s[..s.len().checked_sub(1).ok_or(anyhow!("zero len s"))?];
            let s = std::str::from_utf8(s)?;
            let session = s.parse()?;
            Ok(Message::Connect { session })
        } else if let Some(s) = b.strip_prefix(b"/ack/") {
            let s = &s[..s.len().checked_sub(1).ok_or(anyhow!("zero len s"))?];
            let s = std::str::from_utf8(s)?;
            let mut s = s.split('/');
            let session = s.next().ok_or(anyhow!("no session"))?.parse()?;
            let len = s.next().ok_or(anyhow!("no len"))?.parse()?;
            Ok(Message::Ack { session, len })
        } else if let Some(s) = b.strip_prefix(b"/close/") {
            let s = &s[..s.len().checked_sub(1).ok_or(anyhow!("zero len s"))?];
            let s = std::str::from_utf8(s)?;
            let session = s.parse()?;
            Ok(Message::Close { session })
        } else if let Some(s) = b.strip_prefix(b"/data/") {
            let slash = s
                .iter()
                .position(|b| *b == b'/')
                .ok_or(anyhow!("no first slash"))?;
            let session: u64 = std::str::from_utf8(&s[..slash])?.parse()?;
            let s = &s[slash + 1..];
            let slash = s
                .iter()
                .position(|b| *b == b'/')
                .ok_or(anyhow!("no second slash"))?;
            let pos: u64 = std::str::from_utf8(&s[..slash])?.parse()?;
            let s = &s[slash + 1..];
            let data = &s[..s.len() - 1];
            let valid = {
                let data = data.replace("\\\\", "");
                let data = data.replace("\\/", "");
                !(data.contains(&b'\\') || data.contains(&b'/'))
            };
            if !valid {
                bail!("invalid");
            }
            let data = data.replace("\\\\", "\\");
            let data = data.replace("\\/", "/");
            Ok(Message::Data {
                session,
                pos,
                data: data.to_vec(),
            })
        } else {
            bail!("unknown message {b:?}");
        }
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        match self {
            Self::Connect { session } => {
                let msg = format!("/connect/{session}/");
                Ok(msg.as_bytes().to_vec())
            }
            Self::Data { session, pos, data } => {
                let data = data.replace("\\", "\\\\");
                let data = data.replace("/", "\\/");
                let msg = format!("/data/{session}/{pos}/{}/", std::str::from_utf8(&data)?);
                Ok(msg.as_bytes().to_vec())
            }
            Self::Ack { session, len } => {
                let msg = format!("/ack/{session}/{len}/");
                Ok(msg.as_bytes().to_vec())
            }
            Self::Close { session } => {
                let msg = format!("/close/{session}/");
                Ok(msg.as_bytes().to_vec())
            }
        }
    }
}

struct SessionState {
    id: u64,
    addr: SocketAddr,
    data: Vec<u8>,
    full_lines: usize,
    ch: (Sender<Vec<u8>>, Receiver<Vec<u8>>),
    ack: u64,
    pending: Vec<Message>,
    should_close: bool,
}

impl SessionState {
    async fn add(&mut self, data: &[u8]) {
        self.data.extend(data);
        let old_full_lines = self.full_lines;
        self.full_lines = self.data.iter().filter(|b| **b == b'\n').count();
        if old_full_lines < self.full_lines || self.should_close {
            let new_lines: Vec<_> = self
                .data
                .split(|b| *b == b'\n')
                .skip(old_full_lines)
                .collect();
            let idx = if self.should_close {
                println!("{} - idx {}", self.id, new_lines.len());
                new_lines.len()
            } else {
                println!(
                    "{} - last line to skip: '{:?}'",
                    self.id,
                    new_lines.last().map(|l| std::str::from_utf8(l))
                );
                new_lines.len() - 1
            };
            for (idx, line) in new_lines[..idx].iter().enumerate() {
                println!(
                    "{} - sending to channel '{:?}'",
                    self.id,
                    std::str::from_utf8(line)
                );
                let mut line = line.to_vec();
                line.reverse();
                if idx != new_lines.len() - 1 {
                    line.push(b'\n');
                }
                self.ch.0.send(line).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:4567").await?);
    let mut sessions: Arc<Mutex<HashMap<u64, SessionState>>> = Default::default();

    loop {
        let mut buf = vec![0u8; 1024];
        let (len, addr) = socket.recv_from(&mut buf).await?;
        let msg = Message::parse(&buf[..len]);
        println!("received {msg:?}");
        match msg {
            Err(e) => println!("error: {:?}", e),
            Ok(Message::Ack { session, len }) => {
                if let Some(state) = sessions.lock().await.get_mut(&session) {
                    if len <= state.ack {
                        continue;
                    }
                    if len <= state.data.len() as u64 {
                        state.ack = dbg!(len).max(dbg!(state.ack));
                        let pending: Vec<_> = state
                            .pending
                            .drain(..)
                            .filter(|msg| match msg {
                                Message::Data { session, pos, data } => {
                                    (pos + data.len() as u64) > state.ack
                                }
                                _ => false,
                            })
                            .collect();
                        state.pending = pending;
                        if state.pending.is_empty() && state.should_close {
                            println!("Closing {session} which was pending");
                            socket
                                .send_to(&Message::Close { session }.serialize()?, addr)
                                .await?;
                        }
                    } else {
                        socket
                            .send_to(&Message::Close { session }.serialize()?, addr)
                            .await?;
                    }
                }
            }
            Ok(Message::Close { session }) => {
                if let Some(state) = sessions.lock().await.get_mut(&session) {
                    if state.ack == state.data.len() as u64 {
                        println!(
                            "Closing {session}, all ack ({} vs {})",
                            state.ack,
                            state.data.len()
                        );
                        socket
                            .send_to(&Message::Close { session }.serialize()?, addr)
                            .await?;
                    } else {
                        if !state.should_close {
                            println!(
                                "Marking for closing {session} ({} vs {})",
                                state.ack,
                                state.data.len()
                            );
                            state.should_close = true;
                            state.add(b"").await;
                        }
                    }
                } else {
                    println!("Closing unknown session {session}");
                    socket
                        .send_to(&Message::Close { session }.serialize()?, addr)
                        .await?;
                }
            }
            Ok(Message::Connect { session }) => match sessions.lock().await.entry(session) {
                Occupied(_) => {
                    socket
                        .send_to(&Message::Ack { session, len: 0 }.serialize()?, addr)
                        .await?;
                }
                Vacant(e) => {
                    let ch = unbounded();
                    let addr = addr;
                    tokio::spawn({
                        let r: Receiver<Vec<u8>> = ch.1.clone();
                        let mut pos = 0u64;
                        let socket = socket.clone();
                        let sessions = sessions.clone();
                        let session = session.clone();
                        async move {
                            let mut interval =
                                tokio::time::interval(std::time::Duration::from_secs(3));
                            loop {
                                select! {
                                        _ = interval.tick() => {
                                            if let Some(state) = sessions.lock().await.get_mut(&session) {
                                                for msg in &state.pending {
                                                    println!("{session} - Resending {msg:?}");
                                                    socket
                                                        .send_to(
                                                            &msg
                                                            .serialize()
                                                            .unwrap(),
                                                            state.addr,
                                                    )
                                                    .await;
                                            }
                                        }
                                    },
                                    Ok(mut data) = r.recv() => {
                                        println!(
                                            "{session} - Sending back pos: {pos}, -> {}, data: {:?}",
                                            pos + data.len() as u64, std::str::from_utf8(&data)
                                        );
                                        for chunk in data.chunks(512) {
                                            let msg = Message::Data { session, pos, data: chunk.to_vec() };
                                            socket
                                                .send_to(
                                                    &msg
                                                        .serialize()
                                                        .unwrap(),
                                                    addr,
                                                )
                                                .await;
                                            if let Some(state) = sessions.lock().await.get_mut(&session) {
                                                state.pending.push(msg);
                                            }
                                            pos += chunk.len() as u64;
                                        }
                                    }
                                }
                            }
                        }
                    });
                    e.insert(SessionState {
                        id: session,
                        addr,
                        data: vec![],
                        full_lines: 0,
                        ch,
                        ack: 0,
                        pending: vec![],
                        should_close: false,
                    });
                    socket
                        .send_to(&Message::Ack { session, len: 0 }.serialize()?, addr)
                        .await?;
                }
            },
            Ok(Message::Data { session, pos, data }) => {
                println!(
                    "{session} - received data '{:?}'",
                    std::str::from_utf8(&data)
                );
                if let Some(state) = sessions.lock().await.get_mut(&session) {
                    if state.should_close {
                        println!("{session} - Skipping Data since should_close=true");
                        continue;
                    }
                    if state.data.len() == pos as usize {
                        state.add(&data).await;
                        println!(
                            "{session} - added data, sending ack (total {})",
                            state.data.len()
                        );
                        socket
                            .send_to(
                                &Message::Ack {
                                    session,
                                    len: state.data.len() as u64,
                                }
                                .serialize()?,
                                state.addr,
                            )
                            .await?;
                    } else {
                        println!("{session} - ignored data, sending ack");
                        socket
                            .send_to(
                                &Message::Ack {
                                    session,
                                    len: state.data.len() as u64,
                                }
                                .serialize()?,
                                state.addr,
                            )
                            .await?;
                    }
                } else {
                    println!("Data for unknown session, ignoring");
                }
            }
            Ok(msg) => todo!("msg: {:?}", msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_connect() {
        let input = b"/connect/1234567/";
        let expected = Message::Connect { session: 1234567 };
        assert_eq!(expected, Message::parse(input).unwrap());
        assert_eq!(expected.serialize().unwrap(), input);
    }

    #[test]
    fn parse_data_simple() {
        let input = b"/data/1234567/13/abc/";
        let expected = Message::Data {
            session: 1234567,
            pos: 13,
            data: b"abc".to_vec(),
        };
        assert_eq!(expected, Message::parse(input).unwrap());
        assert_eq!(expected.serialize().unwrap(), input);
    }

    #[test]
    fn parse_data_escape() {
        let input = b"/data/1234567/13/foo\\/bar\\\\baz/";
        let expected = Message::Data {
            session: 1234567,
            pos: 13,
            data: b"foo/bar\\baz".to_vec(),
        };
        assert_eq!(expected, Message::parse(input).unwrap());
        assert_eq!(expected.serialize().unwrap(), input);
    }

    #[test]
    fn parse_data_escape_invalid() {
        let input = b"/data/1234567/13/illegal data/has too many/parts/";
        assert!(Message::parse(input).is_err());
    }

    #[test]
    fn parse_ack() {
        let input = b"/ack/1234567/1024/";
        let expected = Message::Ack {
            session: 1234567,
            len: 1024,
        };
        assert_eq!(expected, Message::parse(input).unwrap());
        assert_eq!(expected.serialize().unwrap(), input);
    }

    #[test]
    fn parse_close() {
        let input = b"/close/1234567/";
        let expected = Message::Close { session: 1234567 };
        assert_eq!(expected, Message::parse(input).unwrap());
        assert_eq!(expected.serialize().unwrap(), input);
    }
}

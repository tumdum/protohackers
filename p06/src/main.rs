use anyhow::Result;
use async_channel::{unbounded, Receiver, Sender};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::sleep;

const ERROR: u8 = 0x10;
const PLATE: u8 = 0x20;
const TICKET: u8 = 0x21;
const WANT_HEARTBEAT: u8 = 0x40;
const HEARTBEAT: u8 = 0x41;
const I_AM_CAMERA: u8 = 0x80;
const I_AM_DISPATCHER: u8 = 0x81;

#[derive(Debug)]
struct Position {
    timestamp: u32,
    mile: u16,
}

async fn handle(
    stream: TcpStream,
    positions: Arc<Mutex<HashMap<(String, u16), Vec<Position>>>>,
    ticket_state: Arc<Mutex<TicketState>>,
) -> Result<()> {
    #[derive(Debug, PartialEq)]
    enum Identity {
        Camera,
        Dispatcher,
    }

    let mut identified = None;
    let mut road = 0;
    let mut mile = 0;
    let mut limit = 0;

    let (mut client_read, client_write) = stream.into_split();
    let client_write = Arc::new(Mutex::new(client_write));
    loop {
        let id = client_read.read_u8().await?;
        match id {
            ERROR => {
                println!("ERROR")
            }
            PLATE => {
                if identified == Some(Identity::Dispatcher) {
                    let mut c = client_write.lock().await;
                    let _ = c.write_u8(ERROR).await;
                    let message = b"plate from Dispatcher";
                    let _ = c.write_u8(message.len() as u8).await;
                    let _ = c.write_all(message).await;
                } else {
                    let len = client_read.read_u8().await? as usize;
                    let mut buf = vec![0u8; len];
                    client_read.read_exact(&mut buf).await?;
                    let plate = std::str::from_utf8(&buf)?;
                    let timestamp = client_read.read_u32().await?;

                    println!("PLATE plate {plate}, timestamp: {timestamp}");
                    {
                        let mut positions = positions.lock().await;
                        let entry = positions.entry((plate.to_owned(), road)).or_default();
                        entry.push(Position { timestamp, mile });
                        let l = entry.len();
                        if l > 1 {
                            let new = entry.last().unwrap();
                            for position in &entry[..l - 1] {
                                let (prev, next) = if new.timestamp <= position.timestamp {
                                    (new, position)
                                } else {
                                    (position, new)
                                };
                                let dist = (next.mile as i64 - prev.mile as i64).abs() as f64;
                                let time = (next.timestamp as i64 - prev.timestamp as i64) as f64
                                    / (60.0 * 60.0);
                                let speed = (dist / time).round() as u16;
                                if speed > limit {
                                    let this_days: HashSet<_> = (prev.timestamp..=next.timestamp)
                                        .map(|t| t / (24 * 60 * 60))
                                        .collect();
                                    let mut ticket_state = ticket_state.lock().await;
                                    let new_days = this_days
                                        .difference(
                                            &ticket_state.days.entry(plate.to_owned()).or_default(),
                                        )
                                        .count();

                                    if new_days == this_days.len() {
                                        let existing_tickets =
                                            ticket_state.days.entry(plate.to_owned()).or_default();
                                        existing_tickets.extend(this_days.clone());
                                        let sender = ticket_state
                                            .queues
                                            .entry(road)
                                            .or_insert_with(|| unbounded())
                                            .0
                                            .clone();
                                        sender
                                            .send(Ticket {
                                                plate: plate.to_owned(),
                                                road,
                                                mile1: prev.mile,
                                                timestamp1: prev.timestamp,
                                                mile2: next.mile,
                                                timestamp2: next.timestamp,
                                                speed: speed * 100,
                                            })
                                            .await?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            TICKET => {
                println!("TICKET")
            }
            WANT_HEARTBEAT => {
                let interval = client_read.read_u32().await?;
                println!("WANT_HEARTBEAT {interval}");
                if interval > 0 {
                    tokio::spawn({
                        let client_write = client_write.clone();
                        let duration = Duration::from_millis(interval as u64 * 100);
                        async move {
                            loop {
                                if client_write.lock().await.write_u8(HEARTBEAT).await.is_err() {
                                    break;
                                }
                                sleep(duration).await;
                            }
                        }
                    });
                }
            }
            HEARTBEAT => {
                println!("HEARTBEAT")
            }
            I_AM_CAMERA => {
                if identified.is_some() {
                    let mut c = client_write.lock().await;
                    let _ = c.write_u8(ERROR).await;
                    let message = b"double I_AM_CAMERA";
                    let _ = c.write_u8(message.len() as u8).await;
                    let _ = c.write_all(message).await;
                } else {
                    identified = Some(Identity::Camera);
                    road = client_read.read_u16().await?;
                    mile = client_read.read_u16().await?;
                    limit = client_read.read_u16().await?;
                    println!("I_AM_CAMERA road {road}, mile {mile}, limit {limit}");
                }
            }
            I_AM_DISPATCHER => {
                if identified.is_some() {
                    let mut c = client_write.lock().await;
                    let _ = c.write_u8(ERROR).await;
                    let message = b"double I_AM_DISPATCHER";
                    let _ = c.write_u8(message.len() as u8).await;
                    let _ = c.write_all(message).await;
                } else {
                    identified = Some(Identity::Dispatcher);
                    let numroads = client_read.read_u8().await?;
                    let mut roads: Vec<u16> = vec![];
                    for _ in 0..numroads {
                        roads.push(client_read.read_u16().await?);
                    }
                    println!("I_AM_DISPATCHER {roads:?}");
                    for road in roads {
                        let receiver = ticket_state
                            .lock()
                            .await
                            .queues
                            .entry(road)
                            .or_insert_with(|| unbounded())
                            .1
                            .clone();
                        let client_write = client_write.clone();
                        tokio::spawn({
                            async move {
                                loop {
                                    let ticket = receiver.recv().await.unwrap();
                                    println!("will send ticket: {ticket:?}");
                                    let mut c = client_write.lock().await;
                                    let _ = c.write_u8(TICKET).await;
                                    let _ = c.write_u8(ticket.plate.len() as u8).await;
                                    let _ = c.write_all(ticket.plate.as_bytes()).await;
                                    let _ = c.write_u16(ticket.road).await;
                                    let _ = c.write_u16(ticket.mile1).await;
                                    let _ = c.write_u32(ticket.timestamp1).await;
                                    let _ = c.write_u16(ticket.mile2).await;
                                    let _ = c.write_u32(ticket.timestamp2).await;
                                    let _ = c.write_u16(ticket.speed).await;
                                }
                            }
                        });
                    }
                }
            }
            other => {
                let mut c = client_write.lock().await;
                let _ = c.write_u8(ERROR).await;
                let message = format!("unexpected message with id: {other}");
                let _ = c.write_u8(message.len() as u8).await;
                let _ = c.write_all(message.as_bytes()).await;
            }
        }
    }
}

// Ticket to be sent out when dispatcher for given road is ready
#[derive(Debug)]
struct Ticket {
    plate: String,
    road: u16,
    mile1: u16,
    timestamp1: u32,
    mile2: u16,
    timestamp2: u32,
    speed: u16,
}

#[derive(Debug, Default)]
struct TicketState {
    // Road -> dispatcher channel
    queues: HashMap<u16, (Sender<Ticket>, Receiver<Ticket>)>,

    // Plate -> Days with tickets
    days: HashMap<String, HashSet<u32>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let list = TcpListener::bind("0.0.0.0:4567").await?;

    // (Plate,Road) -> (Timestamp, Position)
    let positions: Arc<Mutex<HashMap<(String, u16), Vec<Position>>>> =
        Arc::new(Mutex::new(Default::default()));

    let ticket_state = Arc::new(Mutex::new(TicketState::default()));

    loop {
        let (stream, _) = list.accept().await?;
        tokio::spawn(handle(stream, positions.clone(), ticket_state.clone()));
    }
}

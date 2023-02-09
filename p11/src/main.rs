mod messages;
use messages::*;

use anyhow::{bail, Result};
use async_channel::{unbounded, Receiver, Sender};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

async fn handle(
    id: usize,
    stream: TcpStream,
    sites: Arc<Mutex<HashMap<u32, Sender<Event>>>>,
) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);

    let msg = Message::decode(&mut read).await;
    let my_hello = Message::Hello {
        protocol: "pestcontrol".to_owned(),
        version: 1,
    };
    my_hello.encode(&mut write).await?;
    match msg {
        Ok(ref hello) if hello != &my_hello => {
            let err = Message::Error {
                message: "bad hello".to_owned(),
            };
            err.encode(&mut write).await.unwrap();
            bail!("Invalid initial messege: {msg:?}");
        }
        Ok(_) => {}
        Err(ref e) => {
            let err = Message::Error {
                message: format!("[{id}] error: {e}"),
            };
            err.encode(&mut write).await.unwrap();
            use tokio::io::AsyncWriteExt;
            write.flush().await;
            bail!("Invalid initial messege: {msg:?}");
        }
    }

    loop {
        let msg = Message::decode(&mut read).await;
        match msg {
            Ok(Message::SiteVisit { site, populations }) => {
                if !validate_site_visit(&populations) {
                    let err = Message::Error {
                        message: "bad".to_owned(),
                    };
                    err.encode(&mut write).await.unwrap();
                    continue;
                }
                let s = match sites.lock().await.entry(site) {
                    Occupied(e) => e.get().clone(),
                    Vacant(e) => {
                        let s = start_handler(site).await?;
                        e.insert(s.clone());
                        s
                    }
                };
                s.send(Event::SiteVisit { site, populations }).await;
            }
            other => {
                let err = Message::Error {
                    message: format!("error: {other:?}"),
                };
                err.encode(&mut write).await.unwrap();
                unimplemented!("other: {other:?}");
            }
        }
    }

    Ok(())
}

#[derive(Debug, PartialEq)]
enum Event {
    SiteVisit {
        site: u32,
        populations: Vec<ObservedPopulation>,
    },
}

async fn start_handler(id: u32) -> Result<Sender<Event>> {
    let (s, r) = unbounded::<Event>();
    let stream = TcpStream::connect("pestcontrol.protohackers.com:20547").await?;
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    tokio::spawn({
        async move {
            let hello = Message::Hello {
                protocol: "pestcontrol".to_owned(),
                version: 1,
            };
            hello.encode(&mut write).await.unwrap();
            let hello_received = Message::decode(&mut read).await.unwrap();
            assert_eq!(hello, hello_received);
            let dial = Message::DialAuthority { site: id };
            dial.encode(&mut write).await.unwrap();
            let target_populations: HashMap<String, (u32, u32)> =
                match Message::decode(&mut read).await.unwrap() {
                    Message::TargetPopulations { populations, site } => {
                        assert_eq!(id, site);
                        populations
                            .into_iter()
                            .map(|p| (p.species, (p.min, p.max)))
                            .collect()
                    }
                    other => unimplemented!("other: {other:?}"),
                };
            println!("target populations for {id}: {target_populations:?}");
            let mut policies: HashMap<String, (u32, Action)> = HashMap::default();
            loop {
                let event: Result<Event, _> = r.recv().await;
                match event {
                    Ok(Event::SiteVisit {
                        site,
                        mut populations,
                    }) => {
                        println!("event site {id} visit: {populations:?}");
                        // Message::Ok.encode(&mut write).await.unwrap();
                        let seen: HashSet<String> =
                            populations.iter().map(|p| p.species.clone()).collect();
                        let targeted: HashSet<String> =
                            target_populations.keys().cloned().collect();
                        let not_seen = &targeted - &seen;
                        for name in not_seen {
                            populations.push(ObservedPopulation {
                                species: name,
                                count: 0,
                            });
                        }
                        for pop in populations {
                            if let Some((min, max)) = target_populations.get(&pop.species) {
                                let new_action = select_new_action(pop.count, *min, *max);
                                eprintln!("new_action {new_action:?} from count {}, min {}, max {} for site {id} and {}",
                                    pop.count, *min, *max, pop.species);
                                let old_action = policies.get(&pop.species);

                                match (old_action, new_action) {
                                    (None, None) => {
                                        eprintln!(
                                            "Skipping same none action for site {id} and '{}'",
                                            pop.species
                                        );
                                    }
                                    (None, Some(new_action)) => {
                                        let policy = Message::CreatePolicy {
                                            species: pop.species.to_owned(),
                                            action: new_action,
                                        };
                                        policy.encode(&mut write).await.unwrap();

                                        let policy_id = match Message::decode(&mut read).await {
                                            Ok(Message::PolicyResult { policy }) => policy,
                                            other => unimplemented!("other: {other:?}"),
                                        };
                                        policies.insert(
                                            pop.species.to_owned(),
                                            (policy_id, new_action),
                                        );
                                        eprintln!(
                                            "Created policy {policy:?} for site {id} and '{}'",
                                            pop.species
                                        );
                                    }
                                    (Some((_id, old_action)), Some(new_action))
                                        if *old_action == new_action =>
                                    {
                                        eprintln!("Skipping same action: {new_action:?} for side {id} and '{}'", pop.species);
                                    }
                                    (Some((id, old_action)), Some(new_action)) => {
                                        let delete = Message::DeletePolicy { policy: *id };
                                        delete.encode(&mut write).await.unwrap();
                                        match Message::decode(&mut read).await {
                                            Ok(Message::Ok) => {}
                                            other => unimplemented!("other: {other:?}"),
                                        }
                                        eprintln!(
                                            "Delete policy {id} for site {id} and '{}'",
                                            pop.species
                                        );

                                        let policy = Message::CreatePolicy {
                                            species: pop.species.to_owned(),
                                            action: new_action,
                                        };
                                        policy.encode(&mut write).await.unwrap();

                                        let policy_id = match Message::decode(&mut read).await {
                                            Ok(Message::PolicyResult { policy }) => policy,
                                            other => unimplemented!("other: {other:?}"),
                                        };
                                        eprintln!(
                                            "Created policy {policy:?} for site {id} and '{}'",
                                            pop.species
                                        );
                                        policies.insert(
                                            pop.species.to_owned(),
                                            (policy_id, new_action),
                                        );
                                    }
                                    (Some((id, _)), None) => {
                                        let delete = Message::DeletePolicy { policy: *id };
                                        delete.encode(&mut write).await.unwrap();
                                        match Message::decode(&mut read).await {
                                            Ok(Message::Ok) => {}
                                            other => unimplemented!("other: {other:?}"),
                                        }
                                        eprintln!(
                                            "Delete policy {id} for site {id} and '{}'",
                                            pop.species
                                        );
                                        policies.remove(&pop.species);
                                    }
                                }
                            }
                        }
                    }
                    other => {
                        unimplemented!("other: {other:?}")
                    }
                }
            }
        }
    });
    Ok(s)
}

fn select_new_action(count: u32, min: u32, max: u32) -> Option<Action> {
    if count < min {
        Some(Action::Conserve)
    } else if count > max {
        Some(Action::Cull)
    } else {
        None
    }
}

fn validate_site_visit(populations: &[ObservedPopulation]) -> bool {
    let base: HashMap<String, u32> = populations
        .iter()
        .map(|p| (p.species.to_owned(), p.count))
        .collect();
    for pop in populations {
        if *base.get(&pop.species).unwrap() != pop.count {
            return false;
        }
    }
    true
}

#[tokio::main]
async fn main() -> Result<()> {
    let list = TcpListener::bind("0.0.0.0:4567").await?;
    let sites: Arc<Mutex<HashMap<u32, Sender<Event>>>> = Arc::new(Mutex::new(HashMap::default()));
    for i in 0.. {
        let (stream, _) = list.accept().await?;
        tokio::spawn(handle(i, stream, sites.clone()));
    }

    Ok(())
}

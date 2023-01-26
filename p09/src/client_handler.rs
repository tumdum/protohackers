use crate::{Job, JobServer};
use anyhow::{bail, Result};
use fxhash::FxHashSet as HashSet;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "request")]
enum Request {
    put {
        queue: String,
        job: Value,
        pri: u64,
    },
    get {
        queues: Vec<String>,
        #[serde(default)]
        wait: bool,
    },
    delete {
        id: u64,
    },
    abort {
        id: u64,
    },
}

fn next_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    NEXT_ID.fetch_add(1, Relaxed)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct GetOk {
    status: &'static str,
    id: u64,
    job: Value,
    pri: u64,
    queue: String,
}

impl GetOk {
    fn from(job: &Job) -> Self {
        Self {
            status: "ok",
            id: job.id,
            job: job.job.clone(),
            pri: job.pri,
            queue: job.queue.clone(),
        }
    }
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

pub struct ClientHandler {
    pub server: Arc<Mutex<JobServer>>,
    pub read: BufReader<OwnedReadHalf>,
    pub write: OwnedWriteHalf,
    pub in_progress: HashSet<u64>,
}

impl ClientHandler {
    pub async fn run(&mut self) -> Result<()> {
        loop {
            let line = match read_next_line(&mut self.read).await {
                Ok(line) => line,
                Err(_) => {
                    let mut server = self.server.lock().await;
                    for id in &self.in_progress {
                        server.abort(*id);
                    }
                    break;
                }
            };
            let req: Result<Request, _> = serde_json::from_str(&line);
            match req {
                Ok(Request::get { queues, wait }) => self.get(queues, wait).await?,
                Ok(Request::put { queue, job, pri }) => self.put(queue, job, pri).await?,
                Ok(Request::abort { id }) => self.abort(id).await?,
                Ok(Request::delete { id }) => self.delete(id).await?,
                Err(e) => {
                    let reply = json!({
                        "status": "error",
                        "error": e.to_string(),
                    });
                    let msg = serde_json::to_string(&reply)?;
                    write_next_line(&mut self.write, &msg).await?;
                }
            }
        }
        Ok(())
    }

    async fn get(&mut self, queues: Vec<String>, wait: bool) -> Result<()> {
        let job = self.server.lock().await.get(&queues, wait);
        match job {
            Ok(None) => {
                write_next_line(&mut self.write, r#"{"status":"no-job"}"#).await?;
            }
            Ok(Some(job)) => {
                let msg = GetOk::from(&job);
                let msg = serde_json::to_string(&msg)?;
                write_next_line(&mut self.write, &msg).await?;
                self.in_progress.insert(job.id);
            }
            Err(receiver) => {
                let job = receiver.await?;
                let msg = GetOk::from(&job);
                let msg = serde_json::to_string(&msg)?;
                write_next_line(&mut self.write, &msg).await?;
                self.in_progress.insert(job.id);
            }
        }
        Ok(())
    }

    async fn put(&mut self, queue: String, job: Value, pri: u64) -> Result<()> {
        let id = next_id();
        let job = Job {
            id,
            queue,
            job,
            pri,
        };
        self.server.lock().await.put(job);
        let reply = json!({
            "status": "ok",
            "id": id,
        });
        let msg = serde_json::to_string(&reply)?;
        write_next_line(&mut self.write, &msg).await?;
        Ok(())
    }

    async fn abort(&mut self, id: u64) -> Result<()> {
        if !self.in_progress.contains(&id) {
            let reply = json!({
                "status": "error",
                "error": format!("this client is not working on job {id}"),
            });
            let msg = serde_json::to_string(&reply)?;
            write_next_line(&mut self.write, &msg).await?;
        } else {
            self.in_progress.remove(&id);
            if self.server.lock().await.abort(id) {
                write_next_line(&mut self.write, r#"{"status":"ok"}"#).await?;
            } else {
                write_next_line(&mut self.write, r#"{"status":"no-job"}"#).await?;
            }
        }
        Ok(())
    }

    async fn delete(&mut self, id: u64) -> Result<()> {
        if self.server.lock().await.delete(id) {
            write_next_line(&mut self.write, r#"{"status":"ok"}"#).await?;
        } else {
            write_next_line(&mut self.write, r#"{"status":"no-job"}"#).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialization() {
        let input = r#"{"request":"put","queue":"queue1","job":7,"pri":123}"#;
        assert_eq!(
            Request::put {
                job: 7.into(),
                queue: "queue1".to_owned(),
                pri: 123
            },
            serde_json::from_str(input).unwrap()
        );

        let input = r#"{"request":"get","queues":["queue1","queue2"],"wait":true}"#;
        assert_eq!(
            Request::get {
                queues: vec!["queue1".to_owned(), "queue2".to_owned()],
                wait: true,
            },
            serde_json::from_str(input).unwrap()
        );

        let input = r#"{"request":"get","queues":["queue1","queue2"]}"#;
        assert_eq!(
            Request::get {
                queues: vec!["queue1".to_owned(), "queue2".to_owned()],
                wait: false,
            },
            serde_json::from_str(input).unwrap()
        );

        let input = r#"{"request":"delete","id":12345}"#;
        assert_eq!(
            Request::delete { id: 12345 },
            serde_json::from_str(input).unwrap()
        );

        let input = r#"{"request":"abort","id":12345}"#;
        assert_eq!(
            Request::abort { id: 12345 },
            serde_json::from_str(input).unwrap()
        );
    }
}

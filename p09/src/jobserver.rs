use serde_json::Value;
use tokio::sync::oneshot::{channel, Receiver, Sender};

#[derive(Debug, Clone)]
pub struct Job {
    pub id: u64,
    pub queue: String,
    pub job: Value,
    pub pri: u64,
}

#[derive(Default)]
pub struct JobServer {
    ready: Vec<Job>,
    running: Vec<Job>,
    waiters: Vec<(Vec<String>, Sender<Job>)>,
}

impl JobServer {
    pub fn get(
        &mut self,
        queues: &[String],
        wait: bool,
    ) -> std::result::Result<Option<Job>, Receiver<Job>> {
        let candidate_idx = self
            .ready
            .iter()
            .enumerate()
            .filter(|(_, job)| queues.contains(&job.queue))
            .max_by_key(|(_, job)| job.pri)
            .map(|(idx, _)| idx);
        match candidate_idx {
            Some(idx) => {
                let job = self.ready.remove(idx);
                self.running.push(job.clone());
                Ok(Some(job))
            }
            None => {
                if !wait {
                    return Ok(None);
                }
                let (s, r) = channel();
                self.waiters.push((queues.to_vec(), s));
                Err(r)
            }
        }
    }

    pub fn put(&mut self, job: Job) {
        if let Some(idx) = self
            .waiters
            .iter()
            .position(|(queues, _)| queues.contains(&job.queue))
        {
            self.running.push(job.clone());
            self.waiters.remove(idx).1.send(job).unwrap();
        } else {
            self.ready.push(job);
        }
    }

    pub fn delete(&mut self, id: u64) -> bool {
        if let Some(idx) = self.ready.iter().position(|job| job.id == id) {
            self.ready.remove(idx);
            true
        } else if let Some(idx) = self.running.iter().position(|job| job.id == id) {
            self.running.remove(idx);
            true
        } else {
            false
        }
    }

    pub fn abort(&mut self, id: u64) -> bool {
        if let Some(idx) = self.running.iter().position(|job| job.id == id) {
            if let Some(widx) = self
                .waiters
                .iter()
                .position(|(queues, _)| queues.contains(&self.running[idx].queue))
            {
                self.waiters
                    .remove(widx)
                    .1
                    .send(self.running[idx].clone())
                    .unwrap();
            } else {
                let job = self.running.remove(idx);
                self.ready.push(job);
            }
            true
        } else {
            false
        }
    }
}

use std::{
    collections::VecDeque,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::Request;

#[derive(Clone)]
pub struct Pool {
    streams: Arc<Mutex<VecDeque<mpsc::Sender<Request>>>>,
}

impl Pool {
    pub fn new() -> Self {
        Self {
            streams: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn get(&self) -> Option<PooledStream> {
        let mut streams = self.streams.lock();
        Some(PooledStream::new(self.clone(), streams.pop_front()?))
    }

    pub fn put(&self, stream: mpsc::Sender<Request>) {
        let mut streams = self.streams.lock();
        streams.push_back(stream);
    }
}

pub struct PooledStream {
    pool: Pool,
    stream: Option<mpsc::Sender<Request>>,
    closed: bool,
}

impl PooledStream {
    pub fn new(pool: Pool, stream: mpsc::Sender<Request>) -> Self {
        Self {
            pool,
            stream: Some(stream),
            closed: false,
        }
    }

    pub fn close(mut self) {
        self.closed = true
    }
}

impl Drop for PooledStream {
    fn drop(&mut self) {
        let stream = self.stream.take().expect("pooled stream is none");
        if self.closed {
            return;
        }
        self.pool.put(stream)
    }
}

impl Deref for PooledStream {
    type Target = mpsc::Sender<Request>;

    fn deref(&self) -> &Self::Target {
        self.stream.as_ref().expect("pooled stream is none")
    }
}

impl DerefMut for PooledStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.stream.as_mut().expect("pooled stream is none")
    }
}

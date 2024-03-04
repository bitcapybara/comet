use std::{
    collections::VecDeque,
    mem::ManuallyDrop,
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
    stream: ManuallyDrop<mpsc::Sender<Request>>,
}

impl PooledStream {
    pub fn new(pool: Pool, stream: mpsc::Sender<Request>) -> Self {
        Self {
            pool,
            stream: ManuallyDrop::new(stream),
        }
    }
}

impl Drop for PooledStream {
    fn drop(&mut self) {
        let stream = unsafe { ManuallyDrop::take(&mut self.stream) };
        self.pool.put(stream)
    }
}

impl Deref for PooledStream {
    type Target = mpsc::Sender<Request>;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl DerefMut for PooledStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

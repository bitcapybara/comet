use std::sync::{
    atomic::{self, AtomicU64},
    Arc,
};

use super::IdGenerator;

#[derive(Default, Clone)]
pub struct LocalId(Arc<AtomicU64>);

impl LocalId {
    #[tracing::instrument]
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::default()))
    }
}

impl IdGenerator for LocalId {
    #[tracing::instrument(skip(self))]
    fn next_id(&self) -> u64 {
        self.0.fetch_add(1, atomic::Ordering::Relaxed)
    }
}

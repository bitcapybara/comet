use std::sync::{
    atomic::{self, AtomicU64},
    Arc,
};

use super::IdGenerator;

#[derive(Default, Clone)]
pub struct LocalId(Arc<AtomicU64>);

impl IdGenerator for LocalId {
    fn next_id(&self) -> u64 {
        self.0.fetch_add(1, atomic::Ordering::Relaxed)
    }
}

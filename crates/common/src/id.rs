pub mod distribute;
pub mod local;

pub trait IdGenerator: Clone + Send + Sync + 'static {
    fn next_id(&self) -> u64;
}

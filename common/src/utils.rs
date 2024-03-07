pub mod local;
pub mod snoyflake;

pub trait IdGenerator: Clone {
    fn next_id(&self) -> u64;
}

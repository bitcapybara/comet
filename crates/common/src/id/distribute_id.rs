use chrono::{DateTime, Utc};

use super::IdGenerator;

#[derive(Clone)]
pub struct DistributeId(sonyflake::Sonyflake);

impl DistributeId {
    #[tracing::instrument]
    pub fn new(machine_id: u16, start_time: DateTime<Utc>) -> Self {
        DistributeId(sonyflake::Sonyflake::new(machine_id, start_time))
    }
}

impl IdGenerator for DistributeId {
    #[tracing::instrument(skip(self))]
    fn next_id(&self) -> u64 {
        self.0.next_id()
    }
}

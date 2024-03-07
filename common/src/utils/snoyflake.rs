use chrono::prelude::*;
use parking_lot::Mutex;
use std::{sync::Arc, thread, time::Duration};

use super::IdGenerator;

/// bit length of time
const BIT_LEN_TIME: u64 = 39;
/// bit length of sequence number
const BIT_LEN_SEQUENCE: u64 = 8;
/// bit length of machine id
const BIT_LEN_MACHINE_ID: u64 = 63 - BIT_LEN_TIME - BIT_LEN_SEQUENCE;

const GENERATE_MASK_SEQUENCE: u16 = (1 << BIT_LEN_SEQUENCE) - 1;

#[derive(Debug)]
pub(crate) struct Internals {
    elapsed_time: i64,
    sequence: u16,
}

pub(crate) struct SharedSonyflake {
    start_time: i64,
    machine_id: u16,
    internals: Mutex<Internals>,
}

#[derive(Clone)]
/// Sonyflake is a distributed unique ID generator.
pub struct Sonyflake(Arc<SharedSonyflake>);

impl Sonyflake {
    /// Create a new Sonyflake with the default configuration.
    /// For custom configuration see [`builder`].
    ///
    /// [`builder`]: struct.Sonyflake.html#method.builder
    pub fn new(machine_id: u16, start_time: DateTime<Utc>) -> Self {
        Self(Arc::new(SharedSonyflake {
            start_time: to_sonyflake_time(start_time),
            machine_id,
            internals: Mutex::new(Internals {
                elapsed_time: 0,
                sequence: 1 << (BIT_LEN_SEQUENCE - 1),
            }),
        }))
    }

    /// Generate the next unique id.
    /// After the Sonyflake time overflows, next_id returns an error.
    pub fn next_id(&self) -> u64 {
        let mut internals = self.0.internals.lock();

        let current = current_elapsed_time(self.0.start_time);
        if internals.elapsed_time < current {
            internals.elapsed_time = current;
            internals.sequence = 0;
        } else {
            // self.elapsed_time >= current
            internals.sequence = (internals.sequence + 1) & GENERATE_MASK_SEQUENCE;
            if internals.sequence == 0 {
                internals.elapsed_time += 1;
                let overtime = internals.elapsed_time - current;
                thread::sleep(sleep_time(overtime));
            }
        }

        if internals.elapsed_time >= 1 << BIT_LEN_TIME {
            panic!("Sonyflake time overflow");
        }

        (internals.elapsed_time as u64) << (BIT_LEN_SEQUENCE + BIT_LEN_MACHINE_ID)
            | (internals.sequence as u64) << BIT_LEN_MACHINE_ID
            | (self.0.machine_id as u64)
    }
}

const SONYFLAKE_TIME_UNIT: i64 = 10_000; // microseconds, i.e. 10ms

fn to_sonyflake_time(time: DateTime<Utc>) -> i64 {
    time.timestamp_micros() / SONYFLAKE_TIME_UNIT
}

fn current_elapsed_time(start_time: i64) -> i64 {
    to_sonyflake_time(Utc::now()) - start_time
}

fn sleep_time(overtime: i64) -> Duration {
    Duration::from_millis(overtime as u64 * 10)
        - Duration::from_micros((Utc::now().timestamp_micros() % SONYFLAKE_TIME_UNIT) as u64)
}

impl IdGenerator for Sonyflake {
    fn next_id(&self) -> u64 {
        self.next_id()
    }
}

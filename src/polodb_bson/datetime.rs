use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct UTCDateTime {
    timestamp: u64,
}

impl UTCDateTime {

    pub fn new(timestamp: u64) -> UTCDateTime {
        UTCDateTime {
            timestamp,
        }
    }

    pub fn now() -> UTCDateTime {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let in_ms = since_the_epoch.as_secs() * 1000 +
            since_the_epoch.subsec_nanos() as u64 / 1_000_000;

        UTCDateTime {
            timestamp: in_ms,
        }
    }

    #[inline]
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

}

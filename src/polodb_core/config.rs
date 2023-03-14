/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
pub struct Config {
    inner: Arc<ConfigInner>,
}

impl Config {

    pub fn get_init_block_count(&self) -> u64 {
        self.inner.init_block_count.load(Ordering::Relaxed)
    }

    pub fn get_journal_full_size(&self) -> u64 {
        self.inner.journal_full_size.load(Ordering::Relaxed)
    }

}

impl Default for Config {

    fn default() -> Self {
        let inner = ConfigInner::default();
        Config {
            inner: Arc::new(inner),
        }
    }

}

struct ConfigInner {
    pub init_block_count:  AtomicU64,
    pub journal_full_size: AtomicU64,
}

impl Default for ConfigInner {
    fn default() -> Self {
        ConfigInner {
            init_block_count: AtomicU64::new(16),
            journal_full_size: AtomicU64::new(1000),
        }
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

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

    pub fn get_lsm_page_size(&self) -> u32 {
        self.inner.lsm_page_size.load(Ordering::Relaxed)
    }

    pub fn get_lsm_block_size(&self) -> u32 {
        self.inner.lsm_block_size.load(Ordering::Relaxed)
    }

    pub fn get_sync_log_count(&self) -> u64 {
        self.inner.sync_log_count.load(Ordering::Relaxed)
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
    init_block_count:  AtomicU64,
    journal_full_size: AtomicU64,
    lsm_page_size:     AtomicU32,
    lsm_block_size:    AtomicU32,
    sync_log_count:    AtomicU64,
}

#[cfg(not(target_arch = "wasm32"))]
const SYNC_LOG_COUNT: u64 = 1000;

#[cfg(target_arch = "wasm32")]
const SYNC_LOG_COUNT: u64 = 200;

impl Default for ConfigInner {

    fn default() -> Self {
        ConfigInner {
            init_block_count: AtomicU64::new(16),
            journal_full_size: AtomicU64::new(1000),
            lsm_page_size: AtomicU32::new(4096),
            lsm_block_size: AtomicU32::new(4 * 1024 * 1024),
            sync_log_count: AtomicU64::new(SYNC_LOG_COUNT),
        }
    }

}

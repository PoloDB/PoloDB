// Copyright 2024 Vincent Chan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

///
/// Config builder for the database
///
/// ```rust
/// use polodb_core::ConfigBuilder;
/// let mut config_builder = ConfigBuilder::new();
/// config_builder
///     .set_init_block_count(100)
///     .set_journal_full_size(1000);
/// let config = config_builder.take();
/// ```
pub struct ConfigBuilder {
    inner: Config,
}

impl ConfigBuilder {

    pub fn new() -> ConfigBuilder {
        ConfigBuilder {
            inner: Config::default(),
        }
    }

    pub fn get_init_block_count(&self) -> u64 {
        self.inner.init_block_count
    }

    pub fn set_init_block_count(&mut self, v: u64) -> &mut Self {
        self.inner.init_block_count = v;
        self
    }

    pub fn get_journal_full_size(&self) -> u64 {
        self.inner.journal_full_size
    }

    pub fn set_journal_full_size(&mut self, v: u64) -> &mut Self {
        self.inner.journal_full_size = v;
        self
    }

    pub fn get_lsm_page_size(&self) -> u32 {
        self.inner.lsm_page_size
    }

    pub fn set_lsm_page_size(&mut self, v: u32) -> &mut Self {
        self.inner.lsm_page_size = v;
        self
    }

    pub fn get_lsm_block_size(&self) -> u32 {
        self.inner.lsm_block_size
    }

    pub fn set_lsm_block_size(&mut self, v: u32) -> &mut Self {
        self.inner.lsm_block_size = v;
        self
    }

    pub fn get_sync_log_count(&self) -> u64 {
        self.inner.sync_log_count
    }

    pub fn set_sync_log_count(&mut self, v: u64) -> &mut Self {
        self.inner.sync_log_count = v;
        self
    }

    pub fn take(self) -> Config {
        self.inner
    }

}


pub struct Config {
    pub init_block_count:  u64,
    pub journal_full_size: u64,
    pub lsm_page_size:     u32,
    pub lsm_block_size:    u32,
    pub sync_log_count:    u64,
}

const SYNC_LOG_COUNT: u64 = 1000;

impl Default for Config {

    fn default() -> Self {
        Config {
            init_block_count: 16,
            journal_full_size: 1000,
            lsm_page_size: 4096,
            lsm_block_size: 4 * 1024 * 1024,
            sync_log_count: SYNC_LOG_COUNT,
        }
    }

}

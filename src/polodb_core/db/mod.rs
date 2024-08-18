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

mod db;
pub(crate) mod db_inner;
pub mod client_cursor;
mod rocksdb_wrapper;
mod rocksdb_transaction;
mod rocksdb_iterator;
mod rocksdb_options;

pub use db::{Database, Result};
pub(crate) use rocksdb_transaction::RocksDBTransaction;
pub(crate) use rocksdb_iterator::RocksDBIterator;

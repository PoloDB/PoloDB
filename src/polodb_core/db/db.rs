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

#[cfg(feature = "redb")]
mod redb {
    use redb::{Builder, Database};

    use crate::transaction::redb::{ReDBTransaction, TransactionStates};
    use crate::{backend::Backend, errors::Error};
    struct ReDB(Database);

    impl Backend for ReDB {
        type Transaction = ReDBTransaction;

        fn try_open(path: &std::path::Path) -> super::Result<Self> {
            let db = Database::create(path).map_err(|_| Error::DbNotReady)?;
            Ok(Self(db))
        }

        fn try_open_with_config(
            path: &std::path::Path,
            _config: crate::Config,
        ) -> super::Result<Self> {
            Self::try_open(path)
        }

        fn begin_transaction(&self) -> super::Result<Self::Transaction> {
            let transaction = self.0.begin_write().map_err(|_| Error::DbNotReady)?;
            Ok(ReDBTransaction(TransactionStates::Write(transaction)))
        }
    }
}

use super::db_inner::DatabaseInner;
use crate::coll::Collection;
use crate::errors::Error;
use crate::metrics::Metrics;
use crate::{Config, Transaction};
use serde::Serialize;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(crate) static SHOULD_LOG: AtomicBool = AtomicBool::new(false);

///
/// API wrapper for Rust-level
///
/// Use [`Database::open_path`] API to open a database. A main database file will be
/// generated in the path user provided.
///
/// When you own an instance of a Database, the instance holds a file
/// descriptor of the database file. When the Database instance is dropped,
/// the handle of the file will be released.
///
/// # Collection
/// A [`Collection`] is a dataset of a kind of data.
/// You can  se [`Database::create_collection`] to create a data collection.
/// To obtain an exist collection, use [`Database::collection`],
///
#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Database {
    pub fn set_log(v: bool) {
        SHOULD_LOG.store(v, Ordering::SeqCst);
    }

    /// Return the version of package version in string.
    /// Defined in `Cargo.toml`.
    pub fn get_version() -> &'static str {
        const VERSION: &str = env!("CARGO_PKG_VERSION");
        VERSION
    }

    #[deprecated]
    pub fn open_file<P: AsRef<Path>>(path: P) -> Result<Database> {
        Database::open_path(path)
    }

    #[deprecated]
    pub fn open_file_with_config<P: AsRef<Path>>(path: P, config: Config) -> Result<Database> {
        Database::open_path_with_config(path, config)
    }

    pub fn open_path<P: AsRef<Path>>(path: P) -> Result<Database> {
        Database::open_path_with_config(path, Config::default())
    }

    pub fn open_path_with_config<P: AsRef<Path>>(path: P, config: Config) -> Result<Database> {
        let inner = DatabaseInner::open_file(path.as_ref(), config)?;

        Ok(Database {
            inner: Arc::new(inner),
        })
    }

    /// Return the metrics object of the database
    pub fn metrics(&self) -> Metrics {
        self.inner.metrics()
    }

    /// Creates a new collection in the database with the given `name`.
    pub fn create_collection(&self, name: &str) -> Result<()> {
        let _ = self.inner.create_collection(name)?;
        Ok(())
    }

    ///
    /// [error]: ../enum.DbErr.html
    ///
    /// Return an exist collection. If the collection is not exists,
    /// a new collection will be created.
    ///
    pub fn collection<T: Serialize>(&self, col_name: &str) -> Collection<T> {
        Collection::new(Arc::downgrade(&self.inner), col_name)
    }

    #[cfg(not(feature = "redb"))]
    pub fn start_transaction(&self) -> Result<Transaction> {
        let mut inner = self.inner.start_transaction()?;
        inner.set_auto_commit(false);
        Ok(Transaction::new(Arc::downgrade(&self.inner), inner))
    }

    /// Gets the names of the collections in the database.
    pub fn list_collection_names(&self) -> Result<Vec<String>> {
        let txn = self.inner.start_transaction()?;
        self.inner.list_collection_names_with_session(&txn)
    }
}

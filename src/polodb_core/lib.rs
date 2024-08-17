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

#![cfg_attr(docsrs, deny(broken_intra_doc_links))]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! PoloDB is an embedded JSON-based database.
//!
//! PoloDB is a library written in Rust that implements a lightweight MongoDB.
//! PoloDB has no dependency(except for libc), so it can be easily run on most platform(thanks for Rust Language).
//! The data of PoloDB is stored in a file. The file format is stable, cross-platform, and backwards compaitible.
//! The API of PoloDB is very similar to MongoDB. It's very easy to learn and use.
//!
//! [Tutorials](https://www.polodb.org/docs)
//!
//! # Usage
//!
//! The [`Database`] structure provides all the API to get access to the DB file.
//!
//! ## Open a local file
//!
//! ```rust
//! use polodb_core::Database;
//! # let db_path = polodb_core::test_utils::mk_db_path("doc-test-polo-file");
//! let db = Database::open_path(db_path).unwrap();
//! ```
//!
//! # Example
//!
//!  ```rust
//! use polodb_core::{Database, CollectionT};
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Book {
//!     title: String,
//!     author: String,
//! }
//!
//! # let db_path = polodb_core::test_utils::mk_db_path("doc-test-polo-lib");
//! let db = Database::open_path(db_path).unwrap();
//! let collection = db.collection("books");
//! collection.insert_one(Book {
//!     title: "The Three-Body Problem".to_string(),
//!     author: "Liu Cixin".to_string(),
//! }).unwrap();
//! ```
//!
//! ## Inserting documents into a collection
//!
//! ```rust
//! use polodb_core::{Database, CollectionT};
//! use polodb_core::bson::{Document, doc};
//!
//! # let db_path = polodb_core::test_utils::mk_db_path("doc-test-polo-db-collection");
//! let db = Database::open_path(db_path).unwrap();
//! let collection = db.collection::<Document>("books");
//!
//! let docs = vec![
//!     doc! { "title": "1984", "author": "George Orwell" },
//!     doc! { "title": "Animal Farm", "author": "George Orwell" },
//!     doc! { "title": "The Great Gatsby", "author": "F. Scott Fitzgerald" },
//! ];
//! collection.insert_many(docs).unwrap();
//! ```
//!
//! ## Finding documents in a collection
//!
//! ```rust
//! use polodb_core::{Database, CollectionT};
//! use polodb_core::bson::{Document, doc};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Book {
//!    title: String,
//!    author: String,
//! }
//!
//! # let db_path = polodb_core::test_utils::mk_db_path("doc-test-polo-db-find");
//! let db = Database::open_path(db_path).unwrap();
//! let collection = db.collection::<Book>("books");
//!
//! let docs = vec![
//!     Book { title: "1984".to_string(), author: "George Orwell".to_string() },
//!     Book { title: "Animal Farm".to_string(), author: "George Orwell".to_string() },
//!     Book { title: "The Great Gatsby".to_string(), author: "F. Scott Fitzgerald".to_string() },
//! ];
//! collection.insert_many(docs).unwrap();
//!
//! let books = collection.find(None).unwrap();
//! for book in books {
//!     println!("name: {:?}", book);
//! }
//! ```
//!
//! # Transactions
//!
//! A [`Transaction`] is a set of operations that are executed as a single unit.
//!
//! You an manually start a transaction by [`Database::start_transaction`] method.
//! If you don't start it manually, a transaction will be automatically started
//! in your every operation.
//!
//! ## Example
//!
//! ```rust
//! use polodb_core::{Database, CollectionT};
//! use polodb_core::bson::{Document, doc};
//!
//! # let db_path = polodb_core::test_utils::mk_db_path("doc-test-polo-db");
//! let db = Database::open_path(db_path).unwrap();
//!
//! let txn = db.start_transaction().unwrap();
//!
//! let collection = txn.collection::<Document>("books");
//!
//! let docs = vec![
//!     doc! { "title": "1984", "author": "George Orwell" },
//!     doc! { "title": "Animal Farm", "author": "George Orwell" },
//!     doc! { "title": "The Great Gatsby", "author": "F. Scott Fitzgerald" },
//! ];
//! collection.insert_many(docs).unwrap();
//!
//! txn.commit().unwrap();
//! ```

extern crate core;

mod vm;
mod errors;
mod cursor;

mod db;
mod meta_doc_helper;
mod config;
mod macros;
mod transaction;
pub mod results;

pub mod test_utils;
mod metrics;
mod utils;
mod index;
mod coll;

pub use db::{Database, Result};
pub use coll::{Collection, CollectionT, TransactionalCollection};
pub use config::{Config, ConfigBuilder};
pub use transaction::Transaction;
pub use db::client_cursor::ClientCursor;
pub use errors::Error;
pub use metrics::Metrics;
pub use index::{IndexModel, IndexOptions};

pub extern crate bson;

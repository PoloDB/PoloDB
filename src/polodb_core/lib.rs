/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
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
//! let db = Database::open_file(db_path).unwrap();
//! ```
//!
//! ## Open a memory database
//!
//! ```rust
//! use polodb_core::Database;
//!
//! let db = Database::open_memory().unwrap();
//! ```
//!
//! # Example
//!
//!  ```rust
//! use polodb_core::Database;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Book {
//!     title: String,
//!     author: String,
//! }
//!
//! # let db_path = polodb_core::test_utils::mk_db_path("doc-test-polo-lib");
//! let db = Database::open_file(db_path).unwrap();
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
//! use polodb_core::Database;
//! use polodb_core::bson::{Document, doc};
//!
//! let db = Database::open_memory().unwrap();
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
//! use polodb_core::Database;
//! use polodb_core::bson::{Document, doc};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Book {
//!    title: String,
//!    author: String,
//! }
//!
//! let db = Database::open_memory().unwrap();
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
//! # Session
//!
//! A [`ClientSession`] represents a logical session used for ordering sequential
//! operations.
//!
//! You an manually start a transaction by [`ClientSession::start_transaction`] method.
//! If you don't start it manually, a transaction will be automatically started
//! in your every operation.
//!
//! ## Example
//!
//! ```rust
//! use polodb_core::Database;
//! use polodb_core::bson::{Document, doc};
//!
//! # let db_path = polodb_core::test_utils::mk_db_path("doc-test-polo-db");
//! let db = Database::open_file(db_path).unwrap();
//!
//! let mut session = db.start_session().unwrap();
//! session.start_transaction(None).unwrap();
//!
//! let collection = db.collection::<Document>("books");
//!
//! let docs = vec![
//!     doc! { "title": "1984", "author": "George Orwell" },
//!     doc! { "title": "Animal Farm", "author": "George Orwell" },
//!     doc! { "title": "The Great Gatsby", "author": "F. Scott Fitzgerald" },
//! ];
//! collection.insert_many_with_session(docs, &mut session).unwrap();
//!
//! session.commit_transaction().unwrap();
//! ```

extern crate core;

mod page;
mod vm;
mod errors;
mod cursor;
mod session;

mod db;
mod meta_doc_helper;
mod config;
mod macros;
mod transaction;
pub mod lsm;
pub mod results;
pub mod commands;
mod collection_info;

#[cfg(not(target_arch = "wasm32"))]
pub mod test_utils;
mod metrics;
mod utils;

pub use db::{Database, DatabaseServer, Collection, Result};
pub use config::{Config, ConfigBuilder};
pub use transaction::TransactionType;
pub use db::client_cursor::{ClientCursor, ClientSessionCursor};
pub use errors::Error;
pub use session::ClientSession;
pub use metrics::Metrics;
pub use lsm::LsmKv;

pub extern crate bson;

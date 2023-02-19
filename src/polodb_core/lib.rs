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
//! [Database]: ./db/struct.Database.html
//!
//! The [Database] structure provides all the API to get access to the DB file.
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

extern crate core;

mod btree;
mod page;
mod vm;
mod error;
mod cursor;
mod session;

mod db;
mod data_ticket;
mod meta_doc_helper;
pub mod dump;
mod config;
mod macros;
mod backend;
mod transaction;
mod doc_serializer;
mod bson_utils;
pub mod results;
pub mod commands;
mod data_structures;
mod collection_info;

#[cfg(not(target_arch = "wasm32"))]
pub mod test_utils;

pub use db::{Database, Collection, DbResult};
pub use config::Config;
pub use transaction::TransactionType;
pub use db::db_handle::DbHandle;
pub use error::DbErr;
pub use session::ClientSession;

pub extern crate bson;

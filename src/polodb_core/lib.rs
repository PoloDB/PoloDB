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
//! # Example
//!
//!  ```rust
//! use std::rc::Rc;
//! use polodb_core::Database;
//! use polodb_core::bson::doc;
//!
//! let mut db = Database::open_file("/tmp/test-collection").unwrap();
//! let mut collection = db.collection("test");
//! collection.insert_one(doc! {
//!     "_id": 0,
//!     "name": "Vincent Chan",
//!     "score": 99.99,
//! }).unwrap();
//! ```
//!
//! ## Inserting documents into a collection
//!
//! ```rust
//! use polodb_core::Database;
//! use polodb_core::bson::{Document, doc};
//!
//! let mut db = Database::open_memory().unwrap();
//! let mut collection = db.collection::<Document>("books");
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

pub mod db;
mod data_ticket;
mod index_ctx;
mod meta_doc_helper;
mod context;
mod db_handle;
pub mod dump;
mod config;
mod macros;
mod file_lock;
mod backend;
mod transaction;
mod page_handler;
mod doc_serializer;
pub mod msg_ty;
mod bson_utils;
pub mod results;

pub use db::{Database, DbResult};
pub use config::Config;
pub use doc_serializer::SerializeType;
pub use transaction::TransactionType;
pub use context::DbContext;
pub use db_handle::DbHandle;
pub use error::DbErr;

pub extern crate bson;

#![cfg_attr(docsrs, deny(broken_intra_doc_links))]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! PoloDB is an embedded JSON-based database.
//!
//! PoloDB is a library written in Rust that implements a lightweight MongoDB.
//! PoloDB has no dependency(except for libc), so it can be easily run on most platform(thanks for Rust Language).
//! The data of PoloDB is stored in a file. The file format is stable, cross-platform, and backwards compaitible.
//! The API of PoloDB is very similar to MongoDB. It's very easy to learn and use.
//!
//! # Installation
//! ```toml
//! [dependencies]
//! polodb_core = "0.5.2"
//! polodb_bson = "0.5.2"
//! ```
//!
//! # Usage
//!
//! [Database]: ./db/struct.Database.html
//!
//! The [Database] structure provides all the API to get access to the DB file.
//!

mod btree;
mod page;
mod journal;
mod vm;
mod crc64;
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

pub use db::{Database, DbResult};
pub use config::Config;
pub use journal::TransactionType;
pub use context::DbContext;
pub use db_handle::DbHandle;
pub use error::DbErr;

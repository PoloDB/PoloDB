
<img src="./images/brand.png" alt="" width="480" />

[![Crates.io](https://img.shields.io/crates/v/polodb_core.svg)](https://crates.io/crates/polodb_core)
[![Discord](https://img.shields.io/discord/1061903499190865930)](https://discord.gg/NmGQyVx6hH)
[![docs.rs](https://docs.rs/polodb_core/badge.svg)](https://docs.rs/polodb_core)
[![License](https://img.shields.io/badge/license-MPL--2.0-blue)](LICENSE)

PoloDB is an embedded document database.

| [Documentations](https://www.polodb.org/docs) |

# Introduction

PoloDB is a library written in Rust
that implements a lightweight [MongoDB](https://www.mongodb.com/).

# Why

PoloDB aims to offer a modern alternative to SQLite, which is currently the almost exclusive option for client-side data storage.
Although SQLite is an old and stable software, it lacks some modern features.
That's why we developed PoloDB, which is NoSQL, supports multi-threading and multi-sessions,
and retains the embedded and lightweight features of SQLite.

# Features

- Simple and Lightweight
  - can be embedded library or a standalone server
- Easy to learn and use
  - NoSQL
  - MongoDB-like API
- Cross-Platform

# Quick start

PoloDB is easy to learn and use:

```rust
use polodb_core::Database;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
struct Book {
    title: String,
    author: String,
}

let db = Database::open_path(db_path)?;
let collection = db.collection::<Book>("books");
collection.insert_one(Book {
    title: "The Three-Body Problem".to_string(),
    author: "Liu Cixin".to_string(),
})?;
```

# Packages
  
- polodb: The standalone server of PoloDB, which is compatible with MongoDB's wire protocol.
- [polodb_core](https://crates.io/crates/polodb_core): The core library of PoloDB, which can be embedded in your application.

# Platform

Theoretically, PoloDB supports all platforms that the Rust compiler
supports.
But PoloDB is a personal project currently.
Limited by my time, I have only compiled and tested on the following platforms:

- macOS Big Sur x64
- Linux x64 (Tested on Fedora 32)
- Windows 10 x64

# Manual

- [Documentations](https://www.polodb.org/docs)
- [Rust](https://docs.rs/polodb_core)

# Roadmap

The features will be implemented one by one in order.

- [x] Basic database API
  - [x] CRUD
  - [x] Transactions
  - [x] Serde
  - [x] Indexes(Alpha)
  - [x] Aggregation(Alpha)
- [x] Command line Tools
- [ ] Platforms
  - [x] MacOS
  - [x] Linux
  - [x] Windows
  - [ ] iOS
  - [ ] Android
- [ ] Languages
  - [ ] Python
  - [ ] JavaScript
